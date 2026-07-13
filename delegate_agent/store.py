"""Single-writer append-only state store with crash-safe snapshots."""

from __future__ import annotations

import copy
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import tempfile
import threading
from typing import Any, Callable, Protocol
import uuid

from .errors import CorruptJournalError, StateError
from .models import (
    TaskState,
    WorkflowState,
    can_transition_task,
    can_transition_workflow,
)

USAGE_FIELDS = (
    "input_tokens", "cached_input_tokens", "uncached_input_tokens",
    "output_tokens", "reasoning_output_tokens", "total_tokens",
)
CONTROL_REQUEST_TYPES = {
    "pause", "resume", "cancel", "retry", "approve", "reject",
}
MAX_EVENT_PAYLOAD_BYTES = 256_000


class StateStore(Protocol):
    """Persistence contract used by scheduler, gates, budget, and recovery."""

    @property
    def snapshot(self) -> dict[str, Any]: ...

    def append_event(self, event_type: str, payload: dict[str, Any], *, idempotency_key: str | None = None) -> dict[str, Any]: ...

    def transition_task(self, task_id: str, target: str, *, expected_state: str | None = None, expected_seq: int | None = None, fields: dict[str, Any] | None = None, idempotency_key: str | None = None) -> dict[str, Any]: ...

    def add_task(self, node: dict[str, Any], *, idempotency_key: str | None = None) -> dict[str, Any]: ...


def _empty_usage() -> dict[str, int]:
    return {field: 0 for field in USAGE_FIELDS}


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise StateError(f"event payload is not JSON serializable: {exc}") from exc


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CorruptJournalError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise CorruptJournalError(f"{path} must contain a JSON object")
    return value


def _validate_usage(value: Any, label: str = "usage") -> dict[str, int]:
    if not isinstance(value, dict):
        raise StateError(f"{label} must be an object")
    result = _empty_usage()
    for field in ("input_tokens", "cached_input_tokens", "output_tokens", "reasoning_output_tokens"):
        raw = value.get(field, 0)
        if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
            raise StateError(f"{label}.{field} must be a nonnegative integer")
        result[field] = raw
    for field, derived in (
        ("uncached_input_tokens", max(0, result["input_tokens"] - result["cached_input_tokens"])),
        ("total_tokens", result["input_tokens"] + result["output_tokens"]),
    ):
        raw = value.get(field, derived)
        if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
            raise StateError(f"{label}.{field} must be a nonnegative integer")
        if field == "uncached_input_tokens" and raw != derived:
            raise StateError(f"{label}.uncached_input_tokens must equal input_tokens - cached_input_tokens")
        if field == "total_tokens" and raw != derived:
            raise StateError(f"{label}.total_tokens must equal input_tokens + output_tokens")
        result[field] = raw
    return result


def _add_usage(target: dict[str, int], delta: dict[str, int]) -> None:
    for field in USAGE_FIELDS:
        target[field] += delta[field]


def _initial_snapshot(workflow: dict[str, Any], workflow_id: str) -> dict[str, Any]:
    tasks: dict[str, dict[str, Any]] = {}
    for node in workflow["nodes"]:
        tasks[node["id"]] = _task_record(node)
    return {
        "schema_version": 1,
        "workflow_id": workflow_id,
        "name": workflow["name"],
        "state": WorkflowState.CREATED.value,
        "last_event_seq": 0,
        "tasks": tasks,
        "usage": _empty_usage(),
        "budget": {
            "limit_tokens": workflow["budget"]["total_tokens"],
            "used_tokens": 0,
            "reserved_tokens": 0,
        },
    }


def _task_record(node: dict[str, Any]) -> dict[str, Any]:
    """Create the replayable task projection for static or expanded nodes."""
    record = {
        "id": node["id"],
        "kind": node["kind"],
        "model": node.get("model"),
        "model_reason": node.get("model_reason"),
        "sandbox": node.get("sandbox"),
        "isolation": node.get("isolation"),
        "state": TaskState.PENDING.value,
        "attempt": 0,
        "retry_count": 0,
        "depends_on": list(node.get("depends_on", [])),
        "usage": _empty_usage(),
        "reserved_tokens": 0,
        "hard_tokens": node.get("budget", {}).get("hard_tokens"),
        "max_attempts": node.get("retry", {}).get("max_attempts", 1),
        "allow_partial": node.get("allow_partial", False),
        "node": copy.deepcopy(node),
    }
    for field in ("map_parent", "map_key", "map_item", "repeat_parent", "repeat_iteration"):
        if field in node:
            record[field] = copy.deepcopy(node[field])
    return record


class JournalStateStore:
    """A durable state store with one process writing the event journal."""

    def __init__(self, workflow_dir: str | Path):
        self.root = Path(workflow_dir).expanduser().resolve()
        self.state_dir = self.root / "state"
        self.control_dir = self.root / "control"
        self.inbox_dir = self.control_dir / "inbox"
        self.processed_dir = self.control_dir / "processed"
        self.events_path = self.state_dir / "events.jsonl"
        self.snapshot_path = self.state_dir / "snapshot.json"
        self.lock_path = self.state_dir / "runtime.lock"
        self._thread_lock = threading.RLock()
        self._runtime_handle: Any = None
        self._state: dict[str, Any] | None = None
        self._idempotency: dict[str, dict[str, Any]] = {}
        self._load()

    @classmethod
    def create(cls, workflow_dir: str | Path, workflow: dict[str, Any]) -> "JournalStateStore":
        store = cls(workflow_dir)
        store.root.mkdir(parents=True, exist_ok=True)
        workflow_path = store.root / "workflow.json"
        if store.events_path.exists() or store.snapshot_path.exists():
            raise StateError("workflow state already exists; use JournalStateStore.open")
        if workflow_path.exists():
            existing = _read_json(workflow_path)
            if existing != workflow:
                raise StateError("workflow.json already exists with different content")
        else:
            _atomic_json(workflow_path, workflow)
        store.acquire()
        workflow_id = str(uuid.uuid4())
        initial = _initial_snapshot(workflow, workflow_id)
        store._append_event_locked(
            "workflow.created", {"snapshot": initial}, idempotency_key=f"workflow-created:{workflow_id}",
        )
        return store

    @classmethod
    def open(cls, workflow_dir: str | Path) -> "JournalStateStore":
        store = cls(workflow_dir)
        if store._state is None:
            raise StateError(f"workflow has no replayable state: {store.root}")
        return store

    def _load(self) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.inbox_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        snapshot_seq = 0
        if self.snapshot_path.exists():
            self._state = _read_json(self.snapshot_path)
            snapshot_seq = self._validate_snapshot(self._state)
        if not self.events_path.exists():
            return
        try:
            raw_lines = self.events_path.read_bytes().splitlines(keepends=True)
        except OSError as exc:
            raise CorruptJournalError(f"cannot read event journal: {exc}") from exc
        expected_after_snapshot = snapshot_seq + 1
        last_seen = 0
        for index, raw_line in enumerate(raw_lines):
            if not raw_line.strip():
                continue
            if not raw_line.endswith(b"\n"):
                if index == len(raw_lines) - 1:
                    break
                raise CorruptJournalError("event journal contains an incomplete non-final line")
            try:
                event = json.loads(raw_line.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise CorruptJournalError(f"invalid event at journal line {index + 1}: {exc}") from exc
            self._validate_event(event)
            sequence = event["seq"]
            if sequence <= last_seen:
                raise CorruptJournalError(f"event sequence is not increasing at {sequence}")
            last_seen = sequence
            if event.get("idempotency_key"):
                self._idempotency[event["idempotency_key"]] = event
            if sequence <= snapshot_seq:
                continue
            if sequence != expected_after_snapshot:
                raise CorruptJournalError(
                    f"event sequence gap: expected {expected_after_snapshot}, got {sequence}"
                )
            if self._state is None and event["type"] != "workflow.created":
                raise CorruptJournalError("journal does not begin with workflow.created")
            self._state = self._apply_event(self._state, event)
            expected_after_snapshot += 1
        if self._state is not None:
            self._state["last_event_seq"] = max(
                int(self._state.get("last_event_seq", 0)), last_seen,
            )

    @staticmethod
    def _validate_snapshot(state: dict[str, Any]) -> int:
        sequence = state.get("last_event_seq")
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
            raise CorruptJournalError("snapshot.last_event_seq must be a nonnegative integer")
        if not isinstance(state.get("tasks"), dict) or not isinstance(state.get("budget"), dict):
            raise CorruptJournalError("snapshot is missing tasks or budget")
        return sequence

    @staticmethod
    def _validate_event(event: Any) -> None:
        if not isinstance(event, dict):
            raise CorruptJournalError("event must be an object")
        sequence = event.get("seq")
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 1:
            raise CorruptJournalError("event.seq must be a positive integer")
        if not isinstance(event.get("event_id"), str) or not event["event_id"]:
            raise CorruptJournalError("event.event_id must be non-empty")
        if not isinstance(event.get("type"), str) or not event["type"]:
            raise CorruptJournalError("event.type must be non-empty")
        if not isinstance(event.get("payload"), dict):
            raise CorruptJournalError("event.payload must be an object")

    @property
    def snapshot(self) -> dict[str, Any]:
        with self._thread_lock:
            if self._state is None:
                raise StateError("workflow state has not been created")
            return copy.deepcopy(self._state)

    def acquire(self) -> "JournalStateStore":
        with self._thread_lock:
            if self._runtime_handle is not None:
                return self
            self.state_dir.mkdir(parents=True, exist_ok=True)
            handle = self.lock_path.open("a+")
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                handle.close()
                raise StateError(f"workflow is already running: {self.root}") from exc
            self._runtime_handle = handle
            return self

    def release(self) -> None:
        with self._thread_lock:
            if self._runtime_handle is None:
                return
            fcntl.flock(self._runtime_handle.fileno(), fcntl.LOCK_UN)
            self._runtime_handle.close()
            self._runtime_handle = None

    def close(self) -> None:
        self.release()

    def __enter__(self) -> "JournalStateStore":
        return self.acquire()

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.release()

    def _require_lock(self) -> None:
        if self._runtime_handle is None:
            raise StateError("state mutation requires the workflow runtime lock")

    def _append_event_locked(
        self, event_type: str, payload: dict[str, Any], *, idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        self._require_lock()
        if not isinstance(event_type, str) or not event_type:
            raise StateError("event type must be non-empty")
        if not isinstance(payload, dict):
            raise StateError("event payload must be an object")
        encoded_payload = _json_bytes(payload)
        if len(encoded_payload) > MAX_EVENT_PAYLOAD_BYTES:
            raise StateError("event payload exceeds the size limit")
        if idempotency_key and idempotency_key in self._idempotency:
            return copy.deepcopy(self._idempotency[idempotency_key])
        previous = copy.deepcopy(self._state)
        sequence = int(previous.get("last_event_seq", 0) if previous else 0) + 1
        event = {
            "schema_version": 1,
            "seq": sequence,
            "event_id": str(uuid.uuid4()),
            "idempotency_key": idempotency_key,
            "type": event_type,
            "timestamp": _utc_now(),
            "payload": payload,
        }
        self._validate_event(event)
        candidate = copy.deepcopy(previous)
        candidate = self._apply_event(candidate, event)
        line = (_json_bytes(event) + b"\n")
        self.events_path.parent.mkdir(parents=True, exist_ok=True)
        with self.events_path.open("ab") as handle:
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())
        self._state = candidate
        if idempotency_key:
            self._idempotency[idempotency_key] = event
        _atomic_json(self.snapshot_path, self._state)
        return copy.deepcopy(event)

    @staticmethod
    def _apply_event(state: dict[str, Any] | None, event: dict[str, Any]) -> dict[str, Any]:
        event_type = event["type"]
        payload = event["payload"]
        if event_type == "workflow.created":
            if state is not None:
                raise CorruptJournalError("duplicate workflow.created event")
            snapshot = payload.get("snapshot")
            if not isinstance(snapshot, dict):
                raise CorruptJournalError("workflow.created requires a snapshot")
            state = copy.deepcopy(snapshot)
            state["last_event_seq"] = event["seq"]
            return state
        if state is None:
            raise CorruptJournalError(f"event {event_type} appears before workflow.created")
        if event_type == "task.transition":
            task_id = payload.get("task_id")
            task = state["tasks"].get(task_id)
            if task is None:
                raise CorruptJournalError(f"unknown task in event: {task_id}")
            current = task["state"]
            if current != payload.get("from_state"):
                raise CorruptJournalError(
                    f"task {task_id} state mismatch: {current} != {payload.get('from_state')}"
                )
            target = payload.get("to_state")
            if not can_transition_task(current, target):
                raise CorruptJournalError(f"invalid task transition: {current} -> {target}")
            task["state"] = target
            fields = payload.get("fields", {})
            if not isinstance(fields, dict):
                raise CorruptJournalError("task.transition fields must be an object")
            task.update(copy.deepcopy(fields))
        elif event_type == "task.added":
            task = payload.get("task")
            if not isinstance(task, dict) or not isinstance(task.get("id"), str) or not task["id"]:
                raise CorruptJournalError("task.added requires a task object with an id")
            if task["id"] in state["tasks"]:
                raise CorruptJournalError(f"duplicate task added: {task['id']}")
            if task.get("state") != TaskState.PENDING.value:
                raise CorruptJournalError("new tasks must start pending")
            state["tasks"][task["id"]] = copy.deepcopy(task)
        elif event_type == "task.updated":
            task_id = payload.get("task_id")
            task = state["tasks"].get(task_id)
            if task is None:
                raise CorruptJournalError(f"unknown task in event: {task_id}")
            fields = payload.get("fields", {})
            if not isinstance(fields, dict):
                raise CorruptJournalError("task.updated fields must be an object")
            task.update(copy.deepcopy(fields))
        elif event_type == "workflow.transition":
            current = state["state"]
            target = payload.get("to_state")
            if current != payload.get("from_state") or not can_transition_workflow(current, target):
                raise CorruptJournalError(f"invalid workflow transition: {current} -> {target}")
            state["state"] = target
        elif event_type == "usage.recorded":
            delta = _validate_usage(payload.get("usage"), "event.usage")
            _add_usage(state["usage"], delta)
            task_id = payload.get("task_id")
            if task_id is not None:
                task = state["tasks"].get(task_id)
                if task is None:
                    raise CorruptJournalError(f"unknown task in usage event: {task_id}")
                _add_usage(task["usage"], delta)
            state["budget"]["used_tokens"] = state["usage"]["total_tokens"]
        elif event_type == "budget.reserved":
            amount = payload.get("tokens")
            if isinstance(amount, bool) or not isinstance(amount, int) or amount <= 0:
                raise CorruptJournalError("budget.reserved.tokens must be positive")
            budget = state["budget"]
            if budget["used_tokens"] + budget["reserved_tokens"] + amount > budget["limit_tokens"]:
                raise CorruptJournalError("budget reservation exceeds workflow limit")
            budget["reserved_tokens"] += amount
            task_id = payload.get("task_id")
            if task_id is not None and task_id in state["tasks"]:
                state["tasks"][task_id]["reserved_tokens"] += amount
        elif event_type == "budget.released":
            amount = payload.get("tokens")
            if isinstance(amount, bool) or not isinstance(amount, int) or amount <= 0:
                raise CorruptJournalError("budget.released.tokens must be positive")
            if amount > state["budget"]["reserved_tokens"]:
                raise CorruptJournalError("budget release exceeds reserved tokens")
            state["budget"]["reserved_tokens"] -= amount
            task_id = payload.get("task_id")
            if task_id is not None and task_id in state["tasks"]:
                state["tasks"][task_id]["reserved_tokens"] = max(
                    0, state["tasks"][task_id]["reserved_tokens"] - amount,
                )
        # control.* and other audit-only events intentionally do not alter state.
        state["last_event_seq"] = event["seq"]
        return state

    def append_event(self, event_type: str, payload: dict[str, Any], *, idempotency_key: str | None = None) -> dict[str, Any]:
        with self._thread_lock:
            return self._append_event_locked(event_type, payload, idempotency_key=idempotency_key)

    def transition_task(
        self, task_id: str, target: str, *, expected_state: str | None = None,
        expected_seq: int | None = None, fields: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        with self._thread_lock:
            if idempotency_key and idempotency_key in self._idempotency:
                return copy.deepcopy(self._idempotency[idempotency_key])
            current_snapshot = self.snapshot
            task = current_snapshot["tasks"].get(task_id)
            if task is None:
                raise StateError(f"unknown task: {task_id}")
            if expected_state is not None and task["state"] != expected_state:
                raise StateError(f"task {task_id} expected {expected_state}, found {task['state']}")
            if expected_seq is not None and current_snapshot["last_event_seq"] != expected_seq:
                raise StateError("state sequence is stale")
            TaskState(target)
            return self._append_event_locked(
                "task.transition",
                {
                    "task_id": task_id, "from_state": task["state"],
                    "to_state": target, "fields": fields or {},
                },
                idempotency_key=idempotency_key,
            )

    def add_task(self, node: dict[str, Any], *, idempotency_key: str | None = None) -> dict[str, Any]:
        if not isinstance(node, dict) or not isinstance(node.get("id"), str) or not node["id"]:
            raise StateError("dynamic task node must contain a non-empty id")
        with self._thread_lock:
            if idempotency_key and idempotency_key in self._idempotency:
                return copy.deepcopy(self._idempotency[idempotency_key])
            if node["id"] in self.snapshot["tasks"]:
                raise StateError(f"task already exists: {node['id']}")
            return self._append_event_locked(
                "task.added", {"task": _task_record(node)}, idempotency_key=idempotency_key,
            )

    def transition_workflow(
        self, target: str, *, expected_state: str | None = None,
        expected_seq: int | None = None, idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        with self._thread_lock:
            if idempotency_key and idempotency_key in self._idempotency:
                return copy.deepcopy(self._idempotency[idempotency_key])
            current_snapshot = self.snapshot
            current = current_snapshot["state"]
            if expected_state is not None and current != expected_state:
                raise StateError(f"workflow expected {expected_state}, found {current}")
            if expected_seq is not None and current_snapshot["last_event_seq"] != expected_seq:
                raise StateError("state sequence is stale")
            WorkflowState(target)
            return self._append_event_locked(
                "workflow.transition",
                {"from_state": current, "to_state": target},
                idempotency_key=idempotency_key,
            )

    def update_task(self, task_id: str, fields: dict[str, Any], *, idempotency_key: str | None = None) -> dict[str, Any]:
        if not isinstance(fields, dict) or not fields:
            raise StateError("task update fields must be a non-empty object")
        with self._thread_lock:
            if idempotency_key and idempotency_key in self._idempotency:
                return copy.deepcopy(self._idempotency[idempotency_key])
            if task_id not in self.snapshot["tasks"]:
                raise StateError(f"unknown task: {task_id}")
            return self._append_event_locked(
                "task.updated", {"task_id": task_id, "fields": fields},
                idempotency_key=idempotency_key,
            )

    def record_usage(self, usage: dict[str, Any], *, task_id: str | None = None, idempotency_key: str | None = None) -> dict[str, Any]:
        delta = _validate_usage(usage)
        return self.append_event(
            "usage.recorded", {"task_id": task_id, "usage": delta}, idempotency_key=idempotency_key,
        )

    def reserve_budget(self, tokens: int, *, task_id: str | None = None, idempotency_key: str | None = None) -> dict[str, Any]:
        if isinstance(tokens, bool) or not isinstance(tokens, int) or tokens <= 0:
            raise StateError("reserved tokens must be positive")
        with self._thread_lock:
            if idempotency_key and idempotency_key in self._idempotency:
                return copy.deepcopy(self._idempotency[idempotency_key])
            budget = self.snapshot["budget"]
            if budget["used_tokens"] + budget["reserved_tokens"] + tokens > budget["limit_tokens"]:
                raise StateError("budget reservation exceeds remaining workflow budget")
            return self._append_event_locked(
                "budget.reserved", {"task_id": task_id, "tokens": tokens}, idempotency_key=idempotency_key,
            )

    def release_budget(self, tokens: int, *, task_id: str | None = None, idempotency_key: str | None = None) -> dict[str, Any]:
        if isinstance(tokens, bool) or not isinstance(tokens, int) or tokens <= 0:
            raise StateError("released tokens must be positive")
        with self._thread_lock:
            if idempotency_key and idempotency_key in self._idempotency:
                return copy.deepcopy(self._idempotency[idempotency_key])
            if tokens > self.snapshot["budget"]["reserved_tokens"]:
                raise StateError("released tokens exceed reservation")
            return self._append_event_locked(
                "budget.released", {"task_id": task_id, "tokens": tokens}, idempotency_key=idempotency_key,
            )

    def consume_control_requests(self, handler: Callable[[dict[str, Any]], Any]) -> list[dict[str, Any]]:
        """Validate, handle, journal, and archive pending external requests."""
        with self._thread_lock:
            self._require_lock()
            processed: list[dict[str, Any]] = []
            for path in sorted(self.inbox_dir.glob("*.json")):
                request = _read_json(path)
                request_id = request.get("request_id")
                request_type = request.get("type")
                if not isinstance(request_id, str) or not request_id:
                    raise StateError(f"control request has invalid request_id: {path}")
                if request_type not in CONTROL_REQUEST_TYPES:
                    raise StateError(f"unsupported control request: {request_type}")
                result = handler(request)
                self._append_event_locked(
                    "control.processed",
                    {"request_id": request_id, "type": request_type, "result": result},
                    idempotency_key=f"control:{request_id}",
                )
                destination = self.processed_dir / path.name
                os.replace(path, destination)
                _fsync_directory(self.processed_dir)
                processed.append(request)
            return processed


def submit_control_request(
    workflow_dir: str | Path, request_type: str, payload: dict[str, Any] | None = None,
    *, request_id: str | None = None,
) -> Path:
    if request_type not in CONTROL_REQUEST_TYPES:
        raise StateError(f"unsupported control request: {request_type}")
    if payload is not None and not isinstance(payload, dict):
        raise StateError("control request payload must be an object")
    root = Path(workflow_dir).expanduser().resolve()
    inbox = root / "control" / "inbox"
    inbox.mkdir(parents=True, exist_ok=True)
    request_id = request_id or str(uuid.uuid4())
    request = {
        "schema_version": 1,
        "request_id": request_id,
        "type": request_type,
        "payload": payload or {},
        "created_at": _utc_now(),
    }
    encoded = _json_bytes(request)
    if len(encoded) > MAX_EVENT_PAYLOAD_BYTES:
        raise StateError("control request exceeds the size limit")
    descriptor, temporary = tempfile.mkstemp(prefix=".request.", dir=inbox)
    destination = inbox / f"{request_id}.json"
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        _fsync_directory(inbox)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return destination


def workflow_id_from_path(workflow_dir: str | Path) -> str:
    """Return a stable local identifier useful for logs before state exists."""
    return hashlib.sha256(str(Path(workflow_dir).expanduser().resolve()).encode()).hexdigest()[:16]
