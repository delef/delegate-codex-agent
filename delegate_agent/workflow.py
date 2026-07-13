"""Static v1 workflow runtime built on the durable scheduler primitives."""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import copy
import datetime as dt
import hashlib
import json
import re
import shutil
import subprocess
import time
from pathlib import Path
import tempfile
from typing import Any

from .cache import ResultCache, task_fingerprint
from .conditions import evaluate_condition, resolve_pointer
from .errors import SchemaError, StateError
from .gates import GateResult, run_checks
from .hooks import run_hooks
from .artifacts import write_manifest
from .integration import capture_writer_changes
from .models import TaskState, WorkflowState
from .scheduler import DurableScheduler
from .retry import classify_failure
from .recovery import reconcile
from .schema import load_workflow
from .status import build_status
from .store import JournalStateStore
from .worker import WorkerOutcome, WorkerRequest, run_worker, terminate_process


def _load_task_spec(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SchemaError(f"cannot read task spec {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SchemaError(f"task spec {path} must be a JSON object")
    return value


def _list_lines(title: str, values: list[str]) -> list[str]:
    if not values:
        return []
    return [f"## {title}", "", *(f"- {value}" for value in values), ""]


def build_task_prompt(
    spec_path: str | Path,
    *,
    dependency_results: dict[str, dict[str, Any]] | None = None,
    map_item: Any = None,
    repeat_iteration: int | None = None,
) -> str:
    spec = _load_task_spec(Path(spec_path))
    for key in ("name", "objective"):
        if not isinstance(spec.get(key), str) or not spec[key].strip():
            raise SchemaError(f"task spec {spec_path} requires {key}")
    lines = [
        "# Delegated workflow task", "", f"Name: {spec['name']}", "",
        "## Objective", "", spec["objective"].strip(), "",
    ]
    lines += _list_lines("Authorized scope", [str(item) for item in spec.get("scope", [])])
    lines += _list_lines("Constraints", [str(item) for item in spec.get("constraints", [])])
    lines += _list_lines("Acceptance criteria", [str(item) for item in spec.get("acceptance", [])])
    lines += _list_lines("Required checks", [str(item) for item in spec.get("commands", [])])
    if dependency_results:
        lines += ["## Accepted dependency results", ""]
        for task_id in sorted(dependency_results):
            lines += [
                f"### {task_id}", "",
                json.dumps(dependency_results[task_id], ensure_ascii=False, sort_keys=True), "",
            ]
    if map_item is not None:
        lines += ["## Map item", "", json.dumps(map_item, ensure_ascii=False, sort_keys=True), ""]
    if repeat_iteration is not None:
        lines += ["## Repeat iteration", "", str(repeat_iteration), ""]
    lines += [
        "## Required final response", "",
        "Return JSON matching the supplied output schema with result, evidence, changes, verification, risks, and recommended_next_action.",
        "",
    ]
    return "\n".join(lines)


class WorkflowRuntime:
    """Execute agent nodes with durable state and deterministic gates.

    Non-agent nodes are deterministic local transitions; only agent nodes
    launch Codex workers and consume token budget.
    """

    def __init__(self, workflow_dir: str | Path, workflow: dict[str, Any] | None = None):
        self.workflow_dir = Path(workflow_dir).expanduser().resolve()
        events_path = self.workflow_dir / "state" / "events.jsonl"
        self.store = (
            JournalStateStore.create(self.workflow_dir, workflow)
            if workflow is not None and not events_path.exists()
            else JournalStateStore.open(self.workflow_dir)
        )
        self.scheduler = DurableScheduler(self.store)
        workflow_path = self.workflow_dir / "workflow.json"
        self._workflow = json.loads(workflow_path.read_text(encoding="utf-8"))
        self._processes: dict[str, Any] = {}
        self._cache = (
            ResultCache(self.workflow_dir.parent / ".delegate-cache")
            if self._workflow.get("cache", {}).get("enabled", True) else None
        )
        self._cache_fingerprints: dict[str, str] = {}
        self._cancelled = False

    @classmethod
    def from_file(
        cls, workflow_file: str | Path, *, runs_dir: str | Path | None = None,
    ) -> "WorkflowRuntime":
        workflow_path = Path(workflow_file).expanduser().resolve()
        workflow = load_workflow(workflow_path)
        base = Path(runs_dir or (workflow_path.parent / ".delegate-runs")).expanduser().resolve()
        base.mkdir(parents=True, exist_ok=True)
        timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        run_dir = Path(tempfile.mkdtemp(prefix=f"{timestamp}-{workflow['name']}-", dir=base))
        return cls(run_dir, workflow)

    def _node(self, task_id: str) -> dict[str, Any]:
        for node in self._workflow["nodes"]:
            if node["id"] == task_id:
                task = self.store.snapshot["tasks"].get(task_id)
                if task and "depends_on" in task:
                    node = dict(node)
                    node["depends_on"] = list(task["depends_on"])
                if task:
                    for field in ("model", "model_reason"):
                        if task.get(field) is not None:
                            node[field] = task[field]
                return node
        task = self.store.snapshot["tasks"].get(task_id)
        if task and isinstance(task.get("node"), dict):
            return dict(task["node"])
        raise StateError(f"unknown workflow node: {task_id}")

    def _dependencies(self, task_id: str) -> dict[str, dict[str, Any]]:
        node = self._node(task_id)
        results: dict[str, dict[str, Any]] = {}
        for dependency in node.get("depends_on", []):
            result_path = self.workflow_dir / "tasks" / dependency / "result.json"
            if result_path.is_file():
                results[dependency] = json.loads(result_path.read_text(encoding="utf-8"))
        return results

    def _try_cache(self, task_id: str) -> bool:
        if self._cache is None:
            return False
        node = self._node(task_id)
        if node["kind"] != "agent" or node.get("sandbox") != "read-only":
            return False
        try:
            fingerprint = task_fingerprint(
                node, cwd=self._workflow["cwd"], dependency_results=self._dependencies(task_id),
            )
            entry = self._cache.get(fingerprint)
        except (OSError, StateError):
            return False
        if entry is None:
            self._cache_fingerprints[task_id] = fingerprint
            return False
        task_dir = self.workflow_dir / "tasks" / task_id
        attempt = self.store.snapshot["tasks"][task_id]["attempt"] + 1
        attempt_dir = task_dir / f"attempt-{attempt}"
        attempt_dir.mkdir(parents=True, exist_ok=True)
        result_path = task_dir / "result.json"
        attempt_result_path = attempt_dir / "result.json"
        encoded = json.dumps(entry.result, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
        attempt_result_path.write_text(encoded, encoding="utf-8")
        result_path.write_text(encoded, encoding="utf-8")
        gates = run_checks(
            node.get("checks", []), result=entry.result, cwd=self._workflow["cwd"],
            artifact_dir=attempt_dir, writer=False,
        )
        if not all(gate.status == "accepted" for gate in gates):
            return False
        hook = run_hooks(
            self._workflow.get("hooks", {}), "task_completed",
            {"task_id": task_id, "cache_hit": True, "result": entry.result},
            cwd=self._workflow["cwd"],
        )
        if not hook.allowed:
            return False
        self.store.update_task(task_id, {
            "result": entry.result, "gates": [gate.__dict__ for gate in gates],
            "attempt_path": str(attempt_dir), "cache_fingerprint": fingerprint,
            "cache_source": {"workflow": entry.source_workflow, "task": entry.source_task},
        })
        self.scheduler.accept_cached(
            task_id,
            fields={"result_path": str(result_path), "cache_fingerprint": fingerprint},
        )
        return True

    def _run_condition(self, task_id: str) -> None:
        node = self._node(task_id)
        task = self.store.snapshot["tasks"][task_id]
        attempt = task.get("attempt", 0) + 1
        self.store.transition_task(
            task_id, TaskState.RUNNING.value, expected_state=TaskState.READY.value,
            fields={"attempt": attempt},
        )
        try:
            result = evaluate_condition(node, self._dependencies(task_id))
            task_dir = self.workflow_dir / "tasks" / task_id
            attempt_dir = task_dir / f"attempt-{attempt}"
            attempt_dir.mkdir(parents=True, exist_ok=True)
            encoded = json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
            (attempt_dir / "result.json").write_text(encoded, encoding="utf-8")
            (task_dir / "result.json").write_text(encoded, encoding="utf-8")
            self.store.update_task(task_id, {
                "result": result, "attempt_path": str(attempt_dir),
                "selected_branch": result["selected_branch"],
            })
            self.scheduler.accept_computed(
                task_id, fields={"result_path": str(task_dir / "result.json")},
            )
            selected = node.get("on_true", []) if result["matched"] else node.get("on_false", [])
            skipped = node.get("on_false", []) if result["matched"] else node.get("on_true", [])
            for branch_id in skipped:
                self.scheduler.block(branch_id, reason=f"condition:{task_id} selected {result['selected_branch']}")
            # Keep the selected branch explicit in the audit state even when it
            # is empty; this makes a resumed condition deterministic.
            for branch_id in selected:
                self.store.update_task(branch_id, {"selected_by": task_id})
        except BaseException as exc:
            self.scheduler.fail(task_id, reason=str(exc) or type(exc).__name__)

    def _run_check_node(self, task_id: str) -> None:
        node, attempt, attempt_dir = self._computed_attempt_start(task_id)
        try:
            dependencies = self._dependencies(task_id)
            result = next(iter(dependencies.values()), {
                "result": "check", "evidence": "local check node",
                "changes": "none", "verification": "pending",
                "risks": "none", "recommended_next_action": "continue",
            })
            gates = run_checks(
                node.get("checks", []), result=result, cwd=self._workflow["cwd"],
                artifact_dir=attempt_dir, writer=False,
            )
            encoded = json.dumps(
                {"result": result, "gates": [gate.__dict__ for gate in gates]},
                ensure_ascii=False, sort_keys=True, indent=2,
            ) + "\n"
            (attempt_dir / "result.json").write_text(encoded, encoding="utf-8")
            result_path = self.workflow_dir / "tasks" / task_id / "result.json"
            result_path.write_text(encoded, encoding="utf-8")
            self.store.update_task(task_id, {
                "result": result, "gates": [gate.__dict__ for gate in gates],
                "attempt_path": str(attempt_dir),
            })
            self.scheduler.complete(task_id)
            if all(gate.status == "accepted" for gate in gates):
                self.scheduler.accept(task_id, fields={"result_path": str(result_path)})
            else:
                self.scheduler.reject(
                    task_id,
                    reason="; ".join(gate.reason for gate in gates if gate.reason) or "check_failed",
                )
        except BaseException as exc:
            self.scheduler.fail(task_id, reason=str(exc) or type(exc).__name__)

    def _computed_attempt_start(self, task_id: str) -> tuple[dict[str, Any], int, Path]:
        node = self._node(task_id)
        task = self.store.snapshot["tasks"][task_id]
        attempt = task.get("attempt", 0) + 1
        self.store.transition_task(
            task_id, TaskState.RUNNING.value, expected_state=TaskState.READY.value,
            fields={"attempt": attempt},
        )
        task_dir = self.workflow_dir / "tasks" / task_id
        attempt_dir = task_dir / f"attempt-{attempt}"
        attempt_dir.mkdir(parents=True, exist_ok=True)
        return node, attempt, attempt_dir

    def _run_map(self, task_id: str) -> None:
        node, attempt, attempt_dir = self._computed_attempt_start(task_id)
        try:
            source_results = self._dependencies(task_id)
            exists, items = resolve_pointer(source_results[node["source"]], node.get("pointer", ""))
            if not exists or not isinstance(items, list):
                raise StateError("map source pointer must resolve to an array")
            if len(items) > node["max_items"]:
                raise StateError(f"map expanded {len(items)} items, max_items is {node['max_items']}")
            children: list[dict[str, Any]] = []
            seen: set[str] = set()
            for item in items:
                key_exists, raw_key = resolve_pointer(item, node["item_key"])
                if not key_exists or isinstance(raw_key, (dict, list)) or raw_key is None:
                    raise StateError("map item key must resolve to a scalar value")
                key = str(raw_key)
                if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", key) or key in seen:
                    raise StateError(f"map item keys must be unique and non-empty: {key!r}")
                seen.add(key)
                child = copy.deepcopy(node["template"])
                child.update({
                    "id": f"{task_id}::{key}", "depends_on": [task_id],
                    "map_parent": task_id, "map_key": key, "map_item": item,
                })
                children.append(child)
            for child in children:
                self.store.add_task(child, idempotency_key=f"map:{task_id}:{child['map_key']}")
            child_ids = [child["id"] for child in children]
            reduce_id = node.get("reduce")
            if reduce_id:
                reducer_task = self.store.snapshot["tasks"].get(reduce_id)
                if reducer_task is None:
                    raise StateError(f"map reducer task is missing: {reduce_id}")
                dependencies = list(dict.fromkeys(
                    list(reducer_task.get("depends_on", [])) + child_ids,
                ))
                self.store.update_task(reduce_id, {"depends_on": dependencies})
            result = {
                "source": node["source"], "count": len(children),
                "items": [{"key": child["map_key"], "task_id": child["id"]} for child in children],
                "reducer": reduce_id,
            }
            encoded = json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
            (attempt_dir / "result.json").write_text(encoded, encoding="utf-8")
            (self.workflow_dir / "tasks" / task_id / "result.json").write_text(encoded, encoding="utf-8")
            self.store.update_task(task_id, {"result": result, "attempt_path": str(attempt_dir)})
            self.scheduler.accept_computed(
                task_id, fields={"result_path": str(self.workflow_dir / "tasks" / task_id / "result.json")},
            )
        except BaseException as exc:
            self.scheduler.fail(task_id, reason=str(exc) or type(exc).__name__)

    def _run_reduce(self, task_id: str) -> None:
        node, attempt, attempt_dir = self._computed_attempt_start(task_id)
        try:
            snapshot = self.store.snapshot
            children = [
                task for task in snapshot["tasks"].values()
                if task.get("map_parent") == node["source"]
            ]
            children.sort(key=lambda task: str(task.get("map_key", "")))
            rejected = [task["id"] for task in children if task["state"] != TaskState.ACCEPTED.value]
            if rejected and not node.get("allow_partial", False):
                raise StateError(f"reduce has rejected children: {', '.join(rejected)}")
            dependency_results = self._dependencies(task_id)
            items = [
                {"key": task.get("map_key"), "task_id": task["id"], "result": dependency_results[task["id"]]}
                for task in children
                if task["state"] == TaskState.ACCEPTED.value and task["id"] in dependency_results
            ]
            result = {
                "source": node["source"], "items": items,
                "rejected": rejected, "partial": bool(rejected),
            }
            encoded = json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
            (attempt_dir / "result.json").write_text(encoded, encoding="utf-8")
            (self.workflow_dir / "tasks" / task_id / "result.json").write_text(encoded, encoding="utf-8")
            self.store.update_task(task_id, {"result": result, "attempt_path": str(attempt_dir)})
            self.scheduler.accept_computed(
                task_id, fields={"result_path": str(self.workflow_dir / "tasks" / task_id / "result.json")},
            )
        except BaseException as exc:
            self.scheduler.fail(task_id, reason=str(exc) or type(exc).__name__)

    def _run_repeat(self, task_id: str) -> None:
        node, attempt, attempt_dir = self._computed_attempt_start(task_id)
        if node["template"].get("sandbox") != "read-only":
            self.scheduler.fail(task_id, reason="repeat_until writer templates are not supported")
            return
        summaries: list[dict[str, Any]] = []
        previous_fingerprint: str | None = None
        previous_child: str | None = None
        try:
            for iteration in range(1, node["max_iterations"] + 1):
                child = copy.deepcopy(node["template"])
                child_id = f"{task_id}::{iteration}"
                child.update({
                    "id": child_id,
                    "depends_on": [previous_child] if previous_child else [],
                    "repeat_parent": task_id,
                    "repeat_iteration": iteration,
                })
                self.store.add_task(child, idempotency_key=f"repeat:{task_id}:{iteration}")
                self.store.transition_task(child_id, TaskState.READY.value, expected_state=TaskState.PENDING.value)
                self.scheduler.start(
                    child_id, reserve_tokens=child["budget"]["reserve_tokens"],
                )
                _, outcome, result, gates, child_attempt_dir = self._run_one(child_id)
                self.store.update_task(child_id, {
                    "pid": None, "event_count": outcome.event_count,
                    "result": result, "gates": [gate.__dict__ for gate in gates],
                    "worker_usage": outcome.usage,
                })
                if outcome.budget_exhausted:
                    self.scheduler.budget_exhausted(
                        child_id, observed_tokens=outcome.usage["total_tokens"],
                    )
                    raise StateError(f"repeat iteration {iteration} exhausted budget")
                self.scheduler.complete(child_id)
                if not all(gate.status == "accepted" for gate in gates):
                    reasons = [gate.reason for gate in gates if gate.reason]
                    self.scheduler.reject(child_id, reason="; ".join(reasons) or "verification_failed")
                    raise StateError(f"repeat iteration {iteration} rejected")
                child_result_path = self.workflow_dir / "tasks" / child_id / "result.json"
                if child_attempt_dir.joinpath("result.json").is_file():
                    shutil.copyfile(child_attempt_dir / "result.json", child_result_path)
                self.scheduler.accept(child_id, fields={"result_path": str(child_result_path)})
                fingerprint = hashlib.sha256(
                    json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
                ).hexdigest()
                if fingerprint == previous_fingerprint:
                    raise StateError("repeat_no_progress")
                previous_fingerprint = fingerprint
                previous_child = child_id
                evaluation = evaluate_condition(
                    {"source": child_id, **node["condition"]}, {child_id: result},
                )
                summaries.append({"iteration": iteration, "task_id": child_id, "condition": evaluation, "result": result})
                if evaluation["matched"]:
                    final = {"iterations": summaries, "stopped": "condition"}
                    encoded = json.dumps(final, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
                    (attempt_dir / "result.json").write_text(encoded, encoding="utf-8")
                    root_result = self.workflow_dir / "tasks" / task_id / "result.json"
                    root_result.write_text(encoded, encoding="utf-8")
                    self.store.update_task(task_id, {"result": final, "attempt_path": str(attempt_dir)})
                    self.scheduler.accept_computed(task_id, fields={"result_path": str(root_result)})
                    return
            raise StateError("repeat_max_iterations")
        except BaseException as exc:
            if self.store.snapshot["tasks"][task_id]["state"] == TaskState.RUNNING.value:
                self.scheduler.fail(task_id, reason=str(exc) or type(exc).__name__)

    def _run_one(self, task_id: str) -> tuple[str, WorkerOutcome, dict[str, Any], list[Any], Path]:
        node = self._node(task_id)
        if node["kind"] != "agent":
            raise StateError(f"workflow v1 cannot execute node kind: {node['kind']}")
        if node["sandbox"] == "workspace-write" and node.get("isolation") != "worktree":
            raise StateError("workspace-write agents require worktree isolation")
        task_dir = self.workflow_dir / "tasks" / task_id
        attempt = self.store.snapshot["tasks"][task_id]["attempt"]
        attempt_dir = task_dir / f"attempt-{attempt}"
        execution_cwd = Path(self.store.snapshot["tasks"][task_id].get("worktree") or self._workflow["cwd"])
        writer = node["sandbox"] == "workspace-write"
        schema_path = Path(__file__).resolve().parents[1] / "schemas" / "worker-result.schema.json"
        usage_index = 0

        def record_usage(delta: dict[str, int]) -> None:
            nonlocal usage_index
            usage_index += 1
            self.store.record_usage(
                delta, task_id=task_id,
                idempotency_key=f"usage:{task_id}:{attempt}:{usage_index}",
            )

        def on_process(process: Any) -> None:
            self._processes[task_id] = process
            self.store.update_task(task_id, {"pid": process.pid})

        try:
            outcome = run_worker(
                WorkerRequest(
                    binary=("codex",), cwd=execution_cwd, model=node["model"],
                    sandbox=node["sandbox"], prompt=build_task_prompt(
                        node["spec"], dependency_results=self._dependencies(task_id),
                        map_item=node.get("map_item"),
                        repeat_iteration=node.get("repeat_iteration"),
                    ),
                    result_path=attempt_dir / "result.json", events_path=attempt_dir / "events.jsonl",
                    output_schema_path=schema_path,
                    hard_tokens=node["budget"]["hard_tokens"],
                ),
                on_process=on_process,
                on_usage=record_usage,
            )
        finally:
            self._processes.pop(task_id, None)
        try:
            result = json.loads((attempt_dir / "result.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            result = {"result": "", "evidence": f"cannot parse result: {exc}"}
        gates = (
            [GateResult(
                "budget" if outcome.budget_exhausted else "transport",
                "rejected",
                "budget_exhausted" if outcome.budget_exhausted else f"worker exited with code {outcome.exit_code}",
            )]
            if outcome.exit_code != 0 else run_checks(
                node.get("checks", []), result=result, cwd=execution_cwd,
                artifact_dir=attempt_dir, writer=writer,
            )
        )
        if writer:
            base_sha = self.store.snapshot["tasks"][task_id].get("base_sha")
            if not isinstance(base_sha, str) or not base_sha:
                raise StateError(f"writer task {task_id} has no base commit")
            changes = capture_writer_changes(execution_cwd, base_ref=base_sha)
            patch = subprocess.run(
                ["git", "diff", "--binary", base_sha, "--"], cwd=execution_cwd,
                text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
            )
            if patch.returncode != 0:
                raise StateError(patch.stderr.strip() or "cannot capture writer patch")
            patch_text = patch.stdout
            for changed in changes["files"]:
                if changed["status"] != "untracked":
                    continue
                untracked = subprocess.run(
                    ["git", "diff", "--no-index", "--binary", "/dev/null", changed["path"]],
                    cwd=execution_cwd, text=True, stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE, check=False,
                )
                if untracked.returncode not in {0, 1}:
                    raise StateError(untracked.stderr.strip() or "cannot capture untracked writer file")
                patch_text += untracked.stdout
            patch_path = attempt_dir / "changes.patch"
            patch_path.write_text(patch_text, encoding="utf-8")
            manifest_paths = [path for path in (
                attempt_dir / "result.json", attempt_dir / "events.jsonl", patch_path,
            ) if path.is_file()]
            manifest = write_manifest(
                attempt_dir,
                manifest_paths,
                attempt_dir / "artifact_manifest.json",
            )
            self.store.update_task(task_id, {
                "writer_changes": changes, "patch_path": str(patch_path),
                "artifact_manifest": str(manifest),
            })
        return task_id, outcome, result, gates, attempt_dir

    def _prepare_workspace(self, task_id: str) -> None:
        node = self._node(task_id)
        if node.get("sandbox") != "workspace-write":
            return
        if node.get("isolation") != "worktree":
            raise StateError("workspace-write agents require worktree isolation")
        task = self.store.snapshot["tasks"][task_id]
        if task.get("worktree"):
            return
        root = Path(self._workflow["cwd"])
        head = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=root, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
        )
        if head.returncode != 0:
            raise StateError(head.stderr.strip() or "writer cwd is not a git checkout")
        safe_id = re.sub(r"[^A-Za-z0-9._-]+", "_", task_id)
        path = self.workflow_dir / "worktrees" / f"{safe_id}-attempt-{task['attempt']}"
        path.parent.mkdir(parents=True, exist_ok=True)
        completed = subprocess.run(
            ["git", "worktree", "add", "--detach", str(path), head.stdout.strip()],
            cwd=root, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
        )
        if completed.returncode != 0:
            raise StateError(completed.stderr.strip() or f"cannot create worktree for {task_id}")
        self.store.update_task(task_id, {
            "worktree": str(path), "base_sha": head.stdout.strip(), "worktree_isolation": "worktree",
        })

    def _handle_control(self, request: dict[str, Any]) -> dict[str, Any]:
        request_type = request["type"]
        payload = request.get("payload", {})
        if not isinstance(payload, dict):
            return {"accepted": False, "reason": "payload_must_be_object"}
        state = self.store.snapshot["state"]
        if request_type == "pause":
            if state == WorkflowState.RUNNING.value:
                self.store.transition_workflow(
                    WorkflowState.PAUSED.value, expected_state=WorkflowState.RUNNING.value,
                )
                return {"accepted": True, "state": WorkflowState.PAUSED.value}
            return {"accepted": False, "reason": f"workflow_is_{state}"}
        if request_type == "resume":
            if state == WorkflowState.PAUSED.value:
                self.store.transition_workflow(
                    WorkflowState.RUNNING.value, expected_state=WorkflowState.PAUSED.value,
                )
                return {"accepted": True, "state": WorkflowState.RUNNING.value}
            return {"accepted": False, "reason": f"workflow_is_{state}"}
        if request_type == "cancel":
            if state not in {WorkflowState.RUNNING.value, WorkflowState.PAUSED.value}:
                return {"accepted": False, "reason": f"workflow_is_{state}"}
            stop_active = payload.get("stop_active", True)
            if not isinstance(stop_active, bool):
                return {"accepted": False, "reason": "stop_active_must_be_boolean"}
            reason = str(payload.get("reason") or "operator_cancelled")
            cancelled: list[str] = []
            snapshot = self.store.snapshot
            for task_id, task in snapshot["tasks"].items():
                if task["state"] in {
                    TaskState.PENDING.value, TaskState.READY.value,
                    TaskState.RETRY_WAIT.value, TaskState.RUNNING.value,
                }:
                    if task["state"] == TaskState.RUNNING.value and stop_active:
                        process = self._processes.get(task_id)
                        if process is not None:
                            terminate_process(process)
                    self.scheduler.cancel(task_id, reason=reason)
                    cancelled.append(task_id)
            self.store.transition_workflow(
                WorkflowState.CANCELLED.value, expected_state=state,
            )
            self._cancelled = True
            return {
                "accepted": True, "state": WorkflowState.CANCELLED.value,
                "cancelled_tasks": cancelled, "stop_active": stop_active,
            }
        if request_type == "retry":
            task_id = payload.get("task_id")
            if not isinstance(task_id, str) or not task_id:
                return {"accepted": False, "reason": "retry_requires_task_id"}
            try:
                self.scheduler.manual_retry(
                    task_id, reason=str(payload.get("reason") or "operator_retry"),
                )
            except StateError as exc:
                return {"accepted": False, "reason": str(exc)}
            return {"accepted": True, "task_id": task_id, "state": TaskState.READY.value}
        if request_type in {"approve", "reject"}:
            task_id = payload.get("task_id")
            if not isinstance(task_id, str) or not task_id:
                return {"accepted": False, "reason": f"{request_type}_requires_task_id"}
            task = self.store.snapshot["tasks"].get(task_id)
            if task is None:
                return {"accepted": False, "reason": f"unknown_task:{task_id}"}
            if task.get("kind") != "approval":
                return {"accepted": False, "reason": "task_is_not_approval"}
            # A request may arrive before the scheduler's next readiness pass.
            self.scheduler.refresh_ready()
            task = self.store.snapshot["tasks"][task_id]
            if task["state"] != TaskState.READY.value:
                return {"accepted": False, "reason": f"approval_is_{task['state']}"}
            reason = str(payload.get("reason") or f"operator_{request_type}")
            if request_type == "approve":
                self.scheduler.accept_computed(
                    task_id,
                    fields={
                        "approval_status": "approved",
                        "approval_reason": reason,
                        "result": {"approved": True, "reason": reason},
                    },
                )
                return {"accepted": True, "task_id": task_id, "state": TaskState.ACCEPTED.value}
            self.scheduler.block(task_id, reason=reason)
            self.store.update_task(task_id, {"approval_status": "rejected", "approval_reason": reason})
            return {"accepted": True, "task_id": task_id, "state": TaskState.BLOCKED.value}
        return {"accepted": False, "reason": f"{request_type}_unsupported"}

    def _model_slot_available(self, task_id: str, node: dict[str, Any]) -> bool:
        model = node.get("model")
        limit_key = "max_terra_tasks" if model == "terra" else "max_sol_tasks" if model == "sol" else None
        if limit_key is None:
            return True
        limit = int(self._workflow["budget"].get(limit_key, 0))
        started = sum(
            1 for task_id_value, task in self.store.snapshot["tasks"].items()
            if task_id_value != task_id and task.get("model") == model and int(task.get("attempt", 0)) > 0
        )
        return started < limit

    def _maybe_escalate(self, task_id: str, failure_class: str) -> bool:
        task = self.store.snapshot["tasks"][task_id]
        node = self._node(task_id)
        retry = node.get("retry", {})
        if node.get("model") != "luna" or retry.get("escalate_to") != "terra":
            return False
        if failure_class not in retry.get("escalate_on", []):
            return False
        reason = str(retry.get("escalation_reason") or "").strip()
        if not reason:
            return False
        if not self._model_slot_available(task_id, {"model": "terra"}):
            self.store.update_task(task_id, {
                "escalation_blocked": True,
                "escalation_block_reason": "max_terra_tasks_reached",
            })
            return False
        self.store.update_task(task_id, {
            "model": "terra", "model_reason": reason,
            "escalated_from": "luna", "escalation_reason": reason,
        })
        return True

    def run(self) -> dict[str, Any]:
        with self.store:
            if self.store.snapshot["state"] == WorkflowState.RUNNING.value:
                reconcile(self.store)
                if any(task["state"] == TaskState.RUNNING.value for task in self.store.snapshot["tasks"].values()):
                    raise StateError("workflow has live workers; resume requires their reconciliation")
            if self.store.snapshot["state"] in {
                WorkflowState.CREATED.value, WorkflowState.FAILED.value,
                WorkflowState.BUDGET_EXHAUSTED.value, WorkflowState.INTERRUPTED.value,
            }:
                self.store.transition_workflow(
                    WorkflowState.RUNNING.value,
                    expected_state=self.store.snapshot["state"],
                )
            running: dict[Any, str] = {}
            max_workers = int(self._workflow["budget"]["max_workers"])
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                while True:
                    snapshot = self.store.snapshot
                    if snapshot["state"] == WorkflowState.RUNNING.value:
                        self.scheduler.refresh_ready()
                        snapshot = self.store.snapshot
                    self.store.consume_control_requests(self._handle_control)
                    snapshot = self.store.snapshot
                    if snapshot["state"] == WorkflowState.RUNNING.value:
                        self.scheduler.refresh_ready()
                        snapshot = self.store.snapshot
                    ready = [
                        task_id for task_id, task in snapshot["tasks"].items()
                        if task["state"] == TaskState.READY.value
                    ]
                    computed_progress = False
                    if snapshot["state"] == WorkflowState.RUNNING.value:
                        for task_id in ready:
                            if len(running) >= max_workers:
                                break
                            node = self._node(task_id)
                            created_hook = run_hooks(
                                self._workflow.get("hooks", {}), "task_created",
                                {"task_id": task_id, "kind": node["kind"], "attempt": snapshot["tasks"][task_id].get("attempt", 0) + 1},
                                cwd=self._workflow["cwd"],
                            )
                            if not created_hook.allowed:
                                self.scheduler.block(task_id, reason=created_hook.reason)
                                computed_progress = True
                                continue
                            if node["kind"] == "condition":
                                self._run_condition(task_id)
                                computed_progress = True
                                continue
                            if node["kind"] == "check":
                                self._run_check_node(task_id)
                                computed_progress = True
                                continue
                            if node["kind"] == "map":
                                self._run_map(task_id)
                                computed_progress = True
                                continue
                            if node["kind"] == "reduce":
                                self._run_reduce(task_id)
                                computed_progress = True
                                continue
                            if node["kind"] == "repeat_until":
                                if running:
                                    continue
                                self._run_repeat(task_id)
                                computed_progress = True
                                continue
                            if node["kind"] == "approval":
                                if not snapshot["tasks"][task_id].get("approval_status"):
                                    self.store.update_task(task_id, {"approval_status": "pending"})
                                continue
                            if node["kind"] == "agent" and not self._model_slot_available(task_id, node):
                                self.scheduler.block(task_id, reason=f"{node['model']}_task_limit_reached")
                                computed_progress = True
                                continue
                            if self._try_cache(task_id):
                                computed_progress = True
                                continue
                            if self._cache is not None and node["kind"] == "agent" and node.get("sandbox") == "read-only":
                                try:
                                    self._cache_fingerprints[task_id] = task_fingerprint(
                                        node, cwd=self._workflow["cwd"],
                                        dependency_results=self._dependencies(task_id),
                                    )
                                except (OSError, StateError):
                                    pass
                            self.scheduler.start(task_id, reserve_tokens=node["budget"]["reserve_tokens"])
                            try:
                                self._prepare_workspace(task_id)
                            except BaseException as exc:
                                self.scheduler.fail(task_id, reason=str(exc) or type(exc).__name__)
                                continue
                            running[executor.submit(self._run_one, task_id)] = task_id
                    if computed_progress and not running:
                        continue
                    if not running:
                        snapshot = self.store.snapshot
                        if snapshot["state"] == WorkflowState.CANCELLED.value:
                            break
                        if snapshot["state"] == WorkflowState.PAUSED.value:
                            time.sleep(0.05)
                            continue
                        states = {task["state"] for task in snapshot["tasks"].values()}
                        if states and states <= {TaskState.ACCEPTED.value, TaskState.BLOCKED.value} and TaskState.ACCEPTED.value in states:
                            self.store.transition_workflow(
                                WorkflowState.SUCCEEDED.value,
                                expected_state=WorkflowState.RUNNING.value,
                            )
                            break
                        if TaskState.BUDGET_EXHAUSTED.value in states:
                            self.store.transition_workflow(
                                WorkflowState.BUDGET_EXHAUSTED.value,
                                expected_state=WorkflowState.RUNNING.value,
                            )
                            break
                        if any(state in {TaskState.PENDING.value, TaskState.READY.value} for state in states):
                            if any(
                                task["state"] == TaskState.READY.value
                                and task.get("kind") == "approval"
                                for task in snapshot["tasks"].values()
                            ):
                                time.sleep(0.05)
                                continue
                            raise StateError("workflow scheduler stalled")
                        self.store.transition_workflow(
                            WorkflowState.FAILED.value, expected_state=WorkflowState.RUNNING.value,
                        )
                        break
                    completed, _ = wait(running, timeout=0.2, return_when=FIRST_COMPLETED)
                    if not completed:
                        continue
                    for future in completed:
                        task_id = running.pop(future)
                        try:
                            _, outcome, result, gates, attempt_dir = future.result()
                            self.store.update_task(task_id, {
                                "pid": None, "event_count": outcome.event_count,
                                "result": result, "gates": [gate.__dict__ for gate in gates],
                                "worker_usage": outcome.usage,
                                "budget_overshoot": max(
                                    0, outcome.usage["total_tokens"] - self._node(task_id)["budget"]["hard_tokens"],
                                ),
                            })
                            if self.store.snapshot["tasks"][task_id]["state"] == TaskState.CANCELLED.value:
                                continue
                            if outcome.budget_exhausted:
                                self.scheduler.budget_exhausted(
                                    task_id, observed_tokens=outcome.usage["total_tokens"],
                                )
                                continue
                            self.scheduler.complete(task_id)
                            if all(gate.status == "accepted" for gate in gates):
                                completed_hook = run_hooks(
                                    self._workflow.get("hooks", {}), "task_completed",
                                    {"task_id": task_id, "result": result, "attempt": self.store.snapshot["tasks"][task_id].get("attempt", 0)},
                                    cwd=self._workflow["cwd"],
                                )
                                if not completed_hook.allowed:
                                    self.scheduler.reject(task_id, reason=completed_hook.reason)
                                    continue
                                result_path = self.workflow_dir / "tasks" / task_id / "result.json"
                                if attempt_dir.joinpath("result.json").is_file():
                                    shutil.copyfile(attempt_dir / "result.json", result_path)
                                self.scheduler.accept(
                                    task_id,
                                    fields={
                                        "result_path": str(result_path),
                                        "attempt_path": str(attempt_dir),
                                    },
                                )
                                if self._cache is not None:
                                    fingerprint = self._cache_fingerprints.get(task_id)
                                    if fingerprint:
                                        try:
                                            self._cache.put(
                                                fingerprint, result=result,
                                                source_workflow=self.store.snapshot["workflow_id"],
                                                source_task=task_id, sandbox=self._node(task_id)["sandbox"],
                                            )
                                            self.store.update_task(task_id, {"cache_fingerprint": fingerprint})
                                        except (OSError, StateError) as exc:
                                            self.store.update_task(task_id, {"cache_error": str(exc)})
                            else:
                                reasons = [gate.reason for gate in gates if gate.reason]
                                previous_fingerprint = self.store.snapshot["tasks"][task_id].get(
                                    "failure_fingerprint",
                                )
                                failure = classify_failure(
                                    outcome=outcome, gates=gates, result=result,
                                )
                                self.scheduler.reject(
                                    task_id, reason="; ".join(reasons) or failure.reason,
                                    failure=failure.fields(),
                                )
                                self._maybe_escalate(task_id, failure.failure_class)
                                self.scheduler.maybe_retry(
                                    task_id,
                                    failure_class=failure.failure_class,
                                    fingerprint=failure.fingerprint,
                                    evidence=failure.evidence,
                                    allowed_classes=self._node(task_id)["retry"]["retry_on"],
                                    previous_fingerprint=previous_fingerprint,
                                )
                        except BaseException as exc:
                            previous_fingerprint = self.store.snapshot["tasks"][task_id].get(
                                "failure_fingerprint",
                            )
                            failure = classify_failure(error=exc)
                            self.store.update_task(task_id, {
                                "pid": None, "error": str(exc), **failure.fields(),
                            })
                            if self.store.snapshot["tasks"][task_id]["state"] == TaskState.RUNNING.value:
                                self.scheduler.fail(
                                    task_id, reason=failure.reason, failure=failure.fields(),
                                )
                                self._maybe_escalate(task_id, failure.failure_class)
                                self.scheduler.maybe_retry(
                                    task_id,
                                    failure_class=failure.failure_class,
                                    fingerprint=failure.fingerprint,
                                    evidence=failure.evidence,
                                    allowed_classes=self._node(task_id)["retry"]["retry_on"],
                                    previous_fingerprint=previous_fingerprint,
                                )
                return build_status(self.store)

    def close(self) -> None:
        self.store.close()
