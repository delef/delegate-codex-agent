#!/usr/bin/env python3
"""Run a low-cost Codex delegate with a bounded, auditable context packet."""

from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import datetime as dt
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any


MODEL_IDS = {
    "luna": "gpt-5.6-luna",
    "sol": "gpt-5.6-sol",
    "terra": "gpt-5.6-terra",
}
REQUIRED_LISTS = ("scope", "context", "constraints", "acceptance", "commands", "output")
DEFAULT_MAX_CONTEXT_CHARS = 40_000
DEFAULT_MAX_DEPENDENCY_CHARS = 2_000
DEFAULT_HEARTBEAT_SECONDS = 15.0
ACTIVE_IDLE_SECONDS = 60
STALE_HEARTBEAT_SECONDS = 45
RESULT_FIELDS = ("result", "evidence", "changes", "verification", "risks", "recommended_next_action")
ACTIVE_PROCESSES: set[subprocess.Popen[str]] = set()
ACTIVE_LOCK = threading.Lock()
ACTIVE_CANCEL_EVENTS: set[threading.Event] = set()


class SpecError(ValueError):
    pass


def validate_model_use(model: str, model_reason: Any, sandbox: str, label: str) -> None:
    if model == "sol" and (
        not isinstance(model_reason, str) or not model_reason.strip()
    ):
        raise SpecError(
            f"{label} uses {model.title()} and requires a non-empty model reason"
        )
    if model == "sol" and sandbox != "read-only":
        raise SpecError(f"{label} uses Sol, which is restricted to read-only analysis")


def codex_binary() -> str:
    return os.environ.get("DELEGATE_CODEX_BIN", "codex")


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)


def run_git(cwd: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args], cwd=cwd, text=True, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, check=False,
    )
    if completed.returncode != 0:
        raise SpecError(completed.stderr.strip() or f"git {' '.join(args)} failed")
    return completed.stdout.rstrip()


def repository_root(cwd: Path) -> Path:
    return Path(run_git(cwd, "rev-parse", "--show-toplevel")).resolve()


def create_task_worktree(root: Path, batch_dir: Path, task: dict[str, Any]) -> dict[str, Any]:
    worktrees = batch_dir / "worktrees"
    worktrees.mkdir(exist_ok=True)
    suffix = hashlib.sha256(task["id"].encode()).hexdigest()[:8]
    path = worktrees / f"{slug(task['id'])}-{suffix}"
    base_ref = task["base_ref"]
    base_sha = run_git(root, "rev-parse", "--verify", f"{base_ref}^{{commit}}")
    completed = subprocess.run(
        ["git", "worktree", "add", "--detach", str(path), base_sha], cwd=root,
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
    )
    if completed.returncode != 0:
        raise SpecError(completed.stderr.strip() or f"cannot create worktree for {task['id']}")
    return {"path": str(path), "base_ref": base_ref, "base_sha": base_sha}


def finalize_task_worktree(root: Path, metadata: dict[str, Any], run_dir: str | None) -> dict[str, Any]:
    path = Path(metadata["path"])
    head_sha = run_git(path, "rev-parse", "HEAD")
    status = run_git(path, "status", "--porcelain")
    changed = bool(status) or head_sha != metadata["base_sha"]
    result = {
        "worktree": str(path), "base_ref": metadata["base_ref"],
        "base_sha": metadata["base_sha"], "head_sha": head_sha,
        "integration_status": "ready" if changed else "none",
    }
    if changed:
        if run_dir:
            patch_path = Path(run_dir) / "changes.patch"
            patch = run_git(path, "diff", "--binary", metadata["base_sha"])
            patch_path.write_text(patch + ("\n" if patch else ""), encoding="utf-8")
            result["patch"] = str(patch_path)
        result["worktree_status"] = status
    else:
        completed = subprocess.run(
            ["git", "worktree", "remove", str(path)], cwd=root, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
        )
        if completed.returncode != 0:
            result["cleanup_error"] = completed.stderr.strip()
        else:
            result["worktree"] = None
    return result


def inside(root: Path, raw_path: str) -> Path:
    candidate = (root / raw_path).resolve() if not Path(raw_path).is_absolute() else Path(raw_path).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise SpecError(f"path escapes repository: {raw_path}") from exc
    return candidate


def load_spec(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SpecError(f"cannot read spec: {exc}") from exc
    if not isinstance(value, dict):
        raise SpecError("spec must be a JSON object")
    for key in ("name", "objective"):
        if not isinstance(value.get(key), str) or not value[key].strip():
            raise SpecError(f"{key} must be a non-empty string")
    for key in REQUIRED_LISTS:
        if key not in value:
            value[key] = []
        if not isinstance(value[key], list):
            raise SpecError(f"{key} must be an array")
    if not all(isinstance(item, str) and item.strip() for item in value["scope"]):
        raise SpecError("scope entries must be non-empty strings")
    if not all(isinstance(item, dict) and isinstance(item.get("path"), str) for item in value["context"]):
        raise SpecError("context entries must be objects with path")
    for key in ("constraints", "acceptance", "commands", "output"):
        if not all(isinstance(item, str) and item.strip() for item in value[key]):
            raise SpecError(f"{key} entries must be non-empty strings")
    return value


def load_manifest(path: Path) -> list[dict[str, Any]]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SpecError(f"cannot read manifest: {exc}") from exc
    if not isinstance(value, dict) or not isinstance(value.get("tasks"), list) or not value["tasks"]:
        raise SpecError("manifest must contain a non-empty tasks array")
    tasks: list[dict[str, Any]] = []
    ids: set[str] = set()
    for raw in value["tasks"]:
        if not isinstance(raw, dict):
            raise SpecError("manifest tasks must be objects")
        task_id = raw.get("id")
        spec = raw.get("spec")
        if not isinstance(task_id, str) or not task_id.strip():
            raise SpecError("each task id must be a non-empty string")
        if task_id in ids:
            raise SpecError(f"duplicate task id: {task_id}")
        if not isinstance(spec, str) or not spec.strip():
            raise SpecError(f"task {task_id} spec must be a non-empty string")
        model = raw.get("model", "luna")
        model_reason = raw.get("model_reason")
        sandbox = raw.get("sandbox", "read-only")
        isolation = raw.get("isolation", "shared")
        base_ref = raw.get("base_ref", "HEAD")
        dependencies = raw.get("depends_on", [])
        if model not in MODEL_IDS:
            raise SpecError(f"task {task_id} has unsupported model: {model}")
        if model == "terra" and (not isinstance(model_reason, str) or not model_reason.strip()):
            raise SpecError(f"task {task_id} uses Terra and requires a non-empty model_reason")
        if sandbox not in ("read-only", "workspace-write"):
            raise SpecError(f"task {task_id} has unsupported sandbox: {sandbox}")
        validate_model_use(model, model_reason, sandbox, f"task {task_id}")
        if isolation not in ("shared", "worktree"):
            raise SpecError(f"task {task_id} has unsupported isolation: {isolation}")
        if isolation == "worktree" and sandbox != "workspace-write":
            raise SpecError(f"task {task_id} worktree isolation requires workspace-write sandbox")
        if not isinstance(base_ref, str) or not base_ref.strip():
            raise SpecError(f"task {task_id} base_ref must be a non-empty string")
        if not isinstance(dependencies, list) or not all(isinstance(item, str) for item in dependencies):
            raise SpecError(f"task {task_id} depends_on must be an array of ids")
        resolved_spec = Path(spec)
        if not resolved_spec.is_absolute():
            resolved_spec = path.parent / resolved_spec
        tasks.append({
            "id": task_id, "spec": str(resolved_spec.resolve()), "model": model,
            "model_reason": model_reason,
            "sandbox": sandbox, "isolation": isolation, "base_ref": base_ref,
            "depends_on": dependencies,
        })
        ids.add(task_id)
    for task in tasks:
        missing = set(task["depends_on"]) - ids
        if missing:
            raise SpecError(f"task {task['id']} has missing dependencies: {', '.join(sorted(missing))}")
        if task["id"] in task["depends_on"]:
            raise SpecError(f"dependency cycle includes task {task['id']}")
    by_id = {task["id"]: task for task in tasks}
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(task_id: str) -> None:
        if task_id in visiting:
            raise SpecError(f"dependency cycle includes task {task_id}")
        if task_id in visited:
            return
        visiting.add(task_id)
        for dependency in by_id[task_id]["depends_on"]:
            visit(dependency)
        visiting.remove(task_id)
        visited.add(task_id)

    for task in tasks:
        visit(task["id"])
    return tasks


def validate_model_budget(
    tasks: list[dict[str, Any]], max_terra_tasks: int, max_sol_tasks: int = 0,
) -> None:
    if max_terra_tasks < 0:
        raise SpecError("max-terra-tasks must be nonnegative")
    if max_sol_tasks < 0:
        raise SpecError("max-sol-tasks must be nonnegative")
    terra_count = sum(task["model"] == "terra" for task in tasks)
    if terra_count > max_terra_tasks:
        raise SpecError(
            f"Terra task limit exceeded: manifest has {terra_count}, limit is {max_terra_tasks}. "
            "Reduce Terra usage or raise --max-terra-tasks explicitly"
        )
    sol_count = sum(task["model"] == "sol" for task in tasks)
    if sol_count > max_sol_tasks:
        raise SpecError(
            f"Sol task limit exceeded: manifest has {sol_count}, limit is {max_sol_tasks}. "
            "Reduce Sol usage or raise --max-sol-tasks explicitly"
        )


def empty_usage() -> dict[str, int]:
    return {
        "input_tokens": 0, "cached_input_tokens": 0, "uncached_input_tokens": 0,
        "output_tokens": 0, "reasoning_output_tokens": 0, "total_tokens": 0,
    }


def usage_from_event(event: dict[str, Any]) -> dict[str, int]:
    usage = empty_usage()
    raw = event.get("usage") if event.get("type") == "turn.completed" else None
    if not isinstance(raw, dict):
        return usage
    for field in ("input_tokens", "cached_input_tokens", "output_tokens", "reasoning_output_tokens"):
        value = raw.get(field, 0)
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            usage[field] = value
    usage["uncached_input_tokens"] = max(0, usage["input_tokens"] - usage["cached_input_tokens"])
    usage["total_tokens"] = usage["input_tokens"] + usage["output_tokens"]
    return usage


def usage_from_events(path: Path) -> dict[str, int]:
    usage = empty_usage()
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return empty_usage()
    for line in lines:
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict):
            delta = usage_from_event(event)
            for field in usage:
                usage[field] += delta[field]
    return usage


def add_usage(values: list[dict[str, int]]) -> dict[str, int]:
    total = empty_usage()
    for value in values:
        for field in total:
            total[field] += value.get(field, 0)
    return total


def heartbeat_seconds(value: str | None = None) -> float:
    raw = os.environ.get("DELEGATE_HEARTBEAT_SECONDS") if value is None else value
    try:
        seconds = float(raw) if raw else DEFAULT_HEARTBEAT_SECONDS
    except (TypeError, ValueError):
        return DEFAULT_HEARTBEAT_SECONDS
    return seconds if math.isfinite(seconds) and seconds > 0 else DEFAULT_HEARTBEAT_SECONDS


def parse_utc(value: Any) -> dt.datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def health_from_status(
    status: dict[str, Any], now: dt.datetime | None = None,
) -> str:
    if status.get("status") != "running":
        return "finished"
    current = now or dt.datetime.now(dt.timezone.utc)
    heartbeat_at = parse_utc(status.get("heartbeat_at"))
    if heartbeat_at is None:
        return "stale"
    if (current - heartbeat_at).total_seconds() > STALE_HEARTBEAT_SECONDS:
        return "stale"
    if status.get("child_alive") is False:
        return "stale"
    last_event_at = parse_utc(status.get("last_event_at"))
    idle: Any = (
        (current - last_event_at).total_seconds()
        if last_event_at is not None else status.get("idle_seconds", 0)
    )
    if (
        status.get("child_alive") is True and
        isinstance(idle, (int, float)) and idle >= ACTIVE_IDLE_SECONDS
    ):
        return "silent"
    return "active"


_PROCESS_UNSET = object()


class ProgressReporter:
    def __init__(
        self, status_path: Path, state: dict[str, Any], task_id: str,
        interval_seconds: float | None = None, emit: Any = print,
    ):
        self.status_path = status_path
        self.task_id = task_id
        self.interval_seconds = interval_seconds or heartbeat_seconds()
        self.emit = emit
        self._state = dict(state)
        self._state.setdefault("started_at", dt.datetime.now(dt.timezone.utc).isoformat())
        self._state.setdefault("phase", "preparing")
        self._lock = threading.Lock()
        self._write_lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._started_monotonic = time.monotonic()
        self._last_event_monotonic: float | None = None
        self._last_event_at: str | None = None
        self._last_event_type: str | None = None
        self._event_count = 0
        self._usage = empty_usage()
        self._process: subprocess.Popen[str] | None = None
        self._heartbeat_error_reported = False

    def record_event(self, line: str) -> None:
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            value = None
        with self._lock:
            self._last_event_monotonic = time.monotonic()
            self._last_event_at = dt.datetime.now(dt.timezone.utc).isoformat()
            self._event_count += 1
            if isinstance(value, dict):
                event_type = value.get("type")
                self._last_event_type = event_type if isinstance(event_type, str) else "unknown"
                self._usage = add_usage([self._usage, usage_from_event(value)])
            else:
                self._last_event_type = "unparsed"

    def snapshot(self) -> dict[str, Any]:
        now_monotonic = time.monotonic()
        now_wall = dt.datetime.now(dt.timezone.utc).isoformat()
        with self._lock:
            snapshot = dict(self._state)
            elapsed = max(0, int(now_monotonic - self._started_monotonic))
            idle_from = self._last_event_monotonic or self._started_monotonic
            process = self._process
            snapshot.update({
                "task_id": self.task_id,
                "child_alive": None if process is None else process.poll() is None,
                "heartbeat_at": now_wall,
                "last_event_at": self._last_event_at,
                "last_event_type": self._last_event_type,
                "event_count": self._event_count,
                "elapsed_seconds": elapsed,
                "idle_seconds": max(0, int(now_monotonic - idle_from)),
                "usage": dict(self._usage),
            })
        return snapshot

    def _heartbeat_line(self, snapshot: dict[str, Any]) -> str:
        alive = snapshot["child_alive"]
        alive_text = "unknown" if alive is None else str(alive).lower()
        return (
            f"DELEGATE_HEARTBEAT task={slug(self.task_id)} phase={snapshot['phase']} "
            f"child_alive={alive_text} elapsed={snapshot['elapsed_seconds']}s "
            f"idle={snapshot['idle_seconds']}s events={snapshot['event_count']} "
            f"tokens={snapshot['usage']['total_tokens']}"
        )

    def _persist(self) -> dict[str, Any]:
        with self._write_lock:
            snapshot = self.snapshot()
            atomic_json(self.status_path, snapshot)
        return snapshot

    def _write_heartbeat(self) -> None:
        snapshot = self._persist()
        self.emit(self._heartbeat_line(snapshot), flush=True)

    def _loop(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            try:
                self._write_heartbeat()
            except OSError as exc:
                if not self._heartbeat_error_reported:
                    self._heartbeat_error_reported = True
                    self.emit(f"DELEGATE_HEARTBEAT_ERROR error={exc}", flush=True)

    def start(self) -> None:
        self._write_heartbeat()
        self._thread = threading.Thread(target=self._loop, name="delegate-heartbeat", daemon=True)
        self._thread.start()

    def set_phase(
        self, phase: str, process: subprocess.Popen[str] | None | object = _PROCESS_UNSET,
        **state: Any,
    ) -> None:
        with self._lock:
            self._state["phase"] = phase
            self._state.update(state)
            if process is not _PROCESS_UNSET:
                self._process = process  # type: ignore[assignment]
        self._persist()

    def finish(self, status: str, exit_code: int | None, **extra: Any) -> None:
        self._stop.set()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=max(1.0, self.interval_seconds + 1.0))
        with self._lock:
            self._state.update(extra)
            self._state.update({
                "status": status,
                "exit_code": exit_code,
                "finished_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            })
        self._persist()


def structured_result(raw: str) -> dict[str, str]:
    aliases = {
        "result": "result", "evidence": "evidence", "changes": "changes",
        "verification": "verification", "risks": "risks",
        "recommended next action": "recommended_next_action",
    }
    sections = {field: "" for field in RESULT_FIELDS}
    current: str | None = None
    for line in raw.splitlines():
        heading = re.match(r"^\s*#{0,6}\s*([^:#]+?)\s*:??\s*$", line)
        normalized = heading.group(1).strip().lower() if heading else ""
        if normalized in aliases:
            current = aliases[normalized]
            continue
        if current is not None:
            sections[current] += line + "\n"
    for field in sections:
        sections[field] = sections[field].strip()
    if not any(sections.values()):
        sections["result"] = raw.strip()
    return sections


def dependency_summary(result: dict[str, str], max_chars: int = DEFAULT_MAX_DEPENDENCY_CHARS) -> str:
    if max_chars < 1:
        raise SpecError("max dependency chars must be at least 1")
    labels = (
        ("result", "Result"), ("risks", "Risks"),
        ("recommended_next_action", "Recommended next action"),
        ("evidence", "Evidence"), ("changes", "Changes"),
    )
    parts = [f"{label}: {result[field]}" for field, label in labels if result.get(field)]
    compact = "\n".join(parts) or "Result: No structured result supplied."
    if len(compact) <= max_chars:
        return compact
    if max_chars <= 1:
        return compact[:max_chars]
    return compact[:max_chars - 1].rstrip() + "…"


def applicable_agents(root: Path, paths: list[Path]) -> list[Path]:
    found: set[Path] = set()
    for target in [root, *paths]:
        current = target if target.is_dir() else target.parent
        while True:
            candidate = current / "AGENTS.md"
            if candidate.is_file():
                found.add(candidate)
            if current == root:
                break
            if root not in current.parents:
                break
            current = current.parent
    return sorted(found, key=lambda item: (len(item.relative_to(root).parts), str(item)))


def excerpt(path: Path, start: int, end: int) -> str:
    if start < 1 or end < start:
        raise SpecError(f"invalid line range for {path}: {start}-{end}")
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    selected = lines[start - 1:end]
    return "\n".join(f"{number}: {line}" for number, line in enumerate(selected, start=start))


def bullets(title: str, values: list[str]) -> list[str]:
    if not values:
        return []
    return [f"## {title}", "", *(f"- {value}" for value in values), ""]


def build_packet(spec: dict[str, Any], root: Path, model: str, sandbox: str, max_chars: int) -> str:
    scope_paths = [inside(root, value) for value in spec["scope"]]
    context_paths = [inside(root, item["path"]) for item in spec["context"]]
    agents_paths = applicable_agents(root, [*scope_paths, *context_paths])

    lines = [
        "# Delegated task", "",
        f"Name: {spec['name']}",
        f"Model: {MODEL_IDS[model]}",
        f"Sandbox: {sandbox}", "",
        "## Objective", "", spec["objective"].strip(), "",
    ]
    lines += bullets("Authorized scope", [str(path.relative_to(root)) for path in scope_paths])
    lines += bullets("Constraints", spec["constraints"])
    lines += bullets("Acceptance criteria", spec["acceptance"])
    lines += bullets("Required checks", spec["commands"])
    lines += ["## Repository state", "", "```text", run_git(root, "status", "--short"), "```", ""]
    diff_stat = run_git(root, "diff", "--stat")
    if diff_stat:
        lines += ["### Existing unstaged diff summary", "", "```text", diff_stat, "```", ""]

    if agents_paths:
        lines += ["## Applicable instructions", ""]
        for path in agents_paths:
            rel = path.relative_to(root)
            content = path.read_text(encoding="utf-8", errors="replace")
            lines += [f"### {rel}", "", "```markdown", content, "```", ""]

    if spec["context"]:
        lines += ["## Targeted context", ""]
        for item, path in zip(spec["context"], context_paths, strict=True):
            rel = path.relative_to(root)
            reason = item.get("reason", "relevant context")
            if "start" in item or "end" in item:
                if not isinstance(item.get("start"), int) or not isinstance(item.get("end"), int):
                    raise SpecError(f"both integer start/end required for {rel}")
                content = excerpt(path, item["start"], item["end"])
                lines += [f"### {rel}:{item['start']} ({reason})", "", "```text", content, "```", ""]
            else:
                lines += [f"- Read `{rel}` as needed: {reason}"]
        lines.append("")

    lines += [
        "## Working protocol", "",
        "- Work only on the objective and authorized scope.",
        "- Preserve unrelated dirty changes.",
        "- Ask no interactive questions; if blocked, stop and report exact missing context.",
        "- Do not spawn other agents.",
        "- Inspect the final diff before reporting.",
        "",
        "## Required final response", "",
        "Return these headings: Result; Evidence; Changes; Verification; Risks; Recommended next action.",
        *[f"- {item}" for item in spec["output"]],
        "",
    ]
    packet = "\n".join(lines)
    if len(packet) > max_chars:
        raise SpecError(f"context packet is {len(packet)} chars; limit is {max_chars}. Narrow excerpts or decompose the task")
    return packet


def slug(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return cleaned[:48] or "task"


def worktree_lock(root: Path, sandbox: str, cancel_event: threading.Event | None = None):
    lock_root = Path(tempfile.gettempdir()) / "codex-delegate-locks"
    lock_root.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(str(root).encode()).hexdigest()
    handle = (lock_root / f"{digest}.lock").open("a+")
    operation = fcntl.LOCK_EX if sandbox == "workspace-write" else fcntl.LOCK_SH
    try:
        while True:
            if cancel_event is not None and cancel_event.is_set():
                raise InterruptedError("cancelled while waiting for worktree lock")
            try:
                fcntl.flock(handle.fileno(), operation | fcntl.LOCK_NB)
                return handle
            except BlockingIOError:
                time.sleep(0.05)
    except BaseException:
        handle.close()
        raise


def terminate_process(process: subprocess.Popen[str], grace_seconds: float = 3.0) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
        process.wait(timeout=grace_seconds)
    except ProcessLookupError:
        return
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()


def terminate_all_processes() -> None:
    with ACTIVE_LOCK:
        processes = list(ACTIVE_PROCESSES)
    for process in processes:
        terminate_process(process)


class BatchExecutor(ThreadPoolExecutor):
    def __init__(self, max_workers: int, cancel_event: threading.Event):
        super().__init__(max_workers=max_workers)
        self.cancel_event = cancel_event

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool:
        if exc_type is not None and issubclass(exc_type, KeyboardInterrupt):
            self.cancel_event.set()
            terminate_all_processes()
        return super().__exit__(exc_type, exc_value, traceback)


def execute_run(args: argparse.Namespace) -> tuple[int, Path]:
    cwd = Path(args.cwd).resolve()
    root = repository_root(cwd)
    spec = load_spec(Path(args.spec).resolve())
    validate_model_use(
        args.model, getattr(args, "model_reason", None), args.sandbox, "delegate",
    )
    packet = build_packet(spec, root, args.model, args.sandbox, args.max_context_chars)
    dependency_results = getattr(args, "dependency_results", [])
    if dependency_results:
        sections = ["", "## Dependency results", ""]
        for task_id, result in dependency_results:
            sections += [
                f"### {task_id}", "",
                dependency_summary(result, getattr(args, "max_dependency_chars", DEFAULT_MAX_DEPENDENCY_CHARS)),
                "",
            ]
        packet += "\n" + "\n".join(sections)
        if len(packet) > args.max_context_chars:
            raise SpecError(
                f"context packet with dependency results is {len(packet)} chars; "
                f"limit is {args.max_context_chars}. Narrow task outputs or decompose the batch"
            )
    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    base = Path(args.runs_dir).expanduser().resolve()
    base.mkdir(parents=True, exist_ok=True)
    run_dir = Path(tempfile.mkdtemp(prefix=f"{timestamp}-{slug(spec['name'])}-", dir=base))
    (run_dir / "packet.md").write_text(packet, encoding="utf-8")
    print(f"RUN_DIR={run_dir}", flush=True)

    state = {
        "status": "running", "name": spec["name"], "model": MODEL_IDS[args.model],
        "model_reason": getattr(args, "model_reason", None),
        "sandbox": args.sandbox, "cwd": str(root), "started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "phase": "waiting_for_lock", "pid": None, "exit_code": None,
    }
    reporter = ProgressReporter(
        run_dir / "status.json", state,
        task_id=getattr(args, "task_id", spec["name"]),
    )
    reporter.start()
    command = [
        codex_binary(), "exec", "-C", str(root), "-m", MODEL_IDS[args.model],
        "-s", args.sandbox, "--json", "-o", str(run_dir / "result.md"), "-",
    ]
    process: subprocess.Popen[str] | None = None
    lock_handle = None
    events_path = run_dir / "events.jsonl"
    try:
        lock_handle = worktree_lock(root, args.sandbox, getattr(args, "cancel_event", None))
        with events_path.open("w", encoding="utf-8") as events:
            process = subprocess.Popen(
                command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, start_new_session=True,
            )
            with ACTIVE_LOCK:
                ACTIVE_PROCESSES.add(process)
            reporter.set_phase("model_running", process, pid=process.pid)
            print(f"DELEGATE_STARTED pid={process.pid} model={MODEL_IDS[args.model]}", flush=True)
            assert process.stdin is not None and process.stdout is not None
            process.stdin.write(packet)
            process.stdin.close()
            for line in process.stdout:
                events.write(line)
                events.flush()
                reporter.record_event(line)
            exit_code = process.wait()
            reporter.set_phase("finalizing", process)
            if exit_code == 0 and not (run_dir / "result.md").is_file():
                raise SpecError("codex exited successfully without producing result.md")
            if exit_code == 0:
                result = structured_result((run_dir / "result.md").read_text(encoding="utf-8"))
                atomic_json(run_dir / "result.json", result)
    except BaseException as exc:
        if process is not None:
            terminate_process(process)
        reporter.finish(
            "interrupted" if isinstance(exc, (KeyboardInterrupt, InterruptedError)) else "failed",
            process.returncode if process else None,
        )
        raise
    finally:
        if lock_handle is not None:
            lock_handle.close()
        if process is not None:
            with ACTIVE_LOCK:
                ACTIVE_PROCESSES.discard(process)

    final_status = "succeeded" if exit_code == 0 else "failed"
    reporter.finish(final_status, exit_code)
    print(f"DELEGATE_FINISHED status={final_status} exit_code={exit_code}", flush=True)
    return exit_code, run_dir


def command_run(args: argparse.Namespace) -> int:
    try:
        exit_code, _ = execute_run(args)
        return exit_code
    except KeyboardInterrupt:
        print("DELEGATE_INTERRUPTED", flush=True)
        return 130


def command_prepare(args: argparse.Namespace) -> int:
    cwd = Path(args.cwd).resolve()
    root = repository_root(cwd)
    spec = load_spec(Path(args.spec).resolve())
    validate_model_use(
        args.model, getattr(args, "model_reason", None), args.sandbox, "delegate",
    )
    packet = build_packet(spec, root, args.model, args.sandbox, args.max_context_chars)
    if args.output:
        output = Path(args.output).resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(packet, encoding="utf-8")
        print(output)
    else:
        print(packet)
    return 0


def command_inspect(args: argparse.Namespace) -> int:
    run_dir = Path(args.run_dir).expanduser().resolve()
    status_path = run_dir / "status.json"
    if not status_path.is_file():
        raise SpecError(f"missing {status_path}")
    try:
        status = json.loads(status_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SpecError(f"cannot read status: {exc}") from exc
    if not isinstance(status, dict):
        raise SpecError("status must be a JSON object")
    displayed = dict(status)
    displayed["health"] = health_from_status(status)
    print(json.dumps(displayed, ensure_ascii=False, indent=2))
    result = run_dir / "result.md"
    if result.is_file():
        print("\n--- result.md ---")
        print(result.read_text(encoding="utf-8"), end="")
    return 0


def thread_id_from_events(path: Path) -> str:
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                event = json.loads(line)
                if event.get("type") == "thread.started" and isinstance(event.get("thread_id"), str):
                    return event["thread_id"]
    except (OSError, json.JSONDecodeError) as exc:
        raise SpecError(f"cannot read thread id from {path}: {exc}") from exc
    raise SpecError(f"no thread.started event in {path}")


def command_resume(args: argparse.Namespace) -> int:
    previous_dir = Path(args.run_dir).expanduser().resolve()
    try:
        previous = json.loads((previous_dir / "status.json").read_text(encoding="utf-8"))
        feedback = Path(args.feedback_file).resolve().read_text(encoding="utf-8")
    except (OSError, json.JSONDecodeError) as exc:
        raise SpecError(f"cannot prepare resume: {exc}") from exc
    if not feedback.strip():
        raise SpecError("feedback file must not be empty")
    model_id = previous.get("model")
    cwd = Path(previous.get("cwd", "")).resolve()
    sandbox = previous.get("sandbox")
    if model_id not in MODEL_IDS.values() or sandbox not in ("read-only", "workspace-write") or not cwd.is_dir():
        raise SpecError("previous status has unsupported model, sandbox, or cwd")
    thread_id = thread_id_from_events(previous_dir / "events.jsonl")
    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    run_dir = Path(tempfile.mkdtemp(
        prefix=f"{timestamp}-{slug(previous.get('name', 'task'))}-resume-",
        dir=previous_dir.parent,
    ))
    (run_dir / "feedback.md").write_text(feedback, encoding="utf-8")
    print(f"RUN_DIR={run_dir}", flush=True)
    state = {
        "status": "running", "name": f"{previous.get('name', 'task')}-resume",
        "model": model_id, "sandbox": sandbox, "cwd": str(cwd),
        "resumed_thread_id": thread_id, "resumed_from": str(previous_dir),
        "started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "phase": "waiting_for_lock", "pid": None,
        "exit_code": None,
    }
    reporter = ProgressReporter(
        run_dir / "status.json", state, task_id=state["name"],
    )
    reporter.start()
    command = [
        codex_binary(), "exec", "resume", "-m", model_id, "--json",
        "-o", str(run_dir / "result.md"), thread_id, "-",
    ]
    process: subprocess.Popen[str] | None = None
    lock_handle = None
    events_path = run_dir / "events.jsonl"
    try:
        lock_handle = worktree_lock(cwd, sandbox)
        with events_path.open("w", encoding="utf-8") as events:
            process = subprocess.Popen(
                command, cwd=cwd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, text=True, start_new_session=True,
            )
            with ACTIVE_LOCK:
                ACTIVE_PROCESSES.add(process)
            reporter.set_phase("model_running", process, pid=process.pid)
            print(f"DELEGATE_RESUMED pid={process.pid} model={model_id}", flush=True)
            assert process.stdin is not None and process.stdout is not None
            process.stdin.write(feedback)
            process.stdin.close()
            for line in process.stdout:
                events.write(line)
                events.flush()
                reporter.record_event(line)
            exit_code = process.wait()
            reporter.set_phase("finalizing", process)
            if exit_code == 0 and not (run_dir / "result.md").is_file():
                raise SpecError("codex resume exited successfully without producing result.md")
    except BaseException as exc:
        if process is not None:
            terminate_process(process)
        reporter.finish(
            "interrupted" if isinstance(exc, (KeyboardInterrupt, InterruptedError)) else "failed",
            process.returncode if process else None,
        )
        raise
    finally:
        if lock_handle is not None:
            lock_handle.close()
        if process is not None:
            with ACTIVE_LOCK:
                ACTIVE_PROCESSES.discard(process)
    final_status = "succeeded" if exit_code == 0 else "failed"
    reporter.finish(final_status, exit_code)
    print(f"DELEGATE_FINISHED status={final_status} exit_code={exit_code}", flush=True)
    return exit_code


def command_batch(args: argparse.Namespace) -> int:
    if args.max_workers < 1:
        raise SpecError("max-workers must be at least 1")
    if args.max_dependency_chars < 1:
        raise SpecError("max-dependency-chars must be at least 1")
    if args.stop_after_total_tokens is not None and args.stop_after_total_tokens < 1:
        raise SpecError("stop-after-total-tokens must be at least 1")
    cwd = Path(args.cwd).resolve()
    root = repository_root(cwd)
    tasks = load_manifest(Path(args.manifest).resolve())
    validate_model_budget(tasks, args.max_terra_tasks, args.max_sol_tasks)
    base = Path(args.runs_dir).expanduser().resolve()
    base.mkdir(parents=True, exist_ok=True)
    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    batch_dir = Path(tempfile.mkdtemp(prefix=f"{timestamp}-batch-", dir=base))
    task_runs_dir = batch_dir / "runs"
    task_runs_dir.mkdir()
    print(f"BATCH_DIR={batch_dir}", flush=True)
    cancel_event = threading.Event()
    ACTIVE_CANCEL_EVENTS.add(cancel_event)

    states: dict[str, dict[str, Any]] = {}
    for task in tasks:
        states[task["id"]] = {
            "id": task["id"], "status": "pending", "model": MODEL_IDS[task["model"]],
            "model_reason": task["model_reason"],
            "sandbox": task["sandbox"], "isolation": task["isolation"],
            "depends_on": task["depends_on"],
            "run_dir": None, "exit_code": None,
        }
    task_worktrees: dict[str, dict[str, Any]] = {}

    def persist(status: str = "running") -> None:
        atomic_json(batch_dir / "batch-status.json", {
            "status": status, "cwd": str(root),
            "usage": add_usage([
                state["usage"] for state in states.values() if isinstance(state.get("usage"), dict)
            ]),
            "tasks": [states[task["id"]] for task in tasks],
        })

    def task_args(task: dict[str, Any]) -> argparse.Namespace:
        dependency_results = []
        for dependency in task["depends_on"]:
            run_dir = states[dependency]["run_dir"]
            if run_dir:
                result_path = Path(run_dir) / "result.json"
                if result_path.is_file():
                    dependency_results.append((dependency, json.loads(result_path.read_text(encoding="utf-8"))))
        task_cwd = root
        if task["isolation"] == "worktree":
            metadata = create_task_worktree(root, batch_dir, task)
            task_worktrees[task["id"]] = metadata
            task_cwd = Path(metadata["path"])
            states[task["id"]].update({
                "worktree": metadata["path"], "base_ref": metadata["base_ref"],
                "base_sha": metadata["base_sha"],
            })
        return argparse.Namespace(
            cwd=str(task_cwd), spec=task["spec"], model=task["model"], sandbox=task["sandbox"],
            model_reason=task["model_reason"], task_id=task["id"],
            max_context_chars=args.max_context_chars, runs_dir=str(task_runs_dir),
            max_dependency_chars=args.max_dependency_chars,
            dependency_results=dependency_results, cancel_event=cancel_event,
        )

    persist()
    running: dict[Any, str] = {}
    budget_exhausted = False
    try:
        with BatchExecutor(args.max_workers, cancel_event) as executor:
            while True:
                changed = False
                observed_usage = add_usage([
                    state["usage"] for state in states.values() if isinstance(state.get("usage"), dict)
                ])
                if (
                    args.stop_after_total_tokens is not None and
                    observed_usage["total_tokens"] >= args.stop_after_total_tokens
                ):
                    for state in states.values():
                        if state["status"] == "pending":
                            state.update({"status": "skipped", "blocked_by": ["token budget reached"]})
                            changed = True
                            budget_exhausted = True
                for task in tasks:
                    state = states[task["id"]]
                    if state["status"] != "pending":
                        continue
                    blockers = [dep for dep in task["depends_on"] if states[dep]["status"] in ("failed", "skipped", "interrupted")]
                    if blockers:
                        state.update({"status": "skipped", "blocked_by": blockers})
                        changed = True

                free_slots = args.max_workers - len(running)
                writer_running = any(
                    states[task_id]["sandbox"] == "workspace-write" and
                    states[task_id]["isolation"] == "shared"
                    for task_id in running.values()
                )
                ready = [task for task in tasks if states[task["id"]]["status"] == "pending" and
                         all(states[dep]["status"] == "succeeded" for dep in task["depends_on"])]
                if free_slots and not writer_running:
                    parallel_ready = [
                        task for task in ready
                        if task["sandbox"] == "read-only" or task["isolation"] == "worktree"
                    ]
                    if parallel_ready:
                        for task in parallel_ready[:free_slots]:
                            try:
                                prepared_args = task_args(task)
                            except (SpecError, OSError) as exc:
                                states[task["id"]].update({"status": "failed", "error": str(exc)})
                            else:
                                states[task["id"]]["status"] = "running"
                                running[executor.submit(execute_run, prepared_args)] = task["id"]
                            changed = True
                    elif not running:
                        ready_writers = [task for task in ready if task["sandbox"] == "workspace-write"]
                        if ready_writers:
                            task = ready_writers[0]
                            prepared_args = task_args(task)
                            states[task["id"]]["status"] = "running"
                            running[executor.submit(execute_run, prepared_args)] = task["id"]
                            changed = True
                if changed:
                    persist()
                if not running:
                    if all(state["status"] not in ("pending", "running") for state in states.values()):
                        break
                    raise SpecError("batch scheduler stalled")
                completed, _ = wait(running, return_when=FIRST_COMPLETED)
                for future in completed:
                    task_id = running.pop(future)
                    state = states[task_id]
                    try:
                        exit_code, run_dir = future.result()
                        state.update({
                            "status": "succeeded" if exit_code == 0 else "failed",
                            "exit_code": exit_code, "run_dir": str(run_dir),
                        })
                        run_status = json.loads((run_dir / "status.json").read_text(encoding="utf-8"))
                        if isinstance(run_status.get("usage"), dict):
                            state["usage"] = run_status["usage"]
                    except BaseException as exc:
                        state.update({"status": "failed", "error": str(exc) or type(exc).__name__})
                    if task_id in task_worktrees:
                        try:
                            state.update(finalize_task_worktree(root, task_worktrees[task_id], state.get("run_dir")))
                        except (SpecError, OSError) as exc:
                            state.update({"status": "failed", "integration_error": str(exc)})
                persist()
    except KeyboardInterrupt:
        cancel_event.set()
        terminate_all_processes()
        for state in states.values():
            if state["status"] == "running":
                state["status"] = "interrupted"
            elif state["status"] == "pending":
                state["status"] = "skipped"
                state["blocked_by"] = ["batch interrupted"]
        persist("interrupted")
        print("BATCH_INTERRUPTED", flush=True)
        ACTIVE_CANCEL_EVENTS.discard(cancel_event)
        return 130

    final_status = (
        "budget_exhausted" if budget_exhausted else
        "succeeded" if all(state["status"] == "succeeded" for state in states.values()) else
        "failed"
    )
    persist(final_status)
    print(f"BATCH_FINISHED status={final_status}", flush=True)
    ACTIVE_CANCEL_EVENTS.discard(cancel_event)
    return 0 if final_status == "succeeded" else 3 if final_status == "budget_exhausted" else 1


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="build context and run a foreground delegate")
    run.add_argument("--spec", required=True)
    run.add_argument("--cwd", required=True)
    run.add_argument("--model", choices=sorted(MODEL_IDS), default="luna")
    run.add_argument("--model-reason")
    run.add_argument("--sandbox", choices=("read-only", "workspace-write"), default="read-only")
    run.add_argument("--max-context-chars", type=int, default=DEFAULT_MAX_CONTEXT_CHARS)
    run.add_argument("--runs-dir", default=os.path.join(tempfile.gettempdir(), "codex-delegations"))
    run.set_defaults(handler=command_run)
    prepare = commands.add_parser("prepare", help="validate and render context without calling Codex")
    prepare.add_argument("--spec", required=True)
    prepare.add_argument("--cwd", required=True)
    prepare.add_argument("--model", choices=sorted(MODEL_IDS), default="luna")
    prepare.add_argument("--model-reason")
    prepare.add_argument("--sandbox", choices=("read-only", "workspace-write"), default="read-only")
    prepare.add_argument("--max-context-chars", type=int, default=DEFAULT_MAX_CONTEXT_CHARS)
    prepare.add_argument("--output")
    prepare.set_defaults(handler=command_prepare)
    inspect = commands.add_parser("inspect", help="print persisted status and final result")
    inspect.add_argument("--run-dir", required=True)
    inspect.set_defaults(handler=command_inspect)
    resume = commands.add_parser("resume", help="resume the same delegate thread with compact feedback")
    resume.add_argument("--run-dir", required=True)
    resume.add_argument("--feedback-file", required=True)
    resume.set_defaults(handler=command_resume)
    batch = commands.add_parser("batch", help="run a dependency-aware batch of delegates")
    batch.add_argument("--manifest", required=True)
    batch.add_argument("--cwd", required=True)
    batch.add_argument("--max-workers", type=int, default=2)
    batch.add_argument("--max-context-chars", type=int, default=DEFAULT_MAX_CONTEXT_CHARS)
    batch.add_argument("--max-dependency-chars", type=int, default=DEFAULT_MAX_DEPENDENCY_CHARS)
    batch.add_argument("--max-terra-tasks", type=int, default=1)
    batch.add_argument("--max-sol-tasks", type=int, default=0)
    batch.add_argument("--stop-after-total-tokens", type=int)
    batch.add_argument("--runs-dir", default=os.path.join(tempfile.gettempdir(), "codex-delegations"))
    batch.set_defaults(handler=command_batch)
    return root


def main() -> int:
    def interrupt_handler(signum: int, frame: Any) -> None:
        for event in tuple(ACTIVE_CANCEL_EVENTS):
            event.set()
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, interrupt_handler)
    signal.signal(signal.SIGTERM, interrupt_handler)
    signal.signal(signal.SIGHUP, interrupt_handler)
    args = parser().parse_args()
    try:
        return args.handler(args)
    except (SpecError, OSError) as exc:
        print(f"delegate: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        terminate_all_processes()
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
