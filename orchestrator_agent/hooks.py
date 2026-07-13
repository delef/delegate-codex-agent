"""Bounded, non-model lifecycle hooks."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import signal
import subprocess
from typing import Any

from .errors import StateError


HOOK_EVENTS = frozenset({
    "task_created", "task_completed", "task_rejected", "worker_idle",
    "budget_threshold", "before_integration", "after_integration",
})


def _environment(inherit_env: list[str]) -> dict[str, str]:
    keys = {key for key in ("PATH", "HOME", "TMPDIR", "LANG") if key in os.environ}
    keys.update(inherit_env)
    return {key: os.environ[key] for key in keys if key in os.environ}


def _terminate(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
        process.wait(timeout=2)
    except (ProcessLookupError, subprocess.TimeoutExpired):
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    for stream in (process.stdin, process.stdout, process.stderr):
        if stream is not None:
            stream.close()


@dataclass(frozen=True)
class HookResult:
    allowed: bool
    event: str
    exit_code: int | None
    reason: str
    output: str = ""


def run_hook(
    hook: dict[str, Any], *, event: str, payload: dict[str, Any], cwd: str | Path,
) -> HookResult:
    argv = hook.get("argv")
    if not isinstance(argv, list) or not argv or not all(isinstance(item, str) and item for item in argv):
        return HookResult(False, event, None, "hook_argv_invalid")
    timeout = int(hook.get("timeout_seconds", 30))
    output_limit = int(hook.get("output_limit", 64_000))
    policy = hook.get("failure_policy", "fail_closed")
    process: subprocess.Popen[str] | None = None
    try:
        process = subprocess.Popen(
            argv, cwd=Path(cwd), env=_environment(list(hook.get("inherit_env", []))),
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, start_new_session=True,
        )
        output, _ = process.communicate(
            json.dumps({"schema_version": 1, "event": event, "payload": payload}, ensure_ascii=False),
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        if process is not None:
            _terminate(process)
        if policy == "fail_open":
            return HookResult(True, event, None, "hook_timeout_fail_open")
        return HookResult(False, event, None, "hook_timeout")
    except (OSError, ValueError) as exc:
        if policy == "fail_open":
            return HookResult(True, event, None, f"hook_error_fail_open: {exc}")
        return HookResult(False, event, None, f"hook_error: {exc}")
    output = output[:output_limit]
    if process.returncode == 0:
        return HookResult(True, event, process.returncode, "allowed", output)
    if process.returncode == 2:
        return HookResult(False, event, process.returncode, "hook_blocked", output)
    if policy == "fail_open":
        return HookResult(True, event, process.returncode, "hook_failure_fail_open", output)
    return HookResult(False, event, process.returncode, "hook_failure", output)


def run_hooks(
    hooks: dict[str, list[dict[str, Any]]], event: str, payload: dict[str, Any], *, cwd: str | Path,
) -> HookResult:
    if event not in HOOK_EVENTS:
        raise StateError(f"unsupported hook event: {event}")
    for hook in hooks.get(event, []):
        result = run_hook(hook, event=event, payload=payload, cwd=cwd)
        if not result.allowed:
            return result
    return HookResult(True, event, 0, "allowed")
