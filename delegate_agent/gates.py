"""Deterministic acceptance gates for worker outcomes."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import signal
import subprocess
from typing import Any

from .errors import StateError


@dataclass
class GateResult:
    gate_type: str
    status: str
    reason: str | None = None
    evidence: dict[str, Any] = field(default_factory=dict)
    artifact: str | None = None


def validate_result(result: Any, *, writer: bool = False) -> GateResult:
    if not isinstance(result, dict):
        return GateResult("result_schema", "rejected", "result must be an object")
    required = ["result", "evidence", "risks", "recommended_next_action"]
    if writer:
        required += ["changes", "verification"]
    def present(value: Any) -> bool:
        if isinstance(value, str):
            return bool(value.strip())
        if isinstance(value, (dict, list)):
            return bool(value)
        return False
    missing = [field for field in required if not present(result.get(field))]
    if missing:
        return GateResult(
            "result_schema", "rejected", f"missing non-empty fields: {', '.join(missing)}",
            {"missing": missing},
        )
    return GateResult("result_schema", "accepted", evidence={"fields": required})


def _base_environment(inherit_env: list[str]) -> dict[str, str]:
    required = {key for key in ("PATH", "HOME", "TMPDIR", "LANG") if key in os.environ}
    required.update(inherit_env)
    return {key: os.environ[key] for key in required if key in os.environ}


def _terminate(process: subprocess.Popen[str], grace_seconds: float = 2.0) -> None:
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


def run_command_check(
    check: dict[str, Any], *, cwd: str | Path, artifact_dir: str | Path,
) -> GateResult:
    argv = check.get("argv")
    if not isinstance(argv, list) or not argv or not all(isinstance(item, str) and item for item in argv):
        return GateResult("command", "rejected", "argv must be a non-empty array of strings")
    artifact_root = Path(artifact_dir)
    artifact_root.mkdir(parents=True, exist_ok=True)
    output_path = artifact_root / "verification-command.log"
    process: subprocess.Popen[str] | None = None
    try:
        process = subprocess.Popen(
            argv,
            cwd=Path(cwd),
            env=_base_environment(list(check.get("inherit_env", []))),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
        try:
            output, _ = process.communicate(timeout=int(check.get("timeout_seconds", 300)))
        except subprocess.TimeoutExpired as exc:
            _terminate(process)
            output = (exc.output or "") if isinstance(exc.output, str) else ""
            if process.stdout is not None:
                process.stdout.close()
            output_path.write_text(output, encoding="utf-8", errors="replace")
            return GateResult("command", "rejected", "verification_timeout", {"exit_code": None}, str(output_path))
    except (OSError, ValueError) as exc:
        return GateResult("command", "rejected", f"verification_error: {exc}")
    if process.stdout is not None:
        process.stdout.close()
    output_path.write_text(output, encoding="utf-8", errors="replace")
    if process.returncode != 0:
        return GateResult(
            "command", "rejected", "verification_failed", {"exit_code": process.returncode}, str(output_path),
        )
    return GateResult("command", "accepted", evidence={"exit_code": 0}, artifact=str(output_path))


def git_changed_paths(root: str | Path) -> set[str]:
    completed = subprocess.run(
        ["git", "status", "--porcelain=1", "-z"], cwd=Path(root),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
    )
    if completed.returncode != 0:
        raise StateError(completed.stderr.decode(errors="replace").strip() or "git status failed")
    raw = completed.stdout.decode(errors="replace")
    paths: set[str] = set()
    entries = raw.split("\0")
    index = 0
    while index < len(entries):
        entry = entries[index]
        index += 1
        if not entry:
            continue
        if len(entry) < 4:
            raise StateError("unexpected git status entry")
        paths.add(entry[3:])
        if entry[1] in {"R", "C"} and index < len(entries) and entries[index]:
            paths.add(entries[index])
            index += 1
    return paths


def _allowed(path: str, allowed_paths: list[str]) -> bool:
    candidate = Path(path)
    return any(candidate == Path(allowed) or Path(allowed) in candidate.parents for allowed in allowed_paths)


def check_diff_scope(
    root: str | Path, allowed_paths: list[str], *, baseline_paths: set[str] | None = None,
) -> GateResult:
    try:
        changed = git_changed_paths(root)
    except StateError as exc:
        return GateResult("diff_scope", "rejected", str(exc))
    baseline = baseline_paths or set()
    worker_changes = sorted(changed - baseline)
    violations = [path for path in worker_changes if not _allowed(path, allowed_paths)]
    if violations:
        return GateResult("diff_scope", "rejected", "scope_violation", {"paths": violations})
    return GateResult("diff_scope", "accepted", evidence={"paths": worker_changes})


def run_checks(
    checks: list[dict[str, Any]], *, result: dict[str, Any], cwd: str | Path,
    artifact_dir: str | Path, writer: bool = False, baseline_paths: set[str] | None = None,
    approved: bool = False,
) -> list[GateResult]:
    outcomes = [validate_result(result, writer=writer)]
    if outcomes[-1].status != "accepted":
        return outcomes
    for check in checks:
        kind = check.get("type")
        if kind == "result_schema":
            continue
        if kind == "command":
            gate = run_command_check(check, cwd=cwd, artifact_dir=artifact_dir)
        elif kind == "diff_scope":
            gate = check_diff_scope(cwd, list(check.get("paths", [])), baseline_paths=baseline_paths)
        elif kind == "approval":
            gate = GateResult("approval", "accepted" if approved else "rejected", None if approved else "approval_required")
        else:
            gate = GateResult(str(kind), "rejected", f"unsupported gate: {kind}")
        outcomes.append(gate)
        if gate.status != "accepted":
            break
    return outcomes
