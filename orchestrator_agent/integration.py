"""Read-only capture and planning for writer worktree integration."""

from __future__ import annotations

from dataclasses import dataclass
import json
import hashlib
import os
from pathlib import Path
import subprocess
import tempfile
from typing import Any

from .artifacts import sha256_file
from .errors import StateError


def _git(cwd: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args], cwd=cwd, text=False,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
    )
    if completed.returncode != 0:
        message = completed.stderr.decode(errors="replace").strip() or f"git {' '.join(args)} failed"
        raise StateError(message)
    return completed.stdout.decode(errors="replace")


def _safe_relative(root: Path, raw: str) -> Path:
    relative = Path(raw)
    if relative.is_absolute() or ".." in relative.parts or ".git" in relative.parts:
        raise StateError(f"unsupported path: {raw}")
    path = (root / relative).resolve(strict=False)
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise StateError(f"path escapes worktree: {raw}") from exc
    if path.is_symlink() or (root / relative).is_symlink():
        raise StateError(f"symlink changes are unsupported: {raw}")
    return relative


def _status_changes(root: Path) -> dict[str, str]:
    raw = _git(root, "status", "--porcelain=1", "-z")
    tokens = raw.split("\0")
    changes: dict[str, str] = {}
    index = 0
    while index < len(tokens):
        entry = tokens[index]
        index += 1
        if not entry:
            continue
        if len(entry) < 3:
            raise StateError("malformed git status entry")
        status = entry[:2]
        path = entry[3:]
        relative = _safe_relative(root, path)
        if status[0] in {"R", "C"}:
            if index >= len(tokens) or not tokens[index]:
                raise StateError("malformed git rename status entry")
            source = _safe_relative(root, tokens[index])
            index += 1
            changes[str(source)] = "renamed_from"
            changes[str(relative)] = "renamed"
            continue
        if status == "??":
            changes[str(relative)] = "untracked"
        elif "D" in status:
            changes[str(relative)] = "deleted"
        elif "A" in status:
            changes[str(relative)] = "added"
        else:
            changes[str(relative)] = "modified"
    return changes


def _diff_changes(root: Path, base_ref: str) -> dict[str, str]:
    raw = _git(root, "diff", "--name-status", "--find-renames", "-z", base_ref, "--")
    tokens = raw.split("\0")
    changes: dict[str, str] = {}
    index = 0
    while index < len(tokens):
        status = tokens[index]
        index += 1
        if not status:
            continue
        if index >= len(tokens):
            raise StateError("malformed git diff entry")
        first = _safe_relative(root, tokens[index])
        index += 1
        code = status[0]
        if code in {"R", "C"}:
            if index >= len(tokens):
                raise StateError("malformed git rename entry")
            second = _safe_relative(root, tokens[index])
            index += 1
            changes[str(first)] = "renamed_from"
            changes[str(second)] = "renamed"
        else:
            changes[str(first)] = {
                "A": "added", "D": "deleted", "M": "modified",
            }.get(code, "modified")
    return changes


def capture_writer_changes(worktree: str | Path, *, base_ref: str = "HEAD") -> dict[str, Any]:
    root = Path(worktree).expanduser().resolve()
    if not root.is_dir():
        raise StateError(f"worktree does not exist: {root}")
    changes = _diff_changes(root, base_ref)
    changes.update(_status_changes(root))
    files: list[dict[str, Any]] = []
    for relative, change_type in sorted(changes.items()):
        path = root / relative
        item: dict[str, Any] = {"path": relative, "status": change_type}
        if path.exists() and path.is_file():
            item.update({"size": path.stat().st_size, "sha256": sha256_file(path)})
        elif not path.exists() and change_type not in {"deleted", "renamed_from"}:
            raise StateError(f"changed path disappeared: {relative}")
        files.append(item)
    return {
        "schema_version": 1,
        "worktree": str(root),
        "base_ref": base_ref,
        "base_head": _git(root, "rev-parse", base_ref).strip(),
        "head": _git(root, "rev-parse", "HEAD").strip(),
        "files": files,
    }


def _depends_on(writer: str, target: str, dependencies: dict[str, set[str]], seen: set[str] | None = None) -> bool:
    seen = seen or set()
    if writer in seen:
        return False
    seen.add(writer)
    if target in dependencies.get(writer, set()):
        return True
    return any(_depends_on(parent, target, dependencies, seen) for parent in dependencies.get(writer, set()))


def _normalize_integration_checks(checks: Any) -> list[dict[str, Any]]:
    if checks is None:
        return []
    if not isinstance(checks, list):
        raise StateError("integration checks must be an array")
    normalized: list[dict[str, Any]] = []
    for index, check in enumerate(checks):
        if not isinstance(check, dict):
            raise StateError(f"integration check {index} must be an object")
        argv = check.get("argv")
        if not isinstance(argv, list) or not argv or not all(isinstance(item, str) and item for item in argv):
            raise StateError(f"integration check {index}.argv must be a non-empty array of strings")
        timeout = check.get("timeout_seconds", 300)
        if isinstance(timeout, bool) or not isinstance(timeout, int) or timeout <= 0 or timeout > 86_400:
            raise StateError(f"integration check {index}.timeout_seconds is invalid")
        normalized.append({"argv": list(argv), "timeout_seconds": timeout})
    return normalized


def build_integration_plan(
    repo: str | Path, writers: list[dict[str, Any]], *, checks: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    root = Path(repo).expanduser().resolve()
    current_head = _git(root, "rev-parse", "HEAD").strip()
    if not isinstance(writers, list) or not writers:
        raise StateError("integration requires at least one writer")
    ids = [item.get("id") for item in writers]
    if any(not isinstance(item, str) or not item for item in ids) or len(set(ids)) != len(ids):
        raise StateError("writer ids must be unique non-empty strings")
    dependencies = {
        item["id"]: set(item.get("depends_on", []))
        for item in writers
    }
    unknown = sorted({dep for values in dependencies.values() for dep in values} - set(ids))
    if unknown:
        raise StateError(f"integration has unknown writer dependencies: {', '.join(unknown)}")
    captured: list[dict[str, Any]] = []
    path_owners: dict[str, list[str]] = {}
    for writer in writers:
        worktree = writer.get("worktree")
        if not isinstance(worktree, str) or not worktree:
            raise StateError(f"writer {writer['id']} requires worktree")
        changes = capture_writer_changes(
            worktree, base_ref=str(writer.get("base_ref", "HEAD")),
        )
        allowed = [str(path) for path in writer.get("scope", [])]
        violations = [
            item["path"] for item in changes["files"]
            if allowed and not any(item["path"] == path or item["path"].startswith(path.rstrip("/") + "/") for path in allowed)
        ]
        expected_base = writer.get("base_sha")
        base_mismatch = bool(expected_base and expected_base != changes["base_head"])
        item = {
            "id": writer["id"], "depends_on": sorted(dependencies[writer["id"]]),
            "worktree": changes["worktree"], "base_head": changes["base_head"],
            "head": changes["head"], "files": changes["files"],
            "scope_violations": sorted(violations), "base_mismatch": base_mismatch,
        }
        if writer.get("patch") is not None:
            item["patch"] = str(writer["patch"])
        captured.append(item)
        for changed in changes["files"]:
            path_owners.setdefault(changed["path"], []).append(writer["id"])
    conflicts: list[dict[str, Any]] = []
    for path, owners in sorted(path_owners.items()):
        if len(owners) < 2:
            continue
        for left_index, left in enumerate(owners):
            for right in owners[left_index + 1:]:
                if not _depends_on(left, right, dependencies) and not _depends_on(right, left, dependencies):
                    conflicts.append({"path": path, "writers": sorted([left, right])})
    order = _topological_order(ids, dependencies)
    normalized_checks = _normalize_integration_checks(checks)
    errors = [
        {"writer": item["id"], "type": "scope_violation", "paths": item["scope_violations"]}
        for item in captured if item["scope_violations"]
    ]
    errors.extend(
        {"writer": item["id"], "type": "base_mismatch"}
        for item in captured if item["base_mismatch"]
    )
    return {
        "schema_version": 1, "repo": str(root), "repo_head": current_head,
        "order": order, "writers": captured, "conflicts": conflicts,
        "errors": errors, "ready": not conflicts and not errors,
        "checks": normalized_checks, "mutated": False,
    }


def _topological_order(ids: list[str], dependencies: dict[str, set[str]]) -> list[str]:
    remaining = {writer: set(dependencies.get(writer, set())) for writer in ids}
    order: list[str] = []
    while remaining:
        ready = sorted(writer for writer, deps in remaining.items() if not deps)
        if not ready:
            raise StateError("integration writer dependencies contain a cycle")
        order.extend(ready)
        for writer in ready:
            remaining.pop(writer)
        for deps in remaining.values():
            deps.difference_update(ready)
    return order


def write_integration_plan(plan: dict[str, Any], destination: str | Path) -> Path:
    target = Path(destination).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{target.name}.", dir=target.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(plan, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return target


def plan_digest(plan: dict[str, Any]) -> str:
    payload = {key: value for key, value in plan.items() if key not in {"mutated", "applied"}}
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def apply_integration_plan(
    plan: dict[str, Any], *, approval: str | Path | dict[str, Any],
) -> dict[str, Any]:
    if not plan.get("ready"):
        raise StateError("integration plan is not ready")
    expected_digest = plan_digest(plan)
    approval_value: Any = approval
    if isinstance(approval, (str, Path)):
        candidate = Path(approval)
        if candidate.is_file():
            try:
                approval_value = json.loads(candidate.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise StateError(f"cannot read integration approval: {exc}") from exc
    if isinstance(approval_value, dict):
        if approval_value.get("approved") is not True or approval_value.get("plan_sha256") != expected_digest:
            raise StateError("integration approval does not match plan")
    elif approval_value != expected_digest:
        raise StateError("integration approval does not match plan")
    repo = Path(plan["repo"]).expanduser().resolve()
    current_head = _git(repo, "rev-parse", "HEAD").strip()
    if current_head != plan.get("repo_head"):
        raise StateError("repository HEAD changed since integration plan")
    applied: list[str] = []
    for writer in plan["order"]:
        entry = next(item for item in plan["writers"] if item["id"] == writer)
        patch = entry.get("patch")
        if not isinstance(patch, str) or not patch:
            raise StateError(f"writer {writer} has no patch artifact")
        patch_path = Path(patch).expanduser().resolve()
        if not patch_path.is_file():
            raise StateError(f"writer patch is missing: {patch_path}")
        completed = subprocess.run(
            ["git", "apply", "--binary", str(patch_path)], cwd=repo,
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
        )
        if completed.returncode != 0:
            raise StateError(completed.stderr.strip() or f"cannot apply writer patch: {writer}")
        applied.append(writer)
    verification: list[dict[str, Any]] = []
    for check in plan.get("checks", []):
        argv = check.get("argv") if isinstance(check, dict) else None
        timeout = check.get("timeout_seconds", 300) if isinstance(check, dict) else 300
        if not isinstance(argv, list) or not argv or not all(isinstance(item, str) and item for item in argv):
            raise StateError("integration plan contains an invalid verification command")
        try:
            completed = subprocess.run(
                argv, cwd=repo, text=True, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, timeout=int(timeout), check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise StateError(f"integration verification timed out: {' '.join(argv)}") from exc
        verification.append({"argv": argv, "exit_code": completed.returncode})
        if completed.returncode != 0:
            raise StateError(
                f"integration verification failed ({completed.returncode}): {' '.join(argv)}"
            )
    return {
        "schema_version": 1, "repo": str(repo), "applied": applied,
        "verification": verification, "mutated": bool(applied),
    }
