"""Filesystem cache for accepted read-only task results."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Any

from .errors import StateError


def _canonical(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _canonical(value[key]) for key in sorted(value, key=str)}
    if isinstance(value, list):
        return [_canonical(item) for item in value]
    return value


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _atomic_write(path: Path, value: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _git_head(cwd: Path) -> str:
    import subprocess
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=cwd, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, check=False,
        )
    except OSError:
        return "no-git"
    return completed.stdout.strip() if completed.returncode == 0 else "no-git"


def _agents_hashes(cwd: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    current = cwd.resolve()
    for directory in (current, *current.parents):
        path = directory / "AGENTS.md"
        if path.is_file():
            try:
                result[str(path)] = _sha256_file(path)
            except OSError as exc:
                raise StateError(f"cannot hash {path}: {exc}") from exc
    return result


def task_fingerprint(
    node: dict[str, Any], *, cwd: str | Path,
    dependency_results: dict[str, dict[str, Any]] | None = None,
) -> str:
    """Build a stable cache key from all read-only task inputs."""
    spec_path = Path(node["spec"]).resolve()
    try:
        spec_hash = _sha256_file(spec_path)
    except OSError as exc:
        raise StateError(f"cannot hash task spec {spec_path}: {exc}") from exc
    dependencies = dependency_results or {}
    payload = {
        "schema_version": 1,
        "node": {
            key: node.get(key)
            for key in (
                "kind", "spec", "model", "model_id", "sandbox", "isolation", "depends_on", "checks",
                "map_item", "map_key", "map_parent",
            )
        },
        "spec_sha256": spec_hash,
        "model_id": node.get("model_id"),
        "base_commit": _git_head(Path(cwd)),
        "agents": _agents_hashes(Path(cwd)),
        "dependency_results": {
            key: _sha256_bytes(json.dumps(_canonical(dependencies[key]), ensure_ascii=False, sort_keys=True).encode("utf-8"))
            for key in sorted(dependencies)
        },
    }
    encoded = json.dumps(_canonical(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return _sha256_bytes(encoded)


@dataclass(frozen=True)
class CacheEntry:
    fingerprint: str
    result: dict[str, Any]
    source_workflow: str
    source_task: str
    result_sha256: str


class ResultCache:
    def __init__(self, root: str | Path):
        self.root = Path(root).expanduser().resolve()

    def _metadata_path(self, fingerprint: str) -> Path:
        return self.root / f"{fingerprint}.json"

    def _result_path(self, fingerprint: str) -> Path:
        return self.root / f"{fingerprint}.result.json"

    def get(self, fingerprint: str) -> CacheEntry | None:
        metadata_path = self._metadata_path(fingerprint)
        result_path = self._result_path(fingerprint)
        if not metadata_path.is_file() or not result_path.is_file():
            return None
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            result = json.loads(result_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        if not isinstance(metadata, dict) or not isinstance(result, dict):
            return None
        if metadata.get("fingerprint") != fingerprint:
            return None
        try:
            digest = _sha256_file(result_path)
        except OSError:
            return None
        if digest != metadata.get("result_sha256"):
            return None
        return CacheEntry(
            fingerprint=fingerprint, result=result,
            source_workflow=str(metadata.get("source_workflow", "")),
            source_task=str(metadata.get("source_task", "")),
            result_sha256=digest,
        )

    def put(
        self, fingerprint: str, *, result: dict[str, Any], source_workflow: str,
        source_task: str, sandbox: str,
    ) -> CacheEntry:
        if sandbox != "read-only":
            raise StateError("only read-only task results may enter the cache")
        if not isinstance(result, dict):
            raise StateError("cached result must be an object")
        result_bytes = json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8") + b"\n"
        result_path = self._result_path(fingerprint)
        _atomic_write(result_path, result_bytes)
        digest = _sha256_bytes(result_bytes)
        metadata = {
            "schema_version": 1, "fingerprint": fingerprint,
            "source_workflow": source_workflow, "source_task": source_task,
            "result_sha256": digest,
        }
        _atomic_write(
            self._metadata_path(fingerprint),
            (json.dumps(metadata, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8"),
        )
        return CacheEntry(fingerprint, result, source_workflow, source_task, digest)
