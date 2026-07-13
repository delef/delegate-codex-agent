"""Auditable artifact manifests for workflow attempts."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Any, Iterable

from .errors import StateError


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    try:
        with Path(path).open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise StateError(f"cannot hash artifact {path}: {exc}") from exc
    return digest.hexdigest()


def build_manifest(root: str | Path, paths: Iterable[str | Path]) -> dict[str, Any]:
    root_path = Path(root).expanduser().resolve()
    artifacts: list[dict[str, Any]] = []
    seen: set[Path] = set()
    for raw_path in paths:
        path = Path(raw_path).expanduser().resolve()
        if path in seen:
            continue
        seen.add(path)
        try:
            relative = path.relative_to(root_path)
        except ValueError as exc:
            raise StateError(f"artifact escapes root: {path}") from exc
        if not path.is_file():
            raise StateError(f"artifact is not a regular file: {path}")
        artifacts.append({
            "path": str(relative),
            "size": path.stat().st_size,
            "sha256": sha256_file(path),
        })
    return {"schema_version": 1, "artifacts": sorted(artifacts, key=lambda item: item["path"])}


def write_manifest(root: str | Path, paths: Iterable[str | Path], destination: str | Path | None = None) -> Path:
    root_path = Path(root).expanduser().resolve()
    target = Path(destination) if destination is not None else root_path / "artifact_manifest.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    value = build_manifest(root_path, paths)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{target.name}.", dir=target.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return target
