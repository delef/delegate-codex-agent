"""Stable, payload-free workflow status projection."""

from __future__ import annotations

from collections import Counter
import json
from typing import Any

from .store import JournalStateStore


def build_status(store: JournalStateStore) -> dict[str, Any]:
    snapshot = store.snapshot
    tasks = snapshot["tasks"]
    counts = Counter(task["state"] for task in tasks.values())
    ready = sorted(task_id for task_id, task in tasks.items() if task["state"] == "ready")
    approval_pending = sorted(
        task_id for task_id, task in tasks.items()
        if task["state"] == "ready" and task.get("kind") == "approval"
    )
    blockers = [
        {"task_id": task_id, "reason": task.get("blocked_reason", "blocked")}
        for task_id, task in sorted(tasks.items()) if task["state"] == "blocked"
    ]
    blockers.extend({"task_id": task_id, "reason": "approval_required"} for task_id in approval_pending)
    active = []
    for task_id, task in sorted(tasks.items()):
        if task["state"] in {"running", "verifying", "retry_wait"}:
            active.append({
                "id": task_id,
                "state": task["state"],
                "attempt": task.get("attempt", 0),
                "model": task.get("model"),
                "pid": task.get("pid"),
            })
    budget = snapshot["budget"]
    return {
        "schema_version": 1,
        "workflow_id": snapshot["workflow_id"],
        "name": snapshot["name"],
        "state": snapshot["state"],
        "last_event_seq": snapshot["last_event_seq"],
        "counts": dict(sorted(counts.items())),
        "active": active,
        "current": active[0] if active else None,
        "pending": counts.get("pending", 0),
        "blocked": counts.get("blocked", 0),
        "approval_pending": approval_pending,
        "blockers": blockers,
        "retry_count": sum(int(task.get("retry_count", 0)) for task in tasks.values()),
        "next": ready[0] if ready else None,
        "budget": {
            "used_tokens": budget["used_tokens"],
            "reserved_tokens": budget["reserved_tokens"],
            "limit_tokens": budget["limit_tokens"],
            "remaining_tokens": budget["limit_tokens"] - budget["used_tokens"] - budget["reserved_tokens"],
        },
    }


def status_json(store: JournalStateStore) -> str:
    return json.dumps(build_status(store), ensure_ascii=False, indent=2) + "\n"
