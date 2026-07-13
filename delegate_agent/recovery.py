"""Restart reconciliation for workers owned by a workflow."""

from __future__ import annotations

import os
from typing import Callable

from .models import TaskState, WorkflowState
from .store import JournalStateStore


def process_alive(pid: int | None) -> bool:
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except (OSError, ProcessLookupError):
        return False
    return True


def reconcile(
    store: JournalStateStore, *, is_alive: Callable[[int | None], bool] = process_alive,
) -> list[str]:
    """Mark missing workers interrupted and release their reservations.

    The scheduler remains the only journal writer. A live PID is left alone;
    callers can invoke reconciliation again after deciding whether its identity
    matches the recorded process-start token.
    """
    interrupted: list[str] = []
    snapshot = store.snapshot
    for task_id, task in snapshot["tasks"].items():
        if task["state"] != TaskState.RUNNING.value:
            continue
        if is_alive(task.get("pid")):
            continue
        store.transition_task(
            task_id, TaskState.INTERRUPTED.value, expected_state=TaskState.RUNNING.value,
            fields={"recovery_reason": "worker_missing"},
        )
        reserved = store.snapshot["tasks"][task_id]["reserved_tokens"]
        if reserved:
            store.release_budget(
                reserved, task_id=task_id,
                idempotency_key=f"recovery-release:{task_id}:{task.get('attempt', 0)}",
            )
        interrupted.append(task_id)
    current = store.snapshot["state"]
    if current == WorkflowState.RUNNING.value and interrupted:
        store.transition_workflow(WorkflowState.INTERRUPTED.value, expected_state=WorkflowState.RUNNING.value)
    return interrupted
