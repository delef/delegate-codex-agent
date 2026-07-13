"""Stateful readiness and acceptance scheduling primitives."""

from __future__ import annotations

from typing import Any

from .errors import StateError
from .models import TaskState
from .store import JournalStateStore


_UNSET = object()


class DurableScheduler:
    def __init__(self, store: JournalStateStore):
        self.store = store

    def refresh_ready(self) -> list[str]:
        snapshot = self.store.snapshot
        accepted = {
            task_id for task_id, task in snapshot["tasks"].items()
            if task["state"] == TaskState.ACCEPTED.value
        }
        became_ready: list[str] = []
        for task_id, task in snapshot["tasks"].items():
            if task["state"] != TaskState.PENDING.value:
                continue
            dependencies = set(task.get("depends_on", []))
            if dependencies <= accepted:
                self.store.transition_task(task_id, TaskState.READY.value, expected_state=TaskState.PENDING.value)
                became_ready.append(task_id)
            elif task.get("allow_partial"):
                terminal = accepted | {
                    TaskState.REJECTED.value, TaskState.BLOCKED.value,
                    TaskState.CANCELLED.value, TaskState.FAILED.value,
                    TaskState.BUDGET_EXHAUSTED.value, TaskState.INTERRUPTED.value,
                }
                if dependencies and dependencies <= set(
                    dependency_id for dependency_id, dependency in snapshot["tasks"].items()
                    if dependency["state"] in terminal
                ) and snapshot["tasks"].get(task.get("node", {}).get("source"), {}).get("state") == TaskState.ACCEPTED.value:
                    self.store.transition_task(task_id, TaskState.READY.value, expected_state=TaskState.PENDING.value)
                    became_ready.append(task_id)
            elif any(snapshot["tasks"][dep]["state"] in {
                TaskState.REJECTED.value, TaskState.BLOCKED.value, TaskState.CANCELLED.value,
                TaskState.FAILED.value, TaskState.BUDGET_EXHAUSTED.value,
                TaskState.INTERRUPTED.value,
            } for dep in dependencies):
                self.store.transition_task(task_id, TaskState.BLOCKED.value, expected_state=TaskState.PENDING.value)
        return became_ready

    def start(self, task_id: str, *, reserve_tokens: int) -> None:
        attempt = self.store.snapshot["tasks"][task_id]["attempt"] + 1
        reservation_key = f"reserve:{task_id}:{attempt}"
        self.store.reserve_budget(reserve_tokens, task_id=task_id, idempotency_key=reservation_key)
        try:
            self.store.transition_task(
                task_id, TaskState.RUNNING.value, expected_state=TaskState.READY.value,
                fields={"reserved_tokens": reserve_tokens, "attempt": attempt},
            )
        except BaseException:
            self.store.release_budget(reserve_tokens, task_id=task_id, idempotency_key=f"rollback:{reservation_key}")
            raise

    def complete(self, task_id: str) -> None:
        self.store.transition_task(task_id, TaskState.COMPLETED.value, expected_state=TaskState.RUNNING.value)
        self.store.transition_task(task_id, TaskState.VERIFYING.value, expected_state=TaskState.COMPLETED.value)

    def accept(self, task_id: str, *, fields: dict[str, Any] | None = None) -> None:
        self.store.transition_task(
            task_id, TaskState.ACCEPTED.value, expected_state=TaskState.VERIFYING.value,
            fields=fields,
        )
        self._release_task_reservation(task_id)

    def accept_cached(self, task_id: str, *, fields: dict[str, Any] | None = None) -> None:
        """Accept a previously verified read-only result without reserving tokens."""
        task = self.store.snapshot["tasks"].get(task_id)
        if task is None:
            raise StateError(f"unknown task: {task_id}")
        if task["state"] != TaskState.READY.value:
            raise StateError(f"task {task_id} expected ready, found {task['state']}")
        cache_fields = {"cache_hit": True, "attempt": task.get("attempt", 0) + 1}
        if fields:
            cache_fields.update(fields)
        self.store.transition_task(
            task_id, TaskState.RUNNING.value, expected_state=TaskState.READY.value,
            fields=cache_fields,
        )
        self.store.transition_task(task_id, TaskState.COMPLETED.value, expected_state=TaskState.RUNNING.value)
        self.store.transition_task(task_id, TaskState.VERIFYING.value, expected_state=TaskState.COMPLETED.value)
        self.store.transition_task(task_id, TaskState.ACCEPTED.value, expected_state=TaskState.VERIFYING.value)

    def accept_computed(self, task_id: str, *, fields: dict[str, Any] | None = None) -> None:
        """Accept a deterministic non-agent node without a token reservation."""
        task = self.store.snapshot["tasks"].get(task_id)
        if task is None:
            raise StateError(f"unknown task: {task_id}")
        if task["state"] == TaskState.READY.value:
            computed_fields = {"attempt": task.get("attempt", 0) + 1}
            if fields:
                computed_fields.update(fields)
            self.store.transition_task(
                task_id, TaskState.RUNNING.value, expected_state=TaskState.READY.value,
                fields=computed_fields,
            )
        elif task["state"] != TaskState.RUNNING.value:
            raise StateError(f"task {task_id} expected ready or running, found {task['state']}")
        self.store.transition_task(task_id, TaskState.COMPLETED.value, expected_state=TaskState.RUNNING.value)
        self.store.transition_task(task_id, TaskState.VERIFYING.value, expected_state=TaskState.COMPLETED.value)
        self.store.transition_task(task_id, TaskState.ACCEPTED.value, expected_state=TaskState.VERIFYING.value)

    def block(self, task_id: str, *, reason: str) -> None:
        task = self.store.snapshot["tasks"].get(task_id)
        if task is None:
            raise StateError(f"unknown task: {task_id}")
        if task["state"] not in {TaskState.PENDING.value, TaskState.READY.value}:
            return
        self.store.transition_task(
            task_id, TaskState.BLOCKED.value, expected_state=task["state"],
            fields={"blocked_reason": reason},
        )

    def reject(self, task_id: str, *, reason: str, failure: dict[str, Any] | None = None) -> None:
        fields = {"rejection_reason": reason}
        if failure:
            fields.update(failure)
        self.store.transition_task(
            task_id, TaskState.REJECTED.value, expected_state=TaskState.VERIFYING.value,
            fields=fields,
        )
        self._release_task_reservation(task_id)

    def fail(self, task_id: str, *, reason: str, failure: dict[str, Any] | None = None) -> None:
        fields = {"failure_reason": reason}
        if failure:
            fields.update(failure)
        self.store.transition_task(
            task_id, TaskState.FAILED.value, expected_state=TaskState.RUNNING.value,
            fields=fields,
        )
        self._release_task_reservation(task_id)

    def budget_exhausted(self, task_id: str, *, observed_tokens: int) -> None:
        self.store.transition_task(
            task_id, TaskState.BUDGET_EXHAUSTED.value, expected_state=TaskState.RUNNING.value,
            fields={
                "observed_tokens": observed_tokens,
                "failure_class": "budget",
                "failure_reason": "budget_exhausted",
            },
        )
        self._release_task_reservation(task_id)

    def maybe_retry(
        self, task_id: str, *, failure_class: str, fingerprint: str,
        evidence: dict[str, Any], allowed_classes: list[str] | tuple[str, ...],
        previous_fingerprint: str | None | object = _UNSET,
    ) -> bool:
        """Move one failed attempt back to ready only with new evidence."""
        task = self.store.snapshot["tasks"].get(task_id)
        if task is None:
            raise StateError(f"unknown task: {task_id}")
        if failure_class not in allowed_classes:
            return False
        if task["attempt"] >= task.get("max_attempts", 1):
            return False
        prior = task.get("failure_fingerprint") if previous_fingerprint is _UNSET else previous_fingerprint
        if prior == fingerprint:
            return False
        current = task["state"]
        if current not in {
            TaskState.REJECTED.value, TaskState.FAILED.value,
            TaskState.INTERRUPTED.value,
        }:
            return False
        retry_count = int(task.get("retry_count", 0)) + 1
        fields = {
            "failure_class": failure_class,
            "failure_fingerprint": fingerprint,
            "failure_evidence": evidence,
            "retry_count": retry_count,
        }
        self.store.transition_task(
            task_id, TaskState.RETRY_WAIT.value, expected_state=current, fields=fields,
        )
        self.store.transition_task(
            task_id, TaskState.READY.value, expected_state=TaskState.RETRY_WAIT.value,
            fields={"retry_reason": f"retry:{failure_class}"},
        )
        return True

    def manual_retry(self, task_id: str, *, reason: str = "operator_retry") -> None:
        """Explicitly requeue a terminal failed attempt without auto-escalation."""
        task = self.store.snapshot["tasks"].get(task_id)
        if task is None:
            raise StateError(f"unknown task: {task_id}")
        if task["state"] not in {
            TaskState.REJECTED.value, TaskState.FAILED.value,
            TaskState.INTERRUPTED.value, TaskState.BUDGET_EXHAUSTED.value,
        }:
            raise StateError(f"task {task_id} is not retryable from {task['state']}")
        if task["attempt"] >= task.get("max_attempts", 1):
            raise StateError(f"task {task_id} reached max_attempts")
        self.store.transition_task(
            task_id, TaskState.RETRY_WAIT.value, expected_state=task["state"],
            fields={"manual_retry_reason": reason, "retry_count": task.get("retry_count", 0) + 1},
        )
        self.store.transition_task(task_id, TaskState.READY.value, expected_state=TaskState.RETRY_WAIT.value)

    def cancel(self, task_id: str, *, reason: str = "operator_cancelled") -> None:
        """Cancel a non-terminal task and release any attempt reservation."""
        task = self.store.snapshot["tasks"].get(task_id)
        if task is None:
            raise StateError(f"unknown task: {task_id}")
        current = task["state"]
        if current in {
            TaskState.ACCEPTED.value, TaskState.REJECTED.value,
            TaskState.BLOCKED.value, TaskState.CANCELLED.value,
        }:
            return
        if current not in {
            TaskState.PENDING.value, TaskState.READY.value,
            TaskState.RUNNING.value, TaskState.RETRY_WAIT.value,
            TaskState.INTERRUPTED.value, TaskState.FAILED.value,
            TaskState.BUDGET_EXHAUSTED.value,
        }:
            raise StateError(f"cannot cancel task {task_id} in state {current}")
        self.store.transition_task(
            task_id, TaskState.CANCELLED.value, expected_state=current,
            fields={"cancellation_reason": reason},
        )
        self._release_task_reservation(task_id)

    def _release_task_reservation(self, task_id: str) -> None:
        reserved = self.store.snapshot["tasks"][task_id]["reserved_tokens"]
        if reserved:
            attempt = self.store.snapshot["tasks"][task_id]["attempt"]
            self.store.release_budget(
                reserved, task_id=task_id,
                idempotency_key=f"release:{task_id}:{attempt}:{reserved}",
            )
