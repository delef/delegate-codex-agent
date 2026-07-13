"""Value objects and state transition rules for workflows."""

from __future__ import annotations

from enum import Enum


class TaskState(str, Enum):
    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    COMPLETED = "completed"
    VERIFYING = "verifying"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    BLOCKED = "blocked"
    RETRY_WAIT = "retry_wait"
    PAUSED = "paused"
    CANCELLED = "cancelled"
    INTERRUPTED = "interrupted"
    FAILED = "failed"
    BUDGET_EXHAUSTED = "budget_exhausted"


class WorkflowState(str, Enum):
    CREATED = "created"
    RUNNING = "running"
    PAUSED = "paused"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    BUDGET_EXHAUSTED = "budget_exhausted"
    CANCELLED = "cancelled"
    INTERRUPTED = "interrupted"


# These are intentionally explicit. A new state must be added to this table
# before code can transition into it.
TASK_TRANSITIONS: dict[TaskState, frozenset[TaskState]] = {
    TaskState.PENDING: frozenset({TaskState.READY, TaskState.BLOCKED, TaskState.CANCELLED}),
    TaskState.READY: frozenset({TaskState.RUNNING, TaskState.BLOCKED, TaskState.CANCELLED}),
    TaskState.RUNNING: frozenset({
        TaskState.COMPLETED, TaskState.FAILED, TaskState.INTERRUPTED,
        TaskState.CANCELLED, TaskState.BUDGET_EXHAUSTED,
    }),
    TaskState.COMPLETED: frozenset({TaskState.VERIFYING}),
    TaskState.VERIFYING: frozenset({TaskState.ACCEPTED, TaskState.REJECTED, TaskState.RETRY_WAIT}),
    TaskState.RETRY_WAIT: frozenset({TaskState.READY, TaskState.CANCELLED, TaskState.BLOCKED}),
    TaskState.ACCEPTED: frozenset(),
    TaskState.REJECTED: frozenset({TaskState.RETRY_WAIT}),
    TaskState.BLOCKED: frozenset(),
    TaskState.PAUSED: frozenset({TaskState.READY, TaskState.CANCELLED}),
    TaskState.CANCELLED: frozenset(),
    TaskState.INTERRUPTED: frozenset({TaskState.RETRY_WAIT, TaskState.CANCELLED}),
    TaskState.FAILED: frozenset({TaskState.RETRY_WAIT}),
    TaskState.BUDGET_EXHAUSTED: frozenset({TaskState.RETRY_WAIT, TaskState.CANCELLED}),
}

WORKFLOW_TRANSITIONS: dict[WorkflowState, frozenset[WorkflowState]] = {
    WorkflowState.CREATED: frozenset({WorkflowState.RUNNING, WorkflowState.CANCELLED}),
    WorkflowState.RUNNING: frozenset({
        WorkflowState.PAUSED, WorkflowState.SUCCEEDED, WorkflowState.FAILED,
        WorkflowState.BUDGET_EXHAUSTED, WorkflowState.CANCELLED,
        WorkflowState.INTERRUPTED,
    }),
    WorkflowState.PAUSED: frozenset({WorkflowState.RUNNING, WorkflowState.CANCELLED}),
    WorkflowState.SUCCEEDED: frozenset(),
    WorkflowState.FAILED: frozenset({WorkflowState.RUNNING, WorkflowState.CANCELLED}),
    WorkflowState.BUDGET_EXHAUSTED: frozenset({WorkflowState.RUNNING, WorkflowState.CANCELLED}),
    WorkflowState.CANCELLED: frozenset(),
    WorkflowState.INTERRUPTED: frozenset({WorkflowState.RUNNING, WorkflowState.CANCELLED}),
}


def can_transition_task(current: str | TaskState, target: str | TaskState) -> bool:
    current_state = TaskState(current)
    target_state = TaskState(target)
    return target_state in TASK_TRANSITIONS[current_state]


def can_transition_workflow(current: str | WorkflowState, target: str | WorkflowState) -> bool:
    current_state = WorkflowState(current)
    target_state = WorkflowState(target)
    return target_state in WORKFLOW_TRANSITIONS[current_state]
