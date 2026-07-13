"""Budget decisions layered over the StateStore."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .errors import StateError
from .store import JournalStateStore


@dataclass(frozen=True)
class BudgetDecision:
    allowed: bool
    reason: str | None = None
    remaining_tokens: int = 0
    reserved_tokens: int = 0


class BudgetController:
    def __init__(self, store: JournalStateStore):
        self.store = store

    def decide(self, tokens: int) -> BudgetDecision:
        snapshot = self.store.snapshot
        budget = snapshot["budget"]
        remaining = budget["limit_tokens"] - budget["used_tokens"] - budget["reserved_tokens"]
        return BudgetDecision(
            allowed=tokens > 0 and tokens <= remaining,
            reason=None if tokens > 0 and tokens <= remaining else "insufficient_budget",
            remaining_tokens=remaining,
            reserved_tokens=budget["reserved_tokens"],
        )

    def reserve(self, task_id: str, tokens: int) -> BudgetDecision:
        decision = self.decide(tokens)
        if not decision.allowed:
            return decision
        self.store.reserve_budget(tokens, task_id=task_id, idempotency_key=f"reserve:{task_id}")
        return BudgetDecision(
            True,
            remaining_tokens=decision.remaining_tokens - tokens,
            reserved_tokens=decision.reserved_tokens + tokens,
        )

    def release(self, task_id: str, tokens: int) -> None:
        self.store.release_budget(tokens, task_id=task_id, idempotency_key=f"release:{task_id}:{tokens}")

    def record(self, task_id: str, usage: dict[str, Any]) -> bool:
        self.store.record_usage(usage, task_id=task_id)
        snapshot = self.store.snapshot
        task = snapshot["tasks"][task_id]
        return task["usage"]["total_tokens"] >= task.get("hard_tokens", 2**63 - 1)
