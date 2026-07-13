"""Failure classification and deterministic retry evidence."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable


FAILURE_CLASSES = frozenset({
    "transport", "invalid_result", "verification", "scope",
    "budget", "permission", "integration",
})
DEFAULT_RETRY_ON = ("transport", "invalid_result", "verification")


def _canonical(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _canonical(value[key]) for key in sorted(value, key=str)}
    if isinstance(value, list):
        return [_canonical(item) for item in value]
    if isinstance(value, str):
        return " ".join(value.split())
    return value


def _digest(value: Any) -> str:
    encoded = json.dumps(
        _canonical(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class FailureEvidence:
    failure_class: str
    fingerprint: str
    reason: str
    evidence: dict[str, Any]

    def fields(self) -> dict[str, Any]:
        return {
            "failure_class": self.failure_class,
            "failure_fingerprint": self.fingerprint,
            "failure_evidence": asdict(self),
        }


def _artifact_digests(gates: Iterable[Any]) -> dict[str, str]:
    digests: dict[str, str] = {}
    for gate in gates:
        artifact = getattr(gate, "artifact", None)
        if not artifact:
            continue
        path = Path(artifact)
        try:
            digests[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
        except OSError:
            digests[str(path)] = "missing"
    return digests


def classify_failure(
    *, outcome: Any | None = None, gates: Iterable[Any] = (), result: Any = None,
    error: BaseException | None = None,
) -> FailureEvidence:
    """Classify one failed attempt without using model-generated labels."""
    gate_list = list(gates)
    reasons = [str(getattr(gate, "reason", "") or "") for gate in gate_list]
    rejected = [gate for gate in gate_list if getattr(gate, "status", None) != "accepted"]
    if error is not None:
        text = str(error) or type(error).__name__
        lowered = text.lower()
        failure_class = (
            "permission" if "permission" in lowered or "denied" in lowered
            else "integration" if "integration" in lowered or "worktree" in lowered
            else "transport"
        )
        reason = text
    elif outcome is not None and getattr(outcome, "budget_exhausted", False):
        failure_class, reason = "budget", "budget_exhausted"
    elif outcome is not None and getattr(outcome, "exit_code", 0) != 0:
        failure_class, reason = "transport", f"worker exited with code {outcome.exit_code}"
    elif any(getattr(gate, "gate_type", None) == "result_schema" for gate in rejected):
        failure_class = "invalid_result"
        reason = next((item for item in reasons if item), "invalid_result")
    elif any(getattr(gate, "gate_type", None) == "diff_scope" for gate in rejected):
        failure_class, reason = "scope", next((item for item in reasons if item), "scope_violation")
    elif any(getattr(gate, "gate_type", None) == "approval" for gate in rejected):
        failure_class, reason = "integration", next((item for item in reasons if item), "approval_required")
    else:
        failure_class, reason = "verification", next((item for item in reasons if item), "verification_failed")
    payload = {
        "failure_class": failure_class,
        "reason": reason,
        "exit_code": getattr(outcome, "exit_code", None),
        "result": result,
        "gates": [
            {
                "type": getattr(gate, "gate_type", None),
                "status": getattr(gate, "status", None),
                "reason": getattr(gate, "reason", None),
                "evidence": getattr(gate, "evidence", {}),
            }
            for gate in gate_list
        ],
        "artifacts": _artifact_digests(gate_list),
        "error": str(error) if error is not None else None,
    }
    evidence = {
        "reason": reason,
        "result_sha256": _digest(result),
        "gate_reasons": [item for item in reasons if item],
        "artifact_sha256": _artifact_digests(gate_list),
    }
    return FailureEvidence(failure_class, _digest(payload), reason, evidence)
