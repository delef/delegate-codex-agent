"""Deterministic JSON-pointer condition evaluation."""

from __future__ import annotations

from typing import Any

from .errors import StateError


OPERATORS = frozenset({
    "exists", "equals", "not_equals", "contains",
    "greater_than", "less_than", "greater_or_equal", "less_or_equal",
})
_MISSING = object()


def resolve_pointer(value: Any, pointer: str) -> tuple[bool, Any]:
    if pointer == "":
        return True, value
    if not isinstance(pointer, str) or not pointer.startswith("/"):
        raise StateError("condition.pointer must be an RFC 6901 JSON Pointer")
    current = value
    for token in pointer[1:].split("/"):
        token = token.replace("~1", "/").replace("~0", "~")
        if isinstance(current, dict):
            if token not in current:
                return False, None
            current = current[token]
        elif isinstance(current, list):
            if token == "-" or not token.isdigit() or int(token) >= len(current):
                return False, None
            current = current[int(token)]
        else:
            return False, None
    return True, current


def evaluate_condition(node: dict[str, Any], dependency_results: dict[str, dict[str, Any]]) -> dict[str, Any]:
    source = node["source"]
    if source not in dependency_results:
        raise StateError(f"condition source is not accepted: {source}")
    exists, actual = resolve_pointer(dependency_results[source], node.get("pointer", ""))
    operator = node["operator"]
    expected = node.get("value")
    if operator == "exists":
        matched = exists
    elif operator == "equals":
        matched = exists and type(actual) is type(expected) and actual == expected
    elif operator == "not_equals":
        matched = not exists or type(actual) is not type(expected) or actual != expected
    elif operator == "contains":
        if not exists or not isinstance(actual, (str, list, dict)):
            raise StateError("condition.contains requires a string, array, or object value")
        matched = expected in actual
    else:
        if not exists or isinstance(actual, bool) or not isinstance(actual, (int, float)):
            raise StateError(f"condition.{operator} requires a numeric value")
        if isinstance(expected, bool) or not isinstance(expected, (int, float)):
            raise StateError(f"condition.{operator} requires a numeric comparison value")
        matched = {
            "greater_than": actual > expected,
            "less_than": actual < expected,
            "greater_or_equal": actual >= expected,
            "less_or_equal": actual <= expected,
        }[operator]
    return {
        "source": source,
        "pointer": node.get("pointer", ""),
        "operator": operator,
        "value": actual if exists else None,
        "exists": exists,
        "matched": matched,
        "selected_branch": "true" if matched else "false",
    }
