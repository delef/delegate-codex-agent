"""Validation and normalization for versioned workflow documents."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .errors import SchemaError
from .hooks import HOOK_EVENTS
from .retry import DEFAULT_RETRY_ON, FAILURE_CLASSES

SUPPORTED_VERSION = 1
MODELS = {"luna", "terra", "sol"}
SANDBOXES = {"read-only", "workspace-write"}
ISOLATIONS = {"shared", "worktree"}
NODE_KINDS = {"agent", "check", "approval", "condition", "map", "reduce", "repeat_until"}
CHECK_KINDS = {"result_schema", "command", "diff_scope", "approval"}
CONDITION_OPERATORS = {
    "exists", "equals", "not_equals", "contains",
    "greater_than", "less_than", "greater_or_equal", "less_or_equal",
}


def _error(message: str) -> SchemaError:
    return SchemaError(message)


def _nonempty_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _error(f"{label} must be a non-empty string")
    return value.strip()


def _positive_int(value: Any, label: str, *, allow_zero: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise _error(f"{label} must be an integer")
    if value < 0 or (value == 0 and not allow_zero):
        raise _error(f"{label} must be {'nonnegative' if allow_zero else 'positive'}")
    return value


def _path_string(value: Any, label: str, base: Path) -> str:
    raw = _nonempty_string(value, label)
    path = Path(raw)
    if not path.is_absolute():
        path = base / path
    return str(path.resolve())


def _validate_acyclic(nodes: list[dict[str, Any]]) -> None:
    by_id = {node["id"]: node for node in nodes}
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node_id: str) -> None:
        if node_id in visiting:
            raise _error(f"dependency cycle includes node {node_id}")
        if node_id in visited:
            return
        visiting.add(node_id)
        for dependency in by_id[node_id]["depends_on"]:
            visit(dependency)
        visiting.remove(node_id)
        visited.add(node_id)

    for node in nodes:
        visit(node["id"])


def _validate_check(check: Any, label: str) -> dict[str, Any]:
    if not isinstance(check, dict):
        raise _error(f"{label} must be an object")
    kind = _nonempty_string(check.get("type"), f"{label}.type")
    if kind not in CHECK_KINDS:
        raise _error(f"{label}.type is unsupported: {kind}")
    result = dict(check)
    result["type"] = kind
    if kind == "command":
        argv = check.get("argv")
        if not isinstance(argv, list) or not argv or not all(
            isinstance(item, str) and item for item in argv
        ):
            raise _error(f"{label}.argv must be a non-empty array of strings")
        timeout = _positive_int(check.get("timeout_seconds", 300), f"{label}.timeout_seconds")
        if timeout > 86_400:
            raise _error(f"{label}.timeout_seconds is too large")
        env = check.get("inherit_env", [])
        if not isinstance(env, list) or not all(isinstance(item, str) and item for item in env):
            raise _error(f"{label}.inherit_env must be an array of strings")
        result.update({"argv": list(argv), "timeout_seconds": timeout, "inherit_env": list(env)})
    if kind == "diff_scope":
        paths = check.get("paths", [])
        if not isinstance(paths, list) or not all(isinstance(item, str) and item.strip() for item in paths):
            raise _error(f"{label}.paths must be an array of non-empty strings")
        result["paths"] = [item.strip() for item in paths]
    return result


def _validate_node(raw: Any, index: int, base: Path, node_ids: set[str], workflow_limit: int) -> dict[str, Any]:
    label = f"nodes[{index}]"
    if not isinstance(raw, dict):
        raise _error(f"{label} must be an object")
    node_id = _nonempty_string(raw.get("id"), f"{label}.id")
    if node_id in node_ids:
        raise _error(f"duplicate node id: {node_id}")
    kind = _nonempty_string(raw.get("kind", "agent"), f"{label}.kind")
    if kind not in NODE_KINDS:
        raise _error(f"{label}.kind is unsupported: {kind}")
    dependencies = raw.get("depends_on", [])
    if not isinstance(dependencies, list) or not all(
        isinstance(item, str) and item.strip() for item in dependencies
    ):
        raise _error(f"{label}.depends_on must be an array of node ids")
    if node_id in dependencies:
        raise _error(f"dependency cycle includes node {node_id}")
    result = dict(raw)
    result.update({"id": node_id, "kind": kind, "depends_on": list(dependencies)})
    if kind == "agent":
        result["spec"] = _path_string(raw.get("spec"), f"{label}.spec", base)
        model = _nonempty_string(raw.get("model", "luna"), f"{label}.model")
        if model not in MODELS:
            raise _error(f"{label}.model is unsupported: {model}")
        sandbox = _nonempty_string(raw.get("sandbox", "read-only"), f"{label}.sandbox")
        if sandbox not in SANDBOXES:
            raise _error(f"{label}.sandbox is unsupported: {sandbox}")
        isolation = _nonempty_string(raw.get("isolation", "shared"), f"{label}.isolation")
        if isolation not in ISOLATIONS:
            raise _error(f"{label}.isolation is unsupported: {isolation}")
        if isolation == "worktree" and sandbox != "workspace-write":
            raise _error(f"{label}.worktree isolation requires workspace-write sandbox")
        reason = raw.get("model_reason")
        if model in {"terra", "sol"} and not isinstance(reason, str):
            raise _error(f"{label}.model_reason is required for {model}")
        if model in {"terra", "sol"} and not reason.strip():
            raise _error(f"{label}.model_reason must be non-empty for {model}")
        if model == "sol" and sandbox != "read-only":
            raise _error(f"{label}.sol must be read-only")
        budget = raw.get("budget", {})
        if not isinstance(budget, dict):
            raise _error(f"{label}.budget must be an object")
        reserve = _positive_int(budget.get("reserve_tokens", 1), f"{label}.budget.reserve_tokens")
        hard = _positive_int(budget.get("hard_tokens", workflow_limit), f"{label}.budget.hard_tokens")
        if reserve > hard:
            raise _error(f"{label}.budget.reserve_tokens cannot exceed hard_tokens")
        retry = raw.get("retry", {})
        if not isinstance(retry, dict):
            raise _error(f"{label}.retry must be an object")
        attempts = _positive_int(retry.get("max_attempts", 1), f"{label}.retry.max_attempts")
        retry_on = retry.get("retry_on", list(DEFAULT_RETRY_ON))
        if not isinstance(retry_on, list) or not retry_on or not all(
            isinstance(item, str) and item in FAILURE_CLASSES for item in retry_on
        ):
            raise _error(
                f"{label}.retry.retry_on must be a non-empty array of supported failure classes"
            )
        escalate_to = retry.get("escalate_to")
        if escalate_to is not None:
            if model != "luna" or escalate_to != "terra":
                raise _error(f"{label}.retry.escalate_to supports only luna to terra")
            escalation_reason = retry.get("escalation_reason")
            if not isinstance(escalation_reason, str) or not escalation_reason.strip():
                raise _error(f"{label}.retry.escalation_reason is required for escalation")
            escalate_on = retry.get("escalate_on", list(retry_on))
            if not isinstance(escalate_on, list) or not escalate_on or not all(
                isinstance(item, str) and item in FAILURE_CLASSES for item in escalate_on
            ):
                raise _error(f"{label}.retry.escalate_on must be a non-empty array of supported failure classes")
        else:
            escalation_reason = None
            escalate_on = []
        result.update({
            "model": model, "sandbox": sandbox, "isolation": isolation,
            "model_reason": reason, "budget": {"reserve_tokens": reserve, "hard_tokens": hard},
            "retry": {
                **retry, "max_attempts": attempts, "retry_on": list(dict.fromkeys(retry_on)),
                "escalate_to": escalate_to, "escalation_reason": escalation_reason,
                "escalate_on": list(dict.fromkeys(escalate_on)),
            },
        })
    if kind == "condition":
        source = _nonempty_string(raw.get("source"), f"{label}.source")
        if source not in dependencies:
            raise _error(f"{label}.source must be listed in depends_on")
        operator = _nonempty_string(raw.get("operator"), f"{label}.operator")
        if operator not in CONDITION_OPERATORS:
            raise _error(f"{label}.operator is unsupported: {operator}")
        pointer = raw.get("pointer", "")
        if not isinstance(pointer, str) or (pointer and not pointer.startswith("/")):
            raise _error(f"{label}.pointer must be an RFC 6901 JSON Pointer")
        if operator not in {"exists"} and "value" not in raw:
            raise _error(f"{label}.value is required for {operator}")
        for branch in ("on_true", "on_false"):
            values = raw.get(branch, [])
            if not isinstance(values, list) or not all(isinstance(item, str) and item.strip() for item in values):
                raise _error(f"{label}.{branch} must be an array of node ids")
            result[branch] = list(dict.fromkeys(values))
        if set(result["on_true"]) & set(result["on_false"]):
            raise _error(f"{label}.on_true and on_false must be disjoint")
        result.update({"source": source, "operator": operator, "pointer": pointer})
        if "value" in raw:
            result["value"] = raw["value"]
    if kind == "map":
        source = _nonempty_string(raw.get("source"), f"{label}.source")
        if source not in dependencies:
            raise _error(f"{label}.source must be listed in depends_on")
        pointer = raw.get("pointer", "")
        if not isinstance(pointer, str) or (pointer and not pointer.startswith("/")):
            raise _error(f"{label}.pointer must be an RFC 6901 JSON Pointer")
        item_key = _nonempty_string(raw.get("item_key"), f"{label}.item_key")
        if not item_key.startswith("/"):
            raise _error(f"{label}.item_key must be an RFC 6901 JSON Pointer")
        max_items = _positive_int(raw.get("max_items"), f"{label}.max_items")
        if max_items > 10_000:
            raise _error(f"{label}.max_items is too large")
        template_raw = raw.get("template")
        if not isinstance(template_raw, dict):
            raise _error(f"{label}.template must be an agent node object")
        template_input = dict(template_raw)
        template_input["id"] = f"{node_id}.__template"
        template = _validate_node(template_input, index, base, set(), workflow_limit)
        if template["kind"] != "agent":
            raise _error(f"{label}.template.kind must be agent")
        template.pop("id", None)
        reduce_id = raw.get("reduce")
        if reduce_id is not None:
            reduce_id = _nonempty_string(reduce_id, f"{label}.reduce")
        result.update({
            "source": source, "pointer": pointer, "item_key": item_key,
            "max_items": max_items, "template": template, "reduce": reduce_id,
        })
    if kind == "reduce":
        source = _nonempty_string(raw.get("source"), f"{label}.source")
        if source not in dependencies:
            raise _error(f"{label}.source must be listed in depends_on")
        allow_partial = raw.get("allow_partial", False)
        if not isinstance(allow_partial, bool):
            raise _error(f"{label}.allow_partial must be boolean")
        result.update({"source": source, "allow_partial": allow_partial})
    if kind == "repeat_until":
        max_iterations = _positive_int(raw.get("max_iterations"), f"{label}.max_iterations")
        if max_iterations > 100:
            raise _error(f"{label}.max_iterations is too large")
        template_raw = raw.get("template")
        if not isinstance(template_raw, dict):
            raise _error(f"{label}.template must be an agent node object")
        template_input = dict(template_raw)
        template_input["id"] = f"{node_id}.__template"
        template = _validate_node(template_input, index, base, set(), workflow_limit)
        if template["kind"] != "agent":
            raise _error(f"{label}.template.kind must be agent")
        template.pop("id", None)
        condition = raw.get("condition")
        if not isinstance(condition, dict):
            raise _error(f"{label}.condition must be an object")
        operator = _nonempty_string(condition.get("operator"), f"{label}.condition.operator")
        if operator not in CONDITION_OPERATORS:
            raise _error(f"{label}.condition.operator is unsupported: {operator}")
        pointer = condition.get("pointer", "")
        if not isinstance(pointer, str) or (pointer and not pointer.startswith("/")):
            raise _error(f"{label}.condition.pointer must be an RFC 6901 JSON Pointer")
        if operator != "exists" and "value" not in condition:
            raise _error(f"{label}.condition.value is required for {operator}")
        normalized_condition = {"operator": operator, "pointer": pointer}
        if "value" in condition:
            normalized_condition["value"] = condition["value"]
        result.update({
            "max_iterations": max_iterations, "template": template,
            "condition": normalized_condition,
        })
    checks = raw.get("checks", [])
    if not isinstance(checks, list):
        raise _error(f"{label}.checks must be an array")
    result["checks"] = [_validate_check(check, f"{label}.checks[{i}]") for i, check in enumerate(checks)]
    return result


def normalize_workflow(value: Any, *, source: Path | None = None) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise _error("workflow must be a JSON object")
    version = value.get("version")
    if version != SUPPORTED_VERSION:
        raise _error(f"workflow.version must be {SUPPORTED_VERSION}")
    base = (source.parent if source else Path.cwd()).resolve()
    name = _nonempty_string(value.get("name"), "workflow.name")
    cwd = _path_string(value.get("cwd"), "workflow.cwd", base)
    budget = value.get("budget", {})
    if not isinstance(budget, dict):
        raise _error("workflow.budget must be an object")
    total_tokens = _positive_int(budget.get("total_tokens"), "workflow.budget.total_tokens")
    max_workers = _positive_int(budget.get("max_workers", 1), "workflow.budget.max_workers")
    max_terra = _positive_int(budget.get("max_terra_tasks", 0), "workflow.budget.max_terra_tasks", allow_zero=True)
    max_sol = _positive_int(budget.get("max_sol_tasks", 0), "workflow.budget.max_sol_tasks", allow_zero=True)
    raw_nodes = value.get("nodes")
    if not isinstance(raw_nodes, list) or not raw_nodes:
        raise _error("workflow.nodes must be a non-empty array")
    nodes: list[dict[str, Any]] = []
    node_ids: set[str] = set()
    for index, raw_node in enumerate(raw_nodes):
        node = _validate_node(raw_node, index, base, node_ids, total_tokens)
        nodes.append(node)
        node_ids.add(node["id"])
    missing = sorted({dep for node in nodes for dep in node["depends_on"]} - node_ids)
    if missing:
        raise _error(f"workflow has missing dependencies: {', '.join(missing)}")
    for node in nodes:
        if node["kind"] != "condition":
            if node["kind"] == "map" and node.get("reduce"):
                reduce_id = node["reduce"]
                if reduce_id not in node_ids:
                    raise _error(f"{node['id']}.reduce references missing node: {reduce_id}")
                reduce_node = next(item for item in nodes if item["id"] == reduce_id)
                if reduce_node["kind"] != "reduce" or node["id"] not in reduce_node["depends_on"]:
                    raise _error(f"map reducer {reduce_id} must be a reduce node depending on {node['id']}")
            continue
        for branch in ("on_true", "on_false"):
            for branch_id in node.get(branch, []):
                if branch_id not in node_ids:
                    raise _error(f"{node['id']}.{branch} references missing node: {branch_id}")
                if node["id"] not in next(item for item in nodes if item["id"] == branch_id)["depends_on"]:
                    raise _error(f"condition branch {branch_id} must depend on {node['id']}")
    _validate_acyclic(nodes)
    hooks = value.get("hooks", {})
    if not isinstance(hooks, dict):
        raise _error("workflow.hooks must be an object")
    normalized_hooks: dict[str, list[dict[str, Any]]] = {}
    for event, raw_hooks in hooks.items():
        if event not in HOOK_EVENTS:
            raise _error(f"workflow.hooks event is unsupported: {event}")
        if not isinstance(raw_hooks, list):
            raise _error(f"workflow.hooks.{event} must be an array")
        normalized_hooks[event] = []
        for index, raw_hook in enumerate(raw_hooks):
            label = f"workflow.hooks.{event}[{index}]"
            if not isinstance(raw_hook, dict):
                raise _error(f"{label} must be an object")
            argv = raw_hook.get("argv")
            if not isinstance(argv, list) or not argv or not all(isinstance(item, str) and item for item in argv):
                raise _error(f"{label}.argv must be a non-empty array of strings")
            timeout = _positive_int(raw_hook.get("timeout_seconds", 30), f"{label}.timeout_seconds")
            if timeout > 3_600:
                raise _error(f"{label}.timeout_seconds is too large")
            output_limit = _positive_int(raw_hook.get("output_limit", 64_000), f"{label}.output_limit")
            if output_limit > 1_000_000:
                raise _error(f"{label}.output_limit is too large")
            inherit_env = raw_hook.get("inherit_env", [])
            if not isinstance(inherit_env, list) or not all(isinstance(item, str) and item for item in inherit_env):
                raise _error(f"{label}.inherit_env must be an array of strings")
            failure_policy = raw_hook.get("failure_policy", "fail_closed")
            if failure_policy not in {"fail_open", "fail_closed"}:
                raise _error(f"{label}.failure_policy must be fail_open or fail_closed")
            normalized_hooks[event].append({
                "argv": list(argv), "timeout_seconds": timeout,
                "output_limit": output_limit, "inherit_env": list(inherit_env),
                "failure_policy": failure_policy,
            })
    cache = value.get("cache", {})
    if not isinstance(cache, dict):
        raise _error("workflow.cache must be an object")
    cache_enabled = cache.get("enabled", True)
    if not isinstance(cache_enabled, bool):
        raise _error("workflow.cache.enabled must be boolean")
    return {
        "version": SUPPORTED_VERSION,
        "name": name,
        "cwd": cwd,
        "budget": {
            "total_tokens": total_tokens, "max_workers": max_workers,
            "max_terra_tasks": max_terra, "max_sol_tasks": max_sol,
        },
        "nodes": nodes,
        "hooks": normalized_hooks,
        "cache": {"enabled": cache_enabled},
    }


def load_workflow(path: str | Path) -> dict[str, Any]:
    workflow_path = Path(path).expanduser().resolve()
    try:
        value = json.loads(workflow_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SchemaError(f"cannot read workflow: {exc}") from exc
    return normalize_workflow(value, source=workflow_path)
