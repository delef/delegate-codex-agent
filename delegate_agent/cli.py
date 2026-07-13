"""CLI-facing workflow operations used by the legacy executable shim."""

from __future__ import annotations

import json
from pathlib import Path
import time
from typing import Any, Callable

from .errors import OrchestratorError
from .integration import apply_integration_plan, build_integration_plan, write_integration_plan
from .schema import load_workflow
from .status import build_status
from .store import JournalStateStore, submit_control_request
from .workflow import WorkflowRuntime


def start_workflow(
    workflow_file: str | Path, *, runs_dir: str | Path | None = None,
    on_started: Callable[[Path], None] | None = None,
) -> tuple[int, Path, dict[str, Any]]:
    runtime = WorkflowRuntime.from_file(workflow_file, runs_dir=runs_dir)
    try:
        run_dir = runtime.workflow_dir
        if on_started is not None:
            on_started(run_dir)
        result = runtime.run()
        code = 0 if result["state"] == "succeeded" else 1
        return code, run_dir, result
    finally:
        runtime.close()


def resume_workflow(workflow_dir: str | Path) -> tuple[int, dict[str, Any]]:
    runtime = WorkflowRuntime(workflow_dir)
    try:
        result = runtime.run()
        return (0 if result["state"] == "succeeded" else 1), result
    finally:
        runtime.close()


def inspect_workflow(workflow_dir: str | Path) -> dict[str, Any]:
    store = JournalStateStore.open(workflow_dir)
    try:
        return build_status(store)
    finally:
        store.close()


def request_control(
    workflow_dir: str | Path, request_type: str, payload: dict[str, Any] | None = None,
    *, request_id: str | None = None,
) -> Path:
    return submit_control_request(workflow_dir, request_type, payload, request_id=request_id)


def plan_integration(
    repo: str | Path, writers: list[dict[str, Any]], *, destination: str | Path | None = None,
    checks: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    plan = build_integration_plan(repo, writers, checks=checks)
    if destination is not None:
        write_integration_plan(plan, destination)
    return plan


def integrate_plan(plan_file: str | Path, approval: str | Path | dict[str, Any]) -> dict[str, Any]:
    plan_path = Path(plan_file).expanduser().resolve()
    try:
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OrchestratorError(f"cannot read integration plan: {exc}") from exc
    if not isinstance(plan, dict):
        raise OrchestratorError("integration plan must be a JSON object")
    return apply_integration_plan(plan, approval=approval)


def prepare_workflow(workflow_file: str | Path) -> dict[str, Any]:
    """Validate and estimate a workflow without starting a worker."""
    workflow = load_workflow(workflow_file)
    nodes = workflow["nodes"]
    by_id = {node["id"]: node for node in nodes}
    remaining = {node["id"]: set(node.get("depends_on", [])) for node in nodes}
    phases: list[list[str]] = []
    while remaining:
        ready = sorted(node_id for node_id, deps in remaining.items() if not deps)
        if not ready:
            raise OrchestratorError("workflow dependency graph contains a cycle")
        phases.append(ready)
        for node_id in ready:
            remaining.pop(node_id)
        for deps in remaining.values():
            deps.difference_update(ready)

    reserve_tokens = 0
    hard_tokens = 0
    expanded_tasks = 0
    routing: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    specs: dict[str, list[str]] = {}
    scopes: dict[str, list[str]] = {}
    model_counts = {"terra": 0, "sol": 0}
    for node in nodes:
        kind = node["kind"]
        multiplier = 1
        if kind == "map":
            multiplier = node["max_items"]
            template = node["template"]
            reserve_tokens += multiplier * template["budget"]["reserve_tokens"]
            hard_tokens += multiplier * template["budget"]["hard_tokens"]
            expanded_tasks += multiplier
            routing.append({"id": node["id"], "kind": kind, "model": template["model"], "max_items": multiplier})
            if template["model"] in model_counts:
                model_counts[template["model"]] += multiplier
            continue
        if kind == "repeat_until":
            multiplier = node["max_iterations"]
            template = node["template"]
            reserve_tokens += multiplier * template["budget"]["reserve_tokens"]
            hard_tokens += multiplier * template["budget"]["hard_tokens"]
            expanded_tasks += multiplier
            routing.append({"id": node["id"], "kind": kind, "model": template["model"], "max_iterations": multiplier})
            if template["model"] in model_counts:
                model_counts[template["model"]] += multiplier
            continue
        if kind == "agent":
            budget = node["budget"]
            reserve_tokens += budget["reserve_tokens"]
            hard_tokens += budget["hard_tokens"]
            expanded_tasks += 1
            routing.append({
                "id": node["id"], "kind": kind, "model": node["model"],
                "sandbox": node["sandbox"], "isolation": node["isolation"],
            })
            if node["model"] in model_counts:
                model_counts[node["model"]] += 1
            specs.setdefault(node["spec"], []).append(node["id"])
            if not node.get("checks"):
                warnings.append({"type": "missing_checks", "node": node["id"]})
            if node["model"] == "terra" and not node.get("depends_on"):
                warnings.append({"type": "terra_without_evidence", "node": node["id"]})
            if node["model"] == "sol":
                warnings.append({"type": "sol_opt_in", "node": node["id"]})
            if node["sandbox"] == "workspace-write":
                scopes[node["id"]] = [str(path) for path in node.get("scope", [])]
    for spec, ids in sorted(specs.items()):
        if len(ids) > 1:
            warnings.append({"type": "duplicate_discovery", "spec": spec, "nodes": sorted(ids)})
    scoped = sorted(scopes.items())
    for index, (left_id, left_paths) in enumerate(scoped):
        for right_id, right_paths in scoped[index + 1:]:
            overlap = sorted({left for left in left_paths for right in right_paths if left == right})
            if overlap:
                warnings.append({"type": "writer_overlap", "nodes": [left_id, right_id], "paths": overlap})
    if workflow["budget"]["max_workers"] > max(1, max(len(phase) for phase in phases)):
        warnings.append({"type": "unnecessary_parallelism", "max_workers": workflow["budget"]["max_workers"]})
    errors: list[dict[str, Any]] = []
    if reserve_tokens > workflow["budget"]["total_tokens"]:
        errors.append({
            "type": "reservation_over_budget", "reserved_tokens": reserve_tokens,
            "limit_tokens": workflow["budget"]["total_tokens"],
        })
    for model, limit_key in (("terra", "max_terra_tasks"), ("sol", "max_sol_tasks")):
        if model_counts[model] > workflow["budget"][limit_key]:
            errors.append({
                "type": "model_limit_exceeded", "model": model,
                "tasks": model_counts[model], "limit": workflow["budget"][limit_key],
            })
    return {
        "schema_version": 1, "preview_only": True, "ready": not errors,
        "workflow": {"name": workflow["name"], "cwd": workflow["cwd"]},
        "phases": phases,
        "routing": routing,
        "estimates": {
            "reserve_tokens": reserve_tokens, "hard_tokens": hard_tokens,
            "expanded_tasks": expanded_tasks,
            "max_parallel_tasks": max(len(phase) for phase in phases),
            "model_tasks": model_counts,
        },
        "warnings": warnings, "errors": errors,
    }


def print_status(workflow_dir: str | Path) -> int:
    print(json.dumps(inspect_workflow(workflow_dir), ensure_ascii=False, indent=2))
    return 0


def watch_workflow(workflow_dir: str | Path, *, interval: float = 1.0, once: bool = False) -> int:
    while True:
        value = inspect_workflow(workflow_dir)
        print(json.dumps(value, ensure_ascii=False, indent=2), flush=True)
        if once or value["state"] in {"succeeded", "failed", "budget_exhausted", "cancelled", "interrupted"}:
            return 0
        time.sleep(max(0.1, interval))
