#!/usr/bin/env python3
"""Render a compact, read-only workflow status line."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from orchestrator_agent.cli import inspect_workflow  # noqa: E402


def render_status(status: dict, *, width: int | None = None) -> str:
    counts = status.get("counts", {})
    budget = status.get("budget", {})
    active = status.get("active", [])
    current = active[0] if active else None
    task = current.get("id", "-") if current else (status.get("next") or "-")
    model = current.get("model", "-") if current else "-"
    text = (
        f"{status.get('name', '-')}: {status.get('state', '-')} "
        f"task={task} model={model} "
        f"active={len(active)} pending={status.get('pending', 0)} "
        f"blocked={status.get('blocked', 0)} "
        f"budget={budget.get('used_tokens', 0)}/{budget.get('limit_tokens', 0)} "
        f"reserved={budget.get('reserved_tokens', 0)} "
        f"retry={sum(int(item.get('retry_count', 0)) for item in active)}"
    )
    if width is not None and width > 0:
        return text[:width]
    return text


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workflow-dir", required=True)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--width", type=int)
    args = parser.parse_args()
    status = inspect_workflow(args.workflow_dir)
    if args.json:
        print(json.dumps(status, ensure_ascii=False, indent=2))
    else:
        print(render_status(status, width=args.width))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
