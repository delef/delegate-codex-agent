# orchestrator-codex-agent

> [!WARNING]
> This repository is an experimental project for exploring budget-aware Codex delegation. It is not production-ready, and its interfaces, file formats, and behavior may change without notice.

A Codex skill centered on a deterministic, budget-aware workflow orchestrator. It supports acceptance-gated DAGs, recovery, bounded fan-out, isolated Git worktrees, token accounting, and auditable artifacts. One bounded-task mode remains available for lightweight work.

## Installation

### With the Codex skill installer

```bash
python3 "${CODEX_HOME:-$HOME/.codex}/skills/.system/skill-installer/scripts/install-skill-from-github.py" \
  --repo delef/orchestrator-codex-agent \
  --path . \
  --name orchestrator-codex-agent
```

### With Git

```bash
git clone https://github.com/delef/orchestrator-codex-agent.git \
  "${CODEX_HOME:-$HOME/.codex}/skills/orchestrator-codex-agent"
```

Verify the installation:

```bash
python3 "${CODEX_HOME:-$HOME/.codex}/skills/orchestrator-codex-agent/scripts/delegate.py" --help
```

The skill will be available to Codex on the next turn. The runner requires Git, Python 3, and the Codex CLI.

## Goals

- Reduce AI budget usage by preferring Luna and limiting Terra tasks.
- Use Sol selectively to isolate deep analysis from the supervisor context window.
- Run independent tasks concurrently without allowing unsafe workspace overlap.
- Keep delegated context and dependency handoffs small.
- Preserve enough evidence to inspect and resume each run.

## Models

- `luna` is the default routing role for discovery, mechanical work, and focused checks; it does not select a model ID.
- `terra` handles difficult implementation and integration; batch use requires `model_reason` and is limited to one task by default.
- `sol` is a read-only thinking delegate for architecture, ambiguity, and cross-cutting risk analysis. It requires `model_reason` and batch use must be enabled explicitly with `--max-sol-tasks` (default: `0`).

## Two operating modes

### Mode 1: workflow orchestrator (recommended)

Compile a versioned workflow when work has multiple phases, dependencies, retries, approvals, writers, or meaningful parallelism:

```bash
python3 scripts/delegate.py workflow prepare --file /abs/workflow.json --json
python3 scripts/delegate.py workflow start --workflow /abs/workflow.json
python3 scripts/delegate.py workflow inspect --workflow-dir /abs/workflow-run
python3 scripts/delegate.py workflow watch --workflow-dir /abs/workflow-run
```

### Mode 2: direct bounded delegation

Use `prepare`/`run` for one bounded task when a durable DAG would add unnecessary overhead:

```bash
python3 scripts/delegate.py prepare --spec /abs/task.json --cwd /abs/repo --sandbox read-only
python3 scripts/delegate.py run --spec /abs/task.json --cwd /abs/repo --sandbox read-only
```

See [`SKILL.md`](SKILL.md) for the delegation policy and command examples.

## Workflow capabilities

The deterministic runtime supports acceptance-gated DAGs, bounded map/reduce and iteration, durable recovery, model/token limits, approval controls, and isolated writer worktrees:

```bash
python3 scripts/delegate.py workflow prepare --file /abs/workflow.json --json
python3 scripts/delegate.py workflow start --workflow /abs/workflow.json
python3 scripts/delegate.py workflow inspect --workflow-dir /abs/workflow-run
python3 scripts/delegate.py workflow control --workflow-dir /abs/workflow-run \
  --control retry --payload '{"task_id":"task","reason":"new evidence"}'
```

Use `workflow integration-plan` to review writer changes and `workflow integrate` only with an approval object whose SHA-256 matches the plan. The runtime does not make hidden planning calls and does not automatically merge writer patches.

## Live status

Active runs update `status.json` and emit a foreground heartbeat every 15 seconds without making additional AI calls:

```text
DELEGATE_HEARTBEAT task=think phase=model_running child_alive=true elapsed=94s idle=12s events=12 tokens=19320
```

Inspect a run for its persisted progress and derived health:

```bash
python3 scripts/delegate.py inspect --run-dir /path/from/RUN_DIR
```

- `active`: the launcher heartbeat is fresh and events are recent.
- `silent`: the child is alive but has not emitted a recent event; this alone is not a hang.
- `stale`: the launcher heartbeat expired and the run needs diagnosis.
- `finished`: the run reached a terminal status.

Heartbeats include only event types, counters, timing, and token usage. Event payloads and model reasoning are not copied into status.

## Development

The runner itself uses only the Python standard library. The Codex system skill validator imports PyYAML, so install the development dependency in an isolated environment:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-dev.txt
.venv/bin/python "${CODEX_HOME:-$HOME/.codex}/skills/.system/skill-creator/scripts/quick_validate.py" .
.venv/bin/python -m unittest -v tests/test_delegate.py
```
