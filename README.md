# delegate-codex-agent

> [!WARNING]
> This repository is an experimental project for exploring budget-aware Codex delegation. It is not production-ready, and its interfaces, file formats, and behavior may change without notice.

A Codex skill and local runner for delegating bounded repository tasks to cheaper models. It supports compact context packets, Luna-first model routing, dependency-aware parallel batches, isolated Git worktrees, token-usage tracking, and auditable run artifacts.

## Installation

### With the Codex skill installer

```bash
python3 "${CODEX_HOME:-$HOME/.codex}/skills/.system/skill-installer/scripts/install-skill-from-github.py" \
  --repo delef/delegate-codex-agent \
  --path . \
  --name delegate-codex-agent
```

### With Git

```bash
git clone https://github.com/delef/delegate-codex-agent.git \
  "${CODEX_HOME:-$HOME/.codex}/skills/delegate-codex-agent"
```

Verify the installation:

```bash
python3 "${CODEX_HOME:-$HOME/.codex}/skills/delegate-codex-agent/scripts/delegate.py" --help
```

The skill will be available to Codex on the next turn. The runner requires Git, Python 3, and the Codex CLI.

## Goals

- Reduce AI budget usage by preferring Luna and limiting Terra tasks.
- Run independent tasks concurrently without allowing unsafe workspace overlap.
- Keep delegated context and dependency handoffs small.
- Preserve enough evidence to inspect and resume each run.

## Usage

```bash
python3 scripts/delegate.py --help
python3 scripts/delegate.py run --help
python3 scripts/delegate.py batch --help
```

See [`SKILL.md`](SKILL.md) for the delegation policy and command examples.

## Development

The runner itself uses only the Python standard library. The Codex system skill validator imports PyYAML, so install the development dependency in an isolated environment:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-dev.txt
.venv/bin/python "${CODEX_HOME:-$HOME/.codex}/skills/.system/skill-creator/scripts/quick_validate.py" .
.venv/bin/python -m unittest -v tests/test_delegate.py
```
