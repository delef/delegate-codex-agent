# delegate-codex-agent

> [!WARNING]
> This repository is an experimental project for exploring budget-aware Codex delegation. It is not production-ready, and its interfaces, file formats, and behavior may change without notice.

A Codex skill and local runner for delegating bounded repository tasks to cheaper models. It supports compact context packets, Luna-first model routing, dependency-aware parallel batches, isolated Git worktrees, token-usage tracking, and auditable run artifacts.

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
