---
name: orchestrator-codex-agent
description: Use when a bounded repository task should move to another Codex model, independent tasks can save supervisor context, or deep analysis should be isolated from the main context window.
---

# Orchestrator Codex Agent

Keep the primary Sol as supervisor and final decision-maker. The skill has two operating modes; the workflow orchestrator is the default for multi-step work, while direct delegation is the lightweight option for one bounded task. A delegated Sol is a read-only thinking delegate, not a replacement for supervision. Never delegate final diff review or supervisor verification.

Optimize total AI cost. Do not delegate cheaper local work. Prefer one Luna task. Add workers only for independent work.

## Route work

- `luna`: routing role for search, inventory, extraction, mechanical edits, and focused tests.
- `terra`: routing role for multi-file integration, migrations, concurrency, difficult debugging, or a refined retry after a concrete Luna limitation. Batch Terra tasks require `model_reason`; one is allowed by default.
- `sol`: routing role for competing architectural options, ambiguous requirements, cross-cutting risk analysis, or a difficult decision whose exploration would crowd the supervisor context. Require `model_reason`, keep it `read-only`, request conclusions/evidence/risks instead of reasoning narration, and keep batch Sol disabled by default.
- Never retry unchanged with a stronger model. Narrow the task or add missing evidence first.

Use `read-only` for discovery/review and `workspace-write` only for authorized implementation. The launcher preserves Codex config, approvals, hooks, and repository trust.

Routing roles never select private model IDs. Delegates inherit the model from Codex configuration by default. Set `model_id` in a workflow/manifest or pass `--model-id <available-model-id>` only when an explicit model is required.

## Mode 1: workflow orchestrator (default)

For genuinely independent or context-heavy work, the primary Codex compiles a static workflow JSON and previews it before launch:

```bash
python3 scripts/delegate.py workflow prepare --file /abs/workflow.json --json
python3 scripts/delegate.py workflow start --workflow /abs/workflow.json
python3 scripts/delegate.py workflow inspect --workflow-dir /abs/run
python3 scripts/delegate.py workflow watch --workflow-dir /abs/run
python3 scripts/delegate.py workflow control --workflow-dir /abs/run --control approve --payload '{"task_id":"gate"}'
python3 scripts/delegate.py workflow resume --workflow-dir /abs/run
```

Preview performs validation, phase/routing estimation, reservation checks, and policy warnings without a model call. Luna remains the default; Terra escalation requires an explicit evidence-backed reason, and Sol is never selected automatically. Dependents receive only accepted results. Writer worktrees require an integration plan and explicit approval before patch application.

## Mode 2: direct bounded delegation

Use this lightweight mode for one bounded task. Pass a JSON task with `name`, `objective`, `scope`, `context`, `constraints`, `acceptance`, `commands`, and `output`. Applicable `AGENTS.md` and concise git state are added automatically.

```bash
python3 ~/.codex/skills/orchestrator-codex-agent/scripts/delegate.py prepare --spec /abs/task.json --cwd /abs/repo --sandbox read-only
python3 ~/.codex/skills/orchestrator-codex-agent/scripts/delegate.py run --spec /abs/task.json --cwd /abs/repo --sandbox read-only
```

Keep runs in the foreground; poll the same PTY session. Read `DELEGATE_HEARTBEAT`: `active` is fresh, `silent` is alive without recent events, and `stale` requires diagnosis. Never terminate or retry from `silent` alone.

## Workflow fan-out and parallel batches

Use a manifest DAG with `--max-workers`, `--max-terra-tasks`, `--max-sol-tasks`, and `--stop-after-total-tokens`. Defaults are Luna, read-only, shared isolation, two workers, one Terra task, zero Sol tasks, and bounded dependency summaries. Dependents receive `result.json`; verbose verification stays on disk. Pending tasks stop at the token threshold; active tasks may overshoot it.

Shared writers run exclusively. For truly independent writers set `"isolation":"worktree"`; each starts at `base_ref` (`HEAD` default). Clean worktrees are removed. Changed worktrees remain with status, base/head SHA, and a tracked-change patch; untracked files remain inside. Main-checkout uncommitted changes are not copied.

## Verify and recover

Each run persists `status.json`, `packet.md`, `events.jsonl`, `result.md`, structured `result.json`, and local token usage. `batch-status.json` aggregates usage. Exit zero proves transport completion only: inspect results and diff, then run the narrowest independent check.

```bash
python3 ~/.codex/skills/orchestrator-codex-agent/scripts/delegate.py inspect --run-dir /path/from/output
python3 ~/.codex/skills/orchestrator-codex-agent/scripts/delegate.py resume --run-dir /original/run --feedback-file /abs/findings.md
```

Resume with findings, evidence, required fixes, and checks only. Failed branches skip dependents while independent branches continue. Refine failures before retrying; never revert partial edits without authorization.
