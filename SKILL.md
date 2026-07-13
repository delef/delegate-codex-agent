---
name: delegate-codex-agent
description: Use when a bounded repository task should move to another Codex model, independent tasks can save supervisor context, or deep analysis should be isolated from the main context window.
---

# Delegate Codex Agent

Keep the primary Sol as supervisor and final decision-maker. A delegated Sol is a read-only thinking delegate: use it to isolate deep analysis, not to replace supervision. Never delegate final diff review or supervisor verification.

Optimize total AI cost, not agent count. Do not delegate work cheaper to finish locally. Prefer one Luna task. Add workers only for genuinely independent work that avoids duplicated discovery.

## Route work

- `luna`: search, inventory, extraction, mechanical edits, focused tests, and explicit acceptance criteria.
- `terra`: multi-file integration, migrations, concurrency, difficult debugging, or a refined retry after a concrete Luna limitation. Batch Terra tasks require `model_reason`; one is allowed by default.
- `sol`: competing architectural options, ambiguous requirements, cross-cutting risk analysis, or a difficult decision whose exploration would crowd the supervisor context. Require `model_reason`, keep it `read-only`, request conclusions/evidence/risks instead of reasoning narration, and keep batch Sol disabled by default.
- Never retry unchanged with a stronger model. Narrow the task or add missing evidence first.

Use `read-only` for discovery/review and `workspace-write` only for authorized implementation. The launcher preserves Codex config, approvals, hooks, and repository trust.

## Specify one bounded task

```json
{
  "name": "meter-usage",
  "objective": "Persist reported usage.",
  "scope": ["src/client.rs"],
  "context": [{"path": "src/client.rs", "start": 1, "end": 180, "reason": "target"}],
  "constraints": ["Preserve unrelated dirty changes."],
  "acceptance": ["Focused tests pass."],
  "commands": ["cargo test client"],
  "output": ["Report changes, evidence, risks, and next action."]
}
```

Pass decisions, exact paths/symbols, constraints, evidence, and acceptance criteria—not conversation history or reasoning narration. Prefer pointers and targeted excerpts. Applicable `AGENTS.md` and concise git state are added automatically. The packet has a character limit.

```bash
python3 ~/.codex/skills/delegate-codex-agent/scripts/delegate.py prepare --spec /abs/task.json --cwd /abs/repo --model luna --sandbox read-only
python3 ~/.codex/skills/delegate-codex-agent/scripts/delegate.py run --spec /abs/task.json --cwd /abs/repo --model luna --sandbox read-only
python3 ~/.codex/skills/delegate-codex-agent/scripts/delegate.py run --spec /abs/design.json --cwd /abs/repo --model sol --model-reason "compare cross-cutting design risks" --sandbox read-only
```

Keep runs in the foreground; poll the same PTY session. Do not use `nohup` or `&`.

Read `DELEGATE_HEARTBEAT` while waiting; it reports task, phase, child state, timings, events, and tokens locally. Use `inspect` for health: `active` is fresh; `silent` is alive without recent events, not hung; `stale` means an expired heartbeat requiring diagnosis. Never terminate or retry from `silent` alone.

## Parallel batches

```json
{"tasks": [
  {"id": "find", "spec": "/tmp/find.json"},
  {"id": "think", "spec": "/tmp/design.json", "model": "sol",
   "model_reason": "compare cross-cutting design risks outside supervisor context",
   "depends_on": ["find"]},
  {"id": "implement", "spec": "/tmp/impl.json", "model": "terra",
   "model_reason": "multi-file integration after Luna discovery",
   "sandbox": "workspace-write", "depends_on": ["think"]}
]}
```

```bash
python3 ~/.codex/skills/delegate-codex-agent/scripts/delegate.py batch --manifest /abs/tasks.json --cwd /abs/repo --max-workers 2 --max-terra-tasks 1 --max-sol-tasks 1 --stop-after-total-tokens 200000
```

Defaults: Luna, read-only, shared isolation, two workers, one Terra task, zero Sol tasks, and 2,000 dependency characters. Dependencies must form a DAG. Dependents receive bounded `result.json` summaries; verbose verification stays on disk. `--stop-after-total-tokens` stops pending tasks after observed usage reaches the threshold. Already-running tasks may overshoot it.

Shared writers run exclusively. For truly independent writers set `"isolation":"worktree"`; each starts at `base_ref` (`HEAD` default). Clean worktrees are removed. Changed worktrees remain with status, base/head SHA, and a tracked-change patch; untracked files remain inside. Main-checkout uncommitted changes are not copied.

## Verify and recover

Each run persists `status.json`, `packet.md`, `events.jsonl`, `result.md`, structured `result.json`, and local token usage. `batch-status.json` aggregates usage. Exit zero proves transport completion only: inspect results and diff, then run the narrowest independent check.

```bash
python3 ~/.codex/skills/delegate-codex-agent/scripts/delegate.py inspect --run-dir /path/from/output
python3 ~/.codex/skills/delegate-codex-agent/scripts/delegate.py resume --run-dir /original/run --feedback-file /abs/findings.md
```

Resume with findings, evidence, required fixes, and checks only. Failed branches skip dependents while independent branches continue. Refine failures before retrying; never revert partial edits without authorization.
