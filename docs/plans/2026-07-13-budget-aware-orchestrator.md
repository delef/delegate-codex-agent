# Budget-Aware Codex Orchestrator Implementation Plan

**Goal:** Evolve the current delegate runner into a durable, programmable Codex workflow orchestrator while keeping Luna as the default worker and minimizing unnecessary AI calls.

**Target:** Match the useful orchestration properties of isolated subagents and programmable multi-agent workflows. Do not build peer-to-peer agent teams in this project.

**Architecture:** Primary Codex compiles a user goal into a versioned workflow document. A deterministic local runtime persists the workflow, schedules bounded Codex workers, verifies their outputs, enforces scope and budget, and exposes recovery and control commands. A `StateStore` abstraction owns state transitions; its first backend uses one append-only event journal plus atomic snapshots and control-request files. Existing JSON, Markdown, patches, and JSONL files remain immutable or append-only audit artifacts.

**Tech stack:** Python 3 standard library, `argparse`, `subprocess`, `fcntl`, Git worktrees, JSON/JSONL, `unittest`.

---

## Product boundary

The orchestrator must provide:

- isolated Codex workers with explicit model, sandbox, scope, output, and budget contracts;
- durable task state and restart-safe scheduling;
- deterministic acceptance gates before dependents run;
- parallel, conditional, fan-out/fan-in, and bounded iterative workflows;
- budget reservation, live usage accounting, and safe retry rules;
- operator controls and a stable machine-readable status API;
- explicit, reviewable integration of writer worktrees.

The first release must not provide:

- peer-to-peer worker messaging or autonomous teams;
- an additional model call inside the launcher to create plans;
- automatic Sol escalation;
- automatic merge or patch application without supervisor approval;
- arbitrary shell evaluation of verification strings;
- cross-project daemons or a hosted service.

## Success criteria

The project may call itself an orchestrator only when all of these scenarios pass:

1. A read-only discovery task fans out into at least three bounded tasks and a reducer receives only accepted results.
2. A worker that exits zero but returns an invalid result or fails verification does not unblock dependents.
3. Killing the runtime during a batch and resuming it does not rerun already accepted tasks.
4. A workflow refuses to start tasks whose reservations exceed the remaining token budget.
5. A live task reaching its hard task limit is terminated and recorded with the observed overshoot.
6. Two independent writers can work in isolated worktrees; overlapping scopes are rejected before integration.
7. Pause, resume, cancel, retry, approve, reject, inspect, and watch work through stable CLI commands.
8. The toolbar can consume one stable JSON status document without reading internal worker files.

## Compatibility requirements

- Keep `scripts/delegate.py` as the public executable shim.
- Preserve existing `prepare`, `run`, `inspect`, `resume`, and `batch` behavior until equivalent workflow commands are verified.
- Continue accepting current task specs and manifests through a compatibility adapter.
- Preserve the standalone `RUN_DIR=<path>` and batch `BATCH_DIR=<path>` lines.
- Preserve Luna/Terra/Sol routing policy and the rule that Sol is read-only.
- Do not move or rewrite unrelated user changes in target repositories.

## Target file structure

```text
orchestrator_agent/
  __init__.py          Public package metadata
  cli.py               Argument parsing and command dispatch
  errors.py            User-facing exception types
  models.py            Enums and immutable workflow/task value objects
  schema.py            Task and workflow parsing and validation
  store.py             StateStore interface and journal-backed state
  artifacts.py         JSON/JSONL/Markdown/patch artifact management
  worker.py            Codex process lifecycle and progress reporting
  gates.py             Result, command, diff-scope, and approval gates
  budget.py            Reservations, live usage, and budget decisions
  scheduler.py         Durable scheduling and state transitions
  workflow.py          Workflow expansion, conditions, map/reduce, loops
  recovery.py          PID reconciliation and restart behavior
  integration.py       Worktree ownership, conflict checks, patch plans
  hooks.py             Bounded lifecycle hook execution
schemas/
  worker-result.schema.json  Final worker response contract for Codex CLI
scripts/
  delegate.py          Backward-compatible executable shim
tests/
  helpers.py           Fake Codex, repositories, clocks, and process helpers
  test_schema.py
  test_store.py
  test_worker.py
  test_artifacts.py
  test_gates.py
  test_budget.py
  test_scheduler.py
  test_workflow.py
  test_recovery.py
  test_integration.py
  test_cli.py
  test_status.py
  test_legacy.py
```

Do not split the current module in one large rewrite. Extract one responsibility at a time while the full legacy suite stays green.

Each workflow directory uses this control layout:

```text
workflow.json          Immutable validated workflow definition
state/
  events.jsonl         Canonical append-only state journal
  snapshot.json        Atomic materialized state and aggregates
  runtime.lock         Prevents two active schedulers
control/
  inbox/               Atomic pause/resume/cancel/retry/approval requests
  processed/           Consumed requests retained for audit
tasks/<task-id>/       Attempt artifacts and worker event streams
```

Only the active scheduler writes `state/events.jsonl`. Other CLI processes submit uniquely named request files through atomic rename into `control/inbox`; the scheduler validates each request, appends the resulting state event, then moves it to `control/processed`.

## Implementation checkpoint

The current implementation slice in `orchestrator_agent/` is covered by tests: version-1 schema validation, explicit state transitions, append-only journal replay with durable dynamic task additions, atomic snapshots, control requests, the shared Codex worker adapter, result/command/scope gates, accepted-only dependency scheduling, budget reservations with live hard-limit termination, status projection, recovery reconciliation, cancellation/pause/resume/retry runtime control, evidence-driven Luna-to-Terra escalation with an explicit reason and model limits, artifact manifests, bounded evidence-driven retries with attempt artifacts, accepted read-only result caching, deterministic condition nodes with branch blocking, independent check nodes, bounded map/reduce fan-out/fan-in, bounded `repeat_until`, managed writer worktrees with diff/patch manifests, read-only integration planning, explicitly approved patch application with post-merge verification commands, approval-gate controls, lifecycle hooks, toolbar/watch status output, and a no-worker workflow preview. Direct `run` and `resume` commands use the shared worker adapter.

Writer work is preserved as reviewable artifacts and never auto-merged. Goal decomposition remains a primary-Codex responsibility: `workflow prepare` validates and previews a supplied workflow without adding a hidden planner call. A dedicated black-box scenario suite and removal of duplicated compatibility internals remain follow-up hardening work.

### Persistence upgrade triggers

Do not implement a SQLite backend in the initial roadmap. Add `SQLiteStateStore` behind the same interface only after measurements demonstrate at least one of these conditions:

- more than one scheduler must intentionally write the same workflow;
- replay plus snapshot loading exceeds the agreed startup target on realistic workflows;
- workflow histories require indexed queries that cannot be served from bounded snapshots;
- cross-workflow transactional coordination becomes a product requirement.

Any future backend migration must be implemented by replaying versioned journal events into the new store and comparing the resulting materialized state before switching writers.

---

## Milestone 0: Freeze behavior and define contracts

### Task 0.1: Capture legacy behavior

**Files:**

- Create: `tests/test_legacy.py`
- Create: `tests/helpers.py`
- Modify: `tests/test_delegate.py`

- [ ] Move shared fake-Codex and temporary-repository setup into `tests/helpers.py` without changing assertions.
- [ ] Add black-box tests for the exact `RUN_DIR`, `BATCH_DIR`, heartbeat, exit-code, result-artifact, and interruption protocols.
- [ ] Add a test proving current manifests still default to Luna, read-only, shared isolation, two workers, one Terra, and zero Sol.
- [ ] Run `python3 -m unittest -v tests.test_legacy tests.test_delegate`.
- [ ] Expected: all existing and new compatibility tests pass.
- [ ] Commit: `test: freeze delegate runner compatibility`.

### Task 0.2: Define versioned workflow and task schemas

**Files:**

- Create: `orchestrator_agent/__init__.py`
- Create: `orchestrator_agent/errors.py`
- Create: `orchestrator_agent/models.py`
- Create: `orchestrator_agent/schema.py`
- Create: `tests/test_schema.py`

- [ ] Define task states:

  ```python
  class TaskState(str, Enum):
      PENDING = "pending"
      READY = "ready"
      RUNNING = "running"
      COMPLETED = "completed"
      VERIFYING = "verifying"
      ACCEPTED = "accepted"
      REJECTED = "rejected"
      BLOCKED = "blocked"
      RETRY_WAIT = "retry_wait"
      PAUSED = "paused"
      CANCELLED = "cancelled"
      INTERRUPTED = "interrupted"
      FAILED = "failed"
      BUDGET_EXHAUSTED = "budget_exhausted"
  ```

- [ ] Define workflow states: `created`, `running`, `paused`, `succeeded`, `failed`, `budget_exhausted`, `cancelled`, and `interrupted`.
- [ ] Define allowed state transitions as data and reject every transition not present in that table.
- [ ] Define workflow schema version `1` with top-level `name`, `cwd`, `budget`, `nodes`, and `hooks` fields.
- [ ] Define an agent node contract containing `id`, `kind`, `spec`, `model`, `model_reason`, `sandbox`, `isolation`, `depends_on`, `budget`, `retry`, `checks`, and `approval`.
- [ ] Keep existing `commands` as worker instructions. Introduce `checks` as the only machine-executed verification contract.
- [ ] Reject unknown schema versions, duplicate IDs, cycles, missing dependencies, Sol writers, invalid limits, and paths escaping the repository.
- [ ] Add table-driven tests for every validation failure and every allowed state transition.
- [ ] Run `python3 -m unittest -v tests.test_schema`.
- [ ] Expected: schema tests pass without importing `scripts/delegate.py`.
- [ ] Commit: `feat: define orchestrator workflow contracts`.

Example version-1 workflow:

```json
{
  "version": 1,
  "name": "review-auth",
  "cwd": "/absolute/repository",
  "budget": {
    "total_tokens": 100000,
    "max_workers": 2,
    "max_terra_tasks": 1,
    "max_sol_tasks": 0
  },
  "nodes": [
    {
      "id": "discover",
      "kind": "agent",
      "spec": "/absolute/specs/discover.json",
      "model": "luna",
      "sandbox": "read-only",
      "budget": {"reserve_tokens": 12000, "hard_tokens": 20000},
      "checks": [{"type": "result_schema"}]
    }
  ]
}
```

---

## Milestone 1: Build the durable control plane

### Task 1.1: Add the journal-backed StateStore

**Files:**

- Create: `orchestrator_agent/store.py`
- Create: `tests/test_store.py`

- [ ] Define a `StateStore` protocol so scheduling, gates, budget, status, and recovery do not depend on the persistence backend.
- [ ] Implement `JournalStateStore` with immutable `workflow.json`, canonical `state/events.jsonl`, and derived `state/snapshot.json`.
- [ ] Give every event `schema_version`, `seq`, `event_id`, `idempotency_key`, `type`, `timestamp`, and a bounded payload.
- [ ] Store timestamps in UTC ISO-8601 and monotonic durations separately; never compare monotonic values across processes.
- [ ] Hold `state/runtime.lock` for the scheduler lifetime so only one process may advance a workflow.
- [ ] Serialize journal writes inside the scheduler, append one complete JSON line, flush, and `fsync` before publishing the corresponding snapshot.
- [ ] Write snapshots with atomic replace, `fsync` the file and containing directory, and record `last_event_seq`; recovery must replay only later events.
- [ ] Implement `create_workflow`, `transition_task`, `append_event`, `record_usage`, `reserve_budget`, and `release_reservation` as validated event appends.
- [ ] Require callers to provide the expected previous state and event sequence when transitioning; stale operations must fail instead of overwriting newer state.
- [ ] Ignore only an incomplete final journal line after a crash. Treat malformed or non-contiguous events elsewhere as corruption and stop recovery with evidence.
- [ ] Add atomic control requests under `control/inbox`, including directory `fsync`; external CLI processes must never write the state journal directly.
- [ ] Test crash after journal append but before snapshot, crash during snapshot replacement, duplicate request IDs, competing runtime locks, concurrent snapshot readers, and recovery from an incomplete final line.
- [ ] Run `python3 -m unittest -v tests.test_store`.
- [ ] Expected: replay produces the same state as uninterrupted execution and a second scheduler cannot acquire the workflow.
- [ ] Commit: `feat: add durable journal state store`.

### Task 1.2: Separate artifacts from control state

**Files:**

- Create: `orchestrator_agent/artifacts.py`
- Modify: `scripts/delegate.py`
- Create: `tests/test_artifacts.py`

- [ ] Move atomic JSON, packet, event stream, result, feedback, usage, and patch path creation into `artifacts.py`.
- [ ] Keep model event payloads in `events.jsonl`; control state may contain only event type, counters, timestamps, and numeric usage.
- [ ] Add `artifact_manifest.json` containing relative paths, sizes, and SHA-256 hashes after finalization.
- [ ] Ensure a heartbeat or snapshot write failure cannot corrupt the canonical journal or terminate a healthy child.
- [ ] Run `python3 -m unittest -v tests.test_artifacts tests.test_delegate`.
- [ ] Expected: legacy tests remain green and artifact hashes are reproducible.
- [ ] Commit: `refactor: isolate delegate run artifacts`.

### Task 1.3: Extract worker lifecycle

**Files:**

- Create: `orchestrator_agent/worker.py`
- Modify: `scripts/delegate.py`
- Create: `tests/test_worker.py`

- [ ] Move `ProgressReporter`, process launch, termination, event parsing, thread ID extraction, and resume lifecycle into `worker.py`.
- [ ] Introduce `WorkerRequest` and `WorkerOutcome`; the outcome must distinguish transport completion from acceptance.
- [ ] Detect required Codex CLI capabilities at startup and fail clearly when `--json`, `--output-schema`, resume, or the selected sandbox mode is unavailable.
- [ ] Persist PID, process start time, attempt number, thread ID, and last heartbeat in the store.
- [ ] Preserve current foreground output exactly through the compatibility shim.
- [ ] Run `python3 -m unittest -v tests.test_worker tests.test_legacy tests.test_delegate`.
- [ ] Expected: no compatibility regression.
- [ ] Commit: `refactor: extract Codex worker lifecycle`.

---

## Milestone 2: Add deterministic acceptance gates

### Task 2.1: Validate structured results

**Files:**

- Create: `orchestrator_agent/gates.py`
- Create: `schemas/worker-result.schema.json`
- Create: `tests/test_gates.py`

- [ ] Define a JSON Schema for `result`, `evidence`, `changes`, `verification`, `risks`, and `recommended_next_action` and pass it to Codex through `--output-schema`.
- [ ] Implement `result_schema` requiring non-empty `result`, `evidence`, `risks`, and `recommended_next_action`; writer tasks additionally require `changes` and `verification`.
- [ ] Treat Markdown heading parsing as a legacy adapter only. Store new workflow results as JSON with an explicit schema version.
- [ ] Transition successful processes to `completed`, then `verifying`; never directly to `accepted`.
- [ ] Record each gate result with `gate_type`, `status`, `started_at`, `finished_at`, and bounded evidence.
- [ ] Add tests where exit zero returns empty, malformed, partially structured, and fully valid results.
- [ ] Run `python3 -m unittest -v tests.test_gates`.
- [ ] Expected: only valid results pass the schema gate.
- [ ] Commit: `feat: gate delegate results by schema`.

### Task 2.2: Add safe command checks

**Files:**

- Modify: `orchestrator_agent/schema.py`
- Modify: `orchestrator_agent/gates.py`
- Modify: `tests/test_gates.py`

- [ ] Accept command checks only as an argv array plus `cwd`, `timeout_seconds`, and an optional list of inherited environment keys.
- [ ] Do not use `shell=True`; reject redirections, pipelines, substitutions, or a string command.
- [ ] Start from a minimal environment containing platform-required variables, then copy only explicitly allowed inherited variables and orchestrator-owned values.
- [ ] Run checks in the task repository/worktree and save full output to an artifact while bounding journal evidence.
- [ ] Terminate the complete check process group on timeout.
- [ ] Classify failures as `verification_failed`, `verification_timeout`, or `verification_error`.
- [ ] Test success, nonzero exit, timeout, missing executable, large output, and attempted shell syntax.
- [ ] Run `python3 -m unittest -v tests.test_gates`.
- [ ] Expected: deterministic checks cannot invoke a shell implicitly.
- [ ] Commit: `feat: run bounded verification checks`.

### Task 2.3: Enforce repository scope

**Files:**

- Modify: `orchestrator_agent/gates.py`
- Modify: `orchestrator_agent/worker.py`
- Modify: `tests/test_gates.py`

- [ ] Capture base commit, tracked diff, untracked paths, and pre-existing dirty paths before a writer starts.
- [ ] Compare post-run changes with normalized allowed scope without treating pre-existing dirty files as worker changes.
- [ ] Reject symlink escapes, path traversal, writes under `.git`, and changes outside scope.
- [ ] Emit `scope_violation` with changed paths but no file contents.
- [ ] Verify that a scope violation leaves artifacts intact and blocks dependents.
- [ ] Run `python3 -m unittest -v tests.test_gates`.
- [ ] Expected: authorized changes pass and every out-of-scope fixture is rejected.
- [ ] Commit: `feat: enforce delegate write scope`.

### Task 2.4: Gate dependency scheduling

**Files:**

- Create: `orchestrator_agent/scheduler.py`
- Create: `tests/test_scheduler.py`

- [ ] Mark a task ready only when every dependency is `accepted`.
- [ ] Mark dependents blocked when a dependency is rejected, cancelled, or permanently failed.
- [ ] Pass only normalized, bounded accepted results to dependents.
- [ ] Enforce `max_workers`, shared-reader/shared-writer locking, and a single shared writer. Allow parallel worktree writers only when isolation is explicit and declared scopes are disjoint.
- [ ] Ensure an exit-zero-but-rejected task never unblocks its children.
- [ ] Run `python3 -m unittest -v tests.test_scheduler`.
- [ ] Expected: acceptance, not transport status, controls the graph.
- [ ] Commit: `feat: schedule only accepted dependencies`.

---

## Milestone 3: Enforce budget and retry policy

### Task 3.1: Add budget reservations

**Files:**

- Create: `orchestrator_agent/budget.py`
- Create: `tests/test_budget.py`
- Modify: `orchestrator_agent/scheduler.py`

- [ ] Require `reserve_tokens` and `hard_tokens` to be positive with reserve no greater than hard.
- [ ] Atomically reserve before transitioning a task to running.
- [ ] Refuse new work when `used + reserved + requested > workflow limit`.
- [ ] Release unused reservation at terminal attempt states.
- [ ] Preserve separate counters for input, cached input, output, and reasoning output tokens.
- [ ] Test concurrent reservation contention and prove the total reservation cannot exceed the workflow limit.
- [ ] Run `python3 -m unittest -v tests.test_budget`.
- [ ] Expected: exactly one competing reservation succeeds when only one fits.
- [ ] Commit: `feat: reserve workflow token budgets`.

### Task 3.2: Enforce live limits

**Files:**

- Modify: `orchestrator_agent/worker.py`
- Modify: `orchestrator_agent/budget.py`
- Modify: `tests/test_budget.py`

- [ ] Persist usage from each completed-turn event immediately enough for scheduler decisions without rewriting full event history.
- [ ] Stop a worker process group after observed usage reaches its task hard limit.
- [ ] Record `budget_exhausted`, last observed usage, reservation, and overshoot.
- [ ] Document that the hard limit is event-bounded rather than exact because tokens between usage events are not observable.
- [ ] Add soft workflow thresholds that stop new scheduling but allow active tasks to finish.
- [ ] Run `python3 -m unittest -v tests.test_budget tests.test_worker`.
- [ ] Expected: the fake worker is terminated after the first event crossing the limit.
- [ ] Commit: `feat: enforce live delegate token limits`.

### Task 3.3: Add evidence-driven retries

**Files:**

- Modify: `orchestrator_agent/schema.py`
- Modify: `orchestrator_agent/scheduler.py`
- Create: `tests/test_retry.py`

- [ ] Support `max_attempts`, defaulting to one.
- [ ] Classify failures as `transport`, `invalid_result`, `verification`, `scope`, `budget`, `permission`, or `integration`.
- [ ] Allow automatic retry only when new deterministic evidence is attached and policy permits that failure class.
- [ ] Use the existing Codex thread resume for correction attempts where possible.
- [ ] Require a non-empty escalation reason for Luna-to-Terra changes and count it against the Terra limit.
- [ ] Never escalate to Sol automatically; never make Sol writable.
- [ ] Stop retrying when two consecutive attempts produce the same normalized failure fingerprint.
- [ ] Run `python3 -m unittest -v tests.test_retry`.
- [ ] Expected: unchanged retries stop and evidence-bearing corrections can proceed once.
- [ ] Commit: `feat: add bounded evidence-driven retries`.

### Task 3.4: Cache safe read-only results

**Files:**

- Create: `orchestrator_agent/cache.py`
- Create: `tests/test_cache.py`
- Modify: `orchestrator_agent/scheduler.py`

- [ ] Cache only accepted read-only tasks in the first release.
- [ ] Fingerprint schema version, normalized spec, model, base commit, context file hashes, dependency result hashes, and applicable `AGENTS.md` hashes.
- [ ] Never reuse a cache entry after any fingerprint component changes.
- [ ] Record cache hits as accepted attempts with zero new token usage and a reference to the original artifacts.
- [ ] Add `--no-cache` for diagnosis.
- [ ] Run `python3 -m unittest -v tests.test_cache`.
- [ ] Expected: an identical second read-only task performs no Codex launch.
- [ ] Commit: `feat: cache accepted read-only tasks`.

---

## Milestone 4: Add recovery and operator control

### Task 4.1: Reconcile interrupted workflows

**Files:**

- Create: `orchestrator_agent/recovery.py`
- Create: `tests/test_recovery.py`
- Modify: `orchestrator_agent/scheduler.py`

- [ ] Validate a stored PID using process start metadata before treating it as the original worker.
- [ ] On restart, preserve accepted tasks, recover completed artifacts, mark missing active children interrupted, and release abandoned reservations.
- [ ] Do not automatically rerun interrupted writer attempts; require retry or resume policy.
- [ ] Reconcile worktrees and retain every changed worktree.
- [ ] Add a black-box test that kills the scheduler after one accepted task, restarts it, and asserts the accepted task is not launched again.
- [ ] Run `python3 -m unittest -v tests.test_recovery`.
- [ ] Expected: recovery is idempotent across two consecutive restarts.
- [ ] Commit: `feat: recover interrupted workflows`.

### Task 4.2: Add workflow control commands

**Files:**

- Create: `orchestrator_agent/cli.py`
- Modify: `scripts/delegate.py`
- Create: `tests/test_cli.py`

- [ ] Keep legacy commands and add `workflow start`, `workflow inspect`, `workflow watch`, `workflow pause`, `workflow resume`, and `workflow cancel`.
- [ ] Print exactly one `WORKFLOW_DIR=<absolute-path>` line when a workflow is created so another process can attach without scraping logs.
- [ ] Add `task retry`, `task approve`, and `task reject` with idempotency keys.
- [ ] Make `pause` stop scheduling new tasks without killing active tasks; provide an explicit `--stop-active` option.
- [ ] Make `cancel` terminate active children, block pending tasks, release reservations, and preserve artifacts.
- [ ] Return stable documented exit codes for success, invalid input, rejected workflow, budget exhaustion, interruption, and internal failure.
- [ ] Run `python3 -m unittest -v tests.test_cli tests.test_legacy`.
- [ ] Expected: legacy CLI output remains compatible and workflow controls are idempotent.
- [ ] Commit: `feat: add workflow operator controls`.

### Task 4.3: Publish a stable status API

**Files:**

- Create: `orchestrator_agent/status.py`
- Create: `tests/test_status.py`
- Modify: `orchestrator_agent/cli.py`

- [ ] Emit a versioned status object containing workflow state, active/pending/blocked counts, task summaries, budget used/reserved/limit, current blockers, and next runnable task.
- [ ] Keep status generation read-only and derive health without mutating control state.
- [ ] Add `workflow inspect --json` and newline-delimited updates through `workflow watch --jsonl`.
- [ ] Ensure no event payload, prompt, reasoning, or file content appears in the status API.
- [ ] Add golden-file tests for running, paused, failed, budget-exhausted, and succeeded workflows.
- [ ] Run `python3 -m unittest -v tests.test_status`.
- [ ] Expected: toolbar consumers require only the documented status schema.
- [ ] Commit: `feat: expose stable workflow status`.

---

## Milestone 5: Add programmable workflow primitives

### Task 5.1: Implement static workflow execution

**Files:**

- Create: `orchestrator_agent/workflow.py`
- Create: `tests/test_workflow.py`
- Modify: `orchestrator_agent/scheduler.py`

- [ ] Move existing DAG validation and readiness logic behind a `WorkflowRuntime` interface.
- [ ] Support `agent`, `check`, and `approval` node kinds first.
- [ ] Require each generated task to have a stable logical ID and expansion key.
- [ ] Persist expanded tasks before scheduling them so restart produces the same graph.
- [ ] Run `python3 -m unittest -v tests.test_workflow tests.test_scheduler`.
- [ ] Expected: current manifests execute through the new runtime via the compatibility adapter.
- [ ] Commit: `feat: execute versioned workflows`.

### Task 5.2: Add condition nodes

**Files:**

- Modify: `orchestrator_agent/schema.py`
- Modify: `orchestrator_agent/workflow.py`
- Modify: `tests/test_workflow.py`

- [ ] Support deterministic comparisons against normalized result fields through JSON Pointer: `exists`, `equals`, `not_equals`, `contains`, and numeric comparisons.
- [ ] Do not evaluate Python, JavaScript, templates, or shell expressions.
- [ ] Persist the selected branch and input value for audit and restart stability.
- [ ] Test true, false, missing value, wrong type, and resume after branch selection.
- [ ] Run `python3 -m unittest -v tests.test_workflow`.
- [ ] Expected: identical accepted inputs always select the same branch.
- [ ] Commit: `feat: add deterministic workflow conditions`.

### Task 5.3: Add bounded map and fan-in

**Files:**

- Modify: `orchestrator_agent/schema.py`
- Modify: `orchestrator_agent/workflow.py`
- Modify: `tests/test_workflow.py`

- [ ] Add a `map` node that reads an array from an accepted result using JSON Pointer.
- [ ] Require `max_items`, stable item keys, a task template, and per-item budget.
- [ ] Reject duplicate keys and truncate nothing silently.
- [ ] Add a `reduce` node that receives bounded accepted child summaries in stable key order.
- [ ] Permit partial reduction only when `allow_partial` is explicit and rejected child IDs are included.
- [ ] Test empty, single, many, duplicate, over-limit, partial, and restart cases.
- [ ] Run `python3 -m unittest -v tests.test_workflow`.
- [ ] Expected: fan-out expansion is deterministic and respects worker and token limits.
- [ ] Commit: `feat: add bounded workflow fan-out and fan-in`.

### Task 5.4: Add bounded iteration

**Files:**

- Modify: `orchestrator_agent/schema.py`
- Modify: `orchestrator_agent/workflow.py`
- Modify: `tests/test_workflow.py`

- [ ] Add `repeat_until` with `max_iterations`, a task template, a deterministic condition, and a normalized progress fingerprint.
- [ ] Stop successfully when the condition passes.
- [ ] Stop rejected when `max_iterations` is reached or two consecutive iterations produce the same progress fingerprint.
- [ ] Charge every iteration to the same node budget and the workflow budget.
- [ ] Persist every iteration as a separate attempt with stable IDs.
- [ ] Run `python3 -m unittest -v tests.test_workflow`.
- [ ] Expected: no loop can run without both an iteration cap and a no-progress stop.
- [ ] Commit: `feat: add bounded iterative workflows`.

---

## Milestone 6: Make writer integration explicit and safe

### Task 6.1: Track complete writer changes

**Files:**

- Modify: `orchestrator_agent/artifacts.py`
- Create: `orchestrator_agent/integration.py`
- Create: `tests/test_integration.py`

- [ ] Produce `changes.json` listing modified, deleted, renamed, and untracked files with hashes.
- [ ] Preserve binary tracked changes and untracked files as auditable artifacts without silently omitting either.
- [ ] Treat submodules, nested repositories, symlinks leaving the worktree, and `.git` changes as unsupported and reject them.
- [ ] Keep changed worktrees until integration or explicit cleanup.
- [ ] Run `python3 -m unittest -v tests.test_integration`.
- [ ] Expected: fixtures cover text, binary, rename, deletion, and untracked files.
- [ ] Commit: `feat: capture complete writer change sets`.

### Task 6.2: Build an integration plan

**Files:**

- Modify: `orchestrator_agent/integration.py`
- Modify: `tests/test_integration.py`

- [ ] Compare accepted writer scopes and actual changed paths before integration.
- [ ] Reject overlapping paths unless a deterministic ordering dependency already exists.
- [ ] Validate that the target checkout still matches the expected base or can accept the change without conflicts.
- [ ] Create `integration-plan.json` with order, bases, changed paths, checks, conflicts, and required approvals.
- [ ] Do not mutate the target checkout while planning.
- [ ] Run `python3 -m unittest -v tests.test_integration`.
- [ ] Expected: clean independent writers produce a plan and conflicts produce evidence without mutation.
- [ ] Commit: `feat: plan writer integration`.

### Task 6.3: Apply approved integration

**Files:**

- Modify: `orchestrator_agent/integration.py`
- Modify: `orchestrator_agent/cli.py`
- Modify: `tests/test_integration.py`

- [ ] Add `workflow integrate --plan ... --approval ...`.
- [ ] Verify plan hashes, repository state, and approval before applying anything.
- [ ] Apply one writer at a time, stop on first conflict, and never discard already applied user changes.
- [ ] Run workflow-level verification after all writers are applied.
- [ ] Record applied commits or patches and final repository state.
- [ ] Require explicit authorization for rollback; do not implement automatic destructive rollback.
- [ ] Run `python3 -m unittest -v tests.test_integration`.
- [ ] Expected: unauthorized or stale plans make no changes; approved clean plans pass final verification.
- [ ] Commit: `feat: integrate approved writer results`.

---

## Milestone 7: Compile goals without adding planner calls

### Task 7.1: Add workflow preparation and preview

**Files:**

- Modify: `orchestrator_agent/cli.py`
- Modify: `orchestrator_agent/schema.py`
- Create: `tests/test_prepare_workflow.py`

- [ ] Add `workflow prepare --file ...` that validates, expands static nodes, estimates reservations, reports model routing, and prints the planned phases without launching Codex.
- [ ] Warn on unnecessary parallelism, duplicate discovery, Terra without evidence, Sol tasks, writer overlap, missing checks, and unbounded expansion.
- [ ] Add `--json` for supervisor consumption.
- [ ] Run `python3 -m unittest -v tests.test_prepare_workflow`.
- [ ] Expected: invalid or over-budget workflows fail before any worker starts.
- [ ] Commit: `feat: preview orchestrator workflows`.

### Task 7.2: Teach the skill to compile workflows

**Files:**

- Modify: `SKILL.md`
- Modify: `README.md`
- Modify: `agents/openai.yaml`
- Modify: `tests/test_delegate.py`

- [ ] Instruct primary Codex to solve cheap local work directly and compile only genuinely independent or context-heavy work into version-1 workflows.
- [ ] Keep Luna as the default, Terra evidence-gated, and Sol read-only and opt-in.
- [ ] Require preview before starting workflows containing writers, Sol, more than two workers, or more than one retry.
- [ ] Document that planning happens in the primary context and the runtime makes no hidden planning call.
- [ ] Keep `SKILL.md` concise by moving the full workflow schema to a direct reference file only if necessary.
- [ ] Run the skill validator and contract tests.
- [ ] Expected: metadata and instructions describe the implemented behavior, not future features.
- [ ] Commit: `docs: route work through verified workflows`.

---

## Milestone 8: Add policy hooks and toolbar adapter

### Task 8.1: Add bounded lifecycle hooks

**Files:**

- Create: `orchestrator_agent/hooks.py`
- Create: `tests/test_hooks.py`
- Modify: `orchestrator_agent/schema.py`

- [ ] Support `task_created`, `task_completed`, `task_rejected`, `worker_idle`, `budget_threshold`, `before_integration`, and `after_integration` hooks.
- [ ] Pass versioned JSON through stdin and accept no model reasoning or event payloads.
- [ ] Use argv arrays, timeouts, output limits, and no implicit shell.
- [ ] Define exit `0` as allow, exit `2` as block with bounded feedback, and other exits as hook failure according to explicit fail-open/fail-closed policy.
- [ ] Prevent recursive hook-triggered workflow mutations.
- [ ] Run `python3 -m unittest -v tests.test_hooks`.
- [ ] Expected: blocking hooks prevent the corresponding transition event from being appended.
- [ ] Commit: `feat: add orchestrator lifecycle hooks`.

### Task 8.2: Add a local toolbar formatter

**Files:**

- Create: `scripts/delegate-status.py`
- Create: `tests/test_toolbar.py`
- Modify: `README.md`

- [ ] Read only the stable status API or a cached status snapshot; never scan event logs on each refresh.
- [ ] Render compact text containing workflow state, active/pending/blocked counts, current model/task, and used/reserved budget.
- [ ] Complete within 100 ms on a workflow containing 1,000 completed tasks by reading aggregate snapshot fields rather than replaying the journal.
- [ ] Emit no ANSI escapes unless explicitly requested.
- [ ] Add fixture-based tests for narrow and wide output.
- [ ] Run `python3 -m unittest -v tests.test_toolbar`.
- [ ] Expected: formatter output is stable and requires no AI call.
- [ ] Commit: `feat: expose compact orchestrator toolbar status`.

---

## Milestone 9: Final evaluation and migration

### Task 9.1: Run orchestrator scenario tests

**Files:**

- Create: `tests/test_orchestrator_scenarios.py`
- Create: `tests/fixtures/workflows/`

- [ ] Implement the eight success-criteria scenarios from this plan as black-box tests.
- [ ] Record worker launch count, accepted tasks, retries, cache hits, and token usage for every scenario.
- [ ] Assert no scenario starts Sol automatically.
- [ ] Assert rejected results never appear as accepted dependency input.
- [ ] Run `python3 -m unittest -v tests.test_orchestrator_scenarios`.
- [ ] Expected: all scenarios pass using the fake Codex executable.
- [ ] Commit: `test: verify orchestrator end-to-end scenarios`.

### Task 9.2: Retire duplicate legacy internals

**Files:**

- Modify: `scripts/delegate.py`
- Modify: `orchestrator_agent/cli.py`
- Modify: `tests/test_legacy.py`

- [ ] Replace legacy implementations with adapters to the package after behavior parity is proven.
- [ ] Keep legacy commands documented for at least one release cycle.
- [ ] Remove only code proven unreachable by coverage and targeted tests.
- [ ] Run the complete verification suite.
- [ ] Expected: the executable shim contains only import, dispatch, and exit handling.
- [ ] Commit: `refactor: route legacy commands through orchestrator core`.

## Full verification after every milestone

Run:

```bash
.venv/bin/python -m unittest -v
.venv/bin/python -m py_compile scripts/delegate.py orchestrator_agent/*.py tests/*.py
.venv/bin/python "${CODEX_HOME:-$HOME/.codex}/skills/.system/skill-creator/scripts/quick_validate.py" .
git diff --check
```

Expected:

- all tests pass;
- compilation produces no errors;
- skill validation reports `Skill is valid!`;
- `git diff --check` prints nothing;
- no repository documentation contains references to private development workflows or unrelated frameworks.

## Review gates between milestones

Do not start the next milestone until the current milestone meets its exit gate:

| Milestone | Required exit evidence |
| --- | --- |
| 0 | Legacy protocol tests and versioned schema tests pass |
| 1 | Concurrent state transitions and artifact lifecycle are durable |
| 2 | Exit zero cannot bypass schema, command, or scope gates |
| 3 | Reservations prevent overscheduling and retries cannot loop unchanged |
| 4 | Kill-and-resume and all operator controls pass black-box tests |
| 5 | Condition, map/reduce, and bounded iteration survive restart |
| 6 | Integration plans are non-mutating and application requires approval |
| 7 | Primary Codex can preview and launch a workflow without hidden planner calls |
| 8 | Hooks are bounded and toolbar status is fast and token-free |
| 9 | All orchestrator scenarios and legacy compatibility tests pass |

## Recommended implementation sequence

Implement one milestone per branch or review checkpoint. Within a milestone, use the listed commits and keep each commit independently testable. Do not parallelize write-capable implementation across milestones because the early package extraction and state model affect every later subsystem.

The first useful release boundary is Milestone 4: durable verified batches with budget and recovery. The first release that satisfies the orchestrator definition is Milestone 6: programmable workflows plus safe writer integration. Milestones 7–9 make it practical, extensible, and ready to replace the current static-batch workflow.
