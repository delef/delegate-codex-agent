# Live Delegate Status Implementation Plan

**Goal:** Report useful live progress for `run`, `resume`, and batch children without extra AI calls or leaking event payloads.

**Architecture:** Add a thread-safe in-process reporter to `scripts/delegate.py`. It incrementally consumes JSONL event metadata, atomically refreshes `status.json`, prints attributed heartbeat lines, and stops before terminal status is written. `inspect` derives health from persisted timestamps without mutating artifacts.

**Tech stack:** Python standard library, `threading`, monotonic clocks, JSONL, `unittest`.

---

### Task 1: Progress primitives

**Files:**
- Modify: `scripts/delegate.py`
- Test: `tests/test_delegate.py`

- [ ] Add failing unit tests for `heartbeat_seconds`, incremental event metadata/usage, payload privacy, and `health_from_status`.
- [ ] Run:

  ```bash
  .venv/bin/python -m unittest -v tests.test_delegate.ProgressStatusTests
  ```

  Expected: failures because the progress API does not exist.

- [ ] Add constants `DEFAULT_HEARTBEAT_SECONDS = 15.0`, `ACTIVE_IDLE_SECONDS = 60`, and `STALE_HEARTBEAT_SECONDS = 45`.
- [ ] Add pure functions `heartbeat_seconds(value: str | None = None) -> float`, `usage_from_event(event: dict[str, Any]) -> dict[str, int]`, and `health_from_status(status: dict[str, Any], now: dt.datetime | None = None) -> str`.
- [ ] Add `ProgressReporter.start`, `set_phase`, `record_event`, and `finish` methods with the lifecycle defined in the design spec.

- [ ] Refactor `usage_from_events` to aggregate `usage_from_event` so final and live token accounting use identical rules.
- [ ] Re-run the focused tests; expected: all pass.
- [ ] Commit: `Add delegate progress reporter`.

### Task 2: Run and resume lifecycle integration

**Files:**
- Modify: `scripts/delegate.py`
- Test: `tests/test_delegate.py`

- [ ] Add a fake Codex integration test that sleeps after emitting an event. Set `DELEGATE_HEARTBEAT_SECONDS=0.05`, launch with `Popen`, and assert before completion that:

  ```python
  status["status"] == "running"
  status["phase"] == "model_running"
  status["child_alive"] is True
  status["event_count"] >= 1
  "payload" not in json.dumps(status)
  ```

- [ ] Assert captured output contains `DELEGATE_HEARTBEAT`, elapsed/idle/event/token fields, and the run label.
- [ ] Run the new integration test and verify it fails for missing heartbeat fields.
- [ ] Replace direct running-state writes in `execute_run` with `ProgressReporter`: start at `waiting_for_lock`, switch to `model_running`, record each stdout line, switch to `finalizing`, then call `finish` after stopping the heartbeat thread.
- [ ] Apply the same reporter lifecycle to `command_resume`; keep existing signal cleanup and result checks unchanged.
- [ ] Add a regression test proving a late heartbeat cannot overwrite terminal `succeeded`, `failed`, or `interrupted` status.
- [ ] Run the lifecycle tests; expected: all pass.
- [ ] Commit: `Report live delegate progress`.

### Task 3: Inspect health and batch attribution

**Files:**
- Modify: `scripts/delegate.py`
- Test: `tests/test_delegate.py`

- [ ] Add failing tests for `inspect` classifications: `active`, `silent`, `stale`, and `finished`.
- [ ] Change `command_inspect` to load the status object, add derived `health` only to the printed copy, and leave `status.json` byte-for-byte unchanged.
- [ ] Pass `task_id` through `command_batch.task_args`; use the spec name for standalone runs and manifest id for batch children.
- [ ] Add a batch integration assertion matching lines such as:

  ```text
  DELEGATE_HEARTBEAT task=read-a phase=model_running child_alive=true elapsed=1s idle=1s events=2 tokens=120
  ```

- [ ] Preserve the existing standalone `RUN_DIR=<path>` output format.
- [ ] Run focused inspect and batch tests; expected: all pass.
- [ ] Commit: `Expose delegate health and task labels`.

### Task 4: Skill policy, documentation, and verification

**Files:**
- Modify: `SKILL.md`
- Modify: `README.md`
- Test: `tests/test_delegate.py`

- [ ] Document that supervisors should treat fresh heartbeat plus a live child as progress, use `inspect` for diagnosis, and never classify `silent` alone as hung.
- [ ] Keep `SKILL.md` at or below 650 words and add contract assertions for `DELEGATE_HEARTBEAT`, `silent`, and `stale`.
- [ ] Run full verification:

  ```bash
  .venv/bin/python -m unittest -v tests/test_delegate.py
  .venv/bin/python -m py_compile scripts/delegate.py tests/test_delegate.py
  .venv/bin/python "${CODEX_HOME:-$HOME/.codex}/skills/.system/skill-creator/scripts/quick_validate.py" .
  test "$(wc -w < SKILL.md)" -le 650
  git diff --check
  ```

- [ ] Review the final diff for event-payload leakage, heartbeat/thread races, terminal-state overwrites, and unrelated changes.
- [ ] Commit and push the completed implementation.
