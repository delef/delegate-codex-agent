# Delegate Live Status Design

## Goal

Make a foreground supervisor distinguish a healthy long-running delegate from a stalled launcher without extra AI calls or exposing model reasoning. Cover `run`, `resume`, and children started by `batch`.

## Chosen approach

Run one lightweight heartbeat thread inside each active delegate launcher. Keep event-derived progress in memory, persist a bounded snapshot atomically, and emit one machine-readable foreground line every 15 seconds. Do not add a daemon or infer progress from generated text.

Alternative approaches were rejected:

- Event-only updates falsely look stuck while a model or tool is silent.
- A separate monitor process adds lifecycle, cleanup, and PID ownership complexity without enough benefit.

## Progress model

Introduce a thread-safe `ProgressReporter` shared by normal and resumed runs. It owns timing, phase, event counters, incremental usage, and status persistence. It must never reread the complete `events.jsonl` during a run.

Phases:

- `preparing`: packet or feedback and run artifacts are being prepared.
- `waiting_for_lock`: launcher is alive but has not acquired the repository lock.
- `model_running`: child Codex process is active.
- `finalizing`: child exited and result/status artifacts are being completed.
- Terminal state remains the existing `succeeded`, `failed`, or `interrupted` status.

Running `status.json` adds:

```json
{
  "phase": "model_running",
  "task_id": "think",
  "child_alive": true,
  "heartbeat_at": "2026-07-13T12:00:00+00:00",
  "last_event_at": "2026-07-13T11:59:48+00:00",
  "last_event_type": "item.completed",
  "event_count": 12,
  "elapsed_seconds": 94,
  "idle_seconds": 12,
  "usage": {"input_tokens": 18400, "output_tokens": 920, "total_tokens": 19320}
}
```

`child_alive` is `null` before process launch, `true` while `poll()` is `None`, and `false` after exit. Only event type and numeric usage are copied from JSONL; event payloads, messages, and reasoning are never copied.

## Foreground protocol

Emit a line immediately after startup state is available and then every 15 seconds:

```text
DELEGATE_HEARTBEAT task=think phase=model_running child_alive=true elapsed=94s idle=12s events=12 tokens=19320
```

For a single run, `task` uses the spec name. Batch passes its manifest task id separately so concurrent output can be attributed correctly. Preserve the existing standalone `RUN_DIR=...` line for compatibility.

The interval is a constant in production and may be overridden by `DELEGATE_HEARTBEAT_SECONDS` in tests. Invalid or non-positive overrides fall back to 15 seconds.

## Persistence and concurrency

- Protect mutable reporter state with one lock.
- Use monotonic time for elapsed and idle durations; use UTC wall time only for persisted timestamps.
- Update counters in memory for every stdout line. Parse malformed JSON as an `unparsed` event without stopping the delegate.
- Persist on phase changes and heartbeat ticks, not on every event, preventing excessive atomic file replacement.
- Stop and join the heartbeat thread before writing terminal status so it cannot overwrite completion.
- A heartbeat persistence error emits one concise diagnostic and does not terminate a healthy Codex child. Existing mandatory lifecycle writes retain their current failure behavior.

## Inspect health

`inspect` reads `status.json` and adds a derived `health` field to displayed output without mutating the artifact:

- `active`: running, heartbeat age at most 45 seconds, and event idle time below 60 seconds.
- `silent`: heartbeat is fresh and the child is alive, but no event arrived for at least 60 seconds.
- `stale`: running status with heartbeat older than 45 seconds, or no heartbeat in a legacy running status.
- `finished`: terminal status.

`silent` is informational and never triggers termination. No health state automatically kills, retries, or upgrades a model.

## Batch behavior

Each child heartbeat includes `task_id`. Individual child `status.json` files contain full progress. `batch-status.json` remains the scheduler summary in this iteration; it is not updated by worker heartbeat threads, avoiding concurrent mutation of scheduler state. The supervisor can use the emitted `RUN_DIR` plus `inspect` for detailed diagnosis.

## Tests

- Unit-test incremental event parsing, usage totals, phase snapshots, privacy, and health classification.
- Integration-test a silent fake Codex child with a short test interval; assert at least one heartbeat appears before completion and running status contains fresh progress.
- Assert batch heartbeat lines carry the manifest task id.
- Assert the terminal status cannot be overwritten by a late heartbeat.
- Exercise both `run` and `resume` through the shared reporter.
- Run the full existing suite, Python compilation, skill validation, and whitespace checks.

## Non-goals

- Streaming chain-of-thought or event payload text.
- Estimating percent complete or remaining time.
- Automatic timeout, cancellation, retry, or model escalation.
- A persistent external monitoring daemon.
