"""Codex process adapter with bounded, metadata-only event accounting."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import signal
import subprocess
from typing import Callable, Sequence

from .errors import OrchestratorError
from .store import USAGE_FIELDS


def _empty_usage() -> dict[str, int]:
    return {field: 0 for field in USAGE_FIELDS}


def _usage_from_event(event: object) -> dict[str, int]:
    usage = _empty_usage()
    if not isinstance(event, dict) or event.get("type") != "turn.completed":
        return usage
    raw = event.get("usage")
    if not isinstance(raw, dict):
        return usage
    for field in ("input_tokens", "cached_input_tokens", "output_tokens", "reasoning_output_tokens"):
        value = raw.get(field, 0)
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            usage[field] = value
    usage["uncached_input_tokens"] = max(
        0, usage["input_tokens"] - usage["cached_input_tokens"],
    )
    usage["total_tokens"] = usage["input_tokens"] + usage["output_tokens"]
    return usage


def _add_usage(target: dict[str, int], delta: dict[str, int]) -> None:
    for field in USAGE_FIELDS:
        target[field] += delta[field]


def terminate_process(process: subprocess.Popen[str], grace_seconds: float = 3.0) -> None:
    """Terminate a worker process group, escalating to SIGKILL if needed."""
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
        process.wait(timeout=grace_seconds)
    except ProcessLookupError:
        return
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()


@dataclass(frozen=True)
class WorkerRequest:
    """A fully specified launch request; no shell command is accepted."""

    binary: tuple[str, ...]
    cwd: Path
    model: str
    sandbox: str
    prompt: str
    result_path: Path
    events_path: Path
    output_schema_path: Path | None = None
    resume_thread_id: str | None = None
    hard_tokens: int | None = None


@dataclass
class WorkerOutcome:
    exit_code: int
    result_path: Path
    events_path: Path
    thread_id: str | None = None
    usage: dict[str, int] = field(default_factory=_empty_usage)
    event_count: int = 0
    malformed_event_count: int = 0
    budget_exhausted: bool = False


def build_command(request: WorkerRequest) -> list[str]:
    command = list(request.binary) + ["exec"]
    if request.resume_thread_id:
        command += ["resume", "-m", request.model, "--json"]
        if request.output_schema_path is not None:
            command += ["--output-schema", str(request.output_schema_path)]
        command += ["-o", str(request.result_path), request.resume_thread_id, "-"]
        return command
    command += [
        "-C", str(request.cwd), "-m", request.model, "-s", request.sandbox,
        "--json",
    ]
    if request.output_schema_path is not None:
        command += ["--output-schema", str(request.output_schema_path)]
    command += ["-o", str(request.result_path), "-"]
    return command


def check_capabilities(binary: Sequence[str]) -> set[str]:
    """Return supported CLI markers from `codex exec --help` output."""
    command = list(binary) + ["exec", "--help"]
    completed = subprocess.run(
        command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        check=False,
    )
    if completed.returncode != 0:
        raise OrchestratorError(
            f"cannot inspect Codex CLI capabilities (exit {completed.returncode})"
        )
    output = completed.stdout
    required = {"json": "--json", "output_schema": "--output-schema", "sandbox": "--sandbox"}
    return {name for name, marker in required.items() if marker in output}


def run_worker(
    request: WorkerRequest,
    *,
    on_process: Callable[[subprocess.Popen[str]], None] | None = None,
    on_line: Callable[[str], None] | None = None,
    on_usage: Callable[[dict[str, int]], None] | None = None,
) -> WorkerOutcome:
    """Run one Codex process and persist its JSONL stream.

    The adapter intentionally returns transport facts only. Acceptance gates own
    result validation and are applied by the scheduler after this function.
    """
    request.result_path.parent.mkdir(parents=True, exist_ok=True)
    request.events_path.parent.mkdir(parents=True, exist_ok=True)
    process: subprocess.Popen[str] | None = None
    usage = _empty_usage()
    event_count = 0
    malformed = 0
    thread_id: str | None = None
    budget_exhausted = False
    try:
        process = subprocess.Popen(
            build_command(request),
            cwd=request.cwd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
        if on_process is not None:
            on_process(process)
        assert process.stdin is not None and process.stdout is not None
        process.stdin.write(request.prompt)
        process.stdin.close()
        with process.stdout, request.events_path.open("w", encoding="utf-8") as events:
            for line in process.stdout:
                events.write(line)
                events.flush()
                if on_line is not None:
                    on_line(line)
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    malformed += 1
                    continue
                if not isinstance(event, dict):
                    malformed += 1
                    continue
                event_count += 1
                if event.get("type") == "thread.started" and isinstance(event.get("thread_id"), str):
                    thread_id = event["thread_id"]
                delta = _usage_from_event(event)
                _add_usage(usage, delta)
                if on_usage is not None and delta["total_tokens"]:
                    on_usage(delta)
                if request.hard_tokens is not None and usage["total_tokens"] >= request.hard_tokens:
                    budget_exhausted = True
                    terminate_process(process)
                    break
        exit_code = process.wait()
    except BaseException:
        if process is not None:
            terminate_process(process)
        raise
    return WorkerOutcome(
        exit_code=exit_code,
        result_path=request.result_path,
        events_path=request.events_path,
        thread_id=thread_id,
        usage=usage,
        event_count=event_count,
        malformed_event_count=malformed,
        budget_exhausted=budget_exhausted,
    )
