"""eBPF batch uploader — POST /api/v1/runs/{run_id}/telemetry/ebpf.

Builds EbpfBatch payloads from collected events and submits them to the
TraceKV platform API (127.0.0.1:8010 by default).  Handles automatic
splitting into batches of at most MAX_EVENTS_PER_BATCH=2000 events,
monotonic→UTC timestamp conversion, and duplicate-safe retry (same
batch_id + same content → server returns duplicate=true, not an error).

This module is intentionally dependency-free (stdlib only).  It is designed
to run as a local trusted process alongside the platform server on the same
machine; the server only listens on 127.0.0.1.

Platform interface reference:
  contracts/platform-interface-v1.json (after platform branch merge)
  POST /api/v1/runs/{run_id}/telemetry/ebpf → 201 (new) / 200 (duplicate)

Author: 邓智文 (DAGER)
Project: TraceKV — collector/ebpf_uploader.py
"""

from __future__ import annotations

import http.client
import json
import uuid
from typing import Any

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_EVENTS_PER_BATCH = 2000   # platform hard limit
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8010
_TELEMETRY_PATH = "/api/v1/runs/{run_id}/telemetry/ebpf"

VALID_METRICS = frozenset({
    "wakeup_to_run_latency",
    "runnable_wait",
    "on_cpu",
    "blocked",
    "direct_reclaim",
    "major_fault",
    "minor_fault",
})

VALID_SCOPES = frozenset({"target_threads", "comm_match", "system"})


# ---------------------------------------------------------------------------
# Batch builder
# ---------------------------------------------------------------------------

def build_ebpf_batch(
    run_id: str,
    collector_id: str,
    scope: str,
    clock: dict[str, int],
    window_start_ns: int,
    window_end_ns: int,
    events: list[dict[str, Any]],
    target_tids: list[int] | None = None,
    target_comm: str | None = None,
    dropped_events: int | None = None,
    batch_id: str | None = None,
    synthetic: bool = False,
) -> dict[str, Any]:
    """Build a single EbpfBatch payload dict.

    Args:
        run_id:           Experiment identifier.
        collector_id:     Stable string identifying this collector instance
                          (e.g. ``"dager-sched-latency"``).
        scope:            One of ``"target_threads"``, ``"comm_match"``,
                          or ``"system"``.
        clock:            Dict with ``wall_ns`` (CLOCK_REALTIME) and
                          ``monotonic_ns`` (CLOCK_MONOTONIC) captured at the
                          same instant.  Used by the platform to convert
                          event ``timestamp_ns`` to UTC.
        window_start_ns:  CLOCK_MONOTONIC ns for start of observation window.
        window_end_ns:    CLOCK_MONOTONIC ns for end of observation window.
        events:           List of event dicts.  Each must have at minimum:
                          ``metric`` (one of :data:`VALID_METRICS`),
                          ``timestamp_ns`` (int, CLOCK_MONOTONIC),
                          ``value`` (int, nanoseconds for duration metrics),
                          ``tid`` (int).
        target_tids:      Required when scope == ``"target_threads"``.
                          Every event's ``tid`` must be in this set.
        target_comm:      Required when scope == ``"comm_match"``.
        dropped_events:   Known number of events dropped by the kernel BPF map
                          overflow.  Pass ``None`` (not 0) if unknown.
        batch_id:         Stable UUID string for this batch.  Auto-generated
                          if not provided.  Use the same ID on retry.
        synthetic:        Set ``True`` for test/example batches.

    Returns:
        Dict ready to be JSON-serialised and POSTed to the platform API.

    Raises:
        ValueError: On invalid scope, missing target_tids for
                    target_threads scope, or unknown metric names.
    """
    if scope not in VALID_SCOPES:
        raise ValueError(f"scope must be one of {VALID_SCOPES}, got {scope!r}")
    if scope == "target_threads" and not target_tids:
        raise ValueError("target_tids required when scope='target_threads'")
    if scope == "comm_match" and not target_comm:
        raise ValueError("target_comm required when scope='comm_match'")
    if not clock.get("wall_ns") or not clock.get("monotonic_ns"):
        raise ValueError("clock must contain non-zero wall_ns and monotonic_ns")
    if window_end_ns <= window_start_ns:
        raise ValueError("window_end_ns must be greater than window_start_ns")
    if len(events) > MAX_EVENTS_PER_BATCH:
        raise ValueError(
            f"events count {len(events)} exceeds MAX_EVENTS_PER_BATCH={MAX_EVENTS_PER_BATCH}. "
            f"Use split_into_batches() first."
        )

    # Validate event structure
    target_tid_set = set(target_tids) if target_tids else None
    for i, ev in enumerate(events):
        metric = ev.get("metric")
        if metric not in VALID_METRICS:
            raise ValueError(
                f"Event {i}: metric={metric!r} is not one of {sorted(VALID_METRICS)}"
            )
        ts = ev.get("timestamp_ns")
        if not isinstance(ts, int) or ts < 0:
            raise ValueError(f"Event {i}: timestamp_ns must be a non-negative int")
        if not (window_start_ns <= ts <= window_end_ns):
            raise ValueError(
                f"Event {i}: timestamp_ns={ts} outside window "
                f"[{window_start_ns}, {window_end_ns}]"
            )
        if scope == "target_threads" and target_tid_set is not None:
            tid = ev.get("tid")
            if tid not in target_tid_set:
                raise ValueError(
                    f"Event {i}: tid={tid} not in target_tids={target_tids}"
                )

    batch: dict[str, Any] = {
        "run_id": run_id,
        "batch_id": batch_id or str(uuid.uuid4()),
        "collector_id": collector_id,
        "clock": {
            "wall_ns": clock["wall_ns"],
            "monotonic_ns": clock["monotonic_ns"],
        },
        "window_start_ns": window_start_ns,
        "window_end_ns": window_end_ns,
        "scope": scope,
        "events": events,
        "dropped_events": dropped_events,  # None = unknown; 0 requires evidence
        "synthetic": synthetic,
    }
    if target_tids is not None:
        batch["target_tids"] = target_tids
    if target_comm is not None:
        batch["target_comm"] = target_comm
    return batch


def split_into_batches(
    run_id: str,
    collector_id: str,
    scope: str,
    clock: dict[str, int],
    window_start_ns: int,
    window_end_ns: int,
    events: list[dict[str, Any]],
    target_tids: list[int] | None = None,
    target_comm: str | None = None,
    dropped_events: int | None = None,
    synthetic: bool = False,
) -> list[dict[str, Any]]:
    """Split a large event list into multiple EbpfBatch payloads.

    Each batch gets a unique ``batch_id``.  ``dropped_events`` is only
    recorded in the first batch; subsequent batches set it to ``None``.

    Returns a list of batch dicts, each ready for :func:`push_batch`.
    """
    batches = []
    for start in range(0, max(1, len(events)), MAX_EVENTS_PER_BATCH):
        chunk = events[start:start + MAX_EVENTS_PER_BATCH]
        dropped = dropped_events if start == 0 else None
        batches.append(
            build_ebpf_batch(
                run_id=run_id,
                collector_id=collector_id,
                scope=scope,
                clock=clock,
                window_start_ns=window_start_ns,
                window_end_ns=window_end_ns,
                events=chunk,
                target_tids=target_tids,
                target_comm=target_comm,
                dropped_events=dropped,
                synthetic=synthetic,
            )
        )
    return batches


# ---------------------------------------------------------------------------
# HTTP client
# ---------------------------------------------------------------------------

def push_batch(
    batch: dict[str, Any],
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    timeout: float = 10.0,
) -> dict[str, Any]:
    """HTTP POST one EbpfBatch to the platform API.

    Args:
        batch:   Payload dict built by :func:`build_ebpf_batch`.
        host:    Platform host (default ``127.0.0.1``).
        port:    Platform port (default ``8010``).
        timeout: Socket timeout in seconds.

    Returns:
        Response JSON dict from the server.  Key fields:
        - ``batch_id``: echoed back
        - ``received_at_utc``: ISO 8601 timestamp
        - ``event_count``: number of events accepted
        - ``duplicate``: True if same batch_id+content was already received

    Raises:
        ConnectionRefusedError: If the platform server is not running.
        ValueError:             On non-2xx HTTP status (error details in message).
        json.JSONDecodeError:   If the server response is not valid JSON.
    """
    run_id = batch["run_id"]
    path = _TELEMETRY_PATH.format(run_id=run_id)
    body = json.dumps(batch).encode("utf-8")

    conn = http.client.HTTPConnection(host, port, timeout=timeout)
    try:
        conn.request(
            "POST", path, body=body,
            headers={"Content-Type": "application/json"},
        )
        resp = conn.getresponse()
        resp_body = resp.read().decode("utf-8")
    finally:
        conn.close()

    if resp.status == 409:
        raise ValueError(
            f"Conflict (409): batch_id={batch['batch_id']!r} already exists "
            f"with different content. Use a new batch_id. Response: {resp_body}"
        )
    if resp.status not in (200, 201):
        raise ValueError(
            f"HTTP {resp.status} from platform API at {path}. "
            f"Response: {resp_body}"
        )

    return json.loads(resp_body)


def push_all_batches(
    batches: list[dict[str, Any]],
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    timeout: float = 10.0,
) -> list[dict[str, Any]]:
    """Push a list of batches sequentially and return all responses.

    On error, the exception from :func:`push_batch` propagates immediately
    (remaining batches are not sent).
    """
    return [push_batch(b, host=host, port=port, timeout=timeout) for b in batches]


# ---------------------------------------------------------------------------
# Helper: convert JSONL file → events list (Memory-efficient streaming)
# ---------------------------------------------------------------------------

def iter_jsonl_events(
    jsonl_path,
    metric_filter: str | None = None,
    target_only: bool = False,
):
    """Yield eBPF event dicts one-by-one from a JSONL file via streaming.

    Unlike reading the entire file into memory at once, this generator processes
    lines incrementally, keeping memory usage constant O(1) regardless of file size.

    Args:
        jsonl_path:    Path to the JSONL file (e.g. from cpu_runtime.py output).
        metric_filter: If set, only yield events where ``metric == metric_filter``.
        target_only:   If True, only yield events where ``is_target == True``.

    Yields:
        Event dicts with at minimum ``metric``, ``timestamp_ns``, ``value``, ``tid``.
    """
    from pathlib import Path
    p = Path(jsonl_path)
    if not p.exists():
        return

    with open(p, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if metric_filter and rec.get("metric") != metric_filter:
                continue
            if target_only and not rec.get("is_target", False):
                continue
            event: dict[str, Any] = {
                "metric": rec["metric"],
                "timestamp_ns": rec["timestamp_ns"],
                "value": rec["value"],
                "tid": rec.get("tid"),
            }
            for opt in ("cpu", "comm", "nr_reclaimed", "is_target"):
                if opt in rec:
                    event[opt] = rec[opt]
            yield event


def jsonl_to_events(
    jsonl_path,
    metric_filter: str | None = None,
    target_only: bool = False,
) -> list[dict[str, Any]]:
    """Read a JSONL file of eBPF events into a list of event dicts.

    Processes the file via streaming to avoid intermediate string list allocations.

    Args:
        jsonl_path:    Path to the JSONL file.
        metric_filter: If set, only include events where ``metric == metric_filter``.
        target_only:   If True, only include events where ``is_target == True``.

    Returns:
        List of event dicts.
    """
    return list(iter_jsonl_events(jsonl_path, metric_filter=metric_filter, target_only=target_only))


def stream_batches_from_jsonl(
    jsonl_path,
    run_id: str,
    collector_id: str,
    scope: str,
    clock: dict[str, int],
    window_start_ns: int,
    window_end_ns: int,
    target_tids: list[int] | None = None,
    target_comm: str | None = None,
    dropped_events: int | None = None,
    metric_filter: str | None = None,
    target_only: bool = False,
    synthetic: bool = False,
):
    """Stream EbpfBatch payloads directly from a JSONL file without loading all events into RAM.

    Reads up to MAX_EVENTS_PER_BATCH events at a time, yields an EbpfBatch dict,
    and repeats.  Enables processing multi-gigabyte trace files within small,
    bounded memory.

    Yields:
        EbpfBatch dicts ready for :func:`push_batch`.
    """
    batch_idx = 0
    current_chunk: list[dict[str, Any]] = []

    for event in iter_jsonl_events(jsonl_path, metric_filter=metric_filter, target_only=target_only):
        # Bound event timestamp to window if required
        ts = event.get("timestamp_ns")
        if ts is not None and (ts < window_start_ns or ts > window_end_ns):
            continue
        current_chunk.append(event)
        if len(current_chunk) >= MAX_EVENTS_PER_BATCH:
            dropped = dropped_events if batch_idx == 0 else None
            yield build_ebpf_batch(
                run_id=run_id,
                collector_id=collector_id,
                scope=scope,
                clock=clock,
                window_start_ns=window_start_ns,
                window_end_ns=window_end_ns,
                events=current_chunk,
                target_tids=target_tids,
                target_comm=target_comm,
                dropped_events=dropped,
                synthetic=synthetic,
            )
            batch_idx += 1
            current_chunk = []

    # Yield remaining events or empty batch if no events
    if current_chunk or batch_idx == 0:
        dropped = dropped_events if batch_idx == 0 else None
        yield build_ebpf_batch(
            run_id=run_id,
            collector_id=collector_id,
            scope=scope,
            clock=clock,
            window_start_ns=window_start_ns,
            window_end_ns=window_end_ns,
            events=current_chunk,
            target_tids=target_tids,
            target_comm=target_comm,
            dropped_events=dropped,
            synthetic=synthetic,
        )
