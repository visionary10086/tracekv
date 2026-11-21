"""Platform lifecycle adapter for the existing scheduler collector; no privilege escalation."""
import json
import os
from pathlib import Path
import shutil
import threading
import time


def capability():
    binary = shutil.which("bpftrace")
    permitted = os.geteuid() == 0
    try:
        cap = next(line.split()[1] for line in Path("/proc/self/status").read_text().splitlines() if line.startswith("CapEff:"))
        bits = int(cap, 16)
        permitted |= bool(bits & (1 << 21)) or bool(bits & (1 << 38) and bits & (1 << 39))
    except (OSError, StopIteration, ValueError):
        pass
    reason = None if binary and permitted else (
        "bpftrace is not installed" if not binary else "Collector needs root or appropriate BPF/perf capabilities")
    return {"bpftrace_found": bool(binary), "privilege_available": permitted,
            "can_attempt": bool(binary and permitted), "reason": reason,
            "attachment_verified": False}


def tree_tids(root_pid):
    children = {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            fields = (entry / "stat").read_text().rsplit(")", 1)[1].split()
            children.setdefault(int(fields[1]), []).append(int(entry.name))
        except (OSError, ValueError, IndexError):
            pass
    pending, seen = [root_pid], set()
    while pending:
        pid = pending.pop()
        if pid in seen:
            continue
        seen.add(pid)
        pending.extend(children.get(pid, []))
    tids = set()
    for pid in seen:
        try:
            tids.update(int(p.name) for p in Path(f"/proc/{pid}/task").iterdir())
        except OSError:
            pass
    return sorted(tids)


class SchedulerSession:
    def __init__(self, out, server_pid):
        self.out = Path(out)
        self.tids = tree_tids(server_pid)
        self.stop_event = threading.Event()
        self.thread = None
        self.result = None
        self.error = None
        self.anchor = {"wall_ns": time.time_ns(), "monotonic_ns": time.monotonic_ns()}

    def start(self):
        caps = capability()
        if not caps["can_attempt"]:
            raise RuntimeError(caps["reason"])
        if not self.tids:
            raise RuntimeError("No verified inference process-tree threads found")
        def run():
            from collector.cpu_scheduler import run_collector
            try:
                self.result = run_collector(self.out.name, self.out / "sched_events.jsonl",
                                            duration_seconds=None, stop_event=self.stop_event,
                                            target_pids=self.tids)
                self.error = self.result.error
                if not self.stop_event.is_set() and not self.error:
                    self.error = "Collector exited before the experiment requested shutdown"
            except Exception as exc:
                self.error = str(exc)
        self.thread = threading.Thread(target=run, daemon=True, name="tracekv-scheduler")
        self.thread.start()
        return self

    def stop(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=16)
            if self.thread.is_alive():
                self.error = "Collector did not terminate within its shutdown deadline"
        if self.result:
            from collector.cpu_scheduler import stats_to_dict
            summary = stats_to_dict(self.result)
            (self.out / "sched_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2))
            self.publish(summary)
        return self.state()

    def state(self):
        count = self.result.target_stats.event_count if self.result and self.result.target_stats else None
        return {"status": "failed" if self.error else ("ok" if self.result else "starting"),
                "error": self.error, "event_count": count, "scope": "target_threads",
                "target_tids": self.tids, "target_tracking": "process-tree TID snapshot at collector start",
                "dropped_events": self.result.stats.dropped_events_count if self.result and self.result.stats.kernel_matched_count is not None else None}

    def publish(self, summary):
        from app.integrations import accept
        from contracts.platform import EbpfBatch
        anchor = {"wall_ns": summary["start_wall_ns"], "monotonic_ns": summary["start_monotonic_ns"]}
        end = summary.get("end_monotonic_ns") or time.monotonic_ns()
        events = []
        path = self.out / "sched_events.jsonl"
        if path.exists():
            for line in path.read_text().splitlines():
                try:
                    raw = json.loads(line)
                    if raw.get("is_target") and anchor["monotonic_ns"] <= raw["timestamp_ns"] < end:
                        events.append({key: raw.get(key) for key in ("metric", "timestamp_ns", "tid", "pid", "comm", "cpu", "request_id", "value", "unit")})
                except (ValueError, KeyError):
                    continue
        # Disjoint event-time windows avoid overlapping batches for one collector.
        events.sort(key=lambda row: row["timestamp_ns"])
        chunks, chunk = [], []
        for event in events:
            if len(chunk) >= 1500 and event["timestamp_ns"] != chunk[-1]["timestamp_ns"]:
                chunks.append(chunk)
                chunk = []
            chunk.append(event)
        if chunk or not chunks:
            chunks.append(chunk)
        if any(len(rows)>2000 for rows in chunks):
            raise RuntimeError("More than 2000 events share a timestamp; raw events retained, batch publication refused")
        left = anchor["monotonic_ns"]
        for index, rows in enumerate(chunks):
            right = chunks[index+1][0]["timestamp_ns"] if index+1 < len(chunks) else max(end, left+1)
            packet = EbpfBatch(run_id=self.out.name, batch_id=f"managed-{index:06d}",
                collector_id="managed-scheduler-v1", clock=anchor, scope="target_threads",
                target_tids=self.tids, window_start_ns=left, window_end_ns=right,
                dropped_events=summary.get("dropped_events_count") if index == 0 else 0,
                status="ok" if rows or not self.error else "failed", error=self.error, events=rows)
            accept(self.out, "ebpf", packet)
            left = right
