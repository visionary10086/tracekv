"""Linux process identity and advisory port ownership; lock files alone prove no liveness."""
import fcntl
import json
import os
from pathlib import Path
import signal
import tempfile


def identity(pid=None):
    pid = os.getpid() if pid is None else pid
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        if fields[0] == "Z":
            return None
        return {"pid": int(pid), "start_ticks": int(fields[19]),
                "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip()}
    except (OSError, ValueError, IndexError):
        return None


def alive(expected):
    return bool(expected and identity(expected.get("pid")) == expected)


def signal_verified(expected, sig=signal.SIGTERM):
    if not alive(expected):
        raise ProcessLookupError("Process identity changed or process ended")
    pid = expected["pid"]
    # The pidfd prevents PID reuse between the second identity check and delivery.
    if hasattr(os, "pidfd_open") and hasattr(signal, "pidfd_send_signal"):
        fd = os.pidfd_open(pid)
        try:
            if not alive(expected):
                raise ProcessLookupError("Process identity changed")
            signal.pidfd_send_signal(fd, sig)
        finally:
            os.close(fd)
    else:
        raise RuntimeError("Safe process signaling requires Linux pidfd support")


class PortLease:
    def __init__(self, port):
        root = Path(tempfile.gettempdir()) / f"tracekv-{os.getuid()}"
        root.mkdir(mode=0o700, exist_ok=True)
        self.path = root / f"port-{port}.lock"
        self.handle = None

    def acquire(self, run_id):
        self.handle = self.path.open("a+")
        try:
            fcntl.flock(self.handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.handle.close()
            self.handle = None
            raise RuntimeError("Another TraceKV experiment owns this inference port")
        self.handle.seek(0)
        self.handle.truncate()
        json.dump({"run_id": run_id, "runner_identity": identity()}, self.handle)
        self.handle.flush()
        return self

    def release(self):
        if self.handle:
            fcntl.flock(self.handle, fcntl.LOCK_UN)
            self.handle.close()
            self.handle = None


def port_busy(port):
    lease = PortLease(port)
    try:
        lease.acquire("probe")
        return False
    except RuntimeError:
        return True
    finally:
        lease.release()
