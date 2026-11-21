"""Unit and integration tests for the Libbpf native high-performance engine."""

import ctypes
import os
from pathlib import Path
import unittest

from collector.cpu_scheduler import run_collector
from collector.native_engine import (
    HIST_SLOTS_COUNT,
    METRIC_DIRECT_RECLAIM,
    METRIC_ON_CPU,
    METRIC_RUNNABLE_WAIT,
    METRIC_WAKEUP_LATENCY,
    HistBucket,
    NativeEbpfEngine,
    NativeEngineError,
    NativeSchedEvent,
    check_native_engine_environment,
    get_libbpf,
    is_native_available,
    run_native_collector,
)


class TestNativeEngine(unittest.TestCase):
    def test_libbpf_library_loading(self):
        lib = get_libbpf()
        self.assertIsNotNone(lib, "libbpf.so.1 should be discoverable on this system")
        # Check core symbols
        self.assertTrue(hasattr(lib, "bpf_object__open"))
        self.assertTrue(hasattr(lib, "bpf_object__load"))
        self.assertTrue(hasattr(lib, "bpf_object__close"))

    def test_native_engine_environment_check(self):
        diag = check_native_engine_environment()
        self.assertIsInstance(diag, dict)
        self.assertEqual(diag["engine"], "libbpf")
        self.assertIn("libbpf_loaded", diag)
        self.assertIn("btf_available", diag)
        self.assertIn("privilege_ok", diag)
        self.assertIn("bpf_obj_compiled", diag)
        self.assertIn("status", diag)
        self.assertTrue(diag["libbpf_loaded"])
        self.assertIn(diag["status"], ("READY", "FALLBACK_RECOMMENDED"))

    def test_native_sched_event_struct_packing(self):
        ev = NativeSchedEvent()
        ev.timestamp_ns = 1_000_000_000
        ev.wakeup_ns = 999_000_000
        ev.latency_ns = 1_000_000
        ev.pid = 1234
        ev.tid = 5678
        ev.cpu = 3
        ev.metric_type = METRIC_ON_CPU
        ev.is_target = 1
        ev.comm = b"python_worker\x00"

        # Verify packed size and field extraction
        self.assertEqual(ctypes.sizeof(ev), 54)
        self.assertEqual(ev.metric_type, 2)
        self.assertEqual(ev.is_target, 1)
        self.assertEqual(ev.tid, 5678)
        self.assertEqual(ev.pid, 1234)
        self.assertTrue(ev.comm.startswith(b"python_worker"))

    def test_hist_bucket_struct(self):
        bucket = HistBucket()
        self.assertEqual(len(bucket.slots), HIST_SLOTS_COUNT)
        bucket.slots[0] = 42
        bucket.slots[31] = 100
        self.assertEqual(bucket.slots[0], 42)
        self.assertEqual(bucket.slots[31], 100)

    def test_dual_engine_transparent_fallback_under_auto(self):
        # Under auto mode, if .bpf.o is not compiled, falls back gracefully to bpftrace
        res = run_collector(run_id="test-auto-fallback", duration_seconds=0.01, engine="auto")
        self.assertIsNotNone(res)
        self.assertEqual(res.run_id, "test-auto-fallback")

    def test_native_engine_missing_obj_error_handling(self):
        # Force libbpf engine with non-existent bpf.o
        res = run_native_collector(run_id="test-native-missing", duration_seconds=0.01)
        self.assertIsNotNone(res)
        self.assertIsNotNone(res.error)
        self.assertIn("not found", res.error)

    def test_native_engine_class_validation(self):
        with self.assertRaises(NativeEngineError):
            engine = NativeEbpfEngine(bpf_obj_path="/nonexistent/path.bpf.o")
            engine.load_and_attach()


if __name__ == "__main__":
    unittest.main(verbosity=2)
