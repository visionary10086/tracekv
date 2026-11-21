"""Workflow regression tests use temporary fixtures, not performance evidence."""
import copy
import json
from pathlib import Path
import subprocess
import tempfile
import threading
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from app.main import app
from app.jobs import Jobs
from app.runtime import identity, alive, signal_verified, PortLease, port_busy
from app.timeline import build
from app.comparisons import schedule, summarize_comparison, Comparisons, validate_request
from app.recommendations import register, resolve, assess
from app.collectors import SchedulerSession
from engine.histograms import histograms, deltas, summarize_samples
from tools.run_experiment import validate_config, create_run, write_json, append_json, execute
from tools.slo import evaluate

ROOT=Path(__file__).resolve().parents[1]


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.out=self.root/"workflow-test"
        self.out.mkdir()
        self.cfg=validate_config({"model_path":str(self.root),"requests":3})
        self.manifest={"run_id":self.out.name,"config":self.cfg,"status":"completed"}
        write_json(self.out/"manifest.json",self.manifest)

    def test_identity_rejects_reused_pid_and_ended_process(self):
        proc=subprocess.Popen(["/bin/sleep","10"])
        self.addCleanup(lambda:proc.poll() is None and proc.kill())
        token=identity(proc.pid)
        self.assertTrue(alive(token))
        wrong=dict(token,start_ticks=token["start_ticks"]+1)
        with self.assertRaises(ProcessLookupError):
            signal_verified(wrong)
        self.assertIsNone(proc.poll())
        signal_verified(token)
        proc.wait(timeout=2)
        self.assertFalse(alive(token))

    def test_port_lease_is_live_lock_not_stale_metadata(self):
        port=64997
        lease=PortLease(port).acquire("test")
        self.addCleanup(lease.release)
        self.assertTrue(port_busy(port))
        with self.assertRaises(RuntimeError):
            PortLease(port).acquire("another")
        lease.release()
        self.assertFalse(port_busy(port))

    def test_cli_process_is_visible_and_stoppable_without_adoption(self):
        proc=subprocess.Popen(["/bin/sleep","10"])
        self.addCleanup(lambda:proc.poll() is None and proc.kill())
        self.manifest.update(status="running",origin="cli",runner_identity=identity(proc.pid),runner_ready=True)
        write_json(self.out/"manifest.json",self.manifest)
        jobs=Jobs(self.root)
        run=jobs.list()[0]
        self.assertEqual(run["status"],"running")
        self.assertTrue(run["can_stop"])
        jobs.stop(self.out.name)
        proc.wait(timeout=2)
        self.assertTrue((self.out/"cancel.flag").exists())

    def test_unknown_process_is_orphaned_and_never_signalled(self):
        self.manifest.update(status="running",runner_identity={"pid":99999999,"start_ticks":0,"boot_id":"wrong"})
        write_json(self.out/"manifest.json",self.manifest)
        jobs=Jobs(self.root)
        self.assertEqual(jobs.list()[0]["status"],"orphaned")
        with self.assertRaises(ValueError):
            jobs.stop(self.out.name)

    def test_slo_missing_tpot_and_failures_do_not_pass(self):
        rows=[{"request_id":i,"success":ok,"ttft_ms":ttft,"client_tpot_estimate_ms":tp} for i,ok,ttft,tp in
              [(0,True,10,2),(1,True,11,None),(2,True,200,2),(3,False,None,None)]]
        result=evaluate(rows,2,{"requests":5,"slo_ttft_ms":100,"slo_tpot_ms":5})
        self.assertEqual([result[k] for k in ("passed","unknown","violation","failed_request")],[1,1,1,1])
        self.assertEqual(result["goodput_requests_per_second"],.5)
        self.assertEqual(result["attainment_per_planned_request"],.2)
        self.assertEqual(result["unrecorded_planned_requests"],1)
        self.assertFalse(evaluate(rows,2,{})["configured"])

    def test_config_rejects_nonfinite_slo_and_profile(self):
        for patch_cfg in ({"slo_ttft_ms":float("nan")},{"slo_tpot_ms":0},{"trace_profile":"fake"},{"seed":True}):
            with self.assertRaises(ValueError):
                validate_config(dict(self.cfg,**patch_cfg))

    def histogram(self,engine,count,total):
        labels='model_name="test",engine="'+str(engine)+'"'
        return "\n".join([
            'vllm:request_queue_time_seconds_bucket{'+labels+',le="0.1"} '+str(count),
            'vllm:request_queue_time_seconds_bucket{'+labels+',le="+Inf"} '+str(count),
            'vllm:request_queue_time_seconds_count{'+labels+'} '+str(count),
            'vllm:request_queue_time_seconds_sum{'+labels+'} '+str(total)])

    def test_histograms_keep_engine_labels_and_counter_resets(self):
        old=histograms(self.histogram(0,3,.03)+"\n"+self.histogram(1,5,.05))
        now=histograms(self.histogram(0,7,.07)+"\n"+self.histogram(1,2,.02))
        rows=deltas(now,old)
        self.assertEqual(len(rows),2)
        self.assertEqual(rows[0]["delta"]["count"],4)
        self.assertIsNone(rows[1]["delta"])
        self.assertTrue(rows[1]["reset"])
        stats=summarize_samples([{"kv":{"stage_histograms":rows}}])
        self.assertAlmostEqual(stats[0]["mean_ms"],10)
        self.assertEqual(stats[1]["reset_intervals"],1)
        self.assertIsNone(stats[1]["mean_ms"])

    def test_histogram_unknown_sum_not_zero(self):
        rows=deltas(histograms(self.histogram(0,7,.07)),histograms(self.histogram(0,3,.03)))
        rows[0]["delta"]["sum"]=None
        result=summarize_samples([{"kv":{"stage_histograms":rows}}])[0]
        self.assertIsNone(result["mean_ms"])

    def test_timeline_anchor_paging_and_no_cross_run_request(self):
        from app.integrations import accept
        from contracts.platform import EbpfBatch
        row={"run_id":self.out.name,"request_id":7,"case_id":"c","started_at_utc":"1970-01-01T00:00:01Z",
             "finished_at_utc":"1970-01-01T00:00:02Z","success":True,"e2e_ms":1000}
        append_json(self.out/"requests.jsonl",row)
        append_json(self.out/"requests.jsonl",dict(row,run_id="wrong"))
        packet=EbpfBatch(run_id=self.out.name,batch_id="b1",collector_id="fixture",
            clock={"wall_ns":1000000000,"monotonic_ns":100},window_start_ns=100,window_end_ns=1000000100,
            scope="target_threads",target_tids=[42],events=[{"timestamp_ns":500000100,"tid":42,
            "metric":"on_cpu","value":1000000}])
        accept(self.out,"ebpf",packet)
        page=build(self.out,limit=1,cursor=0)
        self.assertEqual(page["total_events"],2)
        self.assertEqual(page["next_cursor"],1)
        self.assertEqual(page["invalid_records"],1)
        event=build(self.out,limit=1,cursor=1)["events"][0]
        self.assertEqual(event["epoch_s"],1.5)
        self.assertIsNone(event["data"]["request_id"])
        self.assertIsNone(event["data"]["dropped_events"])
        self.assertEqual(build(self.out,start="1970-01-01T00:00:02Z")["total_events"],0)
        with self.assertRaises(ValueError):
            build(self.out,start="2026-01-01T00:00:00")

    def test_timeline_synthetic_batches_excluded(self):
        from app.integrations import accept
        from contracts.platform import EbpfBatch
        packet=EbpfBatch(run_id=self.out.name,batch_id="b1",collector_id="fixture",synthetic=True,
            clock={"wall_ns":1000000000,"monotonic_ns":100},window_start_ns=100,window_end_ns=1000,
            scope="system",events=[{"timestamp_ns":200,"tid":42,"metric":"on_cpu","value":10}])
        accept(self.out,"ebpf",packet)
        self.assertEqual(build(self.out)["events"],[])

    def test_run_api_does_not_fall_back_to_host_samples(self):
        with patch("app.main.jobs",Jobs(self.root)):
            client=TestClient(app)
            self.assertEqual(client.get("/api/v1/runs/workflow-test/history").json()["samples"],[])
            self.assertEqual(client.get("/api/v1/runs/workflow-test/snapshot").status_code,503)
            self.assertEqual(client.get("/api/v1/runs/workflow-test/timeline?limit=2001").status_code,422)
            self.assertEqual(client.get("/api/v1/runs/workflow-test/timeline?start=bad").status_code,422)

    def test_randomized_schedule_has_every_cell_each_repeat(self):
        cfg={"order_seed":42,"repetitions":3,"policies":["gpu_only","cpu_lru"],"trace_profiles":["off","sampling"]}
        rows=schedule(cfg)
        self.assertEqual(rows,schedule(cfg))
        self.assertEqual(len(rows),12)
        for i in (1,2,3):
            self.assertEqual({(m["policy"],m["trace_profile"]) for m in rows if m["repeat"]==i},
                {("gpu_only","off"),("gpu_only","sampling"),("cpu_lru","off"),("cpu_lru","sampling")})

    def comparison_fixture(self,requests=3,dirty=False):
        folder=self.root/".comparisons"/"cmp-test";folder.mkdir(parents=True)
        members=[]
        # CPU first catches randomized-order lookup regressions.
        for policy in ("cpu_lru","gpu_only"):
            for i in range(1,4):
                name=f"{policy}-{i}";out=self.root/name;out.mkdir()
                config=dict(self.cfg,policy=policy,requests=requests)
                m={"config":config,"status":"completed","git_worktree_dirty":dirty}
                m.update({k:"same" for k in ("git_commit","workload_sha256","config.json_sha256","tokenizer_config.json_sha256","model_weights_digest")})
                write_json(out/"manifest.json",m)
                write_json(out/"request_summary.json",{"successful_requests":requests,"requests":requests,
                    "client_e2e_ms_p95":80 if policy=="cpu_lru" else 100})
                for j in range(requests):
                    append_json(out/"requests.jsonl",{"request_id":j,"case_id":"c","success":True,"output_text":"same"})
                members.append({"policy":policy,"trace_profile":"sampling","repeat":i,"run_id":name,"status":"completed"})
        write_json(folder/"manifest.json",{"comparison_id":"cmp-test","status":"completed",
            "config":{"effect_threshold_pct":5},"members":members})
        return folder

    def test_short_comparison_never_claims_improvement(self):
        folder=self.comparison_fixture()
        result=summarize_comparison(folder,self.root)
        self.assertEqual(result["cells"]["cpu_lru/sampling"]["comparison_result"],"insufficient_data")
        self.assertTrue((folder/"report.md").exists())

    def test_comparison_gate_and_dirty_code_guard(self):
        folder=self.comparison_fixture(requests=100,dirty=True)
        result=summarize_comparison(folder,self.root)
        self.assertEqual(result["cells"]["cpu_lru/sampling"]["comparison_result"],"incomparable_or_unverified_identity")
        for m in json.loads((folder/"manifest.json").read_text())["members"]:
            path=self.root/m["run_id"]/"manifest.json"
            data=json.loads(path.read_text());data["git_worktree_dirty"]=False;write_json(path,data)
        result=summarize_comparison(folder,self.root)
        self.assertEqual(result["cells"]["cpu_lru/sampling"]["comparison_result"],"observed_improvement")
        self.assertFalse(result["evaluation_rule"]["causality_established"])

    def test_failed_member_is_retained_and_blocks_benefit(self):
        folder=self.comparison_fixture(requests=100)
        data=json.loads((folder/"manifest.json").read_text())
        out=self.root/data["members"][0]["run_id"]
        m=json.loads((out/"manifest.json").read_text());m["status"]="failed";write_json(out/"manifest.json",m)
        result=summarize_comparison(folder,self.root)
        self.assertEqual(result["cells"]["cpu_lru/sampling"]["failed_or_interrupted"],1)
        self.assertEqual(result["cells"]["cpu_lru/sampling"]["comparison_result"],"insufficient_data")

    def test_batch_queue_released_even_when_summary_write_fails(self):
        jobs=Jobs(self.root);jobs.reservation="cmp-test";controller=Comparisons(jobs)
        manifest={"comparison_id":"cmp-test","members":[],"status":"running"}
        with patch("app.comparisons.summarize_comparison",side_effect=OSError("disk error")):
            with self.assertRaises(OSError):
                controller._run(self.out,manifest)
        self.assertIsNone(jobs.reservation)

    def test_ebpf_unavailable_preflight_and_report_without_launch(self):
        caps={"can_attempt":False,"reason":"not installed"}
        with patch("app.comparisons.capability",return_value=caps):
            with self.assertRaises(ValueError):
                validate_request({"base_config":self.cfg,"trace_profiles":["ebpf"]})
        out,m=create_run(dict(self.cfg,trace_profile="ebpf"),self.root/"runs")
        with patch("tools.run_experiment.capability",return_value=caps),patch("tools.run_experiment.subprocess.Popen") as launch:
            self.assertEqual(execute(out,dict(m["config"])),2)
            launch.assert_not_called()
        self.assertEqual(json.loads((out/"manifest.json").read_text())["status"],"failed")
        self.assertTrue((out/"report.md").exists())
        self.assertFalse(port_busy(self.cfg["port"]))

    def test_advice_provenance_is_verified_against_saved_snapshot(self):
        jobs=Jobs(self.root)
        snapshot={"run_id":self.out.name,"timestamp":"2026-11-21T00:00:00Z"}
        advice={"config_patch":{"policy":"gpu_only","gpu_kv_blocks":64,"cpu_kv_blocks":0}}
        unknown=register(jobs,advice,snapshot)
        self.assertFalse(unknown["source_run_verified"])
        append_json(self.out/"snapshots.jsonl",snapshot)
        saved=register(jobs,advice,snapshot)
        self.assertTrue(saved["source_run_verified"])
        record=resolve(jobs,saved["recommendation_id"],self.cfg)
        self.assertEqual(record["source"]["run_id"],self.out.name)
        with self.assertRaises(ValueError):
            resolve(jobs,saved["recommendation_id"],dict(self.cfg,gpu_kv_blocks=65))
        self.manifest["recommendation"]=record
        assessment=assess(self.out,self.manifest,{"successful_requests":3})
        self.assertEqual(assessment["status"],"insufficient_data")

    def test_multiple_engine_ratios_are_not_summed_into_dashboard(self):
        import io
        from engine.vllm_adapter import VllmAdapter
        body=b'vllm:kv_cache_usage_perc{engine="0"} 0.2\nvllm:kv_cache_usage_perc{engine="1"} 0.3\n'
        with patch("engine.vllm_adapter.urllib.request.urlopen",return_value=io.BytesIO(body)):
            self.assertIsNone(VllmAdapter().sample()["usage_ratio"])

    def test_managed_collector_stop_publishes_scope_and_unknown_loss(self):
        from collector.cpu_scheduler import CollectorResult, CollectorStats, TimeAnchor
        from app.integrations import summarize
        started=threading.Event()
        def collect(run_id,path,**kwargs):
            append_json(path,{"metric":"wakeup_to_run_latency","timestamp_ns":200,"tid":42,
                "pid":None,"comm":"worker","cpu":0,"request_id":None,"value":100,"unit":"ns","is_target":True})
            started.set()
            kwargs["stop_event"].wait(timeout=2)
            return CollectorResult(run_id,TimeAnchor(1000000000,100),TimeAnchor(1000001000,1100),
                stats=CollectorStats(event_count=1),target_stats=CollectorStats(event_count=1),
                target_pids=[42],scope="target_threads",bpftrace_exit_code=0)
        with patch("app.collectors.capability",return_value={"can_attempt":True}),patch("app.collectors.tree_tids",return_value=[42]),patch("collector.cpu_scheduler.run_collector",side_effect=collect):
            session=SchedulerSession(self.out,123).start()
            self.assertTrue(started.wait(timeout=2))
            result=session.stop()
        self.assertEqual(result["status"],"ok")
        self.assertFalse(session.thread.is_alive())
        self.assertIsNone(result["dropped_events"])
        telemetry=summarize(self.out)["sources"]["ebpf"]
        self.assertEqual(telemetry["event_count"],1)
        self.assertEqual(telemetry["groups"][0]["scope"],"target_threads")
        self.assertIsNone(build(self.out)["events"][0]["data"]["request_id"])

    def test_unexpected_collector_exit_is_failure(self):
        from collector.cpu_scheduler import CollectorResult, TimeAnchor
        result=CollectorResult(self.out.name,TimeAnchor(1000000000,100))
        with patch("app.collectors.capability",return_value={"can_attempt":True}),patch("app.collectors.tree_tids",return_value=[42]),patch("collector.cpu_scheduler.run_collector",return_value=result):
            session=SchedulerSession(self.out,123).start()
            session.thread.join(timeout=2)
        self.assertIn("before",session.error)
        self.assertEqual(session.state()["status"],"failed")

    def test_batch_cancel_preserves_unstarted_members(self):
        jobs=Jobs(self.root);jobs.reservation="cmp-test";controller=Comparisons(jobs)
        controller.stop_event.set()
        manifest={"comparison_id":"cmp-test","members":[{"policy":"gpu_only","trace_profile":"off",
            "repeat":1,"run_id":None,"status":"pending"}],"status":"running","config":{"effect_threshold_pct":5}}
        with patch.object(jobs,"start") as start:
            controller._run(self.out,manifest)
            start.assert_not_called()
        saved=json.loads((self.out/"manifest.json").read_text())
        self.assertEqual(saved["status"],"interrupted")
        self.assertEqual(saved["members"][0]["status"],"pending")
        self.assertTrue((self.out/"report.md").exists())
        self.assertIsNone(jobs.reservation)

    def test_advice_source_rejects_a_different_current_configuration(self):
        jobs=Jobs(self.root)
        snapshot={"run_id":self.out.name,"timestamp":"2026-11-21T00:00:00Z"}
        append_json(self.out/"snapshots.jsonl",snapshot)
        advice={"config_patch":{"policy":"gpu_only","gpu_kv_blocks":64,"cpu_kv_blocks":0}}
        result=register(jobs,advice,snapshot,{"policy":"cpu_lru","cpu_kv_blocks":72})
        self.assertFalse(result["source_run_verified"])
        result=register(jobs,advice,snapshot,{"policy":"gpu_only","cpu_kv_blocks":0})
        self.assertTrue(result["source_run_verified"])

    def test_comparison_report_is_a_download(self):
        folder=self.comparison_fixture()
        summarize_comparison(folder,self.root)
        controller=Comparisons(Jobs(self.root))
        with patch("app.main.comparisons",controller):
            response=TestClient(app).get("/api/v1/comparisons/cmp-test/files/report.md")
        self.assertEqual(response.status_code,200)
        self.assertIn("attachment",response.headers["content-disposition"])
