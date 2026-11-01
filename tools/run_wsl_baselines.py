"""Start a local vLLM smoke configuration, run real requests, stop only our own server."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.request


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out',type=Path,required=True)
    parser.add_argument('--model',required=True)
    parser.add_argument('--version',default='0.11.2')
    parser.add_argument('--port',type=int,default=8000)
    args=parser.parse_args()
    root=Path(__file__).resolve().parents[1]
    args.out.mkdir(parents=True,exist_ok=False)
    # Never adopt or terminate an unrelated service occupying the port.
    with socket.socket() as sock:
        if sock.connect_ex(('127.0.0.1',args.port))==0:
            raise RuntimeError('Port already occupied; choose another --port')
    env=os.environ.copy()
    env.update({'HF_HUB_OFFLINE':'1','TRANSFORMERS_OFFLINE':'1',
                'VLLM_NO_USAGE_STATS':'1','DO_NOT_TRACK':'1','OMP_NUM_THREADS':'4',
                'XDG_CACHE_HOME':'/opt/tracekv/cache','HF_HOME':'/opt/tracekv/cache/huggingface',
                'TRITON_CACHE_DIR':'/opt/tracekv/cache/triton','VLLM_CACHE_ROOT':'/opt/tracekv/cache/vllm',
                'CUDA_CACHE_PATH':'/opt/tracekv/cache/cuda','TOKENIZERS_PARALLELISM':'false'})
    command=[sys.executable,str(root/'tools/launch_vllm.py'),
             '--model',args.model,'--expected-version',args.version,
             '--served-name','tracekv-model','--port',str(args.port),
             '--max-model-len','1024','--max-num-seqs','4',
             '--gpu-memory-utilization','0.65','--enforce-eager',
             '--out',str(args.out/'server')]
    phase={'name':'startup'}
    stop=threading.Event()
    def monitor():
        with (args.out/'gpu_samples.jsonl').open('w') as handle:
            while not stop.is_set():
                try:
                    r=subprocess.run(['/usr/lib/wsl/lib/nvidia-smi',
                        '--query-gpu=memory.used,utilization.gpu,temperature.gpu,power.draw',
                        '--format=csv,noheader,nounits'],capture_output=True,text=True,timeout=5)
                    handle.write(json.dumps({'time_utc':datetime.now(timezone.utc).isoformat(),
                        'phase':phase['name'],'returncode':r.returncode,
                        'values_memory_MiB_util_percent_temp_C_power_W':r.stdout.strip(),
                        'stderr':r.stderr.strip()})+'\n')
                    handle.flush()
                except Exception as exc:
                    handle.write(json.dumps({'monitor_error':str(exc)})+'\n')
                stop.wait(1)
    result={'started_at_utc':datetime.now(timezone.utc).isoformat(),
            'configuration':'small-model eager-mode functional baseline, no custom cache strategy',
            'runs':[], 'server_ready':False}
    monitor_thread=threading.Thread(target=monitor,daemon=True)
    monitor_thread.start()
    server=None
    started=time.monotonic()
    try:
        with (args.out/'launcher.log').open('w') as log:
            server=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT,
                                    env=env,start_new_session=True)
        base=f'http://127.0.0.1:{args.port}'
        while time.monotonic()-started<600:
            if server.poll() is not None:
                raise RuntimeError(f'Server exited with code {server.returncode}; inspect server/server.log')
            try:
                with urllib.request.urlopen(base+'/health',timeout=2) as response:
                    if response.status==200:
                        result['server_ready']=True
                        break
            except Exception:
                time.sleep(2)
        if not result['server_ready']:
            raise TimeoutError('Server was not healthy within 600 seconds')
        result['startup_seconds']=time.monotonic()-started
        print('Server healthy; starting real model requests.',flush=True)
        cases=[('smoke',1,1,0)]
        cases += [(f'baseline-c{c}-r{r}',c,30,2) for c in (1,2,4) for r in (1,2,3)]
        for name,concurrency,count,warmup in cases:
            phase['name']=name
            benchmark=[sys.executable,str(root/'tools/benchmark.py'),
                '--base-url',base,'--model','tracekv-model',
                '--workload',str(root/'workloads/smoke.jsonl'),
                '--requests',str(count),'--concurrency',str(concurrency),
                '--warmup',str(warmup),'--max-tokens','64','--timeout','120',
                '--cache-state','uncontrolled','--label',name,'--out',str(args.out/name)]
            with (args.out/(name+'.log')).open('w') as log:
                run=subprocess.run(benchmark,env=env,stdout=log,stderr=subprocess.STDOUT,timeout=1200)
            result['runs'].append({'name':name,'returncode':run.returncode})
            (args.out/'suite_status.json').write_text(json.dumps(result,indent=2))
            print(name,'exit',run.returncode,flush=True)
            if run.returncode:
                raise RuntimeError('Request test failed; preserve logs and stop the suite')
        result['completed']=True
    except Exception as exc:
        result['completed']=False
        result['error']=str(exc)
        print(result['error'],flush=True)
    finally:
        phase['name']='shutdown'
        if server is not None:
            # Dedicated process group was created above; no other user process is targeted.
            try:
                os.killpg(server.pid,signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                server.wait(timeout=20)
            except subprocess.TimeoutExpired:
                os.killpg(server.pid,signal.SIGKILL)
                server.wait()
        stop.set()
        monitor_thread.join(timeout=7)
        result['finished_at_utc']=datetime.now(timezone.utc).isoformat()
        (args.out/'suite_status.json').write_text(json.dumps(result,indent=2))
    return 0 if result.get('completed') else 2


if __name__=='__main__':
    raise SystemExit(main())
