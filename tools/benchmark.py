"""Small closed-loop streaming chat benchmark; never treats SSE chunks as tokens."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import time
import urllib.error
import urllib.parse
import urllib.request


def events(response):
    """Parse SSE events, including comments, CRLF and multiple data lines."""
    data = []
    for raw in response:
        line = raw.decode("utf-8").rstrip("\r\n")
        if not line:
            if data:
                yield "\n".join(data)
                data = []
        elif line.startswith("data:"):
            data.append(line[5:].lstrip(" "))
    if data:
        yield "\n".join(data)


def headers():
    result = {"Content-Type": "application/json"}
    key = os.environ.get("TRACEKV_API_KEY")
    if key:
        result["Authorization"] = "Bearer " + key
    return result


def get_resource(url, timeout):
    request = urllib.request.Request(url, headers=headers())
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def request_one(base_url, model, item, index, max_tokens, timeout):
    start = time.perf_counter()
    result = {"request_id": index, "case_id": item["id"],
              "started_at_utc": datetime.now(timezone.utc).isoformat(),
              "success": False, "ttft_ms": None, "completion_tokens": None,
              "prompt_tokens": None, "output_text": "", "finish_reason": None}
    payload = {"model": model, "messages": item["messages"], "temperature": 0,
               "max_tokens": max_tokens, "stream": True,
               "stream_options": {"include_usage": True}}
    request = urllib.request.Request(base_url + "/v1/chat/completions",
                                     data=json.dumps(payload).encode("utf-8"),
                                     headers=headers())
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            done = False
            for event in events(response):
                if event.strip() == "[DONE]":
                    done = True
                    break
                obj = json.loads(event)
                if obj.get("error"):
                    raise ValueError("Server returned a stream error")
                usage = obj.get("usage") or {}
                for key in ("completion_tokens", "prompt_tokens"):
                    value = usage.get(key)
                    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                        result[key] = value
                for choice in obj.get("choices", []):
                    if choice.get("index", 0) != 0:
                        continue
                    delta = choice.get("delta") or {}
                    content = delta.get("content")
                    if isinstance(content, str) and content:
                        if result["ttft_ms"] is None:
                            result["ttft_ms"] = (time.perf_counter() - start) * 1000
                        result["output_text"] += content
                    if choice.get("finish_reason") is not None:
                        result["finish_reason"] = choice["finish_reason"]
            if not done or result["finish_reason"] is None:
                raise ValueError("Incomplete stream: missing DONE or finish_reason")
            if not result["output_text"]:
                raise ValueError("No visible content received; this text-chat smoke test failed")
            result["success"] = True
    except urllib.error.HTTPError as exc:
        result["error"] = f"HTTP {exc.code} {exc.reason}"
    except (OSError, ValueError, urllib.error.URLError) as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    result["e2e_ms"] = (time.perf_counter() - start) * 1000
    return result


def percentile(values, p):
    if not values:
        return None
    values = sorted(values)
    position = (len(values) - 1) * p / 100
    lo, hi = math.floor(position), math.ceil(position)
    return values[lo] + (values[hi] - values[lo]) * (position - lo)


def summarize(records, elapsed):
    good = [r for r in records if r["success"]]
    ttft = [r["ttft_ms"] for r in good if r["ttft_ms"] is not None]
    e2e = [r["e2e_ms"] for r in good]
    complete_usage = bool(good) and all(r["completion_tokens"] is not None for r in good)
    total_tokens = sum(r["completion_tokens"] for r in good) if complete_usage else None
    return {
        "requests": len(records), "successful_requests": len(good),
        "failed_requests": len(records) - len(good),
        "failure_rate": (len(records) - len(good)) / len(records) if records else None,
        "measurement_seconds": elapsed,
        "successful_requests_per_second": len(good) / elapsed if elapsed > 0 else None,
        "successful_output_tokens": total_tokens,
        "successful_output_tokens_per_second": total_tokens / elapsed if total_tokens is not None and elapsed > 0 else None,
        "client_ttft_ms_p50": percentile(ttft, 50), "client_ttft_ms_p95": percentile(ttft, 95),
        "client_e2e_ms_p50": percentile(e2e, 50), "client_e2e_ms_p95": percentile(e2e, 95),
        "small_sample_warning": len(good) < 100,
        "metric_note": "Client TTFT is time to first nonempty visible-content chunk (includes network); "
                       "E2E ends after stream validation. No per-token ITL is inferred from chunks. "
                       "Output throughput uses successful requests only and includes failed-request time in the denominator."
    }


def load_cases(path):
    cases = [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]
    if not cases:
        raise ValueError("Workload is empty")
    seen = set()
    for item in cases:
        if not isinstance(item.get("id"), str) or item["id"] in seen:
            raise ValueError("Case IDs must be unique strings")
        seen.add(item["id"])
        messages = item.get("messages")
        if not isinstance(messages, list) or not messages:
            raise ValueError("Each case needs nonempty messages")
        if any(not isinstance(m, dict) or not isinstance(m.get("content"), str)
               or m.get("role") not in ("system", "user", "assistant") for m in messages):
            raise ValueError("Unsupported text-chat message")
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--model", required=True)
    parser.add_argument("--workload", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--requests", type=int, default=30)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--max-tokens", type=int, default=64)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--cache-state", choices=("cold", "warm", "uncontrolled"), default="uncontrolled")
    parser.add_argument("--cache-note", default="Not reset or verified by this client")
    parser.add_argument("--label", default="baseline")
    args = parser.parse_args()
    if min(args.requests, args.concurrency, args.max_tokens) <= 0 or args.timeout <= 0 or args.warmup < 0:
        parser.error("Counts/timeout must be positive and warmup must be nonnegative")
    if args.cache_state == "cold" and args.warmup:
        parser.error("Cold cache cannot be claimed after workload warmup; use --warmup 0 and externally verify cache state")
    base = args.base_url.rstrip("/")
    parsed = urllib.parse.urlsplit(base)
    if parsed.scheme not in ("http", "https") or not parsed.netloc or parsed.path or parsed.query or parsed.fragment or parsed.username:
        parser.error("base-url must be an HTTP(S) origin, without credentials, path or query")
    cases = load_cases(args.workload)
    # Fail before requests if the destination already exists; never overwrite an experiment.
    args.out.mkdir(parents=True, exist_ok=False)
    manifest = {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()}
    manifest.update({"schema_version": 1, "run_id": args.out.name,
                     "started_at_utc": datetime.now(timezone.utc).isoformat(),
                     "workload_sha256": hashlib.sha256(args.workload.read_bytes()).hexdigest(),
                     "workload_mode": "deterministic cyclic text prompts, closed-loop concurrency",
                     "kind": "HTTP endpoint measurement; verify server identity separately"})
    (args.out / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    def capture(path, filename):
        try:
            (args.out / filename).write_bytes(get_resource(base + path, args.timeout))
        except (OSError, urllib.error.URLError) as exc:
            (args.out / (filename + ".error.txt")).write_text(str(exc), encoding="utf-8")

    capture("/v1/models", "models.json")
    warmup = [request_one(base, args.model, cases[i % len(cases)], i,
                          args.max_tokens, args.timeout) for i in range(args.warmup)]
    (args.out / "warmup.json").write_text(json.dumps(warmup, ensure_ascii=False, indent=2), encoding="utf-8")
    if any(not r["success"] for r in warmup):
        print("Warmup failed; see warmup.json. No baseline was recorded.")
        return 2
    capture("/metrics", "metrics_before.txt")
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        records = list(executor.map(lambda i: request_one(
            base, args.model, cases[i % len(cases)], i, args.max_tokens, args.timeout), range(args.requests)))
    elapsed = time.perf_counter() - started
    with (args.out / "requests.jsonl").open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    summary = summarize(records, elapsed)
    (args.out / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    capture("/metrics", "metrics_after.txt")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary["failed_requests"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
