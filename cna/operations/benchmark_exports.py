"""Opt-in, isolated-lab export benchmark; never scales or restarts services."""
import argparse
from datetime import datetime, timezone
import getpass
import json
import os
import re
import socket
import subprocess
import time
import urllib.request
from urllib.parse import urlsplit


def measure(submit, status, clock, sleep, count, timeout, worker_ids):
    if not 1 <= count <= 100 or not 1 <= timeout <= 300 or len(worker_ids) not in (1, 3):
        raise ValueError("invalid benchmark limits")
    started = clock()
    ids = [submit() for _ in range(count)]
    if len(set(ids)) != count:
        raise ValueError("duplicate job identity")
    remaining, completed, failed = set(ids), set(), set()
    while remaining and clock() - started < timeout:
        for job_id in list(remaining):
            result = status(job_id)
            if result.get("status") == "done":
                completed.add(job_id)
                remaining.remove(job_id)
            elif result.get("status") == "failed":
                failed.add(job_id)
                remaining.remove(job_id)
        if remaining:
            sleep(0.25)
    elapsed = clock() - started
    return {"passed": not failed and not remaining and elapsed <= timeout,
            "submitted": count, "completed": len(completed), "failed": len(failed),
            "pending": len(remaining), "elapsed_seconds": elapsed,
            "jobs_per_minute": len(completed) * 60 / elapsed if elapsed > 0 else 0,
            "replicas_observed": len(worker_ids), "worker_container_ids": worker_ids}


def workers(context, project, runner=subprocess.run):
    if context != "arqweb-lab" or project != "arqweb-lab":
        raise ValueError("only explicitly isolated arqweb-lab context/project supported")
    def docker(args):
        # Explicit context defeats inherited DOCKER_HOST and default-context drift.
        env = {k: v for k, v in os.environ.items() if not k.startswith("DOCKER_")}
        result = runner(["docker", "--context", context, *args], check=True,
                        capture_output=True, text=True, timeout=10, env=env)
        return result.stdout
    metadata = json.loads(docker(["context", "inspect", context]))
    endpoint = metadata[0]["Endpoints"]["docker"]["Host"]
    if not endpoint.startswith("unix:///") or "arqweb-lab" not in endpoint:
        raise ValueError("context must point to the private arqweb-lab Unix socket")
    ids = docker(["ps", "--filter", "label=com.docker.compose.project=" + project,
                  "--filter", "label=com.docker.compose.service=export-worker",
                  "--format", "{{.ID}}", "--no-trunc"]).split()
    if len(ids) not in (1, 3) or any(not re.fullmatch(r"[a-f0-9]{64}", i) for i in ids):
        raise ValueError("expected exactly one or three running export-worker containers")
    return sorted(ids)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--isolated-lab", action="store_true")
    parser.add_argument("--base-url", default="http://127.0.0.1:8004")
    parser.add_argument("--repo", default="team/demo")
    parser.add_argument("--count", type=int, default=50)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--expected-workers", type=int, choices=(1, 3), default=1)
    args = parser.parse_args(argv)
    if not args.execute:
        print(json.dumps({"mode": "PLAN_ONLY", "mutations": False,
                          "next": "Use an authorized isolated lab; run separately with one and three workers."}))
        return 0
    # All guards run before Docker queries, journaling or HTTP traffic.
    url = urlsplit(args.base_url)
    if not args.isolated_lab or url.scheme != "http" or url.hostname not in ("127.0.0.1", "::1"):
        parser.error("explicit isolated-lab attestation and literal loopback HTTP URL required")
    if url.username or url.password or url.query or url.fragment or url.path not in ("", "/"):
        parser.error("base URL must have no credentials, path, query or fragment")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", args.repo):
        parser.error("invalid repository")
    if not 1 <= args.count <= 100 or not 1 <= args.timeout <= 300:
        parser.error("count must be 1..100 and timeout 1..300 seconds")
    observed = workers("arqweb-lab", "arqweb-lab")
    if len(observed) != args.expected_workers:
        parser.error("observed replica count differs from requested experiment")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    deadline = time.monotonic() + args.timeout
    def request(path, payload=None):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("benchmark deadline")
        body = json.dumps(payload).encode() if payload is not None else None
        req = urllib.request.Request(args.base_url.rstrip("/") + path, data=body,
                                     headers={"Content-Type": "application/json"})
        with opener.open(req, timeout=min(3, remaining)) as response:
            result = json.loads(response.read(65536))
        return result
    def submit():
        result = request("/v2/exports", {"repo": args.repo, "format": "csv", "scope": "activity"})
        job_id = result.get("job_id", "")
        if not re.fullmatch(r"job-[a-f0-9]{8,32}", job_id):
            raise ValueError("unexpected job identity")
        return job_id
    record = {"created_utc": datetime.now(timezone.utc).isoformat(),
              "observer": getpass.getuser() + "@" + socket.gethostname(),
              "scope": "EXPORT_LOAD_ONLY_NOT_RUBRIC_ACCEPTANCE"}
    try:
        record.update(measure(submit, lambda j: request("/v2/exports/" + j),
                              time.monotonic, time.sleep, args.count, args.timeout, observed))
        if workers("arqweb-lab", "arqweb-lab") != observed:
            record.update(passed=False, invalidated="worker cohort changed")
    except Exception:
        record.update(passed=False, inconclusive=True,
                      error="collection failed; inspect isolated lab, no credential/URL output")
    print(json.dumps(record, indent=2))
    return 0 if record.get("passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())
