#!/usr/bin/env python3
"""
Primary-Backup client simulator (fixed)

Usage examples (recommended):
  python src/pb_client.py --status-urls http://localhost:8001/status http://localhost:8002/status http://localhost:8003/status --processing-ms 120000 --force-localhost

If you already know the control endpoint, you can skip discovery:
  python src/pb_client.py --request-url http://localhost:8101 --processing-ms 120000

This version fixes host-resolution when running on host OS while servers advertise container hostnames
(like `chat_node1`). It will try the advertised host first and automatically fall back to localhost:<control_port>
if the advertised host is not reachable.
"""

import asyncio
import httpx
import time
import argparse
from typing import List, Optional

DEFAULT_STATUS_URLS = [
    "http://chat_node1:8001/status",
    "http://chat_node2:8002/status",
    "http://chat_node3:8003/status",
]


async def fetch_json(client: httpx.AsyncClient, url: str, timeout: float = 3.0):
    try:
        r = await client.get(url, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


async def derive_control_url_from_status(status: dict, client: httpx.AsyncClient, force_localhost: bool = False) -> Optional[str]:
    """Derive the node control URL from /status JSON.

    Strategy:
      1. If `pb.control_port` present, try advertised host (from grpc_addr) first.
      2. If advertised host doesn't respond at /pb/status, try localhost:<control_port> (for host-side testing).
      3. If force_localhost is True, skip testing advertised host and return localhost:<control_port> if it responds.
      4. Return None if no working control URL could be found.
    """
    try:
        pb = status.get("pb", {})
        control_port = pb.get("control_port")
        if not control_port:
            return None

        grpc_addr = status.get("grpc_addr") or ""
        advertised_host = grpc_addr.split(":")[0] if ":" in grpc_addr else None

        candidates = []
        if advertised_host and not force_localhost:
            candidates.append(f"http://{advertised_host}:{control_port}")
        # always attempt localhost fallback for host-based testing
        candidates.append(f"http://localhost:{control_port}")

        # try each candidate quickly (GET /pb/status) and return the first that responds 200
        for c in candidates:
            try:
                # check pb/status quickly
                r = await client.get(f"{c}/pb/status", timeout=1.0)
                if r.status_code == 200:
                    return c
            except Exception:
                # try next
                continue

        # if none responded, still return the first candidate (useful for debugging / to see error)
        return candidates[0] if candidates else None
    except Exception:
        return None


async def find_primary(client: httpx.AsyncClient, status_urls: List[str], force_localhost: bool = False) -> (Optional[str], Optional[dict]):
    # query statuses and look for bully_election_status.current_leader or pb.role
    candidate_primary = None
    statuses = {}

    # fetch statuses in parallel
    tasks = [fetch_json(client, u) for u in status_urls]
    results = await asyncio.gather(*tasks)
    for u, s in zip(status_urls, results):
        statuses[u] = s
        if s:
            be = s.get("bully_election_status", {})
            leader = be.get("current_leader")
            if leader:
                candidate_primary = leader
                # don't break; we still record all statuses

    # If we found a leader name, find its status object and derive control URL
    if candidate_primary:
        for u, s in statuses.items():
            if s and s.get("server_id") == candidate_primary:
                ctrl = await derive_control_url_from_status(s, client, force_localhost=force_localhost)
                return ctrl, s

    # fallback: pick node whose pb.role == "primary"
    for u, s in statuses.items():
        if s:
            pb = s.get("pb", {})
            if pb.get("role") == "primary":
                ctrl = await derive_control_url_from_status(s, client, force_localhost=force_localhost)
                return ctrl, s

    # final fallback: pick first reachable node's control_url
    for u, s in statuses.items():
        if s:
            ctrl = await derive_control_url_from_status(s, client, force_localhost=force_localhost)
            if ctrl:
                return ctrl, s

    return None, None


async def start_job(client: httpx.AsyncClient, ctrl_url: str, processing_ms: int):
    try:
        url = f"{ctrl_url}/pb/start_job"
        r = await client.post(url, json={"processing_ms": processing_ms}, timeout=10.0)
        # return full response (including body on non-200) for better debugging
        try:
            body = r.json()
        except Exception:
            body = r.text
        if r.status_code != 200:
            print(f"start_job: non-200 returned: {r.status_code} -> {body}")
            return None
        return body
    except Exception as e:
        print("Failed to start job:", repr(e))
        return None


async def poll_job_statuses(client: httpx.AsyncClient, status_urls: List[str], interval: float):
    print("Polling job statuses (ctrl endpoints discovered from /status)...")
    while True:
        rows = []
        finished = False
        for u in status_urls:
            s = await fetch_json(client, u)
            if s:
                pb = s.get("pb", {})
                job = pb.get("job", {})
                rows.append((
                    s.get("server_id"),
                    pb.get("role"),
                    job.get("phase"),
                    job.get("processing_ms"),
                    job.get("started_at"),
                    job.get("result")
                ))
                if job.get("phase") == "finished":
                    finished = True
            else:
                rows.append((u, None, "UNREACHABLE", None, None, None))
        ts = time.strftime("%H:%M:%S")
        print(f"\n[{ts}] Job snapshot:")
        for r in rows:
            print(f"  {r[0]:10} role={r[1]:7} phase={r[2]:12} proc_ms={r[3]} started_at={r[4]} result={r[5]}")
        if finished:
            print("At least one node finished the job. Stopping poll.")
            return
        await asyncio.sleep(interval)


async def main_loop(status_urls: List[str], processing_ms: int, poll_interval: float, force_localhost: bool = False, request_url: Optional[str] = None):
    async with httpx.AsyncClient() as client:
        if request_url:
            ctrl_url = request_url.rstrip("/")
            primary_status = None
            print("Using request-url (skip discovery):", ctrl_url)
        else:
            ctrl_url, primary_status = await find_primary(client, status_urls, force_localhost=force_localhost)
            if not ctrl_url:
                print("Could not find a primary control endpoint via /status discovery. Exiting.")
                return
            print("Discovered primary control endpoint:", ctrl_url, "server status:", primary_status.get("server_id") if primary_status else "?")

        job_response = await start_job(client, ctrl_url, processing_ms)
        if job_response:
            print("Job started:", job_response)
        else:
            print("Failed to start job. Exiting.")
            return

        await poll_job_statuses(client, status_urls, poll_interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--status-urls", nargs="+", default=DEFAULT_STATUS_URLS, help="List of /status URLs")
    parser.add_argument("--processing-ms", type=int, default=5000, help="Job processing time in ms")
    parser.add_argument("--poll-interval", type=float, default=1.0, help="Poll interval in seconds")
    parser.add_argument("--force-localhost", action="store_true", help="Force control URL host to localhost (skip advertised host)")
    parser.add_argument("--request-url", type=str, help="Direct control URL to call (skip discovery), e.g. http://localhost:8101")
    args = parser.parse_args()

    asyncio.run(main_loop(args.status_urls, args.processing_ms, args.poll_interval, force_localhost=args.force_localhost, request_url=args.request_url))
