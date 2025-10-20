# src/pb_client.py
"""
Primary-Backup client simulator

Usage:
    python src/pb_client.py [--status-urls ...] [--processing-ms N] [--poll-interval S]

It will:
 - discover current primary by querying /status endpoints
 - call primary's /pb/start_job to request a long-running job (processing_ms)
 - then poll /pb/status on all nodes and print a timeline until job finishes on one node
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

def derive_control_url_from_status(status: dict) -> Optional[str]:
    try:
        pb = status.get("pb", {})
        control_port = pb.get("control_port")
        if not control_port:
            return None
        grpc_addr = status.get("grpc_addr") or ""
        host = grpc_addr.split(":")[0] if ":" in grpc_addr else None
        if not host:
            return None
        return f"http://{host}:{control_port}"
    except Exception:
        return None

async def find_primary(client: httpx.AsyncClient, status_urls: List[str]):
    # query statuses and look for bully_election_status.current_leader or pb.role
    candidate_primary = None
    statuses = {}
    for u in status_urls:
        s = await fetch_json(client, u)
        statuses[u] = s
        if s:
            be = s.get("bully_election_status", {})
            leader = be.get("current_leader")
            if leader:
                candidate_primary = leader
                break
    # if leader name found, find the corresponding status URL for that server_id
    if candidate_primary:
        for u,s in statuses.items():
            if s and s.get("server_id") == candidate_primary:
                ctrl = derive_control_url_from_status(s)
                return ctrl, s
    # fallback: pick node whose pb.role == "primary"
    for u,s in statuses.items():
        if s:
            pb = s.get("pb", {})
            if pb.get("role") == "primary":
                ctrl = derive_control_url_from_status(s)
                return ctrl, s
    # final fallback: pick the first reachable node's control_url
    for u,s in statuses.items():
        if s:
            ctrl = derive_control_url_from_status(s)
            if ctrl:
                return ctrl, s
    return None, None

async def poll_job_statuses(client: httpx.AsyncClient, status_urls: List[str], interval: float):
    """
    Poll PB job status (via discovered control_url from /status) for each node and print compact output.
    """
    print("Polling job statuses (ctrl endpoints discovered from /status)...")
    while True:
        rows = []
        finished = False
        for u in status_urls:
            s = await fetch_json(client, u)
            if s:
                pb = s.get("pb", {})
                job = pb.get("job", {})
                rows.append((s.get("server_id"), pb.get("role"), job.get("phase"), job.get("processing_ms"), job.get("started_at"), job.get("result")))
                if job.get("phase") == "finished":
                    finished = True
            else:
                rows.append((u, None, "UNREACHABLE", None, None, None))
        # print snapshot
        ts = time.strftime("%H:%M:%S")
        print(f"\n[{ts}] Job snapshot:")
        for r in rows:
            print(f"  {r[0]:10} role={r[1]:7} phase={r[2]:12} proc_ms={r[3]} started_at={r[4]} result={r[5]}")
        if finished:
            print("At least one node finished the job. Stopping poll.")
            return
        await asyncio.sleep(interval)

async def main_loop(status_urls: List[str], processing_ms: int, poll_interval: float):
    async with httpx.AsyncClient() as client:
        ctrl_url, primary_status = await find_primary(client, status_urls)
        if not ctrl_url:
            print("Could not find a primary control endpoint via /status discovery. Exiting.")
            return
        print("Discovered primary control endpoint:", ctrl_url, "server status:", primary_status.get("server_id"))
