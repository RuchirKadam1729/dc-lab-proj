# src/pb_monitor.py
"""
Primary-Backup Monitor

How it works (no assumptions about control-port mapping):
- It first queries each node's `/status` endpoint (e.g. http://chat_node1:8001/status)
  to discover:
    - the node's reported grpc_addr (like chat_node1:50051)
    - the PB control port (status['pb']['control_port']) we stored in get_status()
    - node_id, role, etc.
- Using that information it contacts the node's control endpoint: http://<host>:<control_port>/pb/...
- It polls status repeatedly and will attempt failover:
    - If the current primary's PB control endpoint becomes unreachable for `unresponsive_seconds`
      OR the primary's job has been "processing" far longer than expected, monitor promotes a backup.
- Run:
    python src/pb_monitor.py
  or inside docker network you can run similarly (container must be able to resolve hostnames).
"""
import asyncio
import time
import httpx
import argparse
from typing import List, Dict, Optional

# Default status endpoints for your 3 nodes. Adjust if you re-mapped ports.
DEFAULT_STATUS_URLS = [
    "http://chat_node1:8001/status",
    "http://chat_node2:8002/status",
    "http://chat_node3:8003/status",
]

POLL_INTERVAL = 2.0  # seconds between polls
UNRESPONSIVE_SECONDS = 12.0  # how long primary must be unreachable before promoting backup
STUCK_FACTOR = 1.5  # if processing_ms * STUCK_FACTOR < elapsed_time consider stuck

async def fetch_json(client: httpx.AsyncClient, url: str, timeout: float = 3.0) -> Optional[dict]:
    try:
        r = await client.get(url, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None

def derive_control_url_from_status(status: dict) -> Optional[str]:
    """
    status is the object returned by /status endpoint.
    We expect status['pb']['control_port'] and status['grpc_addr'] or similar to exist.
    """
    try:
        pb = status.get("pb", {})
        control_port = pb.get("control_port")
        if not control_port:
            return None
        grpc_addr = status.get("grpc_addr") or ""
        host = grpc_addr.split(":")[0] if ":" in grpc_addr else None
        if not host:
            # fallback: maybe use status endpoint hostname
            return None
        return f"http://{host}:{control_port}"
    except Exception:
        return None

async def monitor_loop(status_urls: List[str], poll_interval: float, unresponsive_seconds: float):
    async with httpx.AsyncClient() as client:
        last_seen = {}  # node_status_url -> last time status fetch succeeded
        last_status = {}  # node_status_url -> last status json
        print("PB Monitor starting. Polling:", status_urls)

        while True:
            now = time.time()
            # fetch all status endpoints
            tasks = [fetch_json(client, u) for u in status_urls]
            results = await asyncio.gather(*tasks)

            nodes = []  # collect discovered nodes with control urls
            for u, res in zip(status_urls, results):
                if res:
                    last_seen[u] = now
                    last_status[u] = res
                    control_url = derive_control_url_from_status(res)
                    nodes.append({
                        "status_url": u,
                        "status": res,
                        "control_url": control_url,
                        "last_seen": last_seen[u],
                    })
                else:
                    # mark unreachable
                    nodes.append({
                        "status_url": u,
                        "status": None,
                        "control_url": None,
                        "last_seen": last_seen.get(u, None),
                    })

            # determine current leader from nodes' bully_election_status (if available)
            leader = None
            leader_node = None
            for n in nodes:
                s = n["status"]
                if s:
                    bully = s.get("bully_election_status", {})
                    cand = bully.get("current_leader")
                    if cand:
                        leader = cand
                        leader_node = n
                        break
            # fallback: if no bully claim, pick node whose pb.role == primary
            if not leader:
                for n in nodes:
                    s = n["status"]
                    if s:
                        pb = s.get("pb", {})
                        if pb.get("role") == "primary":
                            leader = s.get("server_id")
                            leader_node = n
                            break

            # Print a concise snapshot
            print("\n=== PB Monitor snapshot @", time.strftime("%H:%M:%S"))
            for n in nodes:
                s = n["status"]
                if not s:
                    last = n["last_seen"]
                    age = (time.time() - last) if last else None
                    print(f"{n['status_url']}: UNREACHABLE (last_seen_age={age:.1f}s)")
                else:
                    pb = s.get("pb", {})
                    job = pb.get("job", {})
                    role = pb.get("role")
                    job_phase = job.get("phase")
                    started = job.get("started_at")
                    elapsed = (time.time() - started) if started else None
                    print(f"{s.get('server_id')} @ {n['status_url']} role={role} job_phase={job_phase} elapsed={elapsed:.1f}s control={n['control_url']}")

            # Decide if leader is unresponsive or stuck
            if leader_node and leader_node["status"] is None:
                # leader is unreachable; find a backup and promote it
                print(f">>> Leader {leader} appears unreachable. Checking for backup to promote.")
                # find backup candidate (status available, role backup or not primary)
                candidate = None
                for n in nodes:
                    s = n["status"]
                    if s and s.get("server_id") != leader:
                        pb = s.get("pb", {})
                        if pb.get("role") in ("backup", "monitor") or s.get("server_id") != leader:
                            candidate = n
                            break
                if candidate and candidate["control_url"]:
                    # promote candidate
                    promote_url = f"{candidate['control_url']}/pb/set_role"
                    demote_url = None
                    if leader_node and leader_node.get("control_url"):
                        demote_url = f"{leader_node['control_url']}/pb/set_role"
                    try:
                        print(f">>> Promoting {candidate['status'].get('server_id')} to primary via {promote_url}")
                        await client.post(promote_url, json={"role": "primary"}, timeout=5.0)
                        if demote_url:
                            print(f">>> Demoting {leader} to backup via {demote_url}")
                            await client.post(demote_url, json={"role": "backup"}, timeout=5.0)
                    except Exception as ex:
                        print("Promotion/demotion failed:", ex)
                else:
                    print("No candidate backup found to promote.")
            else:
                # leader reachable -> inspect job progress for stuckness
                if leader_node and leader_node["status"]:
                    s = leader_node["status"]
                    pb = s.get("pb", {})
                    job = pb.get("job", {})
                    if job and job.get("phase") == "processing":
                        started = job.get("started_at")
                        processing_ms = job.get("processing_ms") or 0
                        if started:
                            elapsed = time.time() - started
                            # If elapsed is much larger than expected or primary control endpoint not responding
                            leader_ctrl = leader_node.get("control_url")
                            ctrl_ok = True
                            if leader_ctrl:
                                try:
                                    # quickly ping pb/status
                                    r = await client.get(f"{leader_ctrl}/pb/status", timeout=2.0)
                                    if r.status_code != 200:
                                        ctrl_ok = False
                                except Exception:
                                    ctrl_ok = False
                            if (not ctrl_ok and (leader_node.get("last_seen") is None or (time.time() - leader_node.get("last_seen", 0) > unresponsive_seconds))) or elapsed > (processing_ms/1000.0)*STUCK_FACTOR:
                                # consider promoting backup
                                print(f">>> Leader {leader} looks stuck/unresponsive (elapsed={elapsed:.1f}s, expected={processing_ms/1000.0:.1f}s). Triggering failover.")
                                # pick a backup
                                candidate = None
                                for n in nodes:
                                    s2 = n["status"]
                                    if s2 and s2.get("server_id") != leader:
                                        pb2 = s2.get("pb", {})
                                        if pb2.get("role") in ("backup", "monitor") or s2.get("server_id") != leader:
                                            candidate = n
                                            break
                                if candidate and candidate["control_url"]:
                                    promote_url = f"{candidate['control_url']}/pb/set_role"
                                    demote_url = None
                                    if leader_node and leader_node.get("control_url"):
                                        demote_url = f"{leader_node['control_url']}/pb/set_role"
                                    try:
                                        print(f">>> Promoting {candidate['status'].get('server_id')} to primary via {promote_url}")
                                        await client.post(promote_url, json={"role": "primary"}, timeout=5.0)
                                        if demote_url:
                                            print(f">>> Demoting {leader} to backup via {demote_url}")
                                            await client.post(demote_url, json={"role": "backup"}, timeout=5.0)
                                    except Exception as ex:
                                        print("Promotion/demotion failed:", ex)
                                else:
                                    print("No candidate backup available to promote.")
            # sleep then continue
            await asyncio.sleep(poll_interval)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--status-urls", nargs="+", default=DEFAULT_STATUS_URLS,
                        help="List of /status URLs to poll (e.g. http://chat_node1:8001/status)")
    parser.add_argument("--poll-interval", type=float, default=POLL_INTERVAL)
    parser.add_argument("--unresponsive-seconds", type=float, default=UNRESPONSIVE_SECONDS)
    args = parser.parse_args()

    try:
        asyncio.run(monitor_loop(args.status_urls, args.poll_interval, args.unresponsive_seconds))
    except KeyboardInterrupt:
        print("Monitor stopped by user.")
