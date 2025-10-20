# src/pb_monitor.py
"""
Primary-Backup Monitor

How it works (no assumptions about control-port mapping):
- It first queries each node's `/status` endpoint (e.g. http://chat_node1:8001/status)
  to discover:
    - the node's reported grpc_addr (like chat_node1:50051)
    - the PB control port (status['pb']['control_port']) we stored in get_status()
    - node_id, role, etc.
- Using that information it contacts the node's control endpoint: http://<host>:<control_port>/
- It polls status repeatedly and will attempt failover:
    - If the current primary's PB control endpoint becomes unreachable for `unresponsive_seconds`
      OR the primary's job has been "processing" far longer than expected, monitor promotes a backup.
Run:
    python src/pb_monitor.py --status-urls http://localhost:8001/status http://localhost:8002/status http://localhost:8003/status
"""
import asyncio
import time
import httpx
import argparse
from typing import List, Dict, Optional
from urllib.parse import urlparse

# Default status endpoints for your 3 nodes. Adjust if you re-mapped ports.
DEFAULT_STATUS_URLS = [
    "http://chat_node1:8001/status",
    "http://chat_node2:8002/status",
    "http://chat_node3:8003/status",
]

POLL_INTERVAL = 2.0  # seconds between polls
UNRESPONSIVE_SECONDS = 12.0  # how long primary must be unreachable before promoting backup
STUCK_FACTOR = 1.5  # if processing_ms * STUCK_FACTOR < elapsed_time consider stuck

# reasonable default mapping when pb.control_port missing (compose mapping used earlier)
DEFAULT_CONTROL_PORT_MAP = {
    "chat_node1": 8101,
    "chat_node2": 8102,
    "chat_node3": 8103,
    # fallback default if unknown host
    "default": 8101,
}


async def fetch_json(client: httpx.AsyncClient, url: str, timeout: float = 3.0) -> Optional[dict]:
    try:
        r = await client.get(url, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def _host_from_status_url(status_url: str) -> Optional[str]:
    try:
        p = urlparse(status_url)
        return p.hostname
    except Exception:
        return None


def derive_control_url_from_status(status: dict, status_url: str) -> Optional[str]:
    """
    status is the object returned by /status endpoint.
    We expect status['pb']['control_port'] and status['grpc_addr'] or similar to exist.
    If pb.control_port missing, we use sensible fallbacks:
     - prefer pb.control_port if present
     - else map hostname (chat_node1->8101 etc.) from status_url
    Returns full http://host:port or None
    """
    try:
        # preferred explicit value
        pb = status.get("pb") or {}
        control_port = pb.get("control_port")
        if control_port:
            # try to use a sensible host: prefer grpc_addr host, else status_url host
            grpc_addr = status.get("grpc_addr", "") or ""
            if ":" in grpc_addr:
                host = grpc_addr.split(":", 1)[0]
            else:
                host = _host_from_status_url(status_url)
            if not host:
                return None
            return f"http://{host}:{control_port}"

        # fallback: try to infer from status_url hostname mapping
        host_from_status = _host_from_status_url(status_url)
        if host_from_status:
            # map chat_node1->8101 etc.
            base = DEFAULT_CONTROL_PORT_MAP.get(host_from_status, DEFAULT_CONTROL_PORT_MAP["default"])
            return f"http://{host_from_status}:{base}"

        return None
    except Exception:
        return None


def _safe_get_job_info(status: dict) -> Dict:
    """Return a normalized job dict with sensible defaults."""
    pb = status.get("pb") or {}
    job = pb.get("job") or {}
    # normalize started_at: could be epoch seconds or ms
    started = job.get("started_at")
    if isinstance(started, (int, float)):
        # if milliseconds (greater than a large threshold), convert to seconds
        if started > 1e12:
            started = started / 1000.0
    else:
        started = None
    processing_ms = job.get("processing_ms") or 0
    phase = job.get("phase") or "idle"
    return {
        "phase": phase,
        "started_at": started,
        "processing_ms": processing_ms,
        "raw": job,
    }


async def monitor_loop(status_urls: List[str], poll_interval: float, unresponsive_seconds: float):
    async with httpx.AsyncClient() as client:
        last_seen = {}  # node_status_url -> last time status fetch succeeded
        last_status = {}  # node_status_url -> last status json
        print("PB Monitor starting. Polling:", status_urls)

        while True:
            now = time.time()
            # fetch all status endpoints concurrently
            tasks = [fetch_json(client, u) for u in status_urls]
            results = await asyncio.gather(*tasks)

            nodes = []  # collect discovered nodes with control urls
            for u, res in zip(status_urls, results):
                if res:
                    last_seen[u] = now
                    last_status[u] = res
                    control_url = derive_control_url_from_status(res, u)
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
                    bully = s.get("bully_election_status", {}) or {}
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
                        pb = s.get("pb", {}) or {}
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
                    age_display = f"{age:.1f}s" if isinstance(age, (int, float)) else "N/A"
                    print(f"{n['status_url']}: UNREACHABLE (last_seen_age={age_display})")
                else:
                    # robust extraction
                    server_id = s.get("server_id", "unknown")
                    pb = s.get("pb") or {}
                    role = pb.get("role", "unknown")
                    job_info = _safe_get_job_info(s)
                    job_phase = job_info["phase"]
                    started = job_info["started_at"]
                    elapsed = None
                    if isinstance(started, (int, float)):
                        elapsed = time.time() - started
                    # format elapsed safely
                    elapsed_display = f"{elapsed:.1f}s" if isinstance(elapsed, (int, float)) else "N/A"
                    control = n.get("control_url") or "N/A"
                    print(f"{server_id} @ {n['status_url']} role={role} job_phase={job_phase} elapsed={elapsed_display} control={control}")

            # Decide if leader is unresponsive or stuck
            if leader_node and leader_node["status"] is None:
                # leader is unreachable; find a backup and promote it
                print(f">>> Leader {leader} appears unreachable. Checking for backup to promote.")
                candidate = None
                for n in nodes:
                    s = n["status"]
                    if s and s.get("server_id") != leader:
                        pb = s.get("pb", {}) or {}
                        if pb.get("role") in ("backup", "monitor") or s.get("server_id") != leader:
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
                    print("No candidate backup found to promote.")
            else:
                # leader reachable -> inspect job progress for stuckness
                if leader_node and leader_node["status"]:
                    s = leader_node["status"]
                    pb = s.get("pb") or {}
                    job_info = _safe_get_job_info(s)
                    if job_info and job_info["phase"] == "processing":
                        started = job_info["started_at"]
                        processing_ms = job_info["processing_ms"] or 0
                        if started:
                            elapsed = time.time() - started
                            # If elapsed much larger than expected or primary control not responding
                            leader_ctrl = leader_node.get("control_url")
                            ctrl_ok = True
                            if leader_ctrl:
                                try:
                                    r = await client.get(f"{leader_ctrl}/pb/status", timeout=2.0)
                                    if r.status_code != 200:
                                        ctrl_ok = False
                                except Exception:
                                    ctrl_ok = False
                            # check unresponsive or stuck threshold
                            should_failover = False
                            if not ctrl_ok:
                                last_seen_val = leader_node.get("last_seen")
                                if last_seen_val is None or (time.time() - last_seen_val > unresponsive_seconds):
                                    should_failover = True
                            if elapsed > (processing_ms / 1000.0) * STUCK_FACTOR and processing_ms > 0:
                                should_failover = True

                            if should_failover:
                                print(f">>> Leader {leader} looks stuck/unresponsive (elapsed={elapsed:.1f}s, expected={(processing_ms/1000.0):.1f}s). Triggering failover.")
                                # pick a backup candidate
                                candidate = None
                                for n in nodes:
                                    s2 = n["status"]
                                    if s2 and s2.get("server_id") != leader:
                                        pb2 = s2.get("pb") or {}
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
        print("Monitor stopped by user")
