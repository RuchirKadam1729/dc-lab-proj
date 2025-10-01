# src/load_balancer.py
import asyncio
import json
import logging
import os
import random
import statistics
import time
from collections import defaultdict, deque
from typing import Dict, List, Optional

import httpx
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
app = FastAPI(title="Chat LB", docs_url="/lb/docs")

# configuration (env overrides allowed)
NODES = os.environ.get("LB_NODES", "http://node1:8001,http://node2:8002,http://node3:8003").split(",")
POLL_INTERVAL = int(os.environ.get("LB_POLL_INTERVAL", "2"))  # seconds between polling real /status
HISTORY_SIZE = int(os.environ.get("LB_HISTORY_SIZE", "30"))   # number of response samples kept
DEFAULT_CAPACITY = int(os.environ.get("LB_DEFAULT_CAPACITY", "200"))  # default simulated capacity
PER_CONN_LAT_MS = float(os.environ.get("LB_PER_CONN_LAT_MS", "0.5"))  # added ms per connection (impact on synthetic latency)
JITTER_MS = float(os.environ.get("LB_JITTER_MS", "3.0"))  # jitter for synthetic latency

class NodeMetrics:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.last_status: Optional[dict] = None
        self.last_response_ms: Optional[float] = None
        self.response_history: List[float] = []
        self.last_polled_at: Optional[float] = None

        # simulation fields
        self.simulated_connections: int = 0
        self.simulated_queue: deque = deque()
        self.capacity: int = DEFAULT_CAPACITY
        self.base_latency_ms: float = 10.0  # seeded from /status poll when available

    def avg_response_ms(self) -> float:
        if not self.response_history:
            return float("inf")
        return statistics.mean(self.response_history)

    def effective_connections(self) -> int:
        real = len((self.last_status or {}).get("connected_clients", [])) if self.last_status else 0
        return real + self.simulated_connections

    def queued_count(self) -> int:
        return len(self.simulated_queue)

    def available_resources(self) -> int:
        return max(0, self.capacity - self.effective_connections() - self.queued_count())

    def to_dict(self):
        s = self.last_status or {}
        active = len(s.get("connected_clients", [])) if s else 0
        queued_remote = sum(s.get("queued_messages", {}).values()) if s else 0
        avg = None if self.avg_response_ms() == float("inf") else round(self.avg_response_ms(), 2)
        return {
            "base_url": self.base_url,
            "last_response_ms": None if self.last_response_ms is None else round(self.last_response_ms, 3),
            "avg_response_ms": avg,
            "last_polled_at": self.last_polled_at,
            "status": s,
            "active_connections": active,
            "simulated_connections": self.simulated_connections,
            "simulated_queue_len": self.queued_count(),
            "queued_messages_remote": queued_remote,
            "effective_connections": self.effective_connections(),
            "capacity": self.capacity,
            "available_resources": self.available_resources(),
            "base_latency_ms": round(self.base_latency_ms, 2),
        }

METRICS: Dict[str, NodeMetrics] = {n: NodeMetrics(n) for n in NODES}

# Background poller to fetch /status and seed base latency
async def poller():
    async with httpx.AsyncClient(timeout=5.0) as client:
        while True:
            for base in list(METRICS.keys()):
                nm = METRICS[base]
                url = f"{nm.base_url}/status"
                start = time.time()
                try:
                    resp = await client.get(url)
                    elapsed = (time.time() - start) * 1000.0
                    nm.last_response_ms = elapsed
                    nm.response_history.append(elapsed)
                    if len(nm.response_history) > HISTORY_SIZE:
                        nm.response_history.pop(0)
                    nm.last_polled_at = time.time()
                    nm.last_status = resp.json()
                    # seed base latency from actual average if available
                    if nm.avg_response_ms() != float("inf"):
                        nm.base_latency_ms = max(1.0, nm.avg_response_ms())
                except Exception as e:
                    nm.last_response_ms = None
                    nm.last_status = None
                    logging.debug("poller: failed %s -> %s", url, e)
            await asyncio.sleep(POLL_INTERVAL)

@app.on_event("startup")
async def startup_event():
    app.state.poller_task = asyncio.create_task(poller())
    logging.info("Load Balancer started; polling %d nodes", len(METRICS))

# Synthetic latency: base + per_conn * effective_connections + jitter
def synthetic_latency_sample(nm: NodeMetrics) -> float:
    eff = nm.effective_connections()
    jitter = random.uniform(-JITTER_MS, JITTER_MS)
    sample = nm.base_latency_ms + eff * PER_CONN_LAT_MS + jitter
    return max(0.1, sample)

# Release simulated connection and drain queue if possible
async def release_simulated_connection(node_key: str, processing_ms: int):
    await asyncio.sleep(processing_ms / 1000.0)
    nm = METRICS.get(node_key)
    if nm:
        nm.simulated_connections = max(0, nm.simulated_connections - 1)
        # if queue exists, activate next queued item
        if nm.simulated_queue:
            queued_item = nm.simulated_queue.popleft()
            nm.simulated_connections += 1
            # schedule release for this newly-activated item
            asyncio.create_task(release_simulated_connection(node_key, queued_item.get("processing_ms", processing_ms)))
        logging.debug("Released simulated connection on %s; now active=%d queue=%d", node_key, nm.simulated_connections, nm.queued_count())

# Scoring algorithms (dynamic)
def pick_least_connections() -> Optional[str]:
    best = None
    best_val = float("inf")
    for b, nm in METRICS.items():
        val = nm.effective_connections()
        if val < best_val:
            best_val = val
            best = b
    return best

def pick_least_response_time() -> Optional[str]:
    best = None
    best_val = float("inf")
    for b, nm in METRICS.items():
        val = nm.avg_response_ms()
        if val < best_val:
            best_val = val
            best = b
    return best

def pick_resource_based() -> Optional[str]:
    best = None
    best_score = float("-inf")
    for b, nm in METRICS.items():
        conns = nm.effective_connections()
        queues = nm.queued_count()
        resp = nm.avg_response_ms() if nm.avg_response_ms() != float("inf") else 10000.0
        # score rises for available_resources and lower avg response
        score = (nm.available_resources() / (1 + nm.capacity)) + (1.0 / (1 + resp/100.0))
        score -= (conns / (nm.capacity + 1)) * 0.01
        if score > best_score:
            best_score = score
            best = b
    return best

ALGO_FN = {
    "least_conn": pick_least_connections,
    "least_resp": pick_least_response_time,
    "resource": pick_resource_based,
}

@app.get("/lb/overview")
def overview():
    return JSONResponse({b: METRICS[b].to_dict() for b in METRICS})

@app.get("/lb/choose")
def choose(algo: str = Query("least_conn", pattern="^(least_conn|least_resp|resource)$")):
    fn = ALGO_FN.get(algo)
    if not fn:
        return JSONResponse({"error": "bad algo"}, status_code=400)
    chosen = fn()
    if chosen is None:
        return JSONResponse({"error": "no nodes available"}, status_code=503)
    nm = METRICS[chosen]
    return {"chosen_node": chosen, "status_base": nm.base_url, "avg_response_ms": nm.avg_response_ms()}

@app.get("/lb/simulate")
async def simulate(
    algo: str = Query("least_conn", pattern="^(least_conn|least_resp|resource)$"),
    n: int = Query(10, ge=1, le=2000),
    pause_ms: int = Query(100, ge=0),
    processing_min_ms: int = Query(200, ge=1),
    processing_max_ms: int = Query(600, ge=1),
    capacity: Optional[int] = Query(None),
):
    """
    Dynamic simulation:
     - algo: least_conn | least_resp | resource
     - n: number of requests
     - pause_ms: ms pause between allocation decisions
     - processing_min_ms / processing_max_ms: per-request processing time is random in this inclusive range
     - capacity: optional override applied to all nodes (int)
    """
    if algo not in ALGO_FN:
        return JSONResponse({"error": "invalid algo"}, status_code=400)
    fn = ALGO_FN[algo]
    if capacity is not None:
        for nm in METRICS.values():
            nm.capacity = int(capacity)

    steps = []
    distribution = defaultdict(int)

    for i in range(1, n + 1):
        chosen_key = fn()
        if chosen_key is None:
            steps.append({"index": i, "error": "no nodes available"})
            await asyncio.sleep(pause_ms / 1000.0)
            continue

        nm = METRICS[chosen_key]
        # pick variable processing time between min and max (inclusive)
        processing_ms = random.randint(processing_min_ms, processing_max_ms)

        accepted = False
        if nm.available_resources() > 0:
            nm.simulated_connections += 1
            accepted = True
            # append synthetic latency sample to simulate effect of added load
            lat = synthetic_latency_sample(nm)
            nm.response_history.append(lat)
            if len(nm.response_history) > HISTORY_SIZE:
                nm.response_history.pop(0)
            # schedule release
            asyncio.create_task(release_simulated_connection(chosen_key, processing_ms))
        else:
            # queue the request with its processing time metadata
            nm.simulated_queue.append({"requested_at": time.time(), "processing_ms": processing_ms})
            lat = synthetic_latency_sample(nm)
            nm.response_history.append(lat)
            if len(nm.response_history) > HISTORY_SIZE:
                nm.response_history.pop(0)

        distribution[chosen_key] += 1

        step = {
            "index": i,
            "chosen_node": chosen_key,
            "processing_ms": processing_ms,
            "accepted": accepted,
            "simulated_connections": nm.simulated_connections,
            "simulated_queue_len": nm.queued_count(),
            "effective_connections": nm.effective_connections(),
            "capacity": nm.capacity,
            "available_resources": nm.available_resources(),
            "avg_response_ms": None if nm.avg_response_ms() == float("inf") else round(nm.avg_response_ms(), 2),
        }
        steps.append(step)

        # pause so the system state can change and decisions become visible
        if pause_ms > 0:
            await asyncio.sleep(pause_ms / 1000.0)

    final_sim = {k: {"simulated_connections": METRICS[k].simulated_connections, "queue_len": METRICS[k].queued_count()} for k in METRICS}
    return {"algo": algo, "requested": n, "pause_ms": pause_ms, "processing_min_ms": processing_min_ms, "processing_max_ms": processing_max_ms, "distribution": dict(distribution), "steps": steps, "final_sim": final_sim}

@app.post("/lb/reset_sim")
def reset_sim():
    for nm in METRICS.values():
        nm.simulated_connections = 0
        nm.simulated_queue.clear()
        nm.capacity = DEFAULT_CAPACITY
    return {"status": "reset"}

# UI (improved) - includes controls for processing min/max and shows steps + final
@app.get("/", response_class=HTMLResponse)
def home():
    html = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Load Balancer UI — Dynamic Simulation</title>
  <style>
    body { font-family: system-ui, Arial; margin: 18px; }
    .controls { margin-bottom: 12px; }
    .node { border: 1px solid #ddd; padding: 10px; margin: 6px; display:inline-block; width:300px; vertical-align:top; background:#fafafa; border-radius:6px; box-shadow: 0 1px 2px rgba(0,0,0,0.03); }
    .box { border: 1px solid #bbb; padding: 10px; border-radius:6px; background:#fff; margin-bottom:8px; }
    table { width:100%; border-collapse: collapse; font-size:13px; }
    td, th { border: 1px solid #eee; padding:6px; text-align:left; }
    input[type="number"] { width:80px; }
    button { padding:6px 10px; margin-left:6px; }
    pre { background:#f6f6f6; padding:8px; border-radius:4px; overflow:auto; }
  </style>
</head>
<body>
  <h2>Dynamic Load Balancer — Simulation UI</h2>

  <div class="controls">
    Algorithm:
    <select id="algo">
      <option value="least_conn">Least Connections</option>
      <option value="least_resp">Least Response Time</option>
      <option value="resource">Resource-based</option>
    </select>
    <button id="chooseBtn">Choose node</button>
    <button id="refreshBtn">Refresh metrics</button>
    &nbsp;&nbsp; Simulate N:
    <input id="simN" type="number" value="40" />
    Pause(ms):
    <input id="pauseMs" type="number" value="100" />
    Proc min(ms):
    <input id="procMin" type="number" value="200" />
    Proc max(ms):
    <input id="procMax" type="number" value="600" />
    Capacity (optional):
    <input id="cap" type="number" placeholder="e.g. 50" />
    <button id="simBtn">Simulate</button>
    <button id="resetBtn">Reset Sim</button>
  </div>

  <div id="chosen" style="margin-bottom:10px; font-weight:bold;"></div>

  <div id="nodes"></div>

  <div id="results"></div>

<script>
async function refresh(){
  try {
    const r = await fetch('/lb/overview');
    const j = await r.json();
    const nodes = document.getElementById('nodes');
    nodes.innerHTML = '';
    for(const k of Object.keys(j)){
      const n = j[k];
      const el = document.createElement('div');
      el.className = 'node';
      el.innerHTML = `<h3>${k}</h3>
        <div><b>base</b>: ${n.base_url}</div>
        <div><b>active (real)</b>: ${n.active_connections}</div>
        <div><b>simulated</b>: ${n.simulated_connections}</div>
        <div><b>queue (sim)</b>: ${n.simulated_queue_len}</div>
        <div><b>effective</b>: ${n.effective_connections}</div>
        <div><b>remote queued</b>: ${n.queued_messages_remote}</div>
        <div><b>avg_resp_ms</b>: ${n.avg_response_ms !== null ? n.avg_response_ms : 'n/a'}</div>
        <div><b>base_latency_ms</b>: ${n.base_latency_ms}</div>
        <div><b>capacity</b>: ${n.capacity} &nbsp; <b>avail</b>: ${n.available_resources}</div>
      `;
      nodes.appendChild(el);
    }
  } catch(e){
    document.getElementById('nodes').innerHTML = '<div class="box">Failed to fetch node metrics.</div>';
  }
}

document.getElementById('chooseBtn').onclick = async () => {
  const algo = document.getElementById('algo').value;
  const r = await fetch(`/lb/choose?algo=${algo}`);
  const j = await r.json();
  document.getElementById('chosen').innerText = 'Chosen node: ' + JSON.stringify(j);
};

document.getElementById('refreshBtn').onclick = refresh;

document.getElementById('resetBtn').onclick = async () => {
  await fetch('/lb/reset_sim', {method:'POST'});
  await refresh();
  document.getElementById('results').innerHTML = '<div class="box"><b>Sim counters reset.</b></div>';
};

document.getElementById('simBtn').onclick = async () => {
  const n = parseInt(document.getElementById('simN').value||'40',10);
  const pause = parseInt(document.getElementById('pauseMs').value||'100',10);
  const procMin = parseInt(document.getElementById('procMin').value||'200',10);
  const procMax = parseInt(document.getElementById('procMax').value||'600',10);
  const capRaw = document.getElementById('cap').value.trim();
  const capParam = capRaw ? `&capacity=${capRaw}` : '';
  const algo = document.getElementById('algo').value;
  const resBox = document.getElementById('results');
  resBox.innerHTML = '<div class="box"><i>Simulating...</i></div>';
  const resp = await fetch(`/lb/simulate?algo=${algo}&n=${n}&pause_ms=${pause}&processing_min_ms=${procMin}&processing_max_ms=${procMax}${capParam}`);
  const j = await resp.json();
  // build structured output
  let out = `<div class="box"><h3>Simulation summary</h3>
    <div><b>Algorithm:</b> ${j.algo} &nbsp; <b>Requests:</b> ${j.requested}</div>
    <div style="margin-top:8px"><b>Distribution (counts):</b></div>
    <pre>${JSON.stringify(j.distribution, null, 2)}</pre>
    <div style="margin-top:6px"><b>Final simulated state:</b></div>
    <pre>${JSON.stringify(j.final_sim, null, 2)}</pre>
  </div>`;

  out += '<div class="box"><h3>Step-by-step (first 500 steps shown)</h3>';
  out += '<table><thead><tr><th>#</th><th>Node</th><th>proc_ms</th><th>accepted</th><th>effective</th><th>queue</th><th>avail</th><th>avg_resp</th></tr></thead><tbody>';
  const steps = j.steps || [];
  for(const s of steps.slice(0, 500)){
    out += `<tr>
      <td>${s.index}</td>
      <td>${s.chosen_node}</td>
      <td>${s.processing_ms}</td>
      <td>${s.accepted}</td>
      <td>${s.effective_connections}</td>
      <td>${s.simulated_queue_len}</td>
      <td>${s.available_resources}</td>
      <td>${s.avg_response_ms}</td>
    </tr>`;
  }
  out += '</tbody></table></div>';
  resBox.innerHTML = out;
  await refresh();
};

refresh();
setInterval(refresh, 3000);
</script>

</body>
</html>
"""
    return HTMLResponse(html)

@app.get("/lb/health")
def health():
    return {"status": "ok", "nodes_tracked": len(METRICS)}

@app.on_event("shutdown")
async def shutdown():
    if hasattr(app.state, "poller_task"):
        app.state.poller_task.cancel()
