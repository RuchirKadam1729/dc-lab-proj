# send_message_via_lb.py
import requests, sys, time

LB = "http://localhost:8080"
ALGO = sys.argv[1] if len(sys.argv) > 1 else "least_conn"
N = int(sys.argv[2]) if len(sys.argv) > 2 else 20

choices = {}
for i in range(N):
    r = requests.get(f"{LB}/lb/choose", params={"algo": ALGO}, timeout=5)
    j = r.json()
    node = j.get("chosen_node", "ERR")
    choices[node] = choices.get(node, 0) + 1
    time.sleep(0.05)
print("Distribution:", choices)
