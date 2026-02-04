#!/usr/bin/env python3
"""
Coordinator — Two-Phase Commit (2PC)
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib import request
import argparse
import json
import threading
import time
import os

lock = threading.Lock()

NODE_ID = ""
PORT = 8000
PARTICIPANTS = []
TIMEOUT = 2.0
WAL_PATH = "/tmp/coord.wal"

TX = {}

def jdump(obj):
    return json.dumps(obj).encode()

def jload(b):
    return json.loads(b.decode())

def wal_append(line):
    with open(WAL_PATH, "a") as f:
        f.write(line + "\n")
        f.flush()
        os.fsync(f.fileno())

def post_json(url, payload):
    data = jdump(payload)
    req = request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with request.urlopen(req, timeout=TIMEOUT) as r:
        return jload(r.read())

def two_pc(txid, op):
    wal_append(f"{txid} PREPARE")

    votes = {}
    all_yes = True

    for p in PARTICIPANTS:
        try:
            resp = post_json(p + "/prepare", {"txid": txid, "op": op})
            vote = resp["vote"]
            votes[p] = vote
            if vote != "YES":
                all_yes = False
        except Exception:
            votes[p] = "NO_TIMEOUT"
            all_yes = False

    decision = "COMMIT" if all_yes else "ABORT"
    wal_append(f"{txid} DECISION {decision}")

    # 🔴 COMMENT THIS LINE TO DEMO BLOCKING
    # time.sleep(10); exit(1)

    endpoint = "/commit" if decision == "COMMIT" else "/abort"
    for p in PARTICIPANTS:
        try:
            post_json(p + endpoint, {"txid": txid})
        except Exception:
            pass

    return {
        "txid": txid,
        "protocol": "2PC",
        "decision": decision,
        "votes": votes
    }

class Handler(BaseHTTPRequestHandler):
    def _send(self, code, obj):
        data = jdump(obj)
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/status":
            with lock:
                self._send(200, {"node": NODE_ID, "tx": TX})
            return
        self._send(404, {"error": "not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = jload(self.rfile.read(length))

        if self.path == "/tx/start":
            txid = body["txid"]
            op = body["op"]

            with lock:
                TX[txid] = {"state": "RUNNING"}

            result = two_pc(txid, op)

            with lock:
                TX[txid]["state"] = "DONE"
                TX[txid]["decision"] = result["decision"]

            self._send(200, result)
            return

        self._send(404, {"error": "not found"})

    def log_message(self, *args):
        return

def main():
    global NODE_ID, PORT, PARTICIPANTS
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True)
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--participants", required=True)
    args = ap.parse_args()

    NODE_ID = args.id
    PORT = args.port
    PARTICIPANTS = [p.strip() for p in args.participants.split(",")]

    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[{NODE_ID}] Coordinator running on port {PORT}")
    server.serve_forever()

if __name__ == "__main__":
    main()
