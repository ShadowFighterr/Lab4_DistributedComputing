#!/usr/bin/env python3
"""
Participant — Two-Phase Commit (2PC)
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import argparse
import json
import threading
import time
import os

lock = threading.Lock()

NODE_ID = ""
PORT = 8001
WAL_PATH = ""

TX = {}
KV = {}

def jdump(obj):
    return json.dumps(obj).encode()

def jload(b):
    return json.loads(b.decode())

def wal_append(line):
    if not WAL_PATH:
        return
    with open(WAL_PATH, "a") as f:
        f.write(line + "\n")
        f.flush()
        os.fsync(f.fileno())

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
                self._send(200, {"node": NODE_ID, "tx": TX, "kv": KV})
            return
        self._send(404, {"error": "not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = jload(self.rfile.read(length))
        txid = body["txid"]

        if self.path == "/prepare":
            op = body["op"]
            vote = "YES" if op["type"] == "SET" else "NO"
            with lock:
                TX[txid] = {"state": "READY" if vote == "YES" else "ABORTED", "op": op}
            wal_append(f"{txid} PREPARE {vote}")
            self._send(200, {"vote": vote})
            return

        if self.path == "/commit":
            with lock:
                op = TX[txid]["op"]
                KV[op["key"]] = op["value"]
                TX[txid]["state"] = "COMMITTED"
            wal_append(f"{txid} COMMIT")
            self._send(200, {"state": "COMMITTED"})
            return

        if self.path == "/abort":
            with lock:
                TX[txid] = {"state": "ABORTED"}
            wal_append(f"{txid} ABORT")
            self._send(200, {"state": "ABORTED"})
            return

        self._send(404, {"error": "not found"})

    def log_message(self, *args):
        return

def main():
    global NODE_ID, PORT, WAL_PATH
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--wal", required=True)
    args = ap.parse_args()

    NODE_ID = args.id
    PORT = args.port
    WAL_PATH = args.wal

    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[{NODE_ID}] Participant running on port {PORT}")
    server.serve_forever()

if __name__ == "__main__":
    main()
