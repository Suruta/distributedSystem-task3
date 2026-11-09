"""Part C4: Increment latency, single replica vs three-replica quorum.

Run from the project root:   python3 perf_benchmark.py
Options:                     --requests 2000   (per configuration, minimum 2000)

Method
  * Replicas are real server.py processes (stdout -> /dev/null), started fresh
    for each configuration, so the clients' Python threads do not share a GIL
    with the servers.
  * Latency = wall time of one client.increment() call, measured with
    time.perf_counter(): for the quorum path this is until ALL three replicas
    have answered (that is how ReplicatedClient is implemented).
  * Every client increments the same counter, with a fresh idempotency key per
    request. A short unrecorded warm-up precedes each measurement.
  * Median = statistics.median. p95 = nearest rank: sort ascending, take the
    1-based rank ceil(0.95 * n), i.e. sorted[ceil(0.95 * n) - 1].
  * After each run the benchmark checks that every replica holds exactly
    warm-up + measured increments (no lost or doubled writes) and that no
    request failed.
"""
import argparse
import io
import math
import os
import platform
import socket
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

import grpc

import counter_pb2_grpc
from client import CounterClient, ReplicatedClient
from lamport import LamportClock

ROOT = Path(__file__).resolve().parent
COUNTER = "perf-counter"
WARMUP_TOTAL = 160


def quiet_clock(name):
    return LamportClock(name, io.StringIO())


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def p95(sorted_values):
    """Nearest-rank 95th percentile: 1-based rank ceil(0.95 * n)."""
    rank = math.ceil(0.95 * len(sorted_values))
    return sorted_values[rank - 1]


class Replica:
    def __init__(self, name):
        self.port = free_port()
        self.proc = subprocess.Popen(
            [sys.executable, str(ROOT / "server.py"),
             "--port", str(self.port), "--name", name],
            cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        with grpc.insecure_channel(self.target) as ch:
            grpc.channel_ready_future(ch).result(timeout=20)

    @property
    def target(self):
        return f"localhost:{self.port}"

    def stop(self):
        self.proc.kill()
        self.proc.wait(timeout=5)


def read_value(target):
    with grpc.insecure_channel(target) as ch:
        c = CounterClient(counter_pb2_grpc.CounterStub(ch),
                          clock=quiet_clock("verify"))
        reply = c.get(COUNTER)
    return reply.value if reply.found else None


def run_config(n_replicas, n_clients, total_requests):
    replicas = [Replica(f"replica-{'ABC'[i]}") for i in range(n_replicas)]
    targets = [r.target for r in replicas]
    per_client = math.ceil(total_requests / n_clients)
    warm_per_client = math.ceil(WARMUP_TOTAL / n_clients)

    latencies = [[] for _ in range(n_clients)]
    failures = [0] * n_clients
    ready = threading.Barrier(n_clients + 1)
    go = threading.Barrier(n_clients + 1)

    def worker(i):
        channel = None
        if n_replicas == 1:
            channel = grpc.insecure_channel(targets[0])
            client = CounterClient(counter_pb2_grpc.CounterStub(channel),
                                   clock=quiet_clock(f"client-{i}"))
            send = lambda: client.increment(COUNTER, 1)
            ok = lambda reply: True
        else:
            client = ReplicatedClient(targets, clock=quiet_clock(f"client-{i}"))
            send = lambda: client.increment(COUNTER, 1)
            ok = lambda result: result.committed
        try:
            for _ in range(warm_per_client):          # unrecorded warm-up
                send()
            ready.wait()
            go.wait()
            for _ in range(per_client):
                t0 = time.perf_counter()
                try:
                    reply = send()
                    good = ok(reply)
                except grpc.RpcError:
                    good = False
                elapsed_ms = (time.perf_counter() - t0) * 1000.0
                if good:
                    latencies[i].append(elapsed_ms)
                else:
                    failures[i] += 1
        finally:
            if n_replicas == 1:
                channel.close()
            else:
                client.close()

    threads = [threading.Thread(target=worker, args=(i,))
               for i in range(n_clients)]
    try:
        for t in threads:
            t.start()
        ready.wait()           # all clients connected and warmed up
        go.wait()              # release them together
        for t in threads:
            t.join()
        expected = n_clients * (warm_per_client + per_client) - sum(failures)
        finals = [read_value(t) for t in targets]
    finally:
        for r in replicas:
            r.stop()

    merged = sorted(v for lst in latencies for v in lst)
    return {
        "median": statistics.median(merged),
        "p95": p95(merged),
        "n": len(merged),
        "failures": sum(failures),
        "consistent": all(v == expected for v in finals),
        "finals": finals,
        "expected": expected,
    }


CONFIGS = [
    ("Single replica, 1 client", 1, 1),
    ("Single replica, 16 clients", 1, 16),
    ("Quorum (3 replicas), 1 client", 3, 1),
    ("Quorum (3 replicas), 16 clients", 3, 16),
]


def main():
    parser = argparse.ArgumentParser(description="Increment latency benchmark")
    parser.add_argument("--requests", type=int, default=2000,
                        help="requests per configuration (default: 2000)")
    parser.add_argument("--out", default="perf_results.md")
    args = parser.parse_args()

    print(f"Python {platform.python_version()} on {platform.platform()}, "
          f"{os.cpu_count()} CPUs; {args.requests} requests per configuration",
          file=sys.stderr)

    rows, problems = [], []
    for label, n_replicas, n_clients in CONFIGS:
        print(f"running: {label} ...", file=sys.stderr, flush=True)
        r = run_config(n_replicas, n_clients, args.requests)
        rows.append((label, r))
        if r["failures"]:
            problems.append(f"{label}: {r['failures']} failed requests")
        if not r["consistent"]:
            problems.append(f"{label}: replica values {r['finals']} != "
                            f"expected {r['expected']}")

    lines = ["| Configuration | Median latency (ms) | p95 latency (ms) | Requests |",
             "|---|---|---|---|"]
    for label, r in rows:
        lines.append(f"| {label} | {r['median']:.2f} | {r['p95']:.2f} | {r['n']} |")
    table = "\n".join(lines)
    print(table)
    Path(args.out).write_text(table + "\n")
    print(f"\n(table also written to {args.out})", file=sys.stderr)
    if problems:
        print("WARNING:\n  " + "\n  ".join(problems), file=sys.stderr)
        sys.exit(1)
    print("Sanity checks passed: no failed requests; every replica holds "
          "exactly the expected count.", file=sys.stderr)


if __name__ == "__main__":
    main()
