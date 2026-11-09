"""Task 4: PACELC benchmark. LINEARIZABLE (leaseholder) vs FOLLOWER_STALE (local).

Run from the project root:
    python3 perf_cap_bench.py                  # 1000 iterations, 40 ms simulated WAN RTT
    python3 perf_cap_bench.py --rtt-ms 0       # pure localhost numbers
    python3 perf_cap_bench.py --iterations 1000 --rtt-ms 40 --out perf_cap_results

Setup
  * Three REAL server.py processes (A leaseholder, B, C) on localhost.
  * The client lives "next to" follower B:
      Scenario 1  LINEARIZABLE    -> sent to the Leaseholder A  (remote)
      Scenario 2  FOLLOWER_STALE  -> sent to local follower B   (max_staleness_ms=5000)
  * Everything runs on one machine, so there is no real WAN. `--rtt-ms` adds a
    simulated round trip (time.sleep) before every call to the REMOTE node only.
    Use --rtt-ms 0 to see the raw gRPC cost without simulation.
  * Latency = wall time of one client.get() via time.perf_counter(), over
    SUCCESSFUL reads. Median = statistics.median; p95 = nearest rank
    sorted[ceil(0.95 * n) - 1].
  * Read availability = successful reads / iterations. A read "fails" when the
    client gets any error (leaseholder unreachable, STALE_READ_EXCEEDED, ...).

Part 1 (healthy cluster) fills the required table.
Part 2 (fault) isolates the leaseholder A from B and C ("A | B C") and repeats
the reads, which shows the availability side of PACELC (the "A" in PAC).
"""
import argparse
import json
import math
import socket
import statistics
import subprocess
import sys
import time
from pathlib import Path

import grpc  # noqa: F401  (kept so a missing dependency fails early)

import kvstore_pb2 as pb
import kvstore_pb2_grpc as pbg
from client import (FOLLOWER_STALE, LINEARIZABLE, QUORUM_CP, KVClient,
                    ReplicaUnavailableError, StaleReadExceededError)

ROOT = Path(__file__).resolve().parent
NAMES = ["A", "B", "C"]
KEY = "bench-key"
WARMUP = 50
MAX_STALENESS_MS = 5000


def free_ports(count=3):
    socks = [socket.socket() for _ in range(count)]
    try:
        for s in socks:
            s.bind(("127.0.0.1", 0))
        return [s.getsockname()[1] for s in socks]
    finally:
        for s in socks:
            s.close()


def p95(sorted_values):
    return sorted_values[math.ceil(0.95 * len(sorted_values)) - 1]


def start_cluster():
    ports = dict(zip(NAMES, free_ports()))
    addrs = {n: f"127.0.0.1:{p}" for n, p in ports.items()}
    procs = []
    for n in NAMES:
        peers = ",".join(f"{m}={a}" for m, a in addrs.items() if m != n)
        procs.append(subprocess.Popen(
            [sys.executable, "-u", str(ROOT / "server.py"), "--name", n,
             "--port", str(ports[n]), "--peers", peers, "--sync-interval", "0"],
            cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    return addrs, procs


def wait_ready(clients, timeout=20):
    deadline = time.time() + timeout
    while True:
        try:
            for n in NAMES:
                clients[n].get("probe", FOLLOWER_STALE, 1000)
            return
        except (ReplicaUnavailableError, StaleReadExceededError):
            if time.time() > deadline:
                raise RuntimeError("cluster did not become ready")
            time.sleep(0.1)


def run_reads(client, consistency, staleness, iterations, remote_rtt_s):
    """Returns (latencies_ms of successes, failures)."""
    lat, failures = [], 0
    for _ in range(iterations):
        t0 = time.perf_counter()
        if remote_rtt_s:
            time.sleep(remote_rtt_s)            # simulated network round trip
        try:
            client.get(KEY, consistency, staleness)
        except (ReplicaUnavailableError, StaleReadExceededError):
            failures += 1
            continue
        lat.append((time.perf_counter() - t0) * 1000.0)
    return lat, failures


def summarise(label, target, lat, failures, iterations):
    s = sorted(lat)
    return {
        "mode": label, "target": target, "iterations": iterations,
        "ok": len(lat), "failed": failures,
        "median_ms": statistics.median(s) if s else None,
        "p95_ms": p95(s) if s else None,
        "availability_pct": 100.0 * len(lat) / iterations,
    }


def fmt(x):
    return "n/a" if x is None else f"{x:.2f}"


def table(rows):
    out = ["| Read Mode | Target Node | Median Latency (ms) | p95 Latency (ms) "
           "| Read Availability (%) |", "|---|---|---|---|---|"]
    for r in rows:
        out.append(f"| {r['mode']} | {r['target']} | {fmt(r['median_ms'])} | "
                   f"{fmt(r['p95_ms'])} | {r['availability_pct']:.1f} "
                   f"({r['ok']}/{r['iterations']}) |")
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser(description="PACELC read benchmark")
    ap.add_argument("--iterations", type=int, default=1000)
    ap.add_argument("--rtt-ms", type=float, default=40.0,
                    help="simulated client<->leaseholder round trip (0 = none)")
    ap.add_argument("--out", default="perf_cap_results",
                    help="basename for the .md and .json result files")
    ap.add_argument("--skip-stale-wait", action="store_true",
                    help="skip the 5.5 s wait that lets the staleness bound expire")
    args = ap.parse_args()
    rtt = args.rtt_ms / 1000.0

    addrs, procs = start_cluster()
    clients = {n: KVClient(a, put_retries=0, timeout=5.0) for n, a in addrs.items()}
    results = {"iterations": args.iterations, "rtt_ms": args.rtt_ms,
               "max_staleness_ms": MAX_STALENESS_MS}
    ctl = {n: pbg.NodeControlStub(grpc.insecure_channel(a)) for n, a in addrs.items()}
    try:
        wait_ready(clients)
        clients["A"].put(KEY, "value", QUORUM_CP)           # seed: replicated to all
        time.sleep(0.2)

        def scenario(label, target_name, consistency, staleness, remote):
            c = clients[target_name]
            run_reads(c, consistency, staleness, WARMUP, rtt if remote else 0)
            lat, bad = run_reads(c, consistency, staleness, args.iterations,
                                 rtt if remote else 0)
            return summarise(label, target_name, lat, bad, args.iterations)

        # ---- Part 1: healthy cluster ------------------------------------
        healthy = [
            scenario("Linearizable", "A", LINEARIZABLE, 0, True),
            scenario("Follower Stale", "B", FOLLOWER_STALE, MAX_STALENESS_MS, False),
        ]
        healthy[0]["target"] = "Leaseholder A (remote)"
        healthy[1]["target"] = "Local follower B"
        results["healthy"] = healthy

        # ---- Part 2: leaseholder isolated (A | B C) ---------------------
        for n, blocked in (("A", ["B", "C"]), ("B", ["A"]), ("C", ["A"])):
            ctl[n].SetPartition(pb.PartitionRequest(blocked=blocked), timeout=5)
        t_partition = time.monotonic()
        time.sleep(0.2)
        # Follower scenario FIRST: it must run while the 5 s staleness bound is
        # still open. (The leaseholder scenario below pays the simulated RTT on
        # every failed attempt, so it takes long enough for the bound to expire.)
        within = scenario("Follower Stale (within 5 s bound)", "B", FOLLOWER_STALE,
                          MAX_STALENESS_MS, False)
        lin = scenario("Linearizable", "A", LINEARIZABLE, 0, True)
        fault = [lin, within]
        fault[0]["target"] = "Leaseholder A (isolated)"
        fault[1]["target"] = "Local follower B"
        if not args.skip_stale_wait:
            remaining = t_partition + MAX_STALENESS_MS / 1000.0 + 0.5 - time.monotonic()
            if remaining > 0:                                 # let the bound expire
                time.sleep(remaining)
            late = scenario("Follower Stale (bound expired)", "B", FOLLOWER_STALE,
                            MAX_STALENESS_MS, False)
            late["target"] = "Local follower B"
            fault.append(late)
        results["fault"] = fault
    finally:
        for c in clients.values():
            c.close()
        for p in procs:
            p.kill()
            p.wait()

    md = [f"# PACELC benchmark ({args.iterations} iterations per scenario, "
          f"simulated RTT to remote node = {args.rtt_ms:g} ms)", "",
          "## Healthy cluster", "", table(results["healthy"]), "",
          "## Leaseholder A isolated from B and C (network partition)", "",
          table(results["fault"]), ""]
    text = "\n".join(md)
    print(text)
    Path(ROOT / f"{args.out}.md").write_text(text)
    Path(ROOT / f"{args.out}.json").write_text(json.dumps(results, indent=2))
    print(f"\nwritten: {args.out}.md, {args.out}.json")


if __name__ == "__main__":
    main()
