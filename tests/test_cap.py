"""Lab 6-7 tests: quorum arithmetic, CP vs AP under a network partition, reads.

Run from the project root:   python3 -m pytest -v

Failure injection (Task 3)
  `PartitionInjector` calls the NodeControl.SetPartition RPC on every node. A node
  that is told to block a peer drops all traffic to AND from it, so the cluster
  behaves as if the network were cut. The same wrapper drives in-process nodes
  (fast, most tests) and real `server.py` subprocesses (test_subprocess_*).

Topology used in the partition tests (3 replicas, N=3, majority=2):
        DC-1 = {A, B}  (majority side)        DC-2 = {C}  (minority side)
"""
import itertools
import socket
import subprocess
import sys
import time
from pathlib import Path

import grpc
import pytest

import kvstore_pb2 as pb
import kvstore_pb2_grpc as pbg
from client import (ALWAYS_AP, FOLLOWER_STALE, LINEARIZABLE, QUORUM_CP, KVClient,
                    ReplicaUnavailableError, StaleReadExceededError, quorums_intersect,
                    read_from, write_to)
from server import build_server

ROOT = Path(__file__).resolve().parent.parent
NAMES = ["A", "B", "C"]


def free_ports(count=3):
    """Reserve `count` distinct free ports (all sockets held open together)."""
    socks = [socket.socket() for _ in range(count)]
    try:
        for s in socks:
            s.bind(("127.0.0.1", 0))
        return [s.getsockname()[1] for s in socks]
    finally:
        for s in socks:
            s.close()


# ---------------------------------------------------------------------------
# Failure-injection wrapper
# ---------------------------------------------------------------------------
class PartitionInjector:
    def __init__(self, addrs):
        self.addrs = addrs                                   # name -> host:port
        self._chans = {n: grpc.insecure_channel(
            a, options=[("grpc.use_local_subchannel_pool", 1)])
            for n, a in addrs.items()}
        self._ctl = {n: pbg.NodeControlStub(c) for n, c in self._chans.items()}

    def partition(self, *groups):
        """partition(["A","B"], ["C"]): nodes in different groups cannot talk."""
        group_of = {n: i for i, g in enumerate(groups) for n in g}
        for n in self.addrs:
            blocked = [m for m in self.addrs
                       if m != n and group_of[m] != group_of[n]]
            self._ctl[n].SetPartition(pb.PartitionRequest(blocked=blocked), timeout=5)

    def heal(self):
        """Reconnect everything, then force an anti-entropy round on every node."""
        self.partition(list(self.addrs))
        for n in self.addrs:
            self._ctl[n].SyncNow(pb.SyncNowRequest(), timeout=10)

    def close(self):
        for c in self._chans.values():
            c.close()


class Cluster:
    """Three in-process replicas A (leaseholder), B, C."""

    def __init__(self, sync_interval_s=0.0):
        ports = dict(zip(NAMES, free_ports()))
        self.addrs = {n: f"127.0.0.1:{p}" for n, p in ports.items()}
        self.nodes, self._servers = {}, []
        for n in NAMES:
            peers = {m: a for m, a in self.addrs.items() if m != n}
            server, _, node = build_server(n, ports[n], peers,
                                           sync_interval_s=sync_interval_s)
            server.start()
            self.nodes[n] = node
            self._servers.append(server)
        for node in self.nodes.values():     # only now begin heartbeats
            node.start_background()
        self.clients = {n: KVClient(a, put_retries=0) for n, a in self.addrs.items()}
        self.net = PartitionInjector(self.addrs)
        deadline = time.time() + 5          # wait until heartbeats connected everyone
        while time.time() < deadline and any(
                nd.contact_age_s(p) > 0.5 for nd in self.nodes.values() for p in nd.peers):
            time.sleep(0.02)

    def close(self):
        self.net.close()
        for c in self.clients.values():
            c.close()
        for n in self.nodes.values():
            n.stop()
        for s in self._servers:
            s.stop(grace=0)


@pytest.fixture
def cluster():
    c = Cluster()
    yield c
    c.close()


def stale_value(cluster, node, key):
    r = cluster.clients[node].get(key, FOLLOWER_STALE)
    return r.value if r.found else None


# ---------------------------------------------------------------------------
# 1-2. Quorum arithmetic  R + W > N
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("r,w,n,expected", [
    (2, 2, 3, True), (1, 3, 3, True), (3, 1, 3, True),
    (1, 1, 3, False), (1, 2, 3, False), (2, 1, 3, False),
    (3, 3, 5, True), (2, 3, 5, False),
])
def test_quorum_intersection_formula(r, w, n, expected):
    assert quorums_intersect(r, w, n) is expected


@pytest.mark.parametrize("r,w", [(1, 1), (1, 2), (2, 1), (2, 2), (1, 3), (3, 1)])
def test_rw_quorum_experiment_matches_formula(cluster, r, w):
    """Fully isolate the nodes so each holds only what the client wrote to it.
    Write a new version to every possible write set of size W, read every possible
    read set of size R, and count the reads that miss the new version.
    R + W > N  =>  zero stale reads.     R + W <= N  =>  some stale reads."""
    cluster.net.partition(["A"], ["B"], ["C"])
    stale, total = 0, 0
    for i, (wset, rset) in enumerate(itertools.product(
            itertools.combinations(NAMES, w), itertools.combinations(NAMES, r))):
        key = f"k{i}"
        write_to(list(cluster.clients.values()), key, "old", timestamp=1)  # all nodes
        write_to([cluster.clients[n] for n in wset], key, "new", timestamp=2)
        seen = read_from([cluster.clients[n] for n in rset], key)
        total += 1
        stale += seen[0] != "new"
    if r + w > 3:
        assert stale == 0, f"R={r} W={w}: {stale}/{total} stale reads"
    else:
        assert stale > 0, f"R={r} W={w}: expected stale reads, got none"


# ---------------------------------------------------------------------------
# 3. CP mode (strict quorum) under partition
# ---------------------------------------------------------------------------
def test_cp_write_on_majority_partition_succeeds(cluster):
    cluster.net.partition(["A", "B"], ["C"])
    reply = cluster.clients["B"].put("user:1", "alice", QUORUM_CP)
    assert reply.success and reply.acks_received == 2
    assert stale_value(cluster, "A", "user:1") == "alice"
    assert stale_value(cluster, "B", "user:1") == "alice"
    assert stale_value(cluster, "C", "user:1") is None      # cut off


def test_cp_write_on_minority_partition_is_rejected(cluster):
    cluster.net.partition(["A", "B"], ["C"])
    with pytest.raises(ReplicaUnavailableError, match="quorum lost"):
        cluster.clients["C"].put("user:1", "mallory", QUORUM_CP)
    raw = cluster.clients["C"].put("user:1", "mallory", QUORUM_CP,
                                   raise_on_failure=False)
    assert not raw.success and raw.acks_received == 0
    # Refused write must leave no trace anywhere.
    assert all(stale_value(cluster, n, "user:1") is None for n in NAMES)


def test_cp_write_succeeds_again_after_heal(cluster):
    cluster.net.partition(["A", "B"], ["C"])
    with pytest.raises(ReplicaUnavailableError):
        cluster.clients["C"].put("x", "1", QUORUM_CP)
    cluster.net.heal()
    reply = cluster.clients["C"].put("x", "1", QUORUM_CP)
    assert reply.success and reply.acks_received == 3


# ---------------------------------------------------------------------------
# 4. AP mode (always accept) + LWW merge after recovery
# ---------------------------------------------------------------------------
def test_ap_write_on_minority_partition_is_accepted(cluster):
    cluster.net.partition(["A", "B"], ["C"])
    reply = cluster.clients["C"].put("cart", "milk", ALWAYS_AP)
    assert reply.success and reply.acks_received == 1
    assert stale_value(cluster, "C", "cart") == "milk"
    assert stale_value(cluster, "A", "cart") is None


@pytest.mark.parametrize("ts_dc1,ts_dc2,winner", [
    (100, 200, "from-dc2"),     # newer write was on the minority side
    (300, 200, "from-dc1"),     # newer write was on the majority side
])
def test_partition_recovery_lww_converges_to_latest_timestamp(
        cluster, ts_dc1, ts_dc2, winner):
    cluster.net.partition(["A", "B"], ["C"])
    cluster.clients["A"].put("x", "from-dc1", QUORUM_CP, timestamp=ts_dc1)
    cluster.clients["C"].put("x", "from-dc2", ALWAYS_AP, timestamp=ts_dc2)
    assert stale_value(cluster, "A", "x") == "from-dc1"      # divergence is real
    assert stale_value(cluster, "C", "x") == "from-dc2"
    cluster.net.heal()
    assert cluster.clients["A"].get("x", LINEARIZABLE).value == winner
    assert {stale_value(cluster, n, "x") for n in NAMES} == {winner}


def test_lww_equal_timestamps_break_ties_by_node_name(cluster):
    cluster.net.partition(["A", "B"], ["C"])
    cluster.clients["A"].put("t", "from-A", ALWAYS_AP, timestamp=500)
    cluster.clients["C"].put("t", "from-C", ALWAYS_AP, timestamp=500)
    cluster.net.heal()
    assert {stale_value(cluster, n, "t") for n in NAMES} == {"from-C"}  # "C" > "A"


def test_background_anti_entropy_heals_without_explicit_sync():
    c = Cluster(sync_interval_s=0.2)
    try:
        c.net.partition(["A", "B"], ["C"])
        c.clients["A"].put("k", "old", ALWAYS_AP, timestamp=1)
        c.clients["C"].put("k", "new", ALWAYS_AP, timestamp=2)
        c.net.partition(["A", "B", "C"])                    # reconnect, no SyncNow
        deadline = time.time() + 5
        while time.time() < deadline and {stale_value(c, n, "k") for n in NAMES} != {"new"}:
            time.sleep(0.1)
        assert {stale_value(c, n, "k") for n in NAMES} == {"new"}
    finally:
        c.close()


# ---------------------------------------------------------------------------
# 5. Read paths: linearizable vs follower-stale
# ---------------------------------------------------------------------------
def test_linearizable_read_from_follower_is_proxied_to_leaseholder(cluster):
    cluster.clients["B"].put("k", "v1", QUORUM_CP)
    via_follower = cluster.clients["C"].get("k", LINEARIZABLE)
    assert via_follower.found and via_follower.value == "v1" and not via_follower.is_stale


def test_linearizable_is_strict_but_follower_read_stays_available(cluster):
    cluster.net.partition(["A", "B"], ["C"])
    cluster.clients["C"].put("k", "local-only", ALWAYS_AP)
    # C holds the value, yet a linearizable read must NOT be answered locally.
    with pytest.raises(ReplicaUnavailableError):
        cluster.clients["C"].get("k", LINEARIZABLE)
    r = cluster.clients["C"].get("k", FOLLOWER_STALE)       # PACELC: availability
    assert r.found and r.value == "local-only" and r.is_stale


def test_leaseholder_without_majority_refuses_linearizable_reads(cluster):
    cluster.clients["A"].put("k", "v", QUORUM_CP)
    cluster.net.partition(["A"], ["B", "C"])
    with pytest.raises(ReplicaUnavailableError):
        cluster.clients["A"].get("k", LINEARIZABLE)         # lease lost
    assert cluster.clients["B"].put("k2", "v2", QUORUM_CP).acks_received == 2


def test_follower_read_exceeding_max_staleness_is_rejected(cluster):
    cluster.clients["A"].put("k", "v", QUORUM_CP)
    assert cluster.clients["B"].get("k", FOLLOWER_STALE, 1000).value == "v"
    cluster.net.partition(["A", "C"], ["B"])                # B loses the leaseholder
    time.sleep(0.4)
    with pytest.raises(StaleReadExceededError, match="STALE_READ_EXCEEDED"):
        cluster.clients["B"].get("k", FOLLOWER_STALE, 100)
    assert cluster.clients["B"].get("k", FOLLOWER_STALE, 60000).value == "v"  # within bound
    assert cluster.clients["B"].get("k", FOLLOWER_STALE, 0).value == "v"      # unbounded


def test_non_leaseholder_range_key_uses_majority_quorum_read(cluster):
    """Keys outside [a-z] have no leaseholder, so linearizable reads use a quorum
    and keep working even when the leaseholder node is cut off."""
    cluster.net.partition(["A"], ["B", "C"])
    cluster.clients["B"].put("9lives", "cat", QUORUM_CP)
    assert cluster.clients["C"].get("9lives", LINEARIZABLE).value == "cat"
    with pytest.raises(ReplicaUnavailableError):            # A alone has no quorum
        cluster.clients["A"].get("9lives", LINEARIZABLE)


def test_idempotency_key_makes_put_retries_safe(cluster):
    first = cluster.clients["A"].put("k", "v1", QUORUM_CP, key_id="same-key")
    second = cluster.clients["A"].put("k", "OTHER", QUORUM_CP, key_id="same-key")
    assert first == second
    assert cluster.clients["A"].get("k", LINEARIZABLE).value == "v1"


# ---------------------------------------------------------------------------
# 6. Same injector against REAL server.py processes
# ---------------------------------------------------------------------------
def test_subprocess_cluster_cp_vs_ap_under_partition(tmp_path):
    ports = dict(zip(NAMES, free_ports()))
    addrs = {n: f"127.0.0.1:{p}" for n, p in ports.items()}
    procs = []
    try:
        for n in NAMES:
            peers = ",".join(f"{m}={a}" for m, a in addrs.items() if m != n)
            log = open(tmp_path / f"{n}.log", "w")
            procs.append(subprocess.Popen(
                [sys.executable, "-u", str(ROOT / "server.py"), "--name", n,
                 "--port", str(ports[n]), "--peers", peers, "--sync-interval", "0"],
                cwd=ROOT, stdout=log, stderr=subprocess.STDOUT))
        net = PartitionInjector(addrs)
        clients = {n: KVClient(a, put_retries=0) for n, a in addrs.items()}
        deadline = time.time() + 15                         # wait for every node
        while True:
            try:
                for n in NAMES:
                    clients[n].get("probe", FOLLOWER_STALE, 1000)
                break
            except (ReplicaUnavailableError, StaleReadExceededError):
                assert time.time() < deadline, "cluster did not come up"
                time.sleep(0.1)
        net.partition(["A", "B"], ["C"])
        assert clients["A"].put("k", "v", QUORUM_CP).acks_received == 2
        with pytest.raises(ReplicaUnavailableError):
            clients["C"].put("k2", "v", QUORUM_CP)
        ap = clients["C"].put("k3", "v", ALWAYS_AP)
        assert ap.success and ap.acks_received == 1
        net.close()
    finally:
        for p in procs:
            p.kill()
            p.wait()
