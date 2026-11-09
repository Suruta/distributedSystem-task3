"""Replicated key-value store node (Lab 6-7: Consistency Models & CAP).

One process = one replica. The default cluster is three replicas:

    A  localhost:50051   Leaseholder for keyspace [a-z]
    B  localhost:50052   Follower
    C  localhost:50053   Follower

Reads (Task 2)
  LINEARIZABLE    key in [a-z]  -> served ONLY by the Leaseholder (non-leaseholders
                                   proxy to it). The leaseholder first checks that
                                   it still holds a lease, i.e. it can reach a
                                   majority; otherwise it refuses (UNAVAILABLE).
                  other keys    -> no leaseholder, so a majority quorum read is
                                   done and the highest-timestamp version wins.
  FOLLOWER_STALE  served from the local replica. A follower measures how long ago
                  it last heard from the leaseholder; if that is more than
                  max_staleness_ms the read fails with STALE_READ_EXCEEDED.

Writes (Task 3)
  QUORUM_CP   (CP)  replicate to every reachable peer, succeed only if
                    1 + peer_acks >= N//2 + 1. On quorum loss NOTHING is applied
                    and the reply is success=False (ReplicaUnavailableError).
  ALWAYS_AP   (AP)  apply locally, always succeed, replicate best-effort. Divergent
                    copies are merged later by Last-Write-Wins (anti-entropy).

Conflict resolution: Last-Write-Wins ordered by (timestamp_us, origin_node_name).
The pair is a total order, so every replica converges to the same value.

Failure injection: SetPartition(blocked=[...]) makes this node drop all traffic
to/from the named peers (both directions), emulating a network partition
without touching the OS network stack. Works for in-process and subprocess nodes.
"""
import argparse
import threading
import time
from concurrent import futures

import grpc

import kvstore_pb2 as pb
import kvstore_pb2_grpc as pbg

DEFAULT_CLUSTER = {
    "A": "localhost:50052",
    "B": "localhost:50053",
    "C": "localhost:50054",
}
DEFAULT_LEASEHOLDER = "A"
# Each channel gets its own subchannel pool. With the default global pool, nodes
# that share one Python process (the in-process test clusters) can inherit stale
# connection state from channels that were just closed -> spurious "refused".
# Short reconnect backoff so a peer that starts late (or heals after a crash) is
# reachable again within ~100 ms instead of gRPC's default 1 s -> 120 s.
CHANNEL_OPTIONS = [
    ("grpc.use_local_subchannel_pool", 1),
    ("grpc.initial_reconnect_backoff_ms", 50),
    ("grpc.min_reconnect_backoff_ms", 50),
    ("grpc.max_reconnect_backoff_ms", 200),
]
STALE_READ_EXCEEDED = "STALE_READ_EXCEEDED"


def now_us():
    return time.time_ns() // 1000


def in_leaseholder_range(key):
    """The leaseholder owns keys whose first character is in [a-z]."""
    return bool(key) and "a" <= key[0] <= "z"


class PeerUnreachable(Exception):
    pass


class KVNode(pbg.KeyValueStoreServicer, pbg.NodeControlServicer):
    def __init__(self, name, peers, leaseholder=DEFAULT_LEASEHOLDER,
                 heartbeat_s=0.1, lease_timeout_s=1.0, sync_interval_s=1.0,
                 peer_timeout_s=1.0):
        self.name = name
        self.peers = dict(peers)                  # peer name -> "host:port"
        self.leaseholder = leaseholder
        self.n = len(self.peers) + 1
        self.majority = self.n // 2 + 1
        self.heartbeat_s = heartbeat_s
        self.lease_timeout_s = lease_timeout_s
        self.sync_interval_s = sync_interval_s    # 0 disables background anti-entropy
        self.peer_timeout_s = peer_timeout_s

        self._lock = threading.Lock()             # protects _store and _seen
        self._store = {}                          # key -> (value, ts_us, origin)
        self._seen = {}                           # idempotency_key -> PutReply

        self._net_lock = threading.Lock()         # protects _blocked and _contact
        self._blocked = frozenset()
        self._contact = {}                        # peer -> monotonic time of last contact

        self._channels = {p: grpc.insecure_channel(a, options=CHANNEL_OPTIONS)
                          for p, a in self.peers.items()}
        self._ctl = {p: pbg.NodeControlStub(c) for p, c in self._channels.items()}
        self._kv = {p: pbg.KeyValueStoreStub(c) for p, c in self._channels.items()}
        self._pool = futures.ThreadPoolExecutor(max_workers=max(4, 2 * self.n))
        self._stop = threading.Event()
        self._threads = []

    # ------------------------------------------------------------------ life-cycle
    def start_background(self):
        self._threads = [threading.Thread(target=self._heartbeat_loop, daemon=True)]
        if self.sync_interval_s > 0:
            self._threads.append(threading.Thread(target=self._sync_loop, daemon=True))
        for t in self._threads:
            t.start()

    def stop(self):
        self._stop.set()
        for t in self._threads:
            t.join(timeout=2)
        self._pool.shutdown(wait=False)
        for c in self._channels.values():
            c.close()

    def _heartbeat_loop(self):
        while not self._stop.is_set():
            for p in self.peers:
                try:
                    self._peer_call(p, self._ctl[p].Ping,
                                    pb.PingRequest(sender=self.name), timeout=0.5)
                except PeerUnreachable:
                    pass
            self._stop.wait(self.heartbeat_s)

    def _sync_loop(self):
        while not self._stop.wait(self.sync_interval_s):
            self.anti_entropy()

    # ------------------------------------------------------------------ networking
    def is_blocked(self, peer):
        with self._net_lock:
            return peer in self._blocked

    def _touch(self, peer):
        with self._net_lock:
            self._contact[peer] = time.monotonic()

    def contact_age_s(self, peer):
        """Seconds since we last heard from `peer` (inf if never)."""
        with self._net_lock:
            t = self._contact.get(peer)
        return float("inf") if t is None else time.monotonic() - t

    def _peer_call(self, peer, method, request, timeout=None):
        """Call a peer, honouring the injected partition. Marks contact on success."""
        if self.is_blocked(peer):
            raise PeerUnreachable(f"{self.name}->{peer} partitioned")
        try:
            reply = method(request, timeout=timeout or self.peer_timeout_s)
        except grpc.RpcError as err:
            raise PeerUnreachable(f"{self.name}->{peer}: {err.code().name}") from err
        self._touch(peer)
        return reply

    def _check_sender(self, sender, context):
        """Drop incoming internal traffic from a partitioned peer."""
        if self.is_blocked(sender):
            context.abort(grpc.StatusCode.UNAVAILABLE, f"partitioned from {sender}")
        self._touch(sender)

    def _fanout(self, fn):
        """Run fn(peer) for every peer in parallel; return {peer: result} of successes."""
        futs = {p: self._pool.submit(fn, p) for p in self.peers}
        out = {}
        for p, f in futs.items():
            try:
                out[p] = f.result()
            except PeerUnreachable:
                pass
        return out

    # ------------------------------------------------------------------ storage (LWW)
    def _merge(self, key, value, ts, origin):
        """Apply a version if it is newer under (timestamp, origin). Idempotent."""
        with self._lock:
            cur = self._store.get(key)
            if cur is None or (ts, origin) > (cur[1], cur[2]):
                self._store[key] = (value, ts, origin)
                return True
            return False

    def local_get(self, key):
        with self._lock:
            return self._store.get(key)

    def snapshot(self):
        with self._lock:
            return [pb.VersionedValue(key=k, value=v, timestamp=ts, origin=o)
                    for k, (v, ts, o) in self._store.items()]

    def anti_entropy(self):
        """Exchange full state with every reachable peer; both sides merge by LWW."""
        def one(p):
            reply = self._peer_call(
                p, self._ctl[p].Sync,
                pb.SyncRequest(sender=self.name, items=self.snapshot()))
            for it in reply.items:
                self._merge(it.key, it.value, it.timestamp, it.origin)
            return True
        return len(self._fanout(one))

    # ------------------------------------------------------------------ leases
    def has_lease(self):
        """Leaseholder is valid only while it can reach a majority (itself included)."""
        alive = 1
        for p in self.peers:
            if not self.is_blocked(p) and self.contact_age_s(p) <= self.lease_timeout_s:
                alive += 1
        return alive >= self.majority

    # ------------------------------------------------------------------ Get
    def Get(self, request, context):
        key = request.key
        if not key:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "key is required")
        if request.consistency == pb.LINEARIZABLE:
            return self._get_linearizable(key, context)
        return self._get_follower_stale(request, context)

    def _reply_from(self, key, is_stale):
        cur = self.local_get(key)
        if cur is None:
            return pb.GetReply(value="", found=False, read_timestamp=0, is_stale=is_stale)
        return pb.GetReply(value=cur[0], found=True, read_timestamp=cur[1],
                           is_stale=is_stale)

    def _get_linearizable(self, key, context):
        if not in_leaseholder_range(key):
            return self._quorum_read(key, context)
        if self.name == self.leaseholder:
            if not self.has_lease():
                context.abort(grpc.StatusCode.UNAVAILABLE,
                              "LeaseLost: leaseholder cannot reach a majority")
            return self._reply_from(key, is_stale=False)
        # Not the leaseholder: proxy strictly to it.
        try:
            return self._peer_call(self.leaseholder, self._kv[self.leaseholder].Get,
                                   pb.GetRequest(key=key, consistency=pb.LINEARIZABLE))
        except PeerUnreachable as e:
            context.abort(grpc.StatusCode.UNAVAILABLE,
                          f"LeaseholderUnreachable: {e}")

    def _quorum_read(self, key, context):
        """Read from a majority and return the highest (timestamp, origin) version."""
        def one(p):
            r = self._peer_call(p, self._ctl[p].ReadLocal,
                                pb.ReadLocalRequest(sender=self.name, key=key))
            return (r.item.timestamp, r.item.origin, r.item.value) if r.found else None
        answers = self._fanout(one)
        if 1 + len(answers) < self.majority:
            context.abort(grpc.StatusCode.UNAVAILABLE,
                          f"QuorumUnavailable: reached {1 + len(answers)}/{self.n}")
        cur = self.local_get(key)
        cands = [(cur[1], cur[2], cur[0])] if cur else []
        cands += [a for a in answers.values() if a]
        if not cands:
            return pb.GetReply(found=False)
        ts, _origin, value = max(cands)
        return pb.GetReply(value=value, found=True, read_timestamp=ts, is_stale=False)

    def _get_follower_stale(self, request, context):
        is_follower = self.name != self.leaseholder
        if is_follower and request.max_staleness_ms > 0:
            age_ms = self.contact_age_s(self.leaseholder) * 1000.0
            if age_ms > request.max_staleness_ms:
                context.abort(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    f"{STALE_READ_EXCEEDED}: last contact with leaseholder "
                    f"{self.leaseholder} was {age_ms:.0f} ms ago "
                    f"(max {request.max_staleness_ms} ms)")
        return self._reply_from(request.key, is_stale=is_follower)

    # ------------------------------------------------------------------ Put
    def Put(self, request, context):
        if not request.key:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "key is required")
        if not request.idempotency_key:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          "idempotency_key is required")
        with self._lock:
            done = self._seen.get(request.idempotency_key)
        if done is not None:               # retry of a finished write: same answer
            return done

        ts = request.timestamp or now_us()
        item = pb.VersionedValue(key=request.key, value=request.value,
                                 timestamp=ts, origin=self.name)

        def replicate(p):
            r = self._peer_call(p, self._ctl[p].Replicate,
                                pb.ReplicateRequest(sender=self.name, item=item))
            return r.applied

        if request.strategy == pb.QUORUM_CP:
            peer_acks = len(self._fanout(replicate))
            total = 1 + peer_acks
            if total < self.majority:
                # Quorum lost: refuse. The coordinator does NOT apply the write.
                # (With N=3 a failed write has zero peer acks, so no orphan copy
                # exists; for larger N a real system would use Raft/2PC.)
                return pb.PutReply(
                    success=False, acks_received=peer_acks,
                    error_message=(f"ReplicaUnavailableError: quorum lost, reached "
                                   f"{total}/{self.n} replicas, need {self.majority}"))
            self._merge(item.key, item.value, ts, self.name)
            reply = pb.PutReply(success=True, acks_received=total)
        else:  # ALWAYS_AP
            self._merge(item.key, item.value, ts, self.name)
            peer_acks = len(self._fanout(replicate))
            reply = pb.PutReply(success=True, acks_received=1 + peer_acks)

        with self._lock:
            self._seen[request.idempotency_key] = reply
        return reply

    # ------------------------------------------------------------------ NodeControl
    def Replicate(self, request, context):
        self._check_sender(request.sender, context)
        it = request.item
        return pb.ReplicateReply(applied=self._merge(it.key, it.value, it.timestamp,
                                                     it.origin))

    def Sync(self, request, context):
        self._check_sender(request.sender, context)
        for it in request.items:
            self._merge(it.key, it.value, it.timestamp, it.origin)
        return pb.SyncReply(items=self.snapshot())

    def ReadLocal(self, request, context):
        self._check_sender(request.sender, context)
        cur = self.local_get(request.key)
        if cur is None:
            return pb.ReadLocalReply(found=False)
        return pb.ReadLocalReply(found=True, item=pb.VersionedValue(
            key=request.key, value=cur[0], timestamp=cur[1], origin=cur[2]))

    def Ping(self, request, context):
        self._check_sender(request.sender, context)
        return pb.PingReply(name=self.name)

    def SetPartition(self, request, context):
        unknown = set(request.blocked) - set(self.peers)
        if unknown:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          f"unknown peers: {sorted(unknown)}")
        with self._net_lock:
            self._blocked = frozenset(request.blocked)
        return pb.PartitionReply()

    def SyncNow(self, request, context):
        return pb.SyncNowReply(peers_synced=self.anti_entropy())


# ---------------------------------------------------------------------- bootstrap
def build_server(name, port, peers, leaseholder=DEFAULT_LEASEHOLDER,
                 max_workers=16, **node_kwargs):
    """Create (not start) a gRPC server. Returns (server, bound_port, node)."""
    node = KVNode(name, peers, leaseholder=leaseholder, **node_kwargs)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers),
                         options=[("grpc.so_reuseport", 0)])  # fail loudly on port clash
    pbg.add_KeyValueStoreServicer_to_server(node, server)
    pbg.add_NodeControlServicer_to_server(node, server)
    bound = server.add_insecure_port(f"127.0.0.1:{port}")
    return server, bound, node


def parse_peers(text):
    peers = {}
    for part in filter(None, (p.strip() for p in text.split(","))):
        pname, _, addr = part.partition("=")
        peers[pname.strip()] = addr.strip()
    return peers


def main():
    ap = argparse.ArgumentParser(description="Replicated KV store node")
    ap.add_argument("--name", default="A", help="node name (A, B or C by default)")
    ap.add_argument("--port", type=int, default=None,
                    help="listen port (default: from the default cluster map)")
    ap.add_argument("--peers", default=None,
                    help="comma list name=host:port (default: other default-cluster nodes)")
    ap.add_argument("--leaseholder", default=DEFAULT_LEASEHOLDER)
    ap.add_argument("--sync-interval", type=float, default=1.0,
                    help="background anti-entropy period in seconds (0 = off)")
    args = ap.parse_args()

    port = args.port or int(DEFAULT_CLUSTER[args.name].rsplit(":", 1)[1])
    peers = (parse_peers(args.peers) if args.peers is not None else
             {n: a for n, a in DEFAULT_CLUSTER.items() if n != args.name})

    server, bound, node = build_server(args.name, port, peers, args.leaseholder,
                                       sync_interval_s=args.sync_interval)
    server.start()
    node.start_background()
    role = "LEASEHOLDER" if args.name == args.leaseholder else "follower"
    print(f"KV node '{args.name}' ({role}) listening on port {bound}; "
          f"peers={peers}; N={node.n} majority={node.majority}", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        node.stop()
        server.stop(grace=1)


if __name__ == "__main__":
    main()
