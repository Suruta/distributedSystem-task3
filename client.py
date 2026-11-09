"""KV store client (Lab 6-7).

* One KVClient talks to ONE node. Pick the node = pick the "datacenter" you
  are in (that is how the partition tests target DC-1 or DC-2).
* Put: a fresh idempotency key is created once per logical write and reused on
  every retry (Lab 2 behaviour), so retries are safe. Retries only happen on
  transport errors (DEADLINE_EXCEEDED / UNAVAILABLE).
* Get: no retries by default, so availability measurements are honest.

Errors
  ReplicaUnavailableError   Put refused for lack of a quorum, or a node/leaseholder
                            could not be reached for a read.
  StaleReadExceededError    follower is staler than max_staleness_ms.

CLI examples
  python client.py --target localhost:50051 put user:1 alice --strategy cp
  python client.py --target localhost:50052 get user:1 --consistency stale --max-staleness-ms 5000
"""
import argparse
import sys
import time
import uuid

import grpc

import kvstore_pb2 as pb
import kvstore_pb2_grpc as pbg

TIMEOUT_S = 2.0
PUT_RETRIES = 2
BACKOFF_BASE_S = 0.2
RETRYABLE = (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE)

LINEARIZABLE = pb.LINEARIZABLE
FOLLOWER_STALE = pb.FOLLOWER_STALE
QUORUM_CP = pb.QUORUM_CP
ALWAYS_AP = pb.ALWAYS_AP


class ReplicaUnavailableError(Exception):
    """Quorum lost / replica or leaseholder unreachable."""


class StaleReadExceededError(Exception):
    """The follower's data is older than the requested max_staleness_ms."""


class KVClient:
    def __init__(self, target, timeout=TIMEOUT_S, put_retries=PUT_RETRIES):
        self.target = target
        self.timeout = timeout
        self.put_retries = put_retries
        self._channel = grpc.insecure_channel(
            target, options=[("grpc.use_local_subchannel_pool", 1),
                     ("grpc.initial_reconnect_backoff_ms", 50),
                     ("grpc.max_reconnect_backoff_ms", 200)])
        self.stub = pbg.KeyValueStoreStub(self._channel)

    def close(self):
        self._channel.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ------------------------------------------------------------------ Put
    def put(self, key, value, strategy=QUORUM_CP, timestamp=0, key_id=None,
            raise_on_failure=True):
        """Returns PutReply. For QUORUM_CP with no quorum, raises
        ReplicaUnavailableError unless raise_on_failure=False."""
        req = pb.PutRequest(key=key, value=value,
                            idempotency_key=key_id or str(uuid.uuid4()),
                            strategy=strategy, timestamp=timestamp)
        attempt = 0
        while True:
            try:
                reply = self.stub.Put(req, timeout=self.timeout)
                break
            except grpc.RpcError as err:
                if err.code() not in RETRYABLE or attempt >= self.put_retries:
                    if err.code() in RETRYABLE:
                        raise ReplicaUnavailableError(
                            f"{self.target}: {err.code().name}") from err
                    raise
                time.sleep(BACKOFF_BASE_S * (2 ** attempt))
                attempt += 1
        if not reply.success and raise_on_failure:
            raise ReplicaUnavailableError(reply.error_message)
        return reply

    # ------------------------------------------------------------------ Get
    def get(self, key, consistency=LINEARIZABLE, max_staleness_ms=0):
        req = pb.GetRequest(key=key, consistency=consistency,
                            max_staleness_ms=max_staleness_ms)
        try:
            return self.stub.Get(req, timeout=self.timeout)
        except grpc.RpcError as err:
            details = err.details() or ""
            if (err.code() == grpc.StatusCode.FAILED_PRECONDITION
                    and details.startswith("STALE_READ_EXCEEDED")):
                raise StaleReadExceededError(details) from err
            if err.code() in RETRYABLE:
                raise ReplicaUnavailableError(
                    f"{self.target}: {err.code().name} {details}") from err
            raise


# ---------------------------------------------------------------------- R / W / N helpers
def write_to(clients, key, value, timestamp):
    """Write one version directly to each given node (a 'write set' of size W).
    Uses ALWAYS_AP with an explicit timestamp so nodes stay independent when
    the cluster is partitioned."""
    for c in clients:
        c.put(key, value, strategy=ALWAYS_AP, timestamp=timestamp)


def read_from(clients, key):
    """Read the local copy of each given node (a 'read set' of size R) and return
    the (value, timestamp) with the highest timestamp, or None if none has it."""
    best = None
    for c in clients:
        r = c.get(key, consistency=FOLLOWER_STALE)
        if r.found and (best is None or r.read_timestamp > best[1]):
            best = (r.value, r.read_timestamp)
    return best


def quorums_intersect(r, w, n):
    """Every read set of size r overlaps every write set of size w iff r + w > n."""
    return r + w > n


def main():
    ap = argparse.ArgumentParser(description="KV store client")
    ap.add_argument("--target", default="localhost:50051")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("put")
    p.add_argument("key")
    p.add_argument("value")
    p.add_argument("--strategy", choices=["cp", "ap"], default="cp")
    p.add_argument("--timestamp", type=int, default=0)
    p.add_argument("--idempotency-key", default=None)

    g = sub.add_parser("get")
    g.add_argument("key")
    g.add_argument("--consistency", choices=["linearizable", "stale"],
                   default="linearizable")
    g.add_argument("--max-staleness-ms", type=int, default=0)

    args = ap.parse_args()
    with KVClient(args.target) as c:
        try:
            if args.cmd == "put":
                r = c.put(args.key, args.value,
                          ALWAYS_AP if args.strategy == "ap" else QUORUM_CP,
                          args.timestamp, args.idempotency_key)
                print(f"OK acks={r.acks_received}")
            else:
                r = c.get(args.key,
                          FOLLOWER_STALE if args.consistency == "stale" else LINEARIZABLE,
                          args.max_staleness_ms)
                if r.found:
                    print(f"value={r.value!r} ts={r.read_timestamp} stale={r.is_stale}")
                else:
                    print("not found")
        except (ReplicaUnavailableError, StaleReadExceededError) as e:
            print(f"FAILED: {e}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
