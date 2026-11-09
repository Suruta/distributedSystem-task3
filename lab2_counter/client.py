"""Counter client (Tasks A2 + B1 + C1).

Failure handling (per replica):
  * every RPC carries a deadline (default 2.0 s)
  * on DEADLINE_EXCEEDED / UNAVAILABLE: at most 3 retries, backoff 0.2/0.4/0.8 s
  * the idempotency key is generated ONCE per logical operation and reused on
    every retry and on every replica, so each replica's dedup store can suppress
    double application

Lamport clock: every attempt (including a retry, and each replica a request is
sent to) is its own SEND event. One thread-safe clock is shared by all replica
connections of a client process.

Replication (C1): an Increment is sent to ALL replicas in parallel and is
reported committed only if at least a majority (n // 2 + 1) acknowledge it.
A Get is served by a single replica (first reachable one, in list order).
"""
import argparse
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

import grpc

import counter_pb2
import counter_pb2_grpc
from lamport import LamportClock

DEFAULT_TARGETS = "localhost:50052,localhost:50053,localhost:50054"
TIMEOUT_S = 2.0
MAX_RETRIES = 3
BACKOFF_BASE_S = 0.2
RETRYABLE = (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE)


class CounterClient:
    """Talks to ONE server. Unchanged behaviour; `label` only tags retry lines."""

    def __init__(self, stub, name="client-1", clock=None, label=None):
        self.stub = stub
        self.clock = clock or LamportClock(name)
        self.label = label

    def _call(self, rpc, request, send_text, recv_text, timeout, max_retries,
              backoff_base):
        """Send `request` with retries. The request (and thus the idempotency
        key) is identical on each attempt; only lamport_time is re-stamped."""
        attempt = 0
        while True:
            request.lamport_time = self.clock.tick(send_text)  # SEND event
            try:
                reply = rpc(request, timeout=timeout)
            except grpc.RpcError as err:
                if err.code() not in RETRYABLE or attempt >= max_retries:
                    raise
                delay = backoff_base * (2 ** attempt)  # 0.2, 0.4, 0.8
                attempt += 1
                where = f" {self.label}:" if self.label else ""
                print(f"[retry {attempt}/{max_retries}]{where} "
                      f"{err.code().name}; sleeping {delay:.1f}s",
                      file=sys.stderr)
                time.sleep(delay)
                continue
            self.clock.receive(reply.lamport_time, recv_text(reply))  # RECV
            return reply

    def increment(self, counter_id, delta, key=None, timeout=TIMEOUT_S,
                  max_retries=MAX_RETRIES, backoff_base=BACKOFF_BASE_S):
        """One logical increment. The key is created once, here."""
        key = key or str(uuid.uuid4())
        request = counter_pb2.IncrementRequest(
            counter_id=counter_id, delta=delta, idempotency_key=key)
        return self._call(
            self.stub.Increment, request,
            f"SEND Increment(counter={counter_id}, delta={delta})",
            lambda r: f"RECV IncrementReply(new_value={r.new_value})",
            timeout, max_retries, backoff_base)

    def get(self, counter_id, timeout=TIMEOUT_S, max_retries=MAX_RETRIES,
            backoff_base=BACKOFF_BASE_S):
        request = counter_pb2.GetRequest(counter_id=counter_id)
        return self._call(
            self.stub.Get, request,
            f"SEND Get(counter={counter_id})",
            lambda r: f"RECV GetReply(value={r.value}, found={r.found})",
            timeout, max_retries, backoff_base)


@dataclass
class WriteResult:
    """Outcome of one replicated Increment."""
    committed: bool
    key: str
    acks: int
    total: int
    majority: int
    value: int = 0              # max new_value among acking replicas
    duplicate: bool = False     # True iff every acking replica said duplicate
    replies: dict = field(default_factory=dict)  # target -> IncrementReply
    errors: dict = field(default_factory=dict)   # target -> grpc.RpcError


class ReplicatedClient:
    """Fan-out writes with majority commit; single-replica reads."""

    def __init__(self, targets, name="client-1", clock=None):
        if not targets:
            raise ValueError("at least one target is required")
        self.targets = list(targets)
        self.clock = clock or LamportClock(name)
        self._channels = [grpc.insecure_channel(t) for t in self.targets]
        self._clients = {
            t: CounterClient(counter_pb2_grpc.CounterStub(ch),
                             clock=self.clock, label=t)
            for t, ch in zip(self.targets, self._channels)
        }

    @property
    def majority(self):
        return len(self.targets) // 2 + 1

    def close(self):
        for ch in self._channels:
            ch.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def increment(self, counter_id, delta, key=None, **kwargs):
        """Send the SAME (counter, delta, key) to every replica in parallel.

        The key is generated once here. We wait for every replica to finish
        (reply, or retries exhausted) so the ack count is complete. A replica
        counts as an ack only if it returned an IncrementReply (a reply with
        was_duplicate=True counts: that replica holds the write).
        """
        key = key or str(uuid.uuid4())

        def one(target):
            try:
                return target, self._clients[target].increment(
                    counter_id, delta, key=key, **kwargs), None
            except grpc.RpcError as err:
                return target, None, err

        with ThreadPoolExecutor(max_workers=len(self.targets)) as pool:
            outcomes = list(pool.map(one, self.targets))

        replies = {t: r for t, r, e in outcomes if r is not None}
        errors = {t: e for t, r, e in outcomes if e is not None}
        acks = len(replies)
        result = WriteResult(
            committed=acks >= self.majority, key=key, acks=acks,
            total=len(self.targets), majority=self.majority,
            replies=replies, errors=errors)
        if replies:
            result.value = max(r.new_value for r in replies.values())
            result.duplicate = all(r.was_duplicate for r in replies.values())
        return result

    def get(self, counter_id, **kwargs):
        """Read from the first reachable replica (list order).

        Returns (reply, target). A reply with found=False is still a reply
        (e.g. a restarted, empty replica) and does NOT trigger fallback.
        Raises the last RpcError if every replica is unreachable.
        """
        last_err = None
        for target in self.targets:
            try:
                return self._clients[target].get(counter_id, **kwargs), target
            except grpc.RpcError as err:
                last_err = err
        raise last_err


def main():
    parser = argparse.ArgumentParser(description="Replicated counter client")
    parser.add_argument("--targets", default=DEFAULT_TARGETS,
                        help="comma-separated replica addresses "
                             "(default: %(default)s)")
    parser.add_argument("--target", default=None,
                        help="single server address (overrides --targets)")
    parser.add_argument("--name", default="client-1",
                        help="process name used in Lamport log lines")
    sub = parser.add_subparsers(dest="command", required=True)

    p_incr = sub.add_parser("incr", help="increment a counter")
    p_incr.add_argument("counter_id")
    p_incr.add_argument("--by", type=int, default=1)
    p_incr.add_argument("--key", default=None,
                        help="idempotency key (default: fresh uuid4)")

    p_get = sub.add_parser("get", help="read a counter")
    p_get.add_argument("counter_id")

    args = parser.parse_args()
    raw = args.target if args.target else args.targets
    targets = [t.strip() for t in raw.split(",") if t.strip()]

    with ReplicatedClient(targets, name=args.name) as client:
        if args.command == "incr":
            res = client.increment(args.counter_id, args.by, key=args.key)
            if res.acks < res.total:
                for target, err in res.errors.items():
                    print(f"  {target}: {err.code().name}", file=sys.stderr)
            if res.committed:
                dup = "yes" if res.duplicate else "no"
                print(f"OK committed value={res.value} "
                      f"(replicas acked: {res.acks}/{res.total}, "
                      f"duplicate: {dup})")
            else:
                print(f"FAILED not committed "
                      f"(replicas acked: {res.acks}/{res.total}, "
                      f"need {res.majority}, key={res.key})",
                      file=sys.stderr)
                sys.exit(1)
        else:
            try:
                reply, target = client.get(args.counter_id)
            except grpc.RpcError as err:
                print(f"FAILED: all replicas unreachable "
                      f"({err.code().name})", file=sys.stderr)
                sys.exit(1)
            if reply.found:
                print(f"value={reply.value} (from {target})")
            else:
                print(f"counter '{args.counter_id}' not found (from {target})")


if __name__ == "__main__":
    main()
