"""Counter service (Tasks A2 + A3 + B1).

Synchronization: a single threading.Lock (self._lock) protects BOTH the counter
values and the deduplication store. The idempotency-key check and the counter
mutation happen inside one critical section, so two concurrent retries with the
same key cannot both pass the check.

Lamport clock: each server owns a LamportClock (its own lock). Lock order is
always counter-lock -> clock-lock, never the reverse, so there is no deadlock.
"""
import argparse
import threading
import time
from concurrent import futures

import grpc

import counter_pb2
import counter_pb2_grpc
from lamport import LamportClock


class CounterServicer(counter_pb2_grpc.CounterServicer):
    def __init__(self, name="replica-A", clock=None, delay_ms=0, delay_count=0):
        self._lock = threading.Lock()
        self._values = {}  # counter_id -> int
        self._seen = {}    # idempotency_key -> (counter_id, resulting value)
        self.clock = clock or LamportClock(name)
        # Fault injection (Task C3): delay Increment handling by delay_ms.
        # delay_count = how many requests to delay (0 = every request).
        self._fault_delay_s = delay_ms / 1000.0
        self._fault_delay_left = delay_count if delay_count > 0 else None
        self._fault_lock = threading.Lock()

    def _maybe_delay(self):
        if self._fault_delay_s <= 0:
            return
        with self._fault_lock:
            if self._fault_delay_left is not None:
                if self._fault_delay_left == 0:
                    return
                self._fault_delay_left -= 1
        time.sleep(self._fault_delay_s)  # NOT under the counter lock

    def Increment(self, request, context):
        cid, delta = request.counter_id, request.delta
        self.clock.receive(
            request.lamport_time,
            f"RECV Increment(counter={cid}, delta={delta})")
        self._maybe_delay()

        key = request.idempotency_key
        if not key:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          "idempotency_key is required")
        if not cid:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          "counter_id is required")

        with self._lock:
            if key in self._seen:
                # Duplicate: return the stored result, do NOT apply again.
                _, new_value = self._seen[key]
                was_duplicate = True
                self.clock.tick(
                    f"DEDUP counter={cid} -> {new_value} (key already applied)")
            else:
                new_value = self._values.get(cid, 0) + delta
                self._values[cid] = new_value
                self._seen[key] = (cid, new_value)
                was_duplicate = False
                self.clock.tick(f"APPLY counter={cid} -> {new_value}")

        send_l = self.clock.tick(f"SEND IncrementReply(new_value={new_value})")
        return counter_pb2.IncrementReply(
            new_value=new_value, was_duplicate=was_duplicate,
            lamport_time=send_l)

    def Get(self, request, context):
        cid = request.counter_id
        self.clock.receive(request.lamport_time, f"RECV Get(counter={cid})")
        with self._lock:
            found = cid in self._values
            value = self._values.get(cid, 0)
        send_l = self.clock.tick(f"SEND GetReply(value={value}, found={found})")
        return counter_pb2.GetReply(value=value, found=found,
                                    lamport_time=send_l)


def build_server(port, max_workers=8, name="replica-A", clock=None,
                 delay_ms=0, delay_count=0):
    """Create (but do not start) a server. port=0 picks a free port.

    Returns (server, bound_port, servicer) so tests can reuse it.
    """
    servicer = CounterServicer(name=name, clock=clock,
                               delay_ms=delay_ms, delay_count=delay_count)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    counter_pb2_grpc.add_CounterServicer_to_server(servicer, server)
    bound_port = server.add_insecure_port(f"[::]:{port}")
    return server, bound_port, servicer


def main():
    parser = argparse.ArgumentParser(description="Counter gRPC server")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--name", default="replica-A",
                        help="process name used in Lamport log lines")
    parser.add_argument("--delay-ms", type=int, default=0,
                        help="fault injection: delay Increment handling (ms)")
    parser.add_argument("--delay-count", type=int, default=0,
                        help="with --delay-ms: delay only the first N "
                             "Increment requests (0 = all)")
    args = parser.parse_args()

    server, port, _ = build_server(args.port, name=args.name,
                                   delay_ms=args.delay_ms,
                                   delay_count=args.delay_count)
    server.start()
    print(f"Counter server '{args.name}' listening on port {port}", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=1)


if __name__ == "__main__":
    main()
