"""Part C2: unit and integration tests for the counter service.

Run from the project root:   python3 -m pytest -v

Every test follows Arrange / Act / Assert. Servers are started inside fixtures
on ephemeral ports (port 0, chosen by the OS) and stopped afterwards, so no
manual steps and no port clashes. Lamport logs are sent to a StringIO so test
output stays clean.
"""
import io
import sys
import threading
import time
from concurrent import futures

import grpc
import pytest

import client as client_module
import counter_pb2_grpc
from client import CounterClient, ReplicatedClient
from lamport import LamportClock
from server import CounterServicer, build_server

# Short deadlines so failure-path tests stay fast.
FAST = dict(timeout=0.5, max_retries=1, backoff_base=0.05)


def quiet_clock(name):
    return LamportClock(name, io.StringIO())


# --------------------------------------------------------------------------
# Helpers and fixtures
# --------------------------------------------------------------------------
class RunningServer:
    """A real gRPC server on an ephemeral port."""

    def __init__(self, name, servicer=None):
        self.name = name
        if servicer is None:
            self.server, self.port, self.servicer = build_server(
                0, max_workers=8, name=name, clock=quiet_clock(name))
        else:  # custom servicer (e.g. artificially slow)
            self.servicer = servicer
            self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
            counter_pb2_grpc.add_CounterServicer_to_server(servicer, self.server)
            self.port = self.server.add_insecure_port("[::]:0")
        self.server.start()

    @property
    def target(self):
        return f"localhost:{self.port}"

    def stop(self):
        self.server.stop(0).wait()


class SlowFirstReplyServicer(CounterServicer):
    """Applies the first Increment normally, then delays its REPLY.

    Simulates 'executed, but the reply arrived too late': the client times out
    although the server already applied the write.
    """

    def __init__(self, delay_s):
        super().__init__(name="replica-slow", clock=quiet_clock("replica-slow"))
        self._delay_s = delay_s
        self._meta_lock = threading.Lock()
        self.calls = 0

    def Increment(self, request, context):
        reply = super().Increment(request, context)
        with self._meta_lock:
            self.calls += 1
            is_first = self.calls == 1
        if is_first:
            time.sleep(self._delay_s)
        return reply


class ReplicaGroup:
    def __init__(self):
        self.replicas = [RunningServer(f"replica-{n}") for n in "ABC"]

    @property
    def targets(self):
        return [r.target for r in self.replicas]

    def stop(self, *indexes):
        for i in indexes:
            self.replicas[i].stop()

    def stop_all(self):
        for r in self.replicas:
            r.stop()


@pytest.fixture
def channels():
    opened = []

    def make(target):
        ch = grpc.insecure_channel(target)
        opened.append(ch)
        return ch

    yield make
    for ch in opened:
        ch.close()


@pytest.fixture
def server():
    srv = RunningServer("replica-A")
    yield srv
    srv.stop()


@pytest.fixture
def make_client(channels):
    """Factory: a CounterClient (one server) with its own channel and clock."""
    def make(target, name="client-1"):
        stub = counter_pb2_grpc.CounterStub(channels(target))
        return CounterClient(stub, name=name, clock=quiet_clock(name))
    return make


@pytest.fixture
def replicas():
    group = ReplicaGroup()
    yield group
    group.stop_all()


@pytest.fixture
def group_client(replicas):
    c = ReplicatedClient(replicas.targets, clock=quiet_clock("client-1"))
    yield c
    c.close()


def read_value(target, counter_id):
    """Read a counter straight from one replica; None if it has no such counter."""
    with grpc.insecure_channel(target) as ch:
        c = CounterClient(counter_pb2_grpc.CounterStub(ch),
                          clock=quiet_clock("reader"))
        reply = c.get(counter_id, **FAST)
    return reply.value if reply.found else None


# --------------------------------------------------------------------------
# Single-server tests
# --------------------------------------------------------------------------
def test_increment_applies_delta(server, make_client):
    # Arrange
    client = make_client(server.target)

    # Act
    reply = client.increment("likes:post-42", 5)

    # Assert
    assert reply.new_value == 5
    assert reply.was_duplicate is False
    assert read_value(server.target, "likes:post-42") == 5


def test_duplicate_key_not_reapplied(server, make_client):
    # Arrange
    client = make_client(server.target)
    first = client.increment("likes:post-42", 5, key="key-1")

    # Act: same idempotency key again
    second = client.increment("likes:post-42", 5, key="key-1")

    # Assert
    assert first.was_duplicate is False
    assert second.was_duplicate is True
    assert second.new_value == first.new_value == 5
    assert read_value(server.target, "likes:post-42") == 5  # not 10


def test_concurrent_increments_exact(server, make_client):
    # Arrange: two independent clients, 1,000 increments each, same counter
    per_client = 1000
    errors = []

    def worker(name):
        try:
            client = make_client(server.target, name=name)
            for _ in range(per_client):
                client.increment("hot-counter", 1)  # fresh key every call
        except Exception as exc:  # surfaced by the assertion below
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(f"client-{i}",))
               for i in (1, 2)]

    # Act
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Assert
    assert errors == []
    assert read_value(server.target, "hot-counter") == 2 * per_client


def test_get_missing_counter(server, make_client):
    # Arrange
    client = make_client(server.target)

    # Act (must not raise)
    reply = client.get("no-such-counter")

    # Assert
    assert reply.found is False
    assert reply.value == 0


def test_retry_after_timeout_is_safe(make_client):
    # Arrange: the server applies the first request but replies too late
    slow = RunningServer("replica-slow", SlowFirstReplyServicer(delay_s=0.8))
    try:
        client = make_client(slow.target)

        # Act: 0.3 s deadline -> first attempt times out, retry reuses the key
        reply = client.increment("likes:post-42", 1, timeout=0.3,
                                 max_retries=3, backoff_base=0.05)

        # Assert: the retry was answered from the dedup store; counter moved once
        assert slow.servicer.calls >= 2          # a retry really happened
        assert reply.was_duplicate is True
        assert reply.new_value == 1
        assert read_value(slow.target, "likes:post-42") == 1
    finally:
        slow.stop()


# --------------------------------------------------------------------------
# Three-replica tests
# --------------------------------------------------------------------------
def test_majority_commit_two_acks(replicas, group_client):
    # Arrange: stop one replica (C)
    replicas.stop(2)

    # Act
    result = group_client.increment("x", 1, **FAST)

    # Assert
    assert result.committed is True
    assert (result.acks, result.total, result.majority) == (2, 3, 2)
    assert read_value(replicas.replicas[0].target, "x") == 1
    assert read_value(replicas.replicas[1].target, "x") == 1


def test_no_commit_below_majority(replicas, group_client):
    # Arrange: stop two replicas (B and C); A survives
    replicas.stop(1, 2)

    # Act
    result = group_client.increment("x", 1, **FAST)

    # Assert: reported as NOT committed
    assert result.committed is False
    assert (result.acks, result.total, result.majority) == (1, 3, 2)
    assert len(result.errors) == 2
    # Documented anomaly (see report): the survivor did apply the write even
    # though the client was told it failed. The client must not present it as
    # committed, which is what `committed is False` above guarantees.
    assert read_value(replicas.replicas[0].target, "x") == 1


def test_cli_reports_failure_below_majority(replicas, monkeypatch, capsys):
    # Arrange: two replicas down; drive the real command-line entry point
    replicas.stop(1, 2)
    monkeypatch.setattr(sys, "argv", [
        "client.py", "--targets", ",".join(replicas.targets),
        "incr", "x", "--by", "1", "--key", "cli-key-1"])

    # Act
    with pytest.raises(SystemExit) as exit_info:
        client_module.main()

    # Assert: non-zero exit, FAILED message, never "OK committed"
    captured = capsys.readouterr()
    assert exit_info.value.code == 1
    assert "FAILED" in captured.err
    assert "OK committed" not in captured.out


def test_replicas_converge(replicas, group_client):
    # Arrange: a batch of quorum writes over several counters, all replicas up
    counters = ["a", "b", "c"]
    expected = {c: 0 for c in counters}
    results = []

    # Act
    for i in range(30):
        cid = counters[i % len(counters)]
        delta = (i % 4) + 1
        expected[cid] += delta
        results.append(group_client.increment(cid, delta, **FAST))

    # Assert: every write committed, and all replicas hold identical values
    assert all(r.committed for r in results)
    for cid in counters:
        values = [read_value(t, cid) for t in replicas.targets]
        assert values == [expected[cid]] * 3


def test_replicas_converge_with_one_replica_down(replicas, group_client):
    # Arrange: replica C is down for the whole batch
    replicas.stop(2)

    # Act
    results = [group_client.increment("x", 2, **FAST) for _ in range(10)]

    # Assert: writes commit at 2/3 and the two LIVE replicas agree
    assert all(r.committed for r in results)
    live = [read_value(replicas.replicas[i].target, "x") for i in (0, 1)]
    assert live == [20, 20]
