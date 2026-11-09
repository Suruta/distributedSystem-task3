"""Part C3: failure-injection tests.

Run from the project root:   python3 -m pytest -v test_failure_injection.py

Replicas are REAL server.py processes launched with subprocess.Popen on free
ports (route a). The crash scenario kills one with proc.kill(). The timeout
scenario needs a replica that is slower than the client deadline; for that the
server has a small fault flag (route b): --delay-ms N [--delay-count K], because
pausing a process (SIGSTOP) is not portable to Windows.

Each replica's stdout (the Lamport trace) goes to a log file; the tests read it
to know when a request is in flight and to count APPLY / DEDUP events.
"""
import io
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import grpc
import pytest

import counter_pb2_grpc
from client import CounterClient, ReplicatedClient
from lamport import LamportClock

ROOT = Path(__file__).resolve().parent


def quiet_clock(name):
    return LamportClock(name, io.StringIO())


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# --------------------------------------------------------------------------
# Helpers and fixtures
# --------------------------------------------------------------------------
class ReplicaProcess:
    """A replica running as a separate OS process."""

    def __init__(self, name, log_path, extra_args=()):
        self.name = name
        self.port = free_port()
        self.log_path = Path(log_path)
        self._log = open(self.log_path, "w")
        self.proc = subprocess.Popen(
            [sys.executable, "-u", str(ROOT / "server.py"),
             "--port", str(self.port), "--name", name, *extra_args],
            cwd=ROOT, stdout=self._log, stderr=subprocess.STDOUT)
        self.wait_for_log("listening on port", timeout=15)

    @property
    def target(self):
        return f"localhost:{self.port}"

    def log_text(self):
        return self.log_path.read_text()

    def wait_for_log(self, text, timeout=5.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if text in self.log_text():
                return
            if self.proc.poll() is not None and text not in self.log_text():
                raise AssertionError(
                    f"{self.name} exited early; log:\n{self.log_text()}")
            time.sleep(0.02)
        raise AssertionError(
            f"{self.name}: {text!r} not seen in {timeout}s; log:\n{self.log_text()}")

    def kill(self):
        """Hard crash: SIGKILL (TerminateProcess on Windows), no cleanup."""
        self.proc.kill()
        self.proc.wait(timeout=5)

    def stop(self):
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=5)
        self._log.close()


@pytest.fixture
def launch(tmp_path):
    """Factory: launch(name, *extra_server_args) -> ReplicaProcess."""
    started = []

    def start(name, *extra_args):
        replica = ReplicaProcess(name, tmp_path / f"{name}.log", extra_args)
        started.append(replica)
        return replica

    yield start
    for replica in started:
        replica.stop()


def read_value(target, counter_id):
    """Read a counter straight from one live replica (None if absent)."""
    with grpc.insecure_channel(target) as ch:
        c = CounterClient(counter_pb2_grpc.CounterStub(ch),
                          clock=quiet_clock("reader"))
        reply = c.get(counter_id, timeout=2.0, max_retries=1, backoff_base=0.05)
    return reply.value if reply.found else None


# --------------------------------------------------------------------------
# Scenario 1: replica crash mid-request
# --------------------------------------------------------------------------
def test_replica_crash_mid_request_still_commits(launch):
    # Arrange: C is slow (5 s), so the write is provably IN FLIGHT on C when
    # we kill it: the log shows RECV but the handler is still sleeping.
    a = launch("replica-A")
    b = launch("replica-B")
    c = launch("replica-C", "--delay-ms", "5000")
    client = ReplicatedClient([a.target, b.target, c.target],
                              clock=quiet_clock("client-1"))
    outcome = {}

    def write():
        try:
            outcome["result"] = client.increment(
                "x", 1, key="crash-key",
                timeout=2.0, max_retries=1, backoff_base=0.05)
        except Exception as exc:  # must NOT happen
            outcome["error"] = exc

    writer = threading.Thread(target=write)

    # Act
    writer.start()
    c.wait_for_log("RECV Increment")   # request is in flight on C
    c.kill()                           # crash C mid-request
    writer.join(timeout=20)
    client.close()

    # Assert
    assert not writer.is_alive()
    assert "error" not in outcome, f"exception escaped: {outcome.get('error')!r}"
    result = outcome["result"]
    assert result.committed is True
    assert (result.acks, result.total, result.majority) == (2, 3, 2)
    assert read_value(a.target, "x") == 1
    assert read_value(b.target, "x") == 1
    assert c.proc.poll() is not None   # C really is dead


# --------------------------------------------------------------------------
# Scenario 2: request duplication
# --------------------------------------------------------------------------
def test_duplicate_request_moves_value_once(launch):
    # Arrange
    replicas = [launch(f"replica-{n}") for n in "ABC"]
    client = ReplicatedClient([r.target for r in replicas],
                              clock=quiet_clock("client-1"))

    # Act: the same logical write delivered twice, same idempotency key
    first = client.increment("x", 1, key="dup-key")
    second = client.increment("x", 1, key="dup-key")
    client.close()

    # Assert
    assert first.committed and first.duplicate is False
    assert second.committed and second.duplicate is True
    assert first.value == second.value == 1
    for r in replicas:                              # every replica applied once
        assert read_value(r.target, "x") == 1
        assert r.log_text().count("APPLY counter=x") == 1


def test_simultaneous_duplicate_delivery_applies_once(launch):
    # Arrange: 8 identical requests released at the same instant at ONE replica
    replica = launch("replica-A")
    n = 8
    barrier = threading.Barrier(n)
    replies, errors = [], []

    def send():
        try:
            with grpc.insecure_channel(replica.target) as ch:
                c = CounterClient(counter_pb2_grpc.CounterStub(ch),
                                  clock=quiet_clock("client"))
                barrier.wait()
                replies.append(c.increment("x", 1, key="same-key"))
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=send) for _ in range(n)]

    # Act
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Assert: exactly one request applied, the other 7 were duplicates
    assert errors == []
    assert sorted(r.was_duplicate for r in replies) == [False] + [True] * (n - 1)
    assert {r.new_value for r in replies} == {1}
    assert read_value(replica.target, "x") == 1


# --------------------------------------------------------------------------
# Scenario 3: induced timeout with retry
# --------------------------------------------------------------------------
def test_induced_timeout_retry_applies_once(launch):
    # Arrange: B delays its FIRST Increment by 1 s; the client deadline is 0.3 s
    a = launch("replica-A")
    b = launch("replica-B", "--delay-ms", "1000", "--delay-count", "1")
    c = launch("replica-C")
    client = ReplicatedClient([a.target, b.target, c.target],
                              clock=quiet_clock("client-1"))

    # Act: first attempt to B times out; the retry (same key) succeeds
    result = client.increment("x", 1, timeout=0.3, max_retries=3,
                              backoff_base=0.05)
    client.close()
    b.wait_for_log("DEDUP", timeout=5)  # the slow original finally woke up

    # Assert: committed everywhere, and the counter moved exactly once on B
    assert result.committed is True
    assert result.acks == 3
    assert b.log_text().count("RECV Increment") >= 2      # a retry happened
    assert b.log_text().count("APPLY counter=x") == 1     # applied once
    for r in (a, b, c):
        assert read_value(r.target, "x") == 1
