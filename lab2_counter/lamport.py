"""Thread-safe Lamport clock that logs every event in the assignment's format.

Rules:
  * before any local event (send, apply): L = L + 1
  * on receive: L = max(L, received) + 1, before processing
The tick and the log line happen under the same lock, so the log order of a
process always agrees with its clock values, even with concurrent handlers.
"""
import sys
import threading


class LamportClock:
    def __init__(self, name, stream=None):
        self.name = name
        self._time = 0
        self._lock = threading.Lock()
        self._stream = stream if stream is not None else sys.stdout

    @property
    def time(self):
        with self._lock:
            return self._time

    def _log(self, text, suffix=""):
        print(f"[{self.name}] {text} L={self._time}{suffix}",
              file=self._stream, flush=True)

    def tick(self, text):
        """Local or send event: increment, log, return the new value.
        For a send, the returned value is attached to the outgoing message."""
        with self._lock:
            self._time += 1
            self._log(text)
            return self._time

    def receive(self, received, text):
        """Receive event: L = max(L, received) + 1, log, return the new value."""
        with self._lock:
            self._time = max(self._time, received) + 1
            self._log(text, f" (received L={received})")
            return self._time
