# Distributed KV Store: Consistency Models & CAP (Lab 6-7)

Extends the Lab 2 replicated gRPC counter (kept untouched in `lab2_counter/`).

| File | Purpose |
|---|---|
| `kvstore.proto` | Task 1: `Get`/`Put` + enums exactly as in the handout; extra `NodeControl` service for replication, heartbeats and partition injection |
| `server.py` | Task 2: replica with leaseholder, linearizable + follower-stale reads, CP/AP writes, LWW, anti-entropy |
| `client.py` | Client + CLI, typed errors, `write_to` / `read_from` helpers for R/W experiments |
| `tests/test_cap.py` | Task 3: failure-injection wrapper + 29 pytest cases (16 test functions) |
| `perf_cap_bench.py` | Task 4: PACELC benchmark (1,000 iterations per scenario) |
| `make_report.py` → `Lab_Report_CAP.pdf` | Lab report built from measured results |

## Setup
```
pip install grpcio grpcio-tools pytest reportlab
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. kvstore.proto
```

## Run a 3-node cluster (3 terminals)
```
python server.py --name A      # Leaseholder, port 50051
python server.py --name B      # port 50052
python server.py --name C      # port 50053
```
```
python client.py --target localhost:50051 put user:1 alice --strategy cp
python client.py --target localhost:50052 get user:1 --consistency linearizable
python client.py --target localhost:50052 get user:1 --consistency stale --max-staleness-ms 5000
```

## Behaviour in one table
| Request | Behaviour |
|---|---|
| `LINEARIZABLE`, key in [a-z] | Only the Leaseholder (A) answers; followers proxy to it. A refuses if it cannot reach a majority. |
| `LINEARIZABLE`, other keys | Majority quorum read, highest timestamp wins |
| `FOLLOWER_STALE` | Local read. Fails with `STALE_READ_EXCEEDED` if last contact with the leaseholder is older than `max_staleness_ms` (0 = unbounded) |
| `QUORUM_CP` | Needs N//2+1 acks, else `success=false` / `ReplicaUnavailableError`; nothing is stored |
| `ALWAYS_AP` | Always accepted; replicated best effort; conflicts resolved by Last-Write-Wins `(timestamp, node)` |

## Tests and benchmark
```
python -m pytest -v                                  # 29 passed
python perf_cap_bench.py --rtt-ms 40                 # simulated 40 ms RTT to the leaseholder
python perf_cap_bench.py --rtt-ms 0 --out perf_cap_results_localhost
python -m pytest -q --junitxml=junit.xml && python make_report.py
```
`--rtt-ms` is a *simulated* network delay (everything runs on localhost).
