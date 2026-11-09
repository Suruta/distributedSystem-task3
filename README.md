# Distributed KV Store: Consistency Models & CAP

## How to test it

### Part A

Option 1: automated:
```cmd
python -m pytest -v -k "cp_write_on"
```

Option 2: manual

Step 1: run these commands per terminal:
```cmd
python server.py --name A --port 50052 --peers B=localhost:50053,C=localhost:50054 --sync-interval 0
python server.py --name B --port 50053 --peers A=localhost:50052,C=localhost:50054 --sync-interval 0
python server.py --name C --port 50054 --peers A=localhost:50052,B=localhost:50053 --sync-interval 0
```

Step 2: Create the partition DC-1 = {A, B} and DC-2 = {C}:  
```cmd
python -c "import sys; sys.path.insert(0,'tests'); from test_cap import PartitionInjector; PartitionInjector({'A':'127.0.0.1:50052','B':'127.0.0.1:50053','C':'127.0.0.1:50054'}).partition(['A','B'],['C'])"
```

Step 3: Put to DC-1 (majority)
```cmd
python client.py --target localhost:50052 put user:1 alice --strategy cp
```

Step 4: Put to DC-2 (minority)
```cmd
python client.py --target localhost:50054 put user:2 bob --strategy cp
```

Step 5: health the partition (optional)
```cmd
python -c "import sys; sys.path.insert(0,'tests'); from test_cap import PartitionInjector; PartitionInjector({'A':'127.0.0.1:50052','B':'127.0.0.1:50053','C':'127.0.0.1:50054'}).heal()"
```

### Part B

Option 1: automated:
```cmd
python -m pytest -v -k "ap_write or lww or anti_entropy"
```

Option 2: manual:

Step 1: do the same as Part A's 1-2 steps  
If you healed the partition at the end of Part A, create it again:
```cmd
python -c "import sys; sys.path.insert(0,'tests'); from test_cap import PartitionInjector; PartitionInjector({'A':'127.0.0.1:50052','B':'127.0.0.1:50053','C':'127.0.0.1:50054'}).partition(['A','B'],['C'])"
```

Step 2: AP write to DC-2 (minority):
```cmd
python client.py --target localhost:50054 put x v2-from-dc2 --strategy ap --timestamp 200
```

To create a divergent value, also write the same key on the majority side with an older timestamp:
```cmd
python client.py --target localhost:50052 put x v1-from-dc1 --strategy cp --timestamp 100
```

Step 3: Check the divergence before healing (each node’s local copy):
```cmd
python client.py --target localhost:50052 get x --consistency stale
python client.py --target localhost:50054 get x --consistency stale
```

Step 4: reconnect DC-2 to DC-1:
```cmd
python -c "import sys; sys.path.insert(0,'tests'); from test_cap import PartitionInjector; PartitionInjector({'A':'127.0.0.1:50052','B':'127.0.0.1:50053','C':'127.0.0.1:50054'}).heal()"
```

Step 5: Read after recovery:
```cmd
python client.py --target localhost:50052 get x --consistency linearizable
python client.py --target localhost:50053 get x --consistency stale
python client.py --target localhost:50054 get x --consistency stale
```

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
