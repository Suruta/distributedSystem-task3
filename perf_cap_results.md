# PACELC benchmark (1000 iterations per scenario, simulated RTT to remote node = 40 ms)

## Healthy cluster

| Read Mode | Target Node | Median Latency (ms) | p95 Latency (ms) | Read Availability (%) |
|---|---|---|---|---|
| Linearizable | Leaseholder A (remote) | 45.30 | 46.83 | 100.0 (1000/1000) |
| Follower Stale | Local follower B | 0.15 | 0.23 | 100.0 (1000/1000) |

## Leaseholder A isolated from B and C (network partition)

| Read Mode | Target Node | Median Latency (ms) | p95 Latency (ms) | Read Availability (%) |
|---|---|---|---|---|
| Linearizable | Leaseholder A (isolated) | n/a | n/a | 0.0 (0/1000) |
| Follower Stale (within 5 s bound) | Local follower B | 0.15 | 0.23 | 100.0 (1000/1000) |
| Follower Stale (bound expired) | Local follower B | n/a | n/a | 0.0 (0/1000) |
