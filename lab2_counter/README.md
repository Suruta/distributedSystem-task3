# Distributed System with gRPC

## commands

compile:
```cmd
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. counter.proto
```
run server:
```cmd
python server.py --port 50052
```
send request:
```cmd
python client.py --target localhost:50052 incr likes:post-42 --by 5
python client.py --target localhost:50052 get likes:post-42
```

## script senarios

### First senario

run three replicas:
```cmd
python server.py --port 50052 --name replica-A
python server.py --port 50053 --name replica-B
python server.py --port 50054 --name replica-C
```

run one client:
```cmd
python client.py incr x --by 1
```

terminate replica B.

then run client again:
```cmd
python3 client.py incr x --by 1 --key k
```

show the anomaly:
```cmd
python3 client.py --target localhost:50052 get x
```

### Second senario

run one replica:
```cmd
python3 server.py --port 50052 --name replica-A
```

run one client and send the same key twice:
```cmd
python3 client.py --target localhost:50052 incr likes:post-42 --by 1 --key my-key-1
python3 client.py --target localhost:50052 incr likes:post-42 --by 1 --key my-key-1
python3 client.py --target localhost:50052 get likes:post-42
```

## pytest

```cmd
python3 -m pytest -v
```
```cmd
python3 -m pytest -v test_failure_injection.py
```
```cmd
python3 perf_benchmark.py
```