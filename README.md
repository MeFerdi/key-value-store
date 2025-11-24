# Distributed Key-Value Store

A distributed key-value store implementation in Go, designed to demonstrate core concepts of distributed systems such as replication and consensus.

## Features

- **In-Memory Storage**: Basic key-value storage using Go maps.
- **gRPC API**: Strong typed API for node-to-node and client-to-node communication.
    - `Put(key, value)`
    - `Get(key)`
    - `AppendEntries` (Stub for Raft consensus)

## Configuration

The node can be configured using environment variables and command-line flags.

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `KV_PORT` | Port for the gRPC KV service | `8080` |
| `RAFT_PORT` | Port for Raft consensus traffic | `9090` |
| `DATA_DIR` | Directory to store data | `./data/node1` |
| `NODE_ID` | Unique identifier for the node | `node-<RAFT_PORT>` |

### Flags

- `-bootstrap`: Initialize a new cluster as the leader. Use this for the first node.
- `-join <address>`: Join an existing cluster at the specified leader address (e.g., `localhost:8080`).

## Running the Node

### Single Node (Bootstrap)

To start the first node of a cluster (or a standalone node), use the `-bootstrap` flag:

```bash
# Default configuration (Ports 8080/9090, Data in ./data/node1)
go run . -bootstrap
```

### Running a Cluster

To run a local cluster, you can start multiple nodes on different ports.

**Node 1 (Leader):**
```bash
export DATA_DIR=./data/node1
export KV_PORT=8080
export RAFT_PORT=9090
go run . -bootstrap
```

**Node 2 (Follower):**
```bash
export DATA_DIR=./data/node2
export KV_PORT=8081
export RAFT_PORT=9091
go run . -join localhost:8080
```

**Node 3 (Follower):**
```bash
export DATA_DIR=./data/node3
export KV_PORT=8082
export RAFT_PORT=9092
go run main.go -join localhost:8080
```

## API Usage

The project uses gRPC. You can use the provided test clients to interact with the node. See [test_client/README.md](test_client/README.md) for details.

```bash
go run test_client/client/main.go
```
