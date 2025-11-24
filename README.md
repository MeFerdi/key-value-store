# Distributed Key-Value Store

A distributed key-value store implementation in Go, designed to demonstrate core concepts of distributed systems such as replication and consensus.

## Features

- **In-Memory Storage**: Basic key-value storage using Go maps.
- **gRPC API**: Strong typed API for node-to-node and client-to-node communication.
    - `Put(key, value)`
    - `Get(key)`
    - `AppendEntries` (Stub for Raft consensus)

## Getting Started

### Prerequisites

- Go 1.21+
- Protocol Buffers compiler (`protoc`)

### Running the Node

```bash
go run main.go
```

The server will start on port 8080 by default.

### API Usage

The project uses gRPC. You can use the provided test client or any gRPC client to interact with the node.

```bash
go run test_client/main.go
```
