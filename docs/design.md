# Design Document

## Architecture Overview

The system is currently a single-node key-value store with a gRPC interface. It is designed to be extended into a distributed system using the Raft consensus algorithm.

### Components

#### KVNode
The core struct `KVNode` manages the state of a single node.
- **Storage**: Uses a thread-safe `map[string]string` protected by a `sync.RWMutex`.
- **Communication**: Implements a gRPC server for handling requests.

### API Design

The API is defined using Protocol Buffers (`proto/kv.proto`) to ensure type safety and easy code generation.

#### Methods
- **Put**: Writes a value to the store. Currently direct to memory.
- **Get**: Reads a value from the store.
- **AppendEntries**: Intended for Raft log replication. Currently a stub to validate the gRPC setup.

## Future Considerations

- **Persistence**: The current in-memory store is volatile. WAL (Write-Ahead Logging) will be needed.
- **Consensus**: `AppendEntries` will be fully implemented to support Raft leader election and log replication.
- **Cluster Management**: Dynamic configuration of peer nodes.
