# Test Clients

This directory contains various client tools for testing and verifying the Distributed Key-Value Store.

## Structure

The clients are organized into separate subdirectories to avoid package name conflicts:

- **`client/`**: A general-purpose client that performs basic `Put` and `Get` operations to verify connectivity and basic functionality.
- **`verify_persistence/`**: A test tool that writes a value, reads it back, and confirms it was stored.
- **`verify_restart/`**: A test tool designed to verify that data persists across node restarts (requires manual node restart between steps or a specific test runner).

## Usage

You can run each client using `go run`:

### Basic Client
```bash
go run test_client/client/main.go
```

### Verify Persistence
```bash
go run test_client/verify_persistence/main.go
```

### Verify Restart
```bash
go run test_client/verify_restart/main.go
```
