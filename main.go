package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	pb "distributed-kv-store/proto"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/grpc"
)

// Config holds configuration for the KVNode
type Config struct {
	Port     int
	RaftPort int
	DataDir  string
	RaftDir  string
}

// KVNode represents a node in the distributed key-value store
type KVNode struct {
	pb.UnimplementedKVStoreServer
	mu      sync.RWMutex
	db      *badger.DB
	raft    *raft.Raft
	Config  Config
	Address string
}

// NewKVNode creates a new instance of KVNode
func NewKVNode(port, raftPort int, dataDir string) (*KVNode, error) {
	raftDir := filepath.Join(dataDir, "raft")
	if err := os.MkdirAll(raftDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create raft dir: %v", err)
	}

	opts := badger.DefaultOptions(filepath.Join(dataDir, "badger"))
	opts.Logger = nil // Disable default logger for cleaner output

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger db: %v", err)
	}

	n := &KVNode{
		db: db,
		Config: Config{
			Port:     port,
			RaftPort: raftPort,
			DataDir:  dataDir,
			RaftDir:  raftDir,
		},
		Address: fmt.Sprintf(":%d", port),
	}

	if err := n.setupRaft(); err != nil {
		db.Close()
		return nil, err
	}

	return n, nil
}

func (n *KVNode) setupRaft() error {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(fmt.Sprintf("%d", n.Config.RaftPort))

	// Setup Raft communication
	addr := fmt.Sprintf("127.0.0.1:%d", n.Config.RaftPort)
	transport, err := raft.NewTCPTransport(addr, nil, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return fmt.Errorf("failed to create transport: %v", err)
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(n.Config.RaftDir, 3, os.Stderr)
	if err != nil {
		return fmt.Errorf("failed to create snapshot store: %v", err)
	}

	// Create the log store and stable store.
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(n.Config.RaftDir, "raft-log.bolt"))
	if err != nil {
		return fmt.Errorf("failed to create log store: %v", err)
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(n.Config.RaftDir, "raft-stable.bolt"))
	if err != nil {
		return fmt.Errorf("failed to create stable store: %v", err)
	}

	// Instantiate the FSM.
	fsm := &fsm{db: n.db}

	// Create the Raft system.
	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("failed to create raft: %v", err)
	}
	n.raft = r

	// Bootstrap the cluster (single node for now)
	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      config.LocalID,
				Address: transport.LocalAddr(),
			},
		},
	}
	f := r.BootstrapCluster(configuration)
	if err := f.Error(); err != nil {
		// It's safe to ignore this error if the cluster is already bootstrapped
		log.Printf("Bootstrap cluster (ignored if already bootstrapped): %v", err)
	}

	return nil
}

// Close closes the underlying database
func (n *KVNode) Close() error {
	if n.raft != nil {
		n.raft.Shutdown()
	}
	return n.db.Close()
}

// Put inserts a key-value pair into the store via Raft
func (n *KVNode) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	log.Printf("Put: %s = %s", req.Key, req.Value)

	if n.raft.State() != raft.Leader {
		return &pb.PutResponse{Success: false}, fmt.Errorf("not leader")
	}

	entry := logEntry{Key: req.Key, Value: req.Value}
	data, err := json.Marshal(entry)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal entry: %v", err)
	}

	future := n.raft.Apply(data, 500*time.Millisecond)
	if err := future.Error(); err != nil {
		return &pb.PutResponse{Success: false}, err
	}

	return &pb.PutResponse{Success: true}, nil
}

// Get retrieves a value by key
func (n *KVNode) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	// For now, allow reading from any node (eventual consistency)
	// In a strict system, we should verify leadership or use ReadIndex
	var valCopy []byte
	err := n.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(req.Key))
		if err != nil {
			return err
		}
		valCopy, err = item.ValueCopy(nil)
		return err
	})

	found := err == nil
	log.Printf("Get: %s -> %s (found: %v)", req.Key, string(valCopy), found)
	if err != nil && err != badger.ErrKeyNotFound {
		return &pb.GetResponse{Found: false}, err
	}
	return &pb.GetResponse{Value: string(valCopy), Found: found}, nil
}

// AppendEntries handles log replication (stub for now, Raft handles this internally via transport)
// Note: In a real gRPC Raft transport, we would map this to Raft's AppendEntries.
// Since we are using Raft's built-in TCP transport, this gRPC method is technically unused for Raft consensus itself,
// but kept for the proto definition compatibility if we wanted to implement a gRPC transport later.
func (n *KVNode) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return &pb.AppendEntriesResponse{Term: req.Term, Success: true}, nil
}

func (n *KVNode) start() error {
	lis, err := net.Listen("tcp", n.Address)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterKVStoreServer(s, n)
	log.Printf("Starting gRPC KVNode on port %d (Raft port %d)...", n.Config.Port, n.Config.RaftPort)
	return s.Serve(lis)
}

func main() {
	// Use a temporary directory for now, or a specific path
	dataDir := "./data/node1"
	// Port 8080 for gRPC, 9090 for Raft
	node, err := NewKVNode(8080, 9090, dataDir)
	if err != nil {
		log.Fatalf("Failed to create node: %v", err)
	}
	defer node.Close()

	if err := node.start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
