package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	pb "distributed-kv-store/proto"

	"github.com/dgraph-io/badger/v4"
	"google.golang.org/grpc"
)

// Config holds configuration for the KVNode
type Config struct {
	Port    int
	DataDir string
}

// KVNode represents a node in the distributed key-value store
type KVNode struct {
	pb.UnimplementedKVStoreServer
	mu      sync.RWMutex
	db      *badger.DB
	Config  Config
	Address string
}

// NewKVNode creates a new instance of KVNode
func NewKVNode(port int, dataDir string) (*KVNode, error) {
	opts := badger.DefaultOptions(dataDir)
	opts.Logger = nil // Disable default logger for cleaner output

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger db: %v", err)
	}

	return &KVNode{
		db: db,
		Config: Config{
			Port:    port,
			DataDir: dataDir,
		},
		Address: fmt.Sprintf(":%d", port),
	}, nil
}

// Close closes the underlying database
func (n *KVNode) Close() error {
	return n.db.Close()
}

// Write stores a key-value pair in BadgerDB
func (n *KVNode) Write(key, value string) error {
	return n.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), []byte(value))
	})
}

// Read retrieves a value from BadgerDB
func (n *KVNode) Read(key string) (string, error) {
	var valCopy []byte
	err := n.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		valCopy, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		return "", err
	}
	return string(valCopy), nil
}

// Put inserts a key-value pair into the store
func (n *KVNode) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	log.Printf("Put: %s = %s", req.Key, req.Value)
	err := n.Write(req.Key, req.Value)
	if err != nil {
		return &pb.PutResponse{Success: false}, err
	}
	return &pb.PutResponse{Success: true}, nil
}

// Get retrieves a value by key
func (n *KVNode) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	val, err := n.Read(req.Key)
	found := err == nil
	log.Printf("Get: %s -> %s (found: %v)", req.Key, val, found)
	if err != nil && err != badger.ErrKeyNotFound {
		return &pb.GetResponse{Found: false}, err
	}
	return &pb.GetResponse{Value: val, Found: found}, nil
}

// AppendEntries handles log replication (stub for now)
func (n *KVNode) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	log.Printf("AppendEntries: term=%d, leader=%s, entries=%d", req.Term, req.LeaderId, len(req.Entries))
	return &pb.AppendEntriesResponse{Term: req.Term, Success: true}, nil
}

func (n *KVNode) start() error {
	lis, err := net.Listen("tcp", n.Address)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterKVStoreServer(s, n)
	log.Printf("Starting gRPC KVNode on port %d...", n.Config.Port)
	return s.Serve(lis)
}

func main() {
	// Use a temporary directory for now, or a specific path
	dataDir := "./data/node1"
	node, err := NewKVNode(8080, dataDir)
	if err != nil {
		log.Fatalf("Failed to create node: %v", err)
	}
	defer node.Close()

	if err := node.start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
