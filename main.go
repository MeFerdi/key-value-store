package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc"
)

// Config holds configuration for the KVNode
type Config struct {
	Port int
}

// KVNode represents a node in the distributed key-value store
type KVNode struct {
	pb.UnimplementedKVStoreServer
	mu      sync.RWMutex
	Store   map[string]string
	Config  Config
	Address string
}

// NewKVNode creates a new instance of KVNode
func NewKVNode(port int) *KVNode {
	return &KVNode{
		Store: make(map[string]string),
		Config: Config{
			Port: port,
		},
		Address: fmt.Sprintf(":%d", port),
	}
}

// Put inserts a key-value pair into the store
func (n *KVNode) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Store[req.Key] = req.Value
	log.Printf("Put: %s = %s", req.Key, req.Value)
	return &pb.PutResponse{Success: true}, nil
}

// Get retrieves a value by key
func (n *KVNode) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	val, ok := n.Store[req.Key]
	log.Printf("Get: %s -> %s (found: %v)", req.Key, val, ok)
	return &pb.GetResponse{Value: val, Found: ok}, nil
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
	node := NewKVNode(8080)
	if err := node.start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
