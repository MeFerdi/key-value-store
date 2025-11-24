package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	pb "distributed-kv-store/proto"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Config holds configuration for the KVNode
type Config struct {
	Port      int
	RaftPort  int
	DataDir   string
	RaftDir   string
	NodeID    string
	Bootstrap bool
	JoinAddr  string
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
func NewKVNode(cfg Config) (*KVNode, error) {
	raftDir := filepath.Join(cfg.DataDir, "raft")
	if err := os.MkdirAll(raftDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create raft dir: %v", err)
	}

	opts := badger.DefaultOptions(filepath.Join(cfg.DataDir, "badger"))
	opts.Logger = nil // Disable default logger for cleaner output

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger db: %v", err)
	}

	n := &KVNode{
		db: db,
		Config: Config{
			Port:      cfg.Port,
			RaftPort:  cfg.RaftPort,
			DataDir:   cfg.DataDir,
			RaftDir:   raftDir,
			NodeID:    cfg.NodeID,
			Bootstrap: cfg.Bootstrap,
			JoinAddr:  cfg.JoinAddr,
		},
		Address: fmt.Sprintf(":%d", cfg.Port),
	}

	if err := n.setupRaft(); err != nil {
		db.Close()
		return nil, err
	}

	return n, nil
}

func (n *KVNode) setupRaft() error {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(n.Config.NodeID)

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

	if n.Config.Bootstrap {
		log.Printf("Bootstrapping cluster as leader...")
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
			log.Printf("Bootstrap cluster (ignored if already bootstrapped): %v", err)
		}

		// Wait for leadership and register self
		go func() {
			for {
				time.Sleep(1 * time.Second)
				if r.State() == raft.Leader {
					if err := n.registerPeer(string(transport.LocalAddr()), n.Address); err == nil {
						log.Printf("Successfully registered leader address")
						return
					} else {
						log.Printf("Failed to register leader address: %v", err)
					}
				}
			}
		}()
	}

	return nil
}

// registerPeer stores the gRPC address of a peer in the KV store
func (n *KVNode) registerPeer(raftAddr, grpcAddr string) error {
	key := fmt.Sprintf("_sys_peer_%s", raftAddr)
	log.Printf("Registering peer: %s -> %s", raftAddr, grpcAddr)

	entry := logEntry{Key: key, Value: grpcAddr}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	return n.raft.Apply(data, 500*time.Millisecond).Error()
}

// getLeaderAddress retrieves the gRPC address of the current leader
func (n *KVNode) getLeaderAddress() string {
	leaderAddr, _ := n.raft.LeaderWithID()
	if leaderAddr == "" {
		return ""
	}

	key := fmt.Sprintf("_sys_peer_%s", leaderAddr)
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
		log.Printf("Failed to lookup leader address for %s: %v", leaderAddr, err)
		return ""
	}
	return string(valCopy)
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
		leaderHint := n.getLeaderAddress()
		return &pb.PutResponse{Success: false, LeaderHint: leaderHint}, nil
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

	// If not found, it might be because we are not up to date or it doesn't exist.
	// We can provide a leader hint just in case the client wants strong consistency.
	leaderHint := ""
	if n.raft.State() != raft.Leader {
		leaderHint = n.getLeaderAddress()
	}

	if err != nil && err != badger.ErrKeyNotFound {
		return &pb.GetResponse{Found: false, LeaderHint: leaderHint}, err
	}
	return &pb.GetResponse{Value: string(valCopy), Found: found, LeaderHint: leaderHint}, nil
}

// AppendEntries handles log replication (stub for now)
func (n *KVNode) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return &pb.AppendEntriesResponse{Term: req.Term, Success: true}, nil
}

// Join handles a request to join the cluster
func (n *KVNode) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	log.Printf("Received Join request from node %s at %s (gRPC: %s)", req.NodeId, req.RaftAddress, req.GrpcAddress)

	if n.raft.State() != raft.Leader {
		return &pb.JoinResponse{Success: false}, fmt.Errorf("not leader")
	}

	// Register the peer's gRPC address first
	if err := n.registerPeer(req.RaftAddress, req.GrpcAddress); err != nil {
		return &pb.JoinResponse{Success: false}, fmt.Errorf("failed to register peer: %v", err)
	}

	future := n.raft.AddVoter(raft.ServerID(req.NodeId), raft.ServerAddress(req.RaftAddress), 0, 0)
	if err := future.Error(); err != nil {
		return &pb.JoinResponse{Success: false}, err
	}

	return &pb.JoinResponse{Success: true}, nil
}

func (n *KVNode) start() error {
	lis, err := net.Listen("tcp", n.Address)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterKVStoreServer(s, n)
	log.Printf("Starting gRPC KVNode on port %d (Raft port %d)...", n.Config.Port, n.Config.RaftPort)

	// If joining an existing cluster
	if n.Config.JoinAddr != "" {
		go func() {
			// Give the server a moment to start
			time.Sleep(1 * time.Second)
			if err := n.joinCluster(); err != nil {
				log.Fatalf("Failed to join cluster: %v", err)
			}
		}()
	}

	return s.Serve(lis)
}

func (n *KVNode) joinCluster() error {
	conn, err := grpc.Dial(n.Config.JoinAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to dial leader: %v", err)
	}
	defer conn.Close()

	client := pb.NewKVStoreClient(conn)
	req := &pb.JoinRequest{
		NodeId:      n.Config.NodeID,
		RaftAddress: fmt.Sprintf("127.0.0.1:%d", n.Config.RaftPort),
		GrpcAddress: fmt.Sprintf("127.0.0.1:%d", n.Config.Port),
	}

	log.Printf("Requesting to join cluster at %s...", n.Config.JoinAddr)
	resp, err := client.Join(context.Background(), req)
	if err != nil {
		return fmt.Errorf("join request failed: %v", err)
	}
	if !resp.Success {
		return fmt.Errorf("join request denied")
	}
	log.Println("Successfully joined cluster")
	return nil
}

func main() {
	// Parse flags
	bootstrap := flag.Bool("bootstrap", false, "Bootstrap the cluster as leader")
	join := flag.String("join", "", "Address of the leader to join (e.g., localhost:8080)")
	flag.Parse()

	// Parse env vars with defaults
	kvPort := getEnvInt("KV_PORT", 8080)
	raftPort := getEnvInt("RAFT_PORT", 9090)
	dataDir := getEnv("DATA_DIR", "./data/node1")
	nodeID := getEnv("NODE_ID", fmt.Sprintf("node-%d", raftPort))

	cfg := Config{
		Port:      kvPort,
		RaftPort:  raftPort,
		DataDir:   dataDir,
		RaftDir:   filepath.Join(dataDir, "raft"),
		NodeID:    nodeID,
		Bootstrap: *bootstrap,
		JoinAddr:  *join,
	}

	// Ensure data directory exists
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		log.Fatalf("Failed to create data dir: %v", err)
	}

	node, err := NewKVNode(cfg)
	if err != nil {
		log.Fatalf("Failed to create node: %v", err)
	}
	defer node.Close()

	if err := node.start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if value, ok := os.LookupEnv(key); ok {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return fallback
}
