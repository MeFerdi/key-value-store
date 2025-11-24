package main

import (
	"context"
	"log"
	"time"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.Dial("localhost:8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewKVStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Test Put
	log.Println("Testing Put...")
	_, err = c.Put(ctx, &pb.PutRequest{Key: "foo", Value: "bar"})
	if err != nil {
		log.Fatalf("Put failed: %v", err)
	}
	log.Println("Put success")

	// Test Get
	log.Println("Testing Get...")
	r, err := c.Get(ctx, &pb.GetRequest{Key: "foo"})
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	log.Printf("Get: %s -> %s (found: %v)", "foo", r.Value, r.Found)

	if r.Value != "bar" {
		log.Fatalf("Expected bar, got %s", r.Value)
	}

	// Test AppendEntries (Stub)
	log.Println("Testing AppendEntries...")
	_, err = c.AppendEntries(ctx, &pb.AppendEntriesRequest{
		Term:     1,
		LeaderId: "leader1",
		Entries: []*pb.LogEntry{
			{Term: 1, Key: "k", Value: "v"},
		},
	})
	if err != nil {
		log.Fatalf("AppendEntries failed: %v", err)
	}
	log.Println("AppendEntries success")
}
