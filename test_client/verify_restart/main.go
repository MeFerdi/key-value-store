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

	key := "persistent_key"
	expectedValue := "persistent_value"

	// Get the value to confirm it's still there after restart
	log.Printf("Getting %s", key)
	r, err := c.Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	if r.Value != expectedValue {
		log.Fatalf("Expected %s, got %s", expectedValue, r.Value)
	}
	log.Println("Persistence verified!")
}
