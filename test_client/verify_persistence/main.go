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

	// 1. Put a value
	key := "persistent_key"
	value := "persistent_value"
	log.Printf("Putting %s = %s", key, value)
	_, err = c.Put(ctx, &pb.PutRequest{Key: key, Value: value})
	if err != nil {
		log.Fatalf("Put failed: %v", err)
	}

	// 2. Get the value to confirm it's there
	log.Printf("Getting %s", key)
	r, err := c.Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	if r.Value != value {
		log.Fatalf("Expected %s, got %s", value, r.Value)
	}
	log.Println("Value confirmed in memory/storage.")
}
