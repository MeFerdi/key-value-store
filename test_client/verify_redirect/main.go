package main

import (
	"context"
	"flag"
	"log"
	"time"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	target := flag.String("target", "localhost:8081", "Address of the node to connect to (should be follower)")
	flag.Parse()

	conn, err := grpc.Dial(*target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewKVStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	log.Printf("Sending Put request to %s...", *target)
	r, err := c.Put(ctx, &pb.PutRequest{Key: "redirect_test", Value: "value"})
	if err != nil {
		log.Fatalf("Put failed: %v", err)
	}

	if r.Success {
		log.Println("Put succeeded directly (unexpected if target is follower)")
	} else {
		log.Printf("Put failed as expected. Leader hint: %s", r.LeaderHint)
		if r.LeaderHint != "" {
			log.Println("Redirection verified!")
		} else {
			log.Println("No leader hint received.")
		}
	}
}
