package main

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/redis/go-redis/v9"
	"os"
)

func main() {

	rdb := redis.NewClient(&redis.Options{
		Addr: os.Getenv("REDIS_ADDR"),
	})

	subscriber, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client: rdb,
	}, watermill.NewStdLogger(true, true))

	if err != nil {
		panic(err)
	}

	messages, err := subscriber.Subscribe(context.Background(), "progress")

	for msg := range messages {
		fmt.Printf("Message ID: %s - %s%", msg.UUID, string(msg.Payload))
		msg.Ack()
	}
}
