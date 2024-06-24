package NATS

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func(n *nats_) UpgradeToJS() error{ 
    js, err := jetstream.New(n.Conn)
    if err != nil{
        slog.Error("Failed to proceed with NATS jetstream", "Details", err.Error())
        return err
    }
    n.JS = js
    
    return nil
}

func(n *nats_) CreateJsStream() error{
    ctx := context.Background() 
    _, err := n.JS.CreateStream(ctx, jetstream.StreamConfig{
        Name: "jobs",
        Subjects: []string{"jobs.*.*"},
        Storage: jetstream.FileStorage,
        Replicas: 1,
        Retention: jetstream.WorkQueuePolicy,
        Discard: jetstream.DiscardNew,
        MaxMsgs: 20000,
        MaxAge: 10 * time.Minute,
    })

    if err != nil{
        slog.Error("Failed to create stream", "Details", err.Error())
        return err
    } 
    return nil 
}

func(n *nats_) PUB() error {
    ctx := context.Background()
    _, err := n.JS.Publish(ctx, "jobs.high.send_email", []byte("some-data"))
    if err != nil{
        slog.Error("Failed to publish", "Details", err.Error())
        return err
    }

    fmt.Println("Successfully publish to the stream")

    return nil 
}


func(n *nats_) Consume(){
	priority, id := "high", struct{}{}
	consumerName := fmt.Sprintf("worker_%s", priority)
	workerName := fmt.Sprintf("worker_%s_%s", priority, id)

	log.Default().SetPrefix(fmt.Sprintf("[%s] ", workerName))

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()
	log.Printf("connected to %s", nc.ConnectedUrl())

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	consumer, err := js.CreateOrUpdateConsumer(ctx, "jobs", jetstream.ConsumerConfig{
		Name:        consumerName,
		Durable:     consumerName,
		Description: fmt.Sprintf("Worker pool with priority %s", priority),
		BackOff: []time.Duration{
			5 * time.Second,
			10 * time.Second,
			15 * time.Second,
		},
		MaxDeliver: 4,
		FilterSubjects: []string{
			fmt.Sprintf("jobs.%s.>", priority),
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	c, err := consumer.Consume(func(msg jetstream.Msg) {
		meta, err := msg.Metadata()
		if err != nil {
			log.Printf("Error getting metadata: %s\n", err)
			return
		}
		if priority == "low" {
			log.Println("Error processing job")
			return
		}

		log.Printf("Received message sequence: %d\n", meta.Sequence.Stream)
		time.Sleep(10 * time.Millisecond)
		msg.Ack()
	})
	if err != nil {
		log.Fatal(err)
	}
	// graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt) 
	<-quit
	c.Stop()
}




