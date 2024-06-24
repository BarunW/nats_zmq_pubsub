package KAFKA

import (
	"fmt"
	"log/slog"
	"time"

	kf "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type kafka struct {
   producer *kf.Producer
   consumer *kf.Consumer
}

func NewKafka(configMap kf.ConfigMap) (*kafka, error){
    now := time.Now()
    p, err := kf.NewProducer(&configMap) 
    if err != nil{
        slog.Error("Failed to call producer api", "Details", err.Error())
        return nil, err 
    }
     
    k := &kafka{
        producer: p,
    }
    k.msgReportHandler() 
    fmt.Println("KAFKA Connection time", time.Since(now))
    return k , nil
}

func(ka *kafka) msgReportHandler() {
    go func(){
        for e := range ka.producer.Events(){
            switch ev := e.(type){
            case *kf.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
				    fmt.Println("Message Delievered at", time.Now())	
				}

            }
        }
    }()
}

func(ka *kafka) Publish(topic string, data interface{}, serializeFunc func(i interface{})([]byte, error)){
    dataByt, _ := serializeFunc(data)
    now := time.Now()

    err := ka.producer.Produce(
        &kf.Message{
            TopicPartition: kf.TopicPartition{ Topic: &topic, Partition: kf.PartitionAny },
            Value: dataByt,
        }, nil); 
    if err != nil{
        slog.Error("KAFKA Failed to publish message", "Details", err.Error())
    }

    fmt.Println("KAFKA", topic ,time.Since(now))
}

func(ka *kafka) Subscribe(topic string) { 
	c, err := kf.NewConsumer(&kf.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          topic+"Group",
	})

	if err != nil {
		panic(err)
	}

    if err := c.SubscribeTopics([]string{topic}, nil); err != nil{
        slog.Error("Failed to subscribe to this topic", "Details", err.Error())
        return
    }
    

	// A signal handler or similar could be used to set this to false to break the loop.
	run := true
	for run {
		msg, err := c.ReadMessage(38 * time.Millisecond)
		if err == nil {
            fmt.Println(topic)
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
            fmt.Println("---------------------------------")
		} else if !err.(kf.Error).IsTimeout(){
			// The client will automatically try to recover from all errors.
			// Timeout is not considered an error because it is raised by
			// ReadMessage in absence of messages.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
	c.Close()
}


