package KAFKA

import (
	"benchmark/protos"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	kf "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"google.golang.org/protobuf/proto"
)

type kafka struct {
	producer    *kf.Producer
	adminClient *kf.AdminClient
}

func NewKafka(configMap kf.ConfigMap) (*kafka, error) {
	ac, err := kf.NewAdminClient(&configMap)
	if err != nil {
		slog.Error("Failed to create new adim client", "Details", err.Error())
	}

	now := time.Now()
	p, err := kf.NewProducer(&configMap)
	if err != nil {
		slog.Error("Failed to call producer api", "Details", err.Error())
		return nil, err
	}

	k := &kafka{
		producer:    p,
		adminClient: ac,
	}
	k.msgReportHandler()
	fmt.Println("KAFKA Connection time", time.Since(now))
	return k, nil
}

func (ka *kafka) msgReportHandler() {
	go func() {
		for e := range ka.producer.Events() {
			switch ev := e.(type) {
			case *kf.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Println("OFFSET", ev.TopicPartition.Offset, "PARTITION", ev.TopicPartition.Partition)
					fmt.Println("Message Delievered at", time.Now())
				}
			}
		}
	}()
}
func (ka *kafka) createTopic(topic string, tS ...kf.TopicSpecification) error {
	if tS == nil {
		tS = []kf.TopicSpecification{
			{
				Topic:             topic,
				NumPartitions:     2,
				ReplicationFactor: 1,
			},
		}
	}
	ctx, _ := context.WithCancel(context.Background())
	result, err := ka.adminClient.CreateTopics(ctx, tS)
	if err != nil {
		if err.(kf.Error).Code() == kf.ErrTopicAlreadyExists {
			fmt.Println(err)
			return nil
		}
		slog.Error("Failed to create Topic", "Details", err.Error())
	}

	for _, r := range result {
		fmt.Println(r)
	}

	return nil
}

func (ka *kafka) Publish(topic string, data interface{}, serializeFunc func(i interface{}) ([]byte, error)) {
	sync.OnceFunc(func() {
		if err := ka.createTopic(topic); err != nil {
			return
		}
	})

	dataByt, _ := serializeFunc(data)
	now := time.Now()

	err := ka.producer.Produce(
		&kf.Message{
			TopicPartition: kf.TopicPartition{Topic: &topic, Partition: kf.PartitionAny},
			Value:          dataByt,
		}, nil)
	if err != nil {
		slog.Error("KAFKA Failed to publish message", "Details", err.Error())
	}
	fmt.Println("KAFKA", topic, time.Since(now))
}

func deserializeProtoBuf(byt []byte) (*protos.FrameData, error) {
	fd := protos.FrameData{}
	if err := proto.Unmarshal(byt, &fd); err != nil {
		return nil, err
	}
	return &fd, nil

}

func (ka *kafka) Subscribe(topic string) {
	c, err := kf.NewConsumer(&kf.ConfigMap{
		"bootstrap.servers":  "localhost:9092",
		"group.id":           topic + "Group",
		"enable.auto.commit": false,
	})

	if err != nil {
		panic(err)
	}

	if err := c.Subscribe(topic, nil); err != nil {
		slog.Error("Failed to subscribe to this topic", "Details", err.Error())
		return
	}

	run := true
	for run {
		msg, err := c.ReadMessage(250 * time.Millisecond)
		if err == nil {
			fmt.Println(topic)
			now := time.Now()
			fd, err := deserializeProtoBuf(msg.Value)
			if err != nil {
				slog.Error("Failed to deserialze the msg value", "Details", err.Error())
				return
			}
			fmt.Printf("FRAME META DATA %+v\n", fd.MetaData)
			fmt.Println("---------------------------------", time.Since(now))
			ka.ack(c)
		} else if !err.(kf.Error).IsTimeout() {
			// The client will automatically try to recover from all errors.
			// Timeout is not considered an error because it is raised by
			// ReadMessage in absence of messages.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
	c.Close()
}

func (ka *kafka) ack(cons *kf.Consumer) {
	partitions, err := cons.Commit()
	if err != nil {
		slog.Error("Failed to ack", "Details", err.Error())
	}
	fmt.Printf("%+v", partitions)
}
func handleWriter(fd *protos.FrameData, counter int) {

	fOpenTime := time.Now()
	f, err := os.OpenFile("theoutput.png"+strconv.Itoa(counter), os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		slog.Error("Unable to open file output.png", "Details", err.Error())
		return
	}
	defer f.Close()
	fmt.Println("File Open Time", time.Since(fOpenTime))

	n, err := f.Write(fd.Data)
	if err != nil {
		slog.Error("Failed to write the data to output.png", "Details", err.Error())
		return
	}
	fmt.Printf("%d Byte return", n)
}

func KafkaReplayFromTimeStamp(topic *string, totalMsg uint16) error {
	config := &kf.ConfigMap{
		"metadata.broker.list": "localhost:9092",
		"auto.offset.reset":    "earliest",
		"group.id":             *topic + "Group",
	}
	c, err := kf.NewConsumer(config)
	if err != nil {
		slog.Error("Failed to create new consumer", "Details", err.Error())
	}

	t, err := time.Parse("2024-07-08 06:38:52.131160056", "2024-07-08 04:52:25.878798687")
	if err != nil {
		slog.Error("Failed to parse the time", "Details", err.Error())
	}

	fmt.Println(t)

	partitions := []kf.TopicPartition{
		{
			Topic:     topic,
			Partition: 0,
			Offset:    kf.Offset(t.UnixMilli()),
		},
	}

	offsets, err := c.OffsetsForTimes(partitions, 10000)
	if err != nil {
		slog.Error("Unable to set timestamp offsets", "Details", err.Error())
		return err
	}

	for _, off := range offsets {
		fmt.Println(off.Offset)
	}

	if err := c.Assign(offsets); err != nil {
		slog.Error("Failed to assigned the offsets", "Details", err.Error())
	}

	ps, err := c.SeekPartitions(offsets)
	if err != nil {
		slog.Error("Failed to seek paritions")
		return err
	}
	for _, of := range ps {
		fmt.Println("-------- : < ) -----------", of.Topic, of.Partition)
	}

	counter := 0
    for i := 0; i <= int(totalMsg); i++  {
		fmt.Println("Pooling")
		ev := c.Poll(100)
		switch e := ev.(type) {
		case *kf.Message:
			fd, err := deserializeProtoBuf(e.Value)
			if err != nil {
				fmt.Println("Failed to deserialize protobuf", err.Error())
				return err
			}
			go handleWriter(fd, counter)
			counter++
			fmt.Println(fd.MetaData.Timestamp.AsTime().Format(time.RFC3339))

		case kf.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
            return err
		default:
			fmt.Printf("Ignored %v\n", e)
		}
	}

	return nil
}

//func Replay(topic *string) (*protos.FrameData, error) {
//
//	cons, err := kf.NewConsumer(&kf.ConfigMap{
//		"bootstrap.servers": "localhost:9092",
//		"group.id":          *topic + "Group",
//	})
//	if err != nil {
//		slog.Error(err.Error())
//		return nil, err
//	}
//	//    if err := cons.Subscribe(*topic,nil); err != nil{
//	//        slog.Error("Failed to subscribe", "Details", err.Error())
//	//        return nil, err
//	//    }
//	//t, _ := time.Parse(time.RFC3339Nano, "2024-07-07T23:57:10Z")
//	//    offsets, err := cons.OffsetsForTimes([]kf.TopicPartition{
//	//        {
//	//            Topic: topic,
//	//            Partition: 0,
//	//            Offset: kf.OffsetBeginning,
//	//        },
//	//    }, 10000)
//	//    if err != nil{
//	//       slog.Error("Offsetes For Times Failed", "Details", err.Error())
//	//       return nil, err
//	//    }
//	//
//	//    if err := cons.Assign(offsets); err != nil{
//	//        slog.Error("Failed to assign to consumer", "Details", err.Error())
//	//        return nil, err
//	//    }
//	//    metadata, err := cons.GetMetadata(topic, false, 5000)
//	//    if err != nil {
//	//        slog.Error("Failed to get metadata", "Details", err.Error())
//	//        return nil, err
//	//    }
//	//
//	//    // Check if the topic exists and has partitions
//	//    topicMetadata, exists := metadata.Topics[*topic]
//	//    fmt.Printf("%+v", topicMetadata)
//	//    if !exists || len(topicMetadata.Partitions) == 0 {
//	//        slog.Error("Topic doesn't exist or has no partitions", "Topic", *topic)
//	//        return nil, fmt.Errorf("topic doesn't exist or has no partitions")
//	//    }
//	//
//	tps, err := cons.SeekPartitions([]kf.TopicPartition{
//		{
//			Topic:     topic,
//			Partition: 0,
//			Offset:    kf.OffsetBeginning,
//		},
//	})
//	if err := cons.Assign(tps); err != nil {
//		slog.Error("Failed to assign to consumer", "Details", err.Error())
//		return nil, err
//	}
//
//	for _, tp := range tps {
//		fmt.Println("TP", tp.String())
//		err := tp.Offset.Set(100)
//		if err != nil {
//			slog.Error("Failed to set Offset", "Details", err.Error())
//			return nil, err
//		}
//		fmt.Println("MetaData", tp.Metadata, *tp.Topic)
//		fmt.Println("EPOCH", tp.LeaderEpoch)
//		fmt.Println("TP", tp.String())
//	}
//
//	now := time.Now()
//	msg, err := cons.ReadMessage(10 * time.Second)
//	if err != nil {
//		slog.Error("Unable to get the message", "Details", err.Error())
//		return nil, err
//	}
//	fmt.Println("Reading the message from the offest", time.Since(now))
//
//	go func() {
//		if err := cons.Close(); err != nil {
//			slog.Error("Failed to close consumer", "Details", err.Error())
//		}
//	}()
//
//	dTime := time.Now()
//	fd, err := deserializeProtoBuf(msg.Value)
//	fmt.Println("DeserialzeTime", time.Since(dTime))
//	return fd, err
//
//}
