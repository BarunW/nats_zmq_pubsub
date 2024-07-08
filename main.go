package main

import (
	"benchmark/KAFKA"
	nats_ "benchmark/NATS"
	zmq "benchmark/ZMQ"
	"benchmark/models"
	"benchmark/protos"
	"benchmark/streams"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func NATSPubSub() {
    n, err := nats_.NewNats() 
    if err != nil{
        return
    }   
    <-time.After(1 * time.Second) 

    go streams.GenerateStream(n, "stream-data", stream, 10 * time.Minute)
    go streams.GenerateStream(n, "camera-data", camera, 10 * time.Minute) 
    
    
    go n.Subscribe("stream-data")
    go n.Subscribe("camera-data")
    <- time.After(605 * time.Second)
}

func PullTheFrame(msg chan <- protoreflect.ProtoMessage){    
    ticker := time.NewTicker(200 * time.Millisecond) 
    counter := 1
    for range ticker.C{
        now := time.Now() 
        path := fmt.Sprintf("/home/dbarun/CDPG/frames/frame_%d.png", counter)
        fmt.Println(path)
        f, err := os.Open(path)
        if err != nil{
            slog.Error("Failed to open the file", "Details", err.Error())
            return
        }
        counter++

        byt, err := io.ReadAll(f)
        if err != nil{
            slog.Error("Failed to read the file", "Details", err.Error())
            return
        }
        fmt.Println("Upto Read--------------->", time.Since(now))  
        closingTime := time.Now()
        f.Close()
        fmt.Println("File close time", time.Since(closingTime))
        
        fd := protos.FrameData{
            Data: byt,
            MetaData: &protos.MetaData{
                Timestamp: timestamppb.Now(), 
                FrameNumber: 1,
            },
        }

        msg <- &fd  
    }   
}


func KakfaPubSub(){
    kf, err := KAFKA.NewKafka( kafka.ConfigMap{ "bootstrap.servers": "localhost:9092" } ) 
    if err != nil{
        slog.Error("Failed to initiate the kafka PUB-SUB")
        return
    }
    
     msgChan := make(chan protoreflect.ProtoMessage, 8)
     defer close(msgChan)
     go PullTheFrame(msgChan)
 
     go streams.GenerateStream2(kf, "frame-data", msgChan, 1 * time.Minute)
    
    go kf.Subscribe("frame-data")

    <- time.After(62 * time.Second)
}

func KafkaReplay(){ 
    now := time.Now()
    topic := "frame-data"
    timeForReplayFunc := time.Now()
    fd, err := KAFKA.Replay(&topic)
    if err != nil{
        return
    }
    fmt.Println("Total time taken for replay func", time.Since(timeForReplayFunc))
    
    fOpenTime := time.Now()
    f, err := os.OpenFile("output.png", os.O_CREATE | os.O_WRONLY, os.ModePerm)
    if err != nil{
        slog.Error("Unable to open file output.png", "Details", err.Error())
        return
    }
    defer f.Close()
    fmt.Println("File Open Time", time.Since(fOpenTime))
    
    writeTime := time.Now()
    n, err := f.Write(fd.Data)
    if err != nil{
        slog.Error("Failed to write the data to output.png", "Details", err.Error())
    }
    fmt.Println("Write Time", time.Since(writeTime))
    fmt.Println(fd.MetaData.Timestamp.AsTime().Date())
    fmt.Println("No of bytes return", n)
    fmt.Println("Overall Duration to get a message using offset", time.Since(now))
}

func ZeroMQPubSub() error{
    z, err := zmq.NewZeroMq()  
    if err != nil{
        return err
    }

    defer func(){
        z.ZmqContext.Term()
        z.Publisher.Close()
        z.Subscriber.Close()
    }()
    
    go streams.GenerateStream(z, "stream-data", stream, 1 * time.Minute)
    go streams.GenerateStream(z, "camera-data", stream, 1 * time.Minute)
    go z.Subscribe("stream-data", 1 * time.Minute) 
    go z.Subscribe("camera-data", 1 * time.Minute)
    <- time.After(1 * time.Minute)
    return nil
}

func main(){
//    err := ZeroMQPubSub()
//    if err != nil{
//        slog.Error("Failed to do pub sub on ZeroMq", "Deatils", err.Error())
//        return 
//    }
//     NATSPubSub()
      KakfaPubSub()
     //KafkaReplay()   
}



var camera models.Camera = models.Camera{
    CameraId: "RandomCameraId010",
    UserId: 41,
    ServerId: 1,
    CameraName: "SomeRandomCamera",
    CameraNum: 7,
    CameraUsage: "IDK",
    CameraOrientation: "North East",
    City: "RandomCity",
    Junction: "Corner",
    Location: "RandomLocation",
    UpdatedAt: time.Now(),
    CreatedAT: time.Now(),
}


var stream models.Stream = models.Stream{
    StreamId: "RandomStream",
    UserId: 41 ,
    CameraId: "RandomCameraId010",
    ProvenanceStreamId: "IDK42",
    SourceServerId: 1,
    DestinationServerId: 2,
    ProcessId: 4,
    StreamName: "RandomStream",
    StreamUrl: "HelloWorld",
    StreamType: "RealTime",
    Type: "IDK",
    IsPublic: true,
    IsActive: true,
    IsPublishing: true,
    IsStable: true,
    TotalClients: 5,
    Codec: "h.264",
    Resolution: "1080 * 1920",
    FrameRate: 30,
    BandwidthIn: 3000,
    BandwidthOut: 5000,
    BytesIn: 1500,
    BytesOut: 1500,
    ActiveTime: 30,
    LastAccessed: time.Now(),
    CreatedAT: time.Now(),
    UpdatedAt: time.Now(),

}

