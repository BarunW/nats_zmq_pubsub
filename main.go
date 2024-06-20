package main

import (
	nats_ "benchmark/NATS"
	zmq "benchmark/ZMQ"
	"benchmark/models"
	"benchmark/streams"
	"log/slog"
	"time"
)

func NATSPubSub() {
    n, err := nats_.NewNats() 
    if err != nil{
        return
    }   
    <-time.After(1 * time.Second) 

    go streams.GenerateStream(n, "stream-data", stream, 10 * time.Second)
    go streams.GenerateStream(n, "camera-data", camera, 11 * time.Second) 
    
    
    go n.Subscribe("stream-data")
    go n.Subscribe("camera-data")
    <- time.After(20 * time.Second)
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
    
    go streams.GenerateStream(z, "stream-data", stream, 10 * time.Second)
    go streams.GenerateStream(z, "camera-data", stream, 10 * time.Second)
    go z.Subscribe("stream-data", 10 * time.Second) 
    go z.Subscribe("camera-data", 10 * time.Second)
    <- time.After(13 * time.Second)
    return nil
}

func main(){
    err := ZeroMQPubSub()
    if err != nil{
        slog.Error("Failed to do pub sub on ZeroMq", "Deatils", err.Error())
        return 
    }
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

