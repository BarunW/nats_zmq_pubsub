package test

import (
	"benchmark/NATS"
	"benchmark/models"
	"benchmark/streams"
	"fmt"
	"testing"
	"time"
)

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

func BenchmarkNATSPublish(b *testing.B){
    n, err := NATS.NewNats()
    if err != nil{ b.Fail() }

    for i := 0; i < b.N; i++{
       n.Publish("test-data", stream, streams.SerializeToJson) 
    }
}

func TestCreateJsStream(t *testing.T) {
    n, err := NATS.NewNats()
    if err != nil{
        fmt.Println("Failed to connect to nats-server")
        t.Fail()
    }
    

    err = n.UpgradeToJS()
    if err != nil{
        fmt.Println("Failed to upgrade to jetstream")
        t.Fail()
    }
    
//    err = n.CreateJsStream()
//    if err != nil{
//        fmt.Println("Failed to Create Jet Stream jobs stream")
//        t.Fail()
//    }
    
//    go func(){
//        for {
//            if err := n.PUB(); err != nil{
//                t.Fail()
//            }
//            <-time.After(10 * time.Millisecond)
//        }
//    }()
    
    n.Consume()
 //   <-time.After(1 * time.Second)
    
    
}






