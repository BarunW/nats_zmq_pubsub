package zmq

import (
	"fmt"
	"log/slog"
	"time"

	zmq_ "github.com/pebbe/zmq4/draft"
)

type zeroMq struct{
   ZmqContext *zmq_.Context 
   Publisher *zmq_.Socket
   Subscriber *zmq_.Socket
}


func(z *zeroMq) Publish(announcement string, data interface{}, serializeFunc func(i interface{})([]byte, error)){   
    now := time.Now()
    sub := []byte(announcement) 
    dataByt, _ := serializeFunc(data)
    _, err := z.Publisher.SendBytes(append(sub, dataByt...), 0)
    if err != nil{
        slog.Error("Unable to publish data", "Details", err.Error())
        return
    }
    fmt.Println("ZMQ", announcement, time.Since(now))
}

func(z* zeroMq) Subscribe(announcement string, dur time.Duration) {
//    timer := time.NewTimer(dur)
    err := z.Subscriber.SetSubscribe(announcement)
    if err != nil{
        slog.Error("Failed to subscribe to this announcement", "details", err.Error())
        return
    }
//   outer:
    for {
//        select{
//        case <-timer.C:
//            break outer
//        default:
            message, err := z.Subscriber.RecvBytes(0)
            if err != nil {
                slog.Error("Failed to recieved message:", "Details", err.Error())
            }
            fmt.Println(announcement)
            fmt.Println("Received:", string(message))
            fmt.Println("------------------------")

        }
//    }

}

func NewZeroMq() (*zeroMq, error){
    context, err := zmq_.NewContext()
    if err != nil {
        fmt.Println("Error creating context:", err)
        return nil, err
    }

    // Create a PUB socket
    publisher, err := context.NewSocket(zmq_.PUB)
    if err != nil {
        fmt.Println("Error creating socket:", err)
        return nil, err
    }

    // Bind the socket to a TCP address
    err = publisher.Bind("tcp://*:5555")
    if err != nil {
        fmt.Println("Error binding socket:", err)
        return nil, err
    } 

    subscriber, err := context.NewSocket(zmq_.SUB)
    if err != nil {
        fmt.Println("Error creating socket for subscriber", err)
        return nil, err
    }

    err = subscriber.Connect("tcp://localhost:5555")
    if err != nil{
        slog.Error("Failed to connect to publisher", "Details", err.Error())
        return nil, err
    }
    return &zeroMq{
        ZmqContext: context,
        Publisher : publisher,
        Subscriber: subscriber,
    }, nil
}

