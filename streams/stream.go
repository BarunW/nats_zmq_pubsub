package streams

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)


type MessagingChannel interface{
    Publish(subj string, data interface{}, 
                    serializeFunc func(i interface{})([]byte, error))
}

func SerializeToJson(i interface{}) ([]byte, error){
    // now := time.Now()
    byte, err := json.Marshal(i)
    if err != nil{
        slog.Error("Unable to Serialize to json", "Details", err.Error())
        return nil, err
    }
    // fmt.Println("Serialize Time Taken", time.Since(now))
    return byte, nil
}

func SerializeToProtoBuf(i interface{} ) ([]byte, error){
    switch i.(type){
    case protoreflect.ProtoMessage:
        break
    default:
        return nil, fmt.Errorf("%s", "Invalid Type")
    }

    byte, err := proto.Marshal(i.(protoreflect.ProtoMessage))
    if err != nil {
        slog.Error("Unable to serialize the proto message", "Details", err.Error())
        return nil, err
    }

    return byte, nil
}


func GenerateStream(msgChannel MessagingChannel, sub string, 
data interface{}, streamDuration time.Duration){  
    timer := time.NewTimer(streamDuration)
    ticker := time.NewTicker(120 * time.Millisecond) 
    outer:
    for {
        select{
        case <- timer.C:
            break outer
        case <- ticker.C:
            msgChannel.Publish(sub, data, SerializeToJson) 
        }
    }
}

func GenerateStream2(publisher MessagingChannel, topic string,
    msgChan <- chan protoreflect.ProtoMessage, streamDuration time.Duration){
    timer := time.NewTimer(streamDuration) 
    outer:
    for {
        select{
        case <-timer.C:
            break outer
        case msg := <- msgChan:
             publisher.Publish(topic, msg, SerializeToProtoBuf) 
        }
    }
}
    
