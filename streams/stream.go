package streams

import (
	"encoding/json"
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

func SerializeToProtoBuf(i interface{}) ([]byte, error){
    msg := i.(protoreflect.ProtoMessage) 
    byte, err := proto.Marshal(msg)
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
    msgChan chan <- interface{}, streamDuration time.Duration){

}
    
