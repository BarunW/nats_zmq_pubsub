package test

import (
	"benchmark/protos"
	"fmt"
	"io"
	"os"
	"testing"

	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestProtoReflectMessage(t *testing.T){
    f, err := os.Open("/home/dbarun/CDPG/frames/frame_1.png")
    if err != nil{
        fmt.Println(err.Error())
        t.Fail()
    }
    
    byt, err := io.ReadAll(f)
    if err != nil{
        fmt.Println(err.Error())
        t.Fail()
    }

    fmt.Println(byt)

    fd := protos.FrameData{
        Data: byt,
        MetaData: &protos.MetaData{
           Timestamp: timestamppb.Now(), 
           FrameNumber: 1,
        },
    }
    fmt.Printf("%+v", fd.ProtoReflect())
}
