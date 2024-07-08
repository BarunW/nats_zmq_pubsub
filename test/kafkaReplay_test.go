package test

import (
	"benchmark/KAFKA"
	"fmt"
	"testing"
)


func TestKafkaReplayFromTimeStamp(t *testing.T){
    topic := "frame-data"
    if err := KAFKA.KafkaReplayFromTimeStamp(&topic, 10); err != nil{
        fmt.Println(err.Error())
        t.Fail()
    }
    
}
