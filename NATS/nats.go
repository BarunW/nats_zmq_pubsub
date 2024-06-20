package NATS

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
)

type nats_ struct{
    Conn *nats.Conn
}

func NewNats() (*nats_, error) {
    now := time.Now()
    conn, err  := nats.Connect(nats.DefaultURL)
    if err != nil {
        slog.Error("Error Unable to connect the server", "Details", err.Error())
        return nil, err
    }     
    fmt.Println("NATS Connection Time", time.Since(now))
    return &nats_{ Conn: conn} , nil 
}

func(n *nats_) Publish(subj string, data interface{}, 
                    serializeFunc func(i interface{})([]byte, error)){
    dataByt, _ :=  serializeFunc(data)
    now := time.Now()
    if err := n.Conn.Publish(subj, dataByt); err != nil{
        slog.Error("NATS Error In publishing", "Details", err.Error())
        return
    }
    fmt.Println(subj, time.Since(now))
    
}

func(n *nats_) Subscribe(subj string) {
    _, err := n.Conn.Subscribe(subj, func(msg *nats.Msg){
       fmt.Println(subj)
       fmt.Println(string(msg.Data))
       fmt.Println("-----------------------------------------------------------")
    })
    if err != nil{
       slog.Info("Error SUBS", "Details", err.Error()) 
       return
    }

}
