package test

import (
	"fmt"
	"testing"
	"time"
)

func TestTimeLayout(t *testing.T){
    pt, err := time.Parse("2006-01-02 15:04:05.000000000", "2024-07-08 04:52:25.878798687")
    if err != nil{
        fmt.Println(err.Error())
        t.Fail()
    }

    fmt.Println(pt.UnixNano())

}
