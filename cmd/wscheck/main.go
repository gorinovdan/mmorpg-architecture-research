package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	url := flag.String("url", "ws://localhost:18080/ws?last=0-0", "websocket URL")
	timeout := flag.Duration("timeout", 2*time.Second, "read timeout")
	flag.Parse()

	conn, _, err := websocket.DefaultDialer.Dial(*url, nil)
	if err != nil {
		log.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()

	received := 0
	if err := conn.SetReadDeadline(time.Now().Add(*timeout)); err != nil {
		log.Fatalf("set deadline: %v", err)
	}
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
		received++
		if received >= 3 {
			break
		}
	}

	out := map[string]int{"received": received}
	data, err := json.Marshal(out)
	if err != nil {
		log.Fatalf("marshal result: %v", err)
	}
	fmt.Println(string(data))
	if received == 0 {
		log.Fatalf("no replay messages received")
	}
}
