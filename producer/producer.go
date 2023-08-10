package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"

	"strconv"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

type Record struct {
	Name   string `json:"name"`
	Random int    `json:"random"`
}

func random(min, max int) int {
	return rand.Intn(max-min) + min
}
func KafkaGoProducer(topic string, limit int) {
	MIN := 20
	MAX := 100
	TOTAL := limit
	partition := 0
	conn, err := kafka.DialLeader(context.Background(), "tcp",
		"localhost:9092", topic, partition)
	if err != nil {
		fmt.Printf("%s\n", err)
		return
	}
	for i := 0; i < TOTAL; i++ {
		myrand := random(MIN, MAX)
		temp := Record{strconv.Itoa(i), myrand}
		recordJSON, _ := json.Marshal(temp)
		conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
		conn.WriteMessages(
			kafka.Message{Value: []byte(recordJSON)},
		)
		if i%50 == 0 {
			fmt.Print(".")

		}

	}
	fmt.Println()
	conn.Close()
}
