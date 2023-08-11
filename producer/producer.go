package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"

	"time"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
)

type Message struct {
	ID      uuid.UUID `json:"id"`
	Message int       `json:"message"`
}

func random(min, max int) int {
	return rand.Intn(max-min) + min
}
func KafkaGoProducer(topic string, limit int) {
	MIN := 100
	MAX := 1000
	TOTAL := limit
	partition := 0
	conn, err := kafka.DialLeader(context.Background(), "tcp",
		"localhost:9092", topic, partition)
	if err != nil {
		fmt.Printf("%s\n", err)
		return
	}
	defer conn.Close()
	for i := 0; i < TOTAL; i++ {
		myrand := random(MIN, MAX)
		temp := Message{uuid.New(), myrand}
		recordJSON, _ := json.Marshal(temp)
		conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
		conn.WriteMessages(
			kafka.Message{Value: []byte(recordJSON)},
		)
		if i%50 == 0 {
			fmt.Print(".")
		}

	}
}
