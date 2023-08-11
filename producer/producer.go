// Package producer contains produce functions
package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	kafkaConfig "github.com/eugenshima/kafka/config"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

// Message struct represents a message in a Kafka cluster
type Message struct {
	ID      uuid.UUID `json:"id"`
	Message int       `json:"message"`
}

const (
	MIN       = 100
	MAX       = 1000
	partition = 0
)

// random function generates a random number between min & max
func random(min, max int) int {
	return rand.Intn(max-min) + min
}

// KafkaGoProducer function produces messages to Kafka Cluster
func KafkaGoProducer(topic string, limit int) {
	conn, err := kafka.DialLeader(context.Background(), "tcp",
		kafkaConfig.ConstHost, topic, partition)
	if err != nil {
		logrus.WithFields(logrus.Fields{"topic:": topic, "partition:": partition}).Errorf("DialLeader: %v", err)
		return
	}
	defer func() {
		err = conn.Close()
		if err != nil {
			logrus.Errorf("Close: %v", err)
			return
		}
	}()
	msgCount := 0
	startTimer := time.Now()
	for time.Since(startTimer) < time.Second && msgCount < limit {
		myrand := random(MIN, MAX)
		temp := Message{uuid.New(), myrand}
		recordJSON, _ := json.Marshal(temp)
		err = conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
		if err != nil {
			logrus.Errorf("SetWriteDeadline: %v", err)
			break
		}
		_, err := conn.WriteMessages(kafka.Message{Value: recordJSON})
		if err != nil {
			logrus.Errorf("WriteMessages: %v", err)
			break
		}
		msgCount++
	}
	fmt.Println("timer ends... sent messages: ", msgCount)
}
