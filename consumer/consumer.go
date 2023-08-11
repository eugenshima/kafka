// Package consumer contains consume functions
package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	kafkaConfig "github.com/eugenshima/kafka/config"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	kafka "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

// NewDBPsql function provides Connection with PostgreSQL database
func NewDBPsql() (*pgxpool.Pool, error) {
	// Initialization a connect configuration for a PostgreSQL using pgx driver
	config, err := pgxpool.ParseConfig("postgres://eugen:ur2qly1ini@localhost:5432/eugene")
	if err != nil {
		return nil, fmt.Errorf("error connection to PostgreSQL: %v", err)
	}

	// Establishing a new connection to a PostgreSQL database using the pgx driver
	pool, err := pgxpool.ConnectConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("error connection to PostgreSQL: %v", err)
	}
	// Output to console
	fmt.Println("Connected to PostgreSQL!")

	return pool, nil
}

// Message struct represents a message in a Kafka cluster
type Message struct {
	ID      uuid.UUID `json:"id"`
	Message int       `json:"message"`
}

// KafkaGoConsumer receives messages to Kafka Cluster
func KafkaGoConsumer(topic string, limit int) {
	pool, err := NewDBPsql()
	if err != nil {
		logrus.Errorf("NewDBPsql: %v", err)
	}
	defer pool.Close()

	partition := 0
	fmt.Println("Kafka topic:", topic)
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{kafkaConfig.ConstHost},
		Topic:     topic,
		Partition: partition,
	})
	defer func() {
		err = r.Close()
		if err != nil {
			logrus.Errorf("Close: %v", err)
			return
		}
	}()
	temp := &Message{}
	msgCount := 0
	var rows [][]interface{}
	for time.Since(time.Now()) < time.Second && msgCount < limit {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			logrus.Errorf("ReadMessage: %v", err)
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset,
			string(m.Key), string(m.Value))

		err = json.Unmarshal(m.Value, &temp)
		if err != nil {
			logrus.WithFields(logrus.Fields{"value:": m.Value, "temp:": &temp}).Errorf("Unmarshal: %v", err)
			break
		}

		if err != nil {
			return
		}
		rows = append(rows, []interface{}{
			temp.ID,
			temp.Message,
		})
		fmt.Printf("%T\n", temp)
		msgCount++
	}
	err = Create(context.Background(), pool, rows)
	if err != nil {
		logrus.WithFields(logrus.Fields{"temp: ": &temp}).Errorf("Create: %v", err)
	}
	fmt.Println("timer ends... received messages: ", msgCount)
}

// Create function executes SQL request to insert Kafka message into database
func Create(ctx context.Context, pool *pgxpool.Pool, rows [][]interface{}) error {
	_, err := pool.CopyFrom(
		ctx,
		pgx.Identifier{"kafka", "kafka_storage"},
		[]string{"id", "kafka_message"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("CopyFrom: %W", err)
	}

	fmt.Println()

	return nil
}
