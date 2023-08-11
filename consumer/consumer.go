package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	kafka "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

var mu sync.Mutex

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

type Message struct {
	ID      uuid.UUID `json:"id"`
	Message int       `json:"message"`
}

func KafkaGoConsumer(topic string) {
	pool, err := NewDBPsql()

	if err != nil {
		logrus.Errorf("NewDBPsql: %v", err)
	}
	partition := 0
	fmt.Println("Kafka topic:", topic)
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     topic,
		Partition: partition,
		MinBytes:  10e3,
		MaxBytes:  10e6,
	})
	defer r.Close()
	r.SetOffset(0)
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			logrus.Errorf("ReadMessage: %v", err)
			break
		}

		fmt.Printf("message at offset %d: %s = %s\n", m.Offset,
			string(m.Key), string(m.Value))
		temp := Message{}
		err = json.Unmarshal(m.Value, &temp)
		if err != nil {
			logrus.WithFields(logrus.Fields{"value:": m.Value, "temp: ": &temp}).Errorf("Unmarshal: %v", err)
			break
		}
		go func() {
			mu.Lock()
			err = Create(context.Background(), &temp, pool)
			if err != nil {
				logrus.WithFields(logrus.Fields{"temp: ": &temp}).Errorf("Create: %v", err)
			}
			mu.Unlock()
		}()

		fmt.Printf("%T\n", temp)
	}
}

// Create function executes SQL request to insert person into database
func Create(ctx context.Context, entity *Message, pool *pgxpool.Pool) error {

	entity.ID = uuid.New()

	bd, err := pool.Exec(ctx,
		`INSERT INTO kafka.kafka_storage (id, kafka_message) 
	VALUES($1,$2)`,
		entity.ID, entity.Message)
	if err != nil && !bd.Insert() {
		return fmt.Errorf("Exec(): %w", err)
	}
	return nil
}
