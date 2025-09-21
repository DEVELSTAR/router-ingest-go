
// router-ingest-go/internal/kafka/producer.go

package kafka

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"router-ingest-go/config"
	"router-ingest-go/internal/model"
)

type Producer struct {
	writer *kafka.Writer
	topic  string
}

func NewProducer(cfg *config.Config) *Producer {
	w := &kafka.Writer{
		Addr:     kafka.TCP(cfg.KafkaBrokers...),
		Topic:    cfg.KafkaTopic,
		Balancer: &kafka.Hash{},
		Async:    false, // delivery guarantee
	}
	return &Producer{writer: w, topic: cfg.KafkaTopic}
}

func (p *Producer) Publish(ctx context.Context, m model.Metric) error {
	value, err := json.Marshal(m)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Key:   []byte(m.DeviceID),
		Value: value,
	}

	// timeout context
	ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := p.writer.WriteMessages(ctxTimeout, msg); err != nil {
		log.Println("❌ Kafka write error:", err)
		return err
	}

	log.Printf("✅ Produced metric: device=%s endpoint=%s", m.DeviceID, m.Endpoint)
	return nil
}

func (p *Producer) Close() {
	if err := p.writer.Close(); err != nil {
		log.Println("⚠️ Kafka producer close error:", err)
	} else {
		log.Println("✅ Kafka producer closed")
	}
}