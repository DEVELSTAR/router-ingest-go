package kafka

import (
	"context"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
	"router-ingest-go/config"
	"router-ingest-go/internal/model"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(cfg *config.Config) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(cfg.KafkaBrokers...),
			Topic:    cfg.KafkaTopic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

func (p *Producer) Write(m model.Metric) error {
	value, err := json.Marshal(m)
	if err != nil {
		return err
	}

	err = p.writer.WriteMessages(context.Background(),
		kafka.Message{Value: value},
	)
	if err != nil {
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
