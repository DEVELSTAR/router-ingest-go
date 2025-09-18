package kafka

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	Writer *kafka.Writer
}

func New(brokers, topic string) *Producer {
	w := &kafka.Writer{
		Addr:     kafka.TCP(brokers),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	return &Producer{Writer: w}
}

func (p *Producer) Publish(ctx context.Context, key, value []byte) error {
	return p.Writer.WriteMessages(ctx,
		kafka.Message{
			Key:   key,
			Value: value,
			Time:  time.Now().UTC(),
		},
	)
}
