package config

import (
	"time"

	"github.com/caarlos0/env/v9"
)

type Config struct {
	// Kafka
	KafkaBrokers []string `env:"KAFKA_BROKERS" envSeparator:","`
	KafkaTopic   string   `env:"KAFKA_TOPIC"`
	KafkaGroupID string   `env:"KAFKA_GROUP_ID"`

	// ClickHouse
	CHAddr     string `env:"CH_ADDR" envDefault:"localhost"`
	CHHTTPPort string `env:"CH_HTTP_PORT" envDefault:"8123"`

	// HTTP server
	HTTPPort string `env:"HTTP_PORT" envDefault:"8081"`

	// Batching
	BatchSize     int           `env:"BATCH_SIZE" envDefault:"200"`
	FlushInterval time.Duration `env:"FLUSH_INTERVAL" envDefault:"2s"`
}

func Load() (*Config, error) {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
