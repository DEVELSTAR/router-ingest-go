// router-ingest-go/config/config.go/

package config

import (
	"log"
	"time"

	"github.com/caarlos0/env/v9"
	"github.com/joho/godotenv"
)

type Config struct {
	// Kafka
	KafkaBrokers []string `env:"KAFKA_BROKERS,required" envSeparator:","`
	KafkaTopic   string   `env:"KAFKA_TOPIC,required"`
	KafkaGroupID string   `env:"KAFKA_GROUP_ID,required"`

	// ClickHouse
	CHAddr     string `env:"CH_ADDR,required"`
	CHPort     string `env:"CH_PORT,required"`
	CHHTTPPort string `env:"CH_HTTP_PORT,required"`

	// Redis
	RedisAddr string `env:"REDIS_ADDR,required"`

	// HTTP server
	HTTPPort string `env:"HTTP_PORT,required"`

	// Batching
	BatchSize     int           `env:"BATCH_SIZE" envDefault:"200"`
	FlushInterval time.Duration `env:"FLUSH_INTERVAL" envDefault:"2s"`
}

func Load() (*Config, error) {
	// Auto-load .env in dev
	_ = godotenv.Load(".env")

	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		log.Println("❌ Config load error:", err)
		return nil, err
	}
	return cfg, nil
}
