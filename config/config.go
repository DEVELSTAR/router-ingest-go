package config

import (
	"github.com/caarlos0/env/v9"
)

type Config struct {
	Port         string `env:"PORT" envDefault:"8080"`
	KafkaBrokers string `env:"KAFKA_BROKERS" envDefault:"localhost:9092"`
	KafkaTopic   string `env:"KAFKA_TOPIC" envDefault:"router-metrics"`
	RedisAddr    string `env:"REDIS_ADDR" envDefault:"localhost:6379"`
}

func Load() (*Config, error) {
	cfg := &Config{}
	err := env.Parse(cfg)
	return cfg, err
}
