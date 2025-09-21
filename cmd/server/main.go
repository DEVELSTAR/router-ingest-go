// router-ingest-go/cmd/server/main.go

package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"router-ingest-go/config"
	"router-ingest-go/internal/clickhouse"
	"router-ingest-go/internal/httpserver"
	"router-ingest-go/internal/kafka"
	"router-ingest-go/internal/redis"
)

func main() {
	// Load config
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("❌ Config load error: %v", err)
	}

	// ClickHouse init and connection
	ch, err := clickhouse.Init(cfg)
	if err != nil {
		log.Fatalf("❌ ClickHouse init error: %v", err)
	}
    defer ch.Close()

	// Redis client
	redisClient := redis.New(cfg.RedisAddr)

	// Kafka producer & consumer
	producer := kafka.NewProducer(cfg)
	consumer := kafka.NewConsumer(cfg, ch)

	// Start consumer with context
	ctx, cancel := context.WithCancel(context.Background())
	go consumer.Start(ctx)

	// HTTP server (inject producer and redis client)
	server := httpserver.New(cfg.HTTPPort, producer, redisClient)
	go func() {
		log.Printf("🚀 HTTP server running on port %s", cfg.HTTPPort)
		if err := server.ListenAndServe(); err != nil && err != httpserver.ErrServerClosed {
			log.Fatalf("⚠️ HTTP server stopped unexpectedly: %v", err)
		}
	}()

	// Wait for signal, then graceful shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("⚙️ Shutting down...")

	// stop consumer
	cancel()

	// give some time for shutdown flush
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// close producer/consumer and http server
	producer.Close()
	consumer.Close()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Println("⚠️ HTTP server graceful shutdown error:", err)
	} else {
		log.Println("✅ HTTP server stopped")
	}

	// wait a moment to ensure flush complete
	time.Sleep(500 * time.Millisecond)
}
