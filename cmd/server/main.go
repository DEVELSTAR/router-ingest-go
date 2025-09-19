package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"router-ingest-go/config"
	"router-ingest-go/internal/clickhouse"
	"router-ingest-go/internal/kafka"
	"router-ingest-go/internal/model"
)

func main() {
	// Load config
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("❌ Config load error: %v", err)
	}

	// Ensure Kafka brokers are not empty
	if len(cfg.KafkaBrokers) == 0 {
		log.Fatal("❌ KAFKA_BROKERS is empty. Set it in your environment or .env")
	}

	// Init ClickHouse tables
	clickhouse.Init(cfg)

	// Kafka producer & consumer
	producer := kafka.NewProducer(cfg)
	consumer := kafka.NewConsumer(cfg)

	// Start consumer in background
	go consumer.Start()

	// HTTP endpoints
	http.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		var m model.Metric
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := producer.Write(m); err != nil {
			http.Error(w, "failed to write to Kafka", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	server := &http.Server{Addr: ":" + cfg.HTTPPort}

	// Graceful shutdown
	go func() {
		log.Printf("🚀 HTTP server running on port %s", cfg.HTTPPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("❌ HTTP server error: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("⚙️ Shutting down...")

	producer.Close()
	consumer.Close()

	if err := server.Close(); err != nil {
		log.Println("⚠️ HTTP server close error:", err)
	} else {
		log.Println("✅ HTTP server stopped")
	}
}
