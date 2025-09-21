// router-ingest-go/internal/kafka/consumer.go
package kafka

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	ch "github.com/ClickHouse/clickhouse-go/v2"
	"router-ingest-go/config"
	"router-ingest-go/internal/model"
)

type Consumer struct {
	reader     *kafka.Reader
	batchSize  int
	flushIntvl time.Duration
	buffer     []model.Metric
	mu         sync.Mutex
	chConn     ch.Conn
	closed     bool
}

func NewConsumer(cfg *config.Config, chConn ch.Conn) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.KafkaBrokers,
		Topic:   cfg.KafkaTopic,
		GroupID: cfg.KafkaGroupID,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	return &Consumer{
		reader:     r,
		batchSize:  cfg.BatchSize,
		flushIntvl: cfg.FlushInterval,
		buffer:     make([]model.Metric, 0, cfg.BatchSize),
		chConn:     chConn,
	}
}

func (c *Consumer) Start(ctx context.Context) {
	ticker := time.NewTicker(c.flushIntvl)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("consumer: context canceled, flushing buffer and exiting")
			c.flushBuffer(ctx)
			return

		case <-ticker.C:
			c.flushBuffer(ctx)

		default:
			// Read message with a timeout context to respect shutdown
			m, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					log.Println("consumer: shutting down reader")
					return
				}
				// Non-blocking read failed, sleep to avoid tight loop
				time.Sleep(100 * time.Millisecond)
				continue
			}

			var metric model.Metric
			if err := json.Unmarshal(m.Value, &metric); err != nil {
				log.Println("❌ JSON unmarshal error:", err)
				_ = c.reader.CommitMessages(ctx, m)
				continue
			}

			c.mu.Lock()
			c.buffer = append(c.buffer, metric)
			shouldFlush := len(c.buffer) >= c.batchSize
			c.mu.Unlock()

			if shouldFlush {
				c.flushBuffer(ctx)
			}

			// Commit Kafka offset after processing
			if err := c.reader.CommitMessages(ctx, m); err != nil {
				log.Println("⚠️ Kafka commit error:", err)
			}
		}
	}
}

// flushBuffer becomes context aware
func (c *Consumer) flushBuffer(ctx context.Context) {
	c.mu.Lock()
	if len(c.buffer) == 0 {
		c.mu.Unlock()
		return
	}
	batch := make([]model.Metric, len(c.buffer))
	copy(batch, c.buffer)
	c.buffer = c.buffer[:0]
	c.mu.Unlock()

	insertSQL := `INSERT INTO router_metrics_raw (ts, device_id, endpoint, latency_ms, loss_pct, http_status, isp, location, uplink) VALUES`
	batchWriter, err := c.chConn.PrepareBatch(ctx, insertSQL)
	if err != nil {
		log.Println("❌ PrepareBatch error:", err)
		return
	}

	sent := 0
	for _, m := range batch {
		t, err := time.Parse(time.RFC3339, m.Timestamp)
		if err != nil {
			log.Println("⚠️ invalid timestamp:", m.Timestamp, "using now")
			t = time.Now().UTC()
		}

		if err := batchWriter.Append(
			t, m.DeviceID, m.Endpoint, m.LatencyMs, m.LossPct,
			m.HttpStatus, m.ISP, m.Location, m.Uplink,
		); err != nil {
			log.Println("⚠️ Append error:", err)
			continue
		}
		sent++
	}

	if err := batchWriter.Send(); err != nil {
		log.Println("❌ ClickHouse batch insert error:", err)
	} else {
		log.Printf("✅ Flushed %d rows to ClickHouse", sent)
	}
}

func (c *Consumer) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c.flushBuffer(ctx)

	if err := c.reader.Close(); err != nil {
		log.Println("⚠️ Kafka consumer close error:", err)
	} else {
		log.Println("✅ Kafka consumer closed")
	}
}

