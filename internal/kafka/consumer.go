package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"router-ingest-go/config"
	"router-ingest-go/internal/model"
)

type Consumer struct {
	reader      *kafka.Reader
	sqlEndpoint string
	client      *http.Client
	batchSize   int
	flushIntvl  time.Duration
	buffer      []string
	mu          sync.Mutex
}

func NewConsumer(cfg *config.Config) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.KafkaBrokers,
		Topic:   cfg.KafkaTopic,
		GroupID: cfg.KafkaGroupID,
	})

	sqlEndpoint := fmt.Sprintf("http://%s:%s/", cfg.CHAddr, cfg.CHHTTPPort)

	return &Consumer{
		reader:      r,
		sqlEndpoint: sqlEndpoint,
		client:      &http.Client{Timeout: 5 * time.Second},
		batchSize:   cfg.BatchSize,
		flushIntvl:  cfg.FlushInterval,
		buffer:      make([]string, 0, cfg.BatchSize),
	}
}

func (c *Consumer) Start() {
	// background ticker to flush every N seconds
	ticker := time.NewTicker(c.flushIntvl)
	defer ticker.Stop()

	go func() {
		for range ticker.C {
			c.flush()
		}
	}()

	for {
		m, err := c.reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("❌ Kafka read error:", err)
			time.Sleep(time.Second)
			continue
		}

		var metric model.Metric
		if err := json.Unmarshal(m.Value, &metric); err != nil {
			log.Println("❌ JSON unmarshal error:", err)
			continue
		}

		t, err := time.Parse(time.RFC3339, metric.Timestamp)
		if err != nil {
			t, err = time.Parse("2006-01-02 15:04:05", metric.Timestamp)
			if err != nil {
				log.Println("❌ Invalid timestamp:", metric.Timestamp, err)
				continue
			}
		}
		ts := t.Format("2006-01-02 15:04:05")

		row := fmt.Sprintf("('%s','%s','%s',%d,%.2f,%d,'%s','%s','%s')",
			ts, metric.DeviceID, metric.Endpoint, metric.LatencyMs,
			metric.LossPct, metric.HttpStatus, metric.ISP, metric.Location, metric.Uplink,
		)

		// add row to buffer
		c.mu.Lock()
		c.buffer = append(c.buffer, row)
		// if buffer is full, flush immediately
		if len(c.buffer) >= c.batchSize {
			go c.flush()
		}
		c.mu.Unlock()
	}
}

func (c *Consumer) flush() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.buffer) == 0 {
		return
	}

	sql := fmt.Sprintf(`
		INSERT INTO router_metrics_raw 
		(ts, device_id, endpoint, latency_ms, loss_pct, http_status, isp, location, uplink) 
		VALUES %s`, strings.Join(c.buffer, ","))

	resp, err := c.client.Post(c.sqlEndpoint, "text/plain", bytes.NewBuffer([]byte(sql)))
	if err != nil {
		log.Println("❌ ClickHouse batch insert error:", err)
		c.buffer = nil
		return
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != 200 {
		log.Printf("❌ ClickHouse batch insert failed (%d): %s", resp.StatusCode, string(body))
	} else {
		log.Printf("✅ Flushed %d rows to ClickHouse", len(c.buffer))
	}

	// reset buffer
	c.buffer = make([]string, 0, c.batchSize)
}

func (c *Consumer) Close() {
	c.flush() // final flush before shutdown
	if err := c.reader.Close(); err != nil {
		log.Println("⚠️ Kafka consumer close error:", err)
	} else {
		log.Println("✅ Kafka consumer closed")
	}
}
