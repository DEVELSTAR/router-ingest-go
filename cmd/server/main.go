package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/segmentio/kafka-go"
)

type Metric struct {
	DeviceID   string  `json:"device_id"`
	Endpoint   string  `json:"endpoint"`
	LatencyMs  int     `json:"latency_ms"`
	LossPct    float32 `json:"loss_pct"`
	HttpStatus int     `json:"http_status"`
	Timestamp  string  `json:"ts"`
	ISP        string  `json:"isp"`
	Location   string  `json:"location"`
	Uplink     string  `json:"uplink"`
}

func main() {
	// Kafka writer (reuse)
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "router-metrics",
	})
	defer writer.Close()

	// Kafka consumer
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "router-metrics",
		GroupID: "ch-local-consumer",
	})

	go func() {
		for {
			m, err := r.ReadMessage(context.Background())
			if err != nil {
				log.Println("Error reading message:", err)
				time.Sleep(time.Second)
				continue
			}

			var metric Metric
			if err := json.Unmarshal(m.Value, &metric); err != nil {
				log.Println("JSON unmarshal error:", err)
				continue
			}

			t, err := time.Parse(time.RFC3339, metric.Timestamp)
			if err != nil {
				// fallback for ClickHouse-style timestamp with space
				t, err = time.Parse("2006-01-02 15:04:05", metric.Timestamp)
				if err != nil {
					log.Println("Invalid timestamp format:", metric.Timestamp, err)
					continue
				}
			}
			ts := t.Format("2006-01-02 15:04:05")


			sql := fmt.Sprintf(`INSERT INTO router_metrics_raw 
	(ts, device_id, endpoint, latency_ms, loss_pct, http_status, isp, location, uplink) 
	VALUES ('%s','%s','%s',%d,%.2f,%d,'%s','%s','%s')`,
				ts, metric.DeviceID, metric.Endpoint, metric.LatencyMs,
				metric.LossPct, metric.HttpStatus, metric.ISP, metric.Location, metric.Uplink,
			)

			client := http.Client{Timeout: 5 * time.Second}
			resp, err := client.Post("http://localhost:8123/", "text/plain", bytes.NewBuffer([]byte(sql)))
			if err != nil {
				log.Println("ClickHouse insert error:", err)
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			log.Println("ClickHouse response:", string(body))
		}
	}()

	// HTTP server
	http.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		var m Metric
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		value, _ := json.Marshal(m)
		err := writer.WriteMessages(context.Background(), kafka.Message{Value: value})
		if err != nil {
			log.Println("Kafka produce error:", err)
			http.Error(w, "failed to write to Kafka", http.StatusInternalServerError)
			return
		}
		log.Println("Produced metric to Kafka:", m.DeviceID, m.Endpoint)
		w.WriteHeader(http.StatusOK)
	})

	port := "8081"
	log.Println("HTTP server running on port", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
