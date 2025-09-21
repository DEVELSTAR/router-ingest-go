
// router-ingest-go/internal/httpserver/server.go
package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"router-ingest-go/internal/kafka"
	"router-ingest-go/internal/model"
	redisClient "router-ingest-go/internal/redis"
)

var ErrServerClosed = errors.New("server closed")

type Server struct {
	httpServer *http.Server
	producer   *kafka.Producer
	redis      *redisClient.Client
}

func New(port string, producer *kafka.Producer, redis *redisClient.Client) *Server {
	mux := http.NewServeMux()

	s := &Server{
		httpServer: &http.Server{
			Addr:    ":" + port,
			Handler: nil, // set below
		},
			producer: producer,
			redis:    redis,
	}

	mux.HandleFunc("/ingest", s.handleIngest)
	mux.HandleFunc("/health", s.handleHealth)

	// You can add middleware chain here if needed
	s.httpServer.Handler = mux
	return s
}

func (s *Server) ListenAndServe() error {
	return s.httpServer.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var m model.Metric
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		log.Println("invalid JSON payload:", err)
		http.Error(w, "invalid JSON payload", http.StatusBadRequest)
		return
	}

	// Basic validation
	if m.DeviceID == "" || m.Endpoint == "" || m.Timestamp == "" {
		http.Error(w, "missing required fields: device_id, endpoint, ts", http.StatusBadRequest)
		return
	}

	// Enrich from redis if available
	meta, _ := s.redis.GetMetadata(r.Context(), m.DeviceID)
	if isp, ok := meta["isp"]; ok && m.ISP == "" {
		m.ISP = isp
	}
	if loc, ok := meta["location"]; ok && m.Location == "" {
		m.Location = loc
	}
	if up, ok := meta["uplink"]; ok && m.Uplink == "" {
		m.Uplink = up
	}

	// defaults
	if m.ISP == "" {
		m.ISP = "unknown"
	}
	if m.Location == "" {
		m.Location = "unknown"
	}
	if m.Uplink == "" {
		m.Uplink = "unknown"
	}

	// Validate timestamp quickly
	if _, err := time.Parse(time.RFC3339, m.Timestamp); err != nil {
		http.Error(w, "invalid timestamp format, must be RFC3339", http.StatusBadRequest)
		return
	}

	// Produce to Kafka
	if err := s.producer.Publish(r.Context(), m); err != nil {
		log.Println("failed to produce metric:", err)
		http.Error(w, "failed to produce metric", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	_, _ = fmt.Fprint(w, "accepted")
}