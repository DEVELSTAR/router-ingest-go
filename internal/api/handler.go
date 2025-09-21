// router-ingest-go/internal/api/handler.go

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"router-ingest-go/internal/kafka"
	"router-ingest-go/internal/model"
	redisClient "router-ingest-go/internal/redis"
)

type Handler struct {
	Kafka *kafka.Producer
	Redis *redisClient.Client
}

func (h *Handler) Ingest(w http.ResponseWriter, r *http.Request) {
	var metric model.Metric
	if err := json.NewDecoder(r.Body).Decode(&metric); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}

	// Enrich with Redis metadata (with timeout)
	ctx, cancel := context.WithTimeout(r.Context(), 1*time.Second)
	defer cancel()
	meta, _ := h.Redis.GetMetadata(ctx, metric.DeviceID)

	if isp, ok := meta["isp"]; ok {
		metric.ISP = isp
	}
	if loc, ok := meta["location"]; ok {
		metric.Location = loc
	}
	if uplink, ok := meta["uplink"]; ok {
		metric.Uplink = uplink
	}

	// Publish to Kafka with timeout
	ctxKafka, cancelKafka := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancelKafka()
	err := h.Kafka.Publish(ctxKafka, metric)
	if err != nil {
		http.Error(w, "failed to publish", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}
