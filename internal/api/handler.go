package api

import (
	"context"
	"encoding/json"
	"net/http"
	"router-ingest-go/internal/kafka"
	"router-ingest-go/internal/models"
	redisClient "router-ingest-go/internal/redis"
)

type Handler struct {
	Kafka *kafka.Producer
	Redis *redisClient.Client
}

func (h *Handler) Ingest(w http.ResponseWriter, r *http.Request) {
	var metric models.Metric
	if err := json.NewDecoder(r.Body).Decode(&metric); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}

	// enrich with Redis metadata
	meta, _ := h.Redis.GetMetadata(metric.DeviceID)
	if isp, ok := meta["isp"]; ok {
		metric.ISP = isp
	}
	if loc, ok := meta["location"]; ok {
		metric.Location = loc
	}
	if uplink, ok := meta["uplink"]; ok {
		metric.Uplink = uplink
	}

	data, _ := json.Marshal(metric)
	err := h.Kafka.Publish(context.Background(), []byte(metric.DeviceID), data)
	if err != nil {
		http.Error(w, "failed to publish", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}
