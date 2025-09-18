package models

import "time"

type Metric struct {
    DeviceID   string    `json:"device_id"`
    Endpoint   string    `json:"endpoint"`
    LatencyMs  int       `json:"latency_ms"`
    LossPct    float32   `json:"loss_pct"`
    HttpStatus int       `json:"http_status"`
    Timestamp  time.Time `json:"ts"`
    ISP        string    `json:"isp,omitempty"`
    Location   string    `json:"location,omitempty"`
    Uplink     string    `json:"uplink,omitempty"`
}
