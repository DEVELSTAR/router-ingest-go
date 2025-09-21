// router-ingest-go/internal/model/metric.go

package model

type Metric struct {
    DeviceID   string  `json:"device_id"`
    Endpoint   string  `json:"endpoint"`
    LatencyMs  int     `json:"latency_ms"`
    LossPct    float32 `json:"loss_pct"`
    HttpStatus int     `json:"http_status"`
    Timestamp  string  `json:"ts"` // RFC3339 string; parsed later in consumer
    ISP        string  `json:"isp"`
    Location   string  `json:"location"`
    Uplink     string  `json:"uplink"`
}