ATTACH MATERIALIZED VIEW _ UUID '1a4abea3-6e4e-44d7-b259-9f60357c0595' TO default.router_metrics_raw
(
    `device_id` String,
    `endpoint` String,
    `latency_ms` Int32,
    `loss_pct` Float32,
    `http_status` Int32,
    `ts` DateTime,
    `isp` String,
    `location` String,
    `uplink` String
)
AS SELECT *
FROM default.router_metrics_kafka
