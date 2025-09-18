ATTACH MATERIALIZED VIEW _ UUID 'b1c23397-618b-4581-9ef4-0ca0b2015409' TO default.router_metrics_rollup
(
    `ts_min` DateTime,
    `device_id` String,
    `endpoint` String,
    `isp` String,
    `location` String,
    `avg_latency` Float64,
    `max_latency` Int32,
    `min_latency` Int32,
    `packet_loss_count` UInt64,
    `http_failures` UInt64,
    `samples` UInt64
)
AS SELECT
    toStartOfMinute(ts) AS ts_min,
    device_id,
    endpoint,
    isp,
    location,
    avg(latency_ms) AS avg_latency,
    max(latency_ms) AS max_latency,
    min(latency_ms) AS min_latency,
    countIf(loss_pct > 0) AS packet_loss_count,
    countIf(http_status >= 500) AS http_failures,
    count() AS samples
FROM default.router_metrics_raw
GROUP BY
    ts_min,
    device_id,
    endpoint,
    isp,
    location
