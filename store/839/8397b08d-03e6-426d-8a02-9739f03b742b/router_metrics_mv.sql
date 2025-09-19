ATTACH MATERIALIZED VIEW _ UUID '3256675c-fb99-4c62-bee5-016ce9557d88' TO default.router_metrics_rollup
(
    `ts_min` DateTime,
    `endpoint` String,
    `location` String,
    `isp` String,
    `sum_latency` Int64,
    `sum_loss` Float64,
    `samples` UInt64
)
AS SELECT
    toStartOfMinute(ts) AS ts_min,
    endpoint,
    location,
    isp,
    sum(latency_ms) AS sum_latency,
    sum(loss_pct) AS sum_loss,
    count() AS samples
FROM default.router_metrics_raw
GROUP BY
    ts_min,
    endpoint,
    location,
    isp
