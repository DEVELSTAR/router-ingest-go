ATTACH TABLE _ UUID 'ee355161-0041-4f17-981f-b7638b6e53e4'
(
    `ts` DateTime,
    `device_id` String,
    `endpoint` String,
    `latency_ms` Int32,
    `loss_pct` Float32,
    `http_status` Int32,
    `isp` String,
    `location` String,
    `uplink` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (ts, device_id, endpoint)
TTL ts + toIntervalDay(7)
SETTINGS index_granularity = 8192
