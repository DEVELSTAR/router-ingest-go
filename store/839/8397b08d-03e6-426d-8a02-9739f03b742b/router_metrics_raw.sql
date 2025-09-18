ATTACH TABLE _ UUID '2d6adb71-750d-4f9c-b4f2-fb4f9a8dff35'
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
PARTITION BY toDate(ts)
ORDER BY (ts, device_id)
TTL ts + toIntervalDay(7)
SETTINGS index_granularity = 8192
