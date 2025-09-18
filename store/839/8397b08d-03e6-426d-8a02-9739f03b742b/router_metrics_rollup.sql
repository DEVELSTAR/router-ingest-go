ATTACH TABLE _ UUID '108382eb-1d8f-4e8c-846a-636755f8f4af'
(
    `ts_min` DateTime,
    `device_id` String,
    `endpoint` String,
    `isp` String,
    `location` String,
    `avg_latency` Float32,
    `max_latency` Float32,
    `min_latency` Float32,
    `packet_loss_count` UInt32,
    `http_failures` UInt32,
    `samples` UInt32
)
ENGINE = SummingMergeTree
PARTITION BY toDate(ts_min)
ORDER BY (ts_min, device_id, endpoint, isp, location)
TTL ts_min + toIntervalDay(365)
SETTINGS index_granularity = 8192
