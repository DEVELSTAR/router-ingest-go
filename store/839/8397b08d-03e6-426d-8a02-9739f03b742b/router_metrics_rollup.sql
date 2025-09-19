ATTACH TABLE _ UUID 'ca8e2617-571a-4274-afa9-d03cfd184bdf'
(
    `ts_min` DateTime,
    `endpoint` String,
    `location` String,
    `isp` String,
    `sum_latency` Float32,
    `sum_loss` Float32,
    `samples` UInt32
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(ts_min)
ORDER BY (ts_min, endpoint, location, isp)
TTL ts_min + toIntervalDay(365)
SETTINGS index_granularity = 8192
