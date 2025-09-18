ATTACH TABLE _ UUID 'b4cffc83-1e17-4dbc-8414-ca946444aad7'
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
ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:9092', kafka_topic_list = 'router-metrics', kafka_group_name = 'ch-consumer', kafka_format = 'JSONEachRow', kafka_num_consumers = 12, kafka_skip_broken_messages = 100
