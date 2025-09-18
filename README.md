# Router Ingestion Service (Go)

## Overview
This Go service collects metrics from routers (latency, packet loss, HTTP status) and publishes them to Kafka.  
It also enriches data with device metadata from Redis (ISP, location, uplink).

---

## Tech Stack
- Go
- Redis (for device metadata)
- Kafka (router-metrics topic)

---

## Prerequisites
- Go >= 1.21
- Redis
- Kafka
- `.env` file with:

```env
PORT=8080
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=router-metrics
REDIS_ADDR=localhost:6379
