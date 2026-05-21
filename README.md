# btc-streaming-pipeline

Real-time Bitcoin trade event pipeline built with Python, Kafka, and Apache Flink.

## Architecture

```
Fake BTC trades (Python producer)
  → Kafka topic: btc_trades_raw
    → Flink job (10-second tumbling window → avg price, volume)
      → Kafka topic: btc_alerts
        → Alert consumer (Python, prints window summaries)
```

## Quickstart

### 1. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 2. Build and start Kafka + Flink

```bash
docker compose up -d --build
```

> **First run only:** building the Flink image downloads `apache-flink` (~400 MB) and takes ~5 minutes. Subsequent starts are instant.

Wait ~30 seconds for Kafka to be ready, then wait another ~30 seconds for the Flink job to connect and start processing.

### 3. Run the alert consumer (Terminal 1)

```bash
python consumer/alert_consumer.py
```

### 4. Run the producer (Terminal 2)

```bash
python producer/producer.py --count 60
```

Every 10 seconds you'll see a window summary in the alert consumer:

```
[WINDOW] 2026-05-21 11:00:00.000 → 2026-05-21 11:00:10.000 | 10 trades | avg $65,234.56 | vol 2.543210 BTC
```

### 5. Stop everything

```bash
docker compose down
```

## Debugging

Watch raw trades flowing into Kafka (bypasses Flink):

```bash
python consumer/consumer.py
```

Check Flink job logs:

```bash
docker compose logs flink-job -f
```

## Environment variables

| Variable | Default | Used by |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Python scripts (host) |
| `KAFKA_BOOTSTRAP_SERVERS_INTERNAL` | `kafka:29092` | Flink container |
| `KAFKA_TOPIC_RAW` | `btc_trades_raw` | Producer, Flink source |
| `KAFKA_TOPIC_ALERTS` | `btc_alerts` | Flink sink, alert consumer |

## Project structure

```
.
├── docker-compose.yml          # Kafka (KRaft) + Flink job
├── producer/
│   └── producer.py             # Publishes fake BTC trades (--count N to limit)
├── consumer/
│   ├── consumer.py             # Debug: reads raw trades from Kafka
│   └── alert_consumer.py       # Reads 10-second window summaries from Flink
├── flink/
│   ├── Dockerfile              # Python + Java + apache-flink + Kafka connector
│   └── jobs/
│       └── btc_aggregator.py   # PyFlink job: tumbling window aggregation
├── requirements.txt
└── .env.example
```
