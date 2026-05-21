# btc-streaming-pipeline

A real-time Bitcoin trade pipeline built with Python, Kafka, Apache Flink, and a live web dashboard. Streams real trades from the Coinbase WebSocket feed, aggregates them with Flink, and visualises everything in a browser.

## Architecture

```
Coinbase WebSocket (live BTC-USD trades)
  │
  ▼
producer_ws.py          ← Python WebSocket client
  │
  ▼
Kafka topic: btc_trades_raw    ← raw trade events (KRaft, no Zookeeper)
  │
  ▼
btc_aggregator.py       ← PyFlink job: 10-second processing-time tumbling window
  │                        computes trade count, avg price, total volume
  ▼
Kafka topic: btc_alerts        ← one summary message per 10s window
  │
  ├──▶ alert_consumer.py       ← terminal output
  └──▶ dashboard/app.py        ← FastAPI + SSE → browser dashboard
```

## Stack

| Layer | Technology |
|---|---|
| Message broker | Apache Kafka 7.6 (KRaft mode — no Zookeeper) |
| Stream processing | Apache Flink 1.17 via PyFlink |
| Live data source | Coinbase Advanced Trade WebSocket API |
| Dashboard backend | FastAPI + Server-Sent Events |
| Dashboard frontend | Vanilla JS + Chart.js |
| Local infra | Docker Compose |

## Quickstart

### 1. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 2. Start Kafka and the Flink job

```bash
docker compose up -d --build
```

> **First run only:** the Flink image installs `apache-flink` (~400 MB) and takes ~5 minutes to build. Subsequent starts are instant.

Kafka and the Flink job are ready when:

```bash
docker compose logs flink-job | grep "Flink job started"
```

### 3. Start the dashboard (Terminal 1)

```bash
python dashboard/app.py
```

In GitHub Codespaces, a prompt will appear to open port 8000 in your browser. Click it.

### 4. Start the live producer (Terminal 2)

```bash
python producer/producer_ws.py
```

The dashboard will immediately start populating with real BTC-USD trades from Coinbase. Every 10 seconds a Flink window summary card appears on the right.

### 5. Stop everything

```bash
# Ctrl+C in both terminals, then:
docker compose down
```

## Dashboard

The browser dashboard at `http://localhost:8000` shows two panels updated in real time:

- **Left** — rolling 60-point BTC price chart + live trade feed with BUY/SELL direction indicators
- **Right** — Flink 10-second window cards (avg price, trade count, total volume), colour-coded green/red based on price direction vs the previous window

## Producers

Two producers are available. The pipeline is identical regardless of which you use.

| Command | Source |
|---|---|
| `python producer/producer_ws.py` | Live Coinbase WebSocket feed (real prices) |
| `python producer/producer.py` | Simulated trades (offline / testing) |

The fake producer accepts `--count N` to send exactly N trades and stop.

## Flink job

`flink/jobs/btc_aggregator.py` uses the PyFlink Table API with Flink SQL. It runs as a containerised mini-cluster (no separate JobManager needed).

**What it does:**
- Reads JSON trade events from `btc_trades_raw`
- Groups them into 10-second **processing-time tumbling windows**
- Computes: window start/end, trade count, average price, total volume
- Writes one JSON summary per window to `btc_alerts`

**Processing time vs event time:** the job currently uses processing time (wall clock) for simplicity. Switching to event time would use the `timestamp` field from each trade and require watermark configuration — appropriate if trades could arrive out of order or with significant latency.

## Debugging

Watch raw trades in Kafka, bypassing Flink:

```bash
python consumer/consumer.py
```

Tail the Flink job container logs:

```bash
docker compose logs flink-job -f
```

## Environment variables

| Variable | Default | Used by |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Python scripts on host |
| `KAFKA_BOOTSTRAP_SERVERS_INTERNAL` | `kafka:29092` | Flink container (Docker network) |
| `KAFKA_TOPIC_RAW` | `btc_trades_raw` | Producer → Flink source |
| `KAFKA_TOPIC_ALERTS` | `btc_alerts` | Flink sink → consumers |

## Project structure

```
.
├── docker-compose.yml              # Kafka (KRaft) + topic setup + Flink job
├── producer/
│   ├── producer_ws.py              # Live Coinbase WebSocket producer
│   └── producer.py                 # Fake trade generator (offline testing)
├── consumer/
│   ├── alert_consumer.py           # Prints Flink window summaries to terminal
│   └── consumer.py                 # Debug: prints raw trades from Kafka
├── flink/
│   ├── Dockerfile                  # python:3.10-bullseye + Java 11 + apache-flink + Kafka connector
│   └── jobs/
│       └── btc_aggregator.py       # PyFlink job: tumbling window aggregation
├── dashboard/
│   ├── app.py                      # FastAPI server with SSE fan-out
│   └── index.html                  # Single-page dashboard (Chart.js, vanilla JS)
├── requirements.txt
└── .env.example
```
