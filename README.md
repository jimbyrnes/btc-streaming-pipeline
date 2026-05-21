# btc-streaming-pipeline

Real-time Bitcoin trade event pipeline built with Python, Kafka, and Docker.

## Architecture

```
Fake BTC trades (Python)
  → Kafka topic: btc_trades_raw
    → Alert consumer (Python, prints to stdout)
```

Future milestone will add Flink between Kafka topics for windowed aggregations and alerts.

## Quickstart

### 1. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 2. Start Kafka

```bash
docker compose up -d
```

Kafka takes ~10 seconds to be ready. You can verify with:

```bash
docker compose logs kafka | tail -20
```

### 3. Run the consumer (in one terminal)

```bash
python consumer/consumer.py
```

### 4. Run the producer (in a second terminal)

```bash
python producer/producer.py
```

You should see the consumer printing trade events roughly once per second.

### 5. Stop everything

```bash
# Ctrl+C in both terminal windows, then:
docker compose down
```

## Environment variables

Copy `.env.example` to `.env` and adjust if needed (defaults work out of the box):

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC_RAW` | `btc_trades_raw` | Topic for raw trade events |

To use a `.env` file, either `export` the vars manually or use a tool like `python-dotenv`.

## Project structure

```
.
├── docker-compose.yml      # Kafka + Zookeeper
├── producer/
│   └── producer.py         # Publishes fake BTC trades to Kafka
├── consumer/
│   └── consumer.py         # Reads trades from Kafka and prints them
├── requirements.txt
└── .env.example
```
