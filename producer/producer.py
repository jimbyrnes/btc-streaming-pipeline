import argparse
import json
import os
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC_RAW", "btc_trades_raw")

producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})


def fake_trade() -> dict:
    return {
        "exchange":  "coinbase-sim",
        "symbol":    "BTC-USD",
        "price":     round(random.uniform(60_000, 70_000), 2),
        "quantity":  round(random.uniform(0.001, 0.5), 6),
        "side":      random.choice(["BUY", "SELL"]),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=None, help="Number of trades to send (omit for infinite)")
    args = parser.parse_args()

    print(f"Producer started — sending to topic '{TOPIC}' on {BOOTSTRAP_SERVERS}")
    sent = 0
    while args.count is None or sent < args.count:
        trade = fake_trade()
        producer.produce(TOPIC, json.dumps(trade).encode("utf-8"))
        producer.poll(0)
        print(f"Sent: {trade}")
        sent += 1
        time.sleep(1)
    producer.flush()
    print(f"Done — sent {sent} trades.")
