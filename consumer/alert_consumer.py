import json
import os

from confluent_kafka import Consumer, KafkaError, KafkaException

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC_ALERTS", "btc_alerts")

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "btc-alert-printer",
    "auto.offset.reset": "latest",
})
consumer.subscribe([TOPIC])

if __name__ == "__main__":
    print(f"Alert consumer started — reading from '{TOPIC}' on {BOOTSTRAP_SERVERS}")
    print("Waiting for 10-second window summaries from Flink...\n")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    continue
                raise KafkaException(msg.error())
            try:
                alert = json.loads(msg.value().decode("utf-8"))
                print(
                    f"[WINDOW] {alert['window_start']} → {alert['window_end']} | "
                    f"{alert['trade_count']} trades | "
                    f"avg ${alert['avg_price']:,.2f} | "
                    f"vol {alert['total_volume']} BTC"
                )
            except (KeyError, ValueError):
                print(f"Skipping malformed message: {msg.value()}")
    finally:
        consumer.close()
