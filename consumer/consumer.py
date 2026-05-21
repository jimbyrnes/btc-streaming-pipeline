import json
import os

from confluent_kafka import Consumer, KafkaError, KafkaException

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC_RAW", "btc_trades_raw")

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "btc-alert-consumer",
    "auto.offset.reset": "earliest",
})
consumer.subscribe([TOPIC])

if __name__ == "__main__":
    print(f"Consumer started — reading from topic '{TOPIC}' on {BOOTSTRAP_SERVERS}")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                # UNKNOWN_TOPIC_OR_PART is transient — topic may not exist yet
                if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    continue
                raise KafkaException(msg.error())
            try:
                trade = json.loads(msg.value().decode("utf-8"))
                print(
                    f"[{trade['timestamp']}] {trade['symbol']} "
                    f"price=${trade['price']:,.2f}  qty={trade['quantity']}"
                )
            except (KeyError, ValueError):
                print(f"Skipping malformed message: {msg.value()}")
    finally:
        consumer.close()
