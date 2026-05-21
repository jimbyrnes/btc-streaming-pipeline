import json
import os

from confluent_kafka import Consumer, KafkaError, KafkaException

BOOTSTRAP_SERVERS   = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC               = os.environ.get("KAFKA_TOPIC_ALERTS", "btc_alerts")
SPIKE_THRESHOLD_PCT = float(os.environ.get("SPIKE_THRESHOLD_PCT", "0.5"))

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "btc-alert-printer",
    "auto.offset.reset": "latest",
})
consumer.subscribe([TOPIC])

if __name__ == "__main__":
    print(f"Alert consumer started — reading from '{TOPIC}' on {BOOTSTRAP_SERVERS}")
    print(f"Spike threshold: >{SPIKE_THRESHOLD_PCT}% price change between windows\n")
    prev_price = None
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
                price = alert["avg_price"]

                change_pct = (
                    abs(price - prev_price) / prev_price * 100
                    if prev_price is not None else 0.0
                )
                is_spike = prev_price is not None and change_pct >= SPIKE_THRESHOLD_PCT
                prev_price = price

                tag = f"[SPIKE ⚡ {change_pct:+.2f}%]" if is_spike else "[WINDOW]     "
                print(
                    f"{tag} {alert['window_start'][11:19]} → {alert['window_end'][11:19]} | "
                    f"{alert['trade_count']} trades | "
                    f"avg ${price:,.2f} | "
                    f"vol {alert['total_volume']} BTC"
                )
            except (KeyError, ValueError):
                print(f"Skipping malformed message: {msg.value()}")
    finally:
        consumer.close()
