import asyncio
import json
import os

import websockets
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC  = os.environ.get("KAFKA_TOPIC_RAW", "btc_trades_raw")
WS_URL = "wss://advanced-trade-ws.coinbase.com"

producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})


def _normalize(trade: dict) -> dict:
    return {
        "exchange":  "coinbase",
        "symbol":    trade["product_id"],
        "price":     float(trade["price"]),
        "quantity":  float(trade["size"]),
        "side":      trade["side"],
        "timestamp": trade["time"],
    }


async def _stream() -> None:
    print(f"Connecting to Coinbase Advanced Trade WebSocket...")
    async with websockets.connect(WS_URL) as ws:
        await ws.send(json.dumps({
            "type": "subscribe",
            "channel": "market_trades",
            "product_ids": ["BTC-USD"],
        }))
        print(f"Subscribed — streaming BTC-USD trades to '{TOPIC}' on {BOOTSTRAP_SERVERS}")
        async for raw in ws:
            msg = json.loads(raw)
            if msg.get("channel") != "market_trades":
                continue
            for event in msg.get("events", []):
                for trade in event.get("trades", []):
                    normalized = _normalize(trade)
                    producer.produce(TOPIC, json.dumps(normalized).encode())
                    producer.poll(0)
                    print(
                        f"  {normalized['timestamp'][11:19]}"
                        f"  ${normalized['price']:>12,.2f}"
                        f"  {normalized['side']:<4}"
                        f"  qty={normalized['quantity']}"
                    )


async def main() -> None:
    while True:
        try:
            await _stream()
        except Exception as e:
            print(f"Disconnected ({e}). Reconnecting in 3s...")
            await asyncio.sleep(3)


if __name__ == "__main__":
    asyncio.run(main())
