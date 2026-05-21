import json
import os
import queue
import threading
from contextlib import asynccontextmanager
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, KafkaException
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RAW    = os.environ.get("KAFKA_TOPIC_RAW",    "btc_trades_raw")
TOPIC_ALERTS = os.environ.get("KAFKA_TOPIC_ALERTS", "btc_alerts")

_lock = threading.Lock()
_subs: dict[str, list[queue.Queue]] = {"trades": [], "alerts": []}


def _consume(topic: str, channel: str) -> None:
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": f"dashboard-{channel}",
        "auto.offset.reset": "latest",
    })
    c.subscribe([topic])
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                raise KafkaException(msg.error())
            continue
        data = json.loads(msg.value())
        with _lock:
            dead = [q for q in _subs[channel] if not _try_put(q, data)]
            for q in dead:
                _subs[channel].remove(q)


def _try_put(q: queue.Queue, data: dict) -> bool:
    try:
        q.put_nowait(data)
        return True
    except queue.Full:
        return False


@asynccontextmanager
async def lifespan(app: FastAPI):
    for topic, ch in [(TOPIC_RAW, "trades"), (TOPIC_ALERTS, "alerts")]:
        threading.Thread(target=_consume, args=(topic, ch), daemon=True).start()
    yield


app = FastAPI(lifespan=lifespan)


def _sse(channel: str):
    q: queue.Queue = queue.Queue(maxsize=100)
    with _lock:
        _subs[channel].append(q)
    try:
        while True:
            try:
                yield f"data: {json.dumps(q.get(timeout=20))}\n\n"
            except queue.Empty:
                yield ": keepalive\n\n"
    finally:
        with _lock:
            try:
                _subs[channel].remove(q)
            except ValueError:
                pass


@app.get("/stream/trades")
def stream_trades():
    return StreamingResponse(
        _sse("trades"),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/stream/alerts")
def stream_alerts():
    return StreamingResponse(
        _sse("alerts"),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/")
def index():
    return HTMLResponse((Path(__file__).parent / "index.html").read_text())


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
