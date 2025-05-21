import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Producer
from websockets.sync.client import connect

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

KRAKEN_WS_URL = "wss://ws.kraken.com/v2"
TRADING_PAIRS = ["BTC/USD"]
TOPIC = "crypto-trades"

# ✅ Use local Redpanda broker (inside Docker)
producer = Producer({
    "bootstrap.servers": "redpanda-0:9092"
})

running = True

def delivery_report(err, msg):
    if err:
        logging.error(f"Delivery failed: {err}")
    else:
        logging.info(f"Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

def iso_to_unix_ms(timestamp_str):
    dt = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

def handle_shutdown(signum, frame):
    global running
    logging.info("Shutdown signal received. Exiting...")
    running = False

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

def stream_trades():
    subscription_msg = {
        "method": "subscribe",
        "params": {"channel": "trade", "symbol": TRADING_PAIRS},
    }

    while running:
        last_ping = time.time()
        PING_INTERVAL = 20

        try:
            with connect(KRAKEN_WS_URL) as websocket:
                websocket.send(json.dumps(subscription_msg))
                logging.info(f"Subscribed to trades: {TRADING_PAIRS}")

                while running:
                    if time.time() - last_ping > PING_INTERVAL:
                        websocket.send(json.dumps({"method": "ping"}))
                        last_ping = time.time()

                    try:
                        message = websocket.recv()
                        data = json.loads(message)

                        if data.get("channel") in ("heartbeat", "status"):
                            continue

                        for trade in data.get("data", []):
                            trade["timestamp_ms"] = iso_to_unix_ms(trade["timestamp"])
                            trade["timestamp"] = trade["timestamp"].replace("T", " ")
                            trade["symbol"] = trade["symbol"].replace("/", "_")

                            producer.produce(
                                topic=TOPIC,
                                key=trade["symbol"],
                                value=json.dumps(trade),
                                callback=delivery_report,
                            )

                        producer.poll(0)

                    except Exception as e:
                        logging.warning(f"Error inside message loop: {e}")
                        break

        except Exception as e:
            logging.error(f"WebSocket connection failed: {e}")

        logging.info("Retrying WebSocket connection in 5 seconds...")
        time.sleep(5)

    logging.info("Flushing producer...")
    producer.flush()

if __name__ == "__main__":
    stream_trades()
