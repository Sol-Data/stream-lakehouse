import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

BROKER = os.getenv("BROKER", "redpanda:9092")
TOPIC = os.getenv("TOPIC", "energy_ticks")

ZONES = ["DE-LU", "FR", "NL", "AT"]
random.seed(42)

def iso_now():
    return datetime.now(timezone.utc).isoformat()

def build_tick():
    zone = random.choice(ZONES)
    price = round(random.uniform(20, 120), 2)   # €/MWh
    demand = random.randint(1000, 10000)        # MW
    return {
        "market_ts": iso_now(),
        "zone": zone,
        "price_eur_mwh": price,
        "demand_mw": demand,
        "source": "sim"
    }

if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
        acks="all",                 # verify that the broker has received the request
        retries=5,                  # Retry due to temporary failure
        request_timeout_ms=30000,   # Request timeout
        api_version_auto_timeout_ms=10000
    )

    print(f"[producer] broker={BROKER}, topic={TOPIC}")
    try:
        while True:
            tick = build_tick()

            future = producer.send(TOPIC, tick)
            try:
                metadata = future.get(timeout=10)   # Success or failure determined here
                print("f-> sent ok: topic={metadata.topic} partition={metadata.partition} offset={metadata.offset} | {tick}")
            except Exception as e:
                print("f[ERROR] send failed: {e} | {tick}")

            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n[producer] stopped")
    finally:
        producer.flush()
        producer.close()
                 