import json
import os
import time
import uuid
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
ORDERS_TOPIC = os.environ.get("ORDERS_TOPIC", "orders")
INTERVAL = int(os.environ.get("PRODUCER_INTERVAL", "2"))

def create_producer():
    print(f"[INFO] Trying to connect to Kafka: {KAFKA_BROKER}")

    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5
            )
            print("[INFO] Connected to Kafka successfully!")
            return producer
        except NoBrokersAvailable:
            print("[WARN] Kafka not available yet. Retrying in 2 seconds...")
            time.sleep(2)

def main():
    producer = create_producer()

    while True:
        order = {
            "order_id": str(uuid.uuid4()),
            "amount": 100,
            "status": "CREATED"
        }

        producer.send(ORDERS_TOPIC, order)
        producer.flush()

        print("[SENT]", order)

        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()

