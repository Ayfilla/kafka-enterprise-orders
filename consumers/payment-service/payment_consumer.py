import json
import os
from kafka import KafkaConsumer, KafkaProducer


def env(k, d):
    return os.environ.get(k, d)


KAFKA_BROKER = env("KAFKA_BROKER", "kafka:9092")

# читаем "чистые" заказы из fraud-service
CLEAN_TOPIC = "orders-clean"

# куда отправлять событие после успешного платежа
PAYMENTS_TOPIC = env("PAYMENTS_TOPIC", "payments")


def create_consumer():
    return KafkaConsumer(
        CLEAN_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id="payment-service-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: int(k.decode()) if k else None,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
    )


def process_payment(order):
    # логика платежа — имитация
    return {
        "order_id": order["order_id"],
        "customer_id": order["customer_id"],
        "amount": order["amount"],
        "currency": order["currency"],
        "status": "PAID"
    }


def main():
    consumer = create_consumer()
    producer = create_producer()

    print("[payment-service] Started. Waiting for approved orders...")

    for msg in consumer:
        order = msg.value
        order_id = order["order_id"]

        print(f"[payment-service] Processing payment for order {order_id}...")

        payment_event = process_payment(order)

        producer.send(PAYMENTS_TOPIC, key=order_id, value=payment_event)
        producer.flush()

        print(f"[payment-service] PAYMENT SUCCESS → {payment_event}")


if __name__ == "__main__":
    main()

