import json
import os
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092")
ORDERS_TOPIC = os.environ.get("ORDERS_TOPIC", "orders")
ALERT_TOPIC = os.environ.get("ALERT_TOPIC", "fraud-alerts")


def create_consumer():
    return KafkaConsumer(
        ORDERS_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="fraud-service-group"
    )


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )


def is_fraud(order):
    """
    Простая логика:
    если сумма заказа > 500 → подозрительный
    """
    amount = order.get("amount", 0)
    return amount > 500


def main():
    print("🔍 Fraud service started...")

    consumer = create_consumer()
    producer = create_producer()

    for msg in consumer:
        order = msg.value
        order_id = order.get("order_id")

        print(f"Получен заказ {order_id}: сумма={order.get('amount')}")

        if is_fraud(order):
            print(f"🚨 FRAUD DETECTED! Order {order_id}")

            producer.send(ALERT_TOPIC, {
                "order_id": order_id,
                "amount": order.get("amount"),
                "flag": "fraud_detected"
            })

            producer.flush()


if __name__ == "__main__":
    main()

