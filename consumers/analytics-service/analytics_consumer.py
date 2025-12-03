import json
import os
from collections import Counter

from kafka import KafkaConsumer
from couchbase.cluster import Cluster, ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterTimeoutOptions


def env(k, d):
    return os.environ.get(k, d)


KAFKA_BROKER = env("KAFKA_BROKER", "kafka:9092")
ORDERS_TOPIC = env("ORDERS_TOPIC", "orders")

PRINT_EVERY = int(env("ANALYTICS_PRINT_EVERY", "10"))

COUCHBASE_HOST = env("COUCHBASE_HOST", "couchbase")
COUCHBASE_BUCKET = env("COUCHBASE_BUCKET", "order_analytics")
COUCHBASE_USERNAME = env("COUCHBASE_USERNAME", "Administrator")
COUCHBASE_PASSWORD = env("COUCHBASE_PASSWORD", "password")


def create_consumer():
    return KafkaConsumer(
        ORDERS_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id="analytics-service-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: int(k.decode()) if k else None,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )


def connect_couchbase():
    try:
        cluster = Cluster(
            f"couchbase://{COUCHBASE_HOST}",
            ClusterOptions(
                PasswordAuthenticator(COUCHBASE_USERNAME, COUCHBASE_PASSWORD),
                timeout_options=ClusterTimeoutOptions(kv_timeout=5)
            )
        )
        bucket = cluster.bucket(COUCHBASE_BUCKET)
        collection = bucket.default_collection()
        print(f"[analytics-service] Connected to Couchbase bucket '{COUCHBASE_BUCKET}'")
        return collection
    except Exception as e:
        print(f"[analytics-service] Couchbase connection failed: {e}")
        return None


def main():
    consumer = create_consumer()
    collection = connect_couchbase()

    total_orders = 0
    total_amount = 0.0
    orders_by_country = Counter()

    for msg in consumer:
        order = msg.value
        order_id = order.get("order_id")
        country = order.get("country", "UNKNOWN")
        amount = float(order.get("amount", 0))

        total_orders += 1
        total_amount += amount
        orders_by_country[country] += 1

        print(f"[analytics-service] Received order: {order}")

        if collection:
            doc_id = f"order::{order_id}"
            try:
                collection.upsert(doc_id, order)
                print(f"[analytics-service] Stored in Couchbase → {doc_id}")
            except Exception as e:
                print(f"[analytics-service] Couchbase store failed: {e}")

        if total_orders % PRINT_EVERY == 0:
            avg_amount = total_amount / total_orders if total_orders else 0

            print("\n===== ANALYTICS REPORT =====")
            print(f"Total orders: {total_orders}")
            print(f"Total amount: {total_amount:.2f}")
            print(f"Average amount: {avg_amount:.2f}")
            print("Orders by country:")
            for c, cnt in orders_by_country.items():
                print(f"  {c}: {cnt}")
            print("============================\n")


if __name__ == "__main__":
    main()

