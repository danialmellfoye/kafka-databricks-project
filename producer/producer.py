import json
import os
import random
import time

from confluent_kafka import Producer


config = {
    "bootstrap.servers": f"{os.environ['KAFKA_HOST']}:{os.environ['KAFKA_PORT']}",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ["KAFKA_USERNAME"],
    "sasl.password": os.environ["KAFKA_PASSWORD"],
}


producer = Producer(config)


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered: "
            f"topic={msg.topic()}, "
            f"partition={msg.partition()}, "
            f"offset={msg.offset()}"
        )


order = {
    "order_id": random.randint(10000, 99999),
    "customer_id": random.randint(1, 1000),
    "product_id": random.randint(1, 100),
    "quantity": random.randint(1, 5),
    "price": round(random.uniform(100, 5000), 2),
    "event_time": time.strftime("%Y-%m-%dT%H:%M:%SZ")
}


producer.produce(
    topic="ecommerce_orders",
    key=str(order["order_id"]),
    value=json.dumps(order),
    callback=delivery_report
)

producer.flush()

print("Order sent:")
print(json.dumps(order, indent=2))
