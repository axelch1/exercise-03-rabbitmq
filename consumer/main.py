import json
import os
import time

import pika

RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")


def callback(ch, method, properties, body):
    event = json.loads(body)
    print(f"EVENT: {event['event']} | node: {event['node_name']} | time: {event['timestamp']}", flush=True)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def wait_for_rabbitmq():
    for i in range(30):
        try:
            params = pika.URLParameters(RABBITMQ_URL)
            connection = pika.BlockingConnection(params)
            connection.close()
            return
        except Exception:
            print(f"Waiting for RabbitMQ... attempt {i + 1}/30", flush=True)
            time.sleep(2)
    raise RuntimeError("Could not connect to RabbitMQ after 30 attempts")


def main():
    wait_for_rabbitmq()
    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue="node_events", durable=True)
    channel.basic_consume(queue="node_events", on_message_callback=callback)
    print("Consumer started, waiting for messages...", flush=True)
    channel.start_consuming()


if __name__ == "__main__":
    main()
