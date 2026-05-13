import json
import os

import pika

RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")


def callback(ch, method, properties, body):
    event = json.loads(body)
    print(f"EVENT: {event['event']} | node: {event['node_name']} | time: {event['timestamp']}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue="node_events", durable=True)
    channel.basic_consume(queue="node_events", on_message_callback=callback)
    print("Consumer started, waiting for messages...")
    channel.start_consuming()


if __name__ == "__main__":
    main()
