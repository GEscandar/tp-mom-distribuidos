import pika

# from pika.exceptions import AMQPConnectionError
import random
import string
from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareCloseError,
    MessageMiddlewareDeleteError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
)


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.consuming = False
        self.conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        self.channel = self.conn.channel()
        
    def declare(self):
        self.channel.queue_declare(
            queue=self.queue_name, durable=True, arguments={"x-queue-type": "quorum"}
        )

    def start_consuming(self, on_message_callback):
        def _callback(ch, method, properties, body):
            print(f"[Queue] Received message: {body}")
            on_message_callback(
                body,
                lambda: ch.basic_ack(delivery_tag=method.delivery_tag),
                lambda: ch.basic_nack(delivery_tag=method.delivery_tag),
            )

        if self.consuming:
            return
        self.consuming = True
        self.declare()
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=_callback)
        try:
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError("Connection lost while consuming")

    def stop_consuming(self):
        if not self.consuming:
            return
        self.consuming = False
        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError(
                "Connection lost while stopping consumption"
            )

    def send(self, message):
        self.declare()
        self.channel.basic_publish(
            exchange="",
            routing_key=self.queue_name,
            body=message,
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
        )

    def close(self):
        self.stop_consuming()
        try:
            self.channel.close()
            self.conn.close()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareCloseError(
                "Connection lost while closing connection"
            )

    def __exit__(self, exc_type, exc, tb):
        self.close()


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys):
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.consuming = False
        self.conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        self.channel = self.conn.channel()
        
    def declare(self):
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type="direct")

    def start_consuming(self, on_message_callback):
        def _callback(ch, method, properties, body):
            print(f"Received message with routing key {method.routing_key}: {body}")
            on_message_callback(
                body,
                lambda: ch.basic_ack(delivery_tag=method.delivery_tag),
                lambda: ch.basic_nack(delivery_tag=method.delivery_tag),
            )

        if self.consuming:
            return
        self.consuming = True
        self.declare()
        result = self.channel.queue_declare(queue="", exclusive=True)
        queue_name = result.method.queue
        for routing_key in self.routing_keys:
            self.channel.queue_bind(
                exchange=self.exchange_name, queue=queue_name, routing_key=routing_key
            )
        self.channel.basic_consume(queue=queue_name, on_message_callback=_callback)
        try:
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError("Connection lost while consuming")

    def stop_consuming(self):
        if not self.consuming:
            return
        self.consuming = False
        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError(
                "Connection lost while stopping consumption"
            )

    def send(self, message):
        self.declare()
        try:
            for routing_key in self.routing_keys:
                self.channel.basic_publish(
                    exchange=self.exchange_name, routing_key=routing_key, body=message
                )
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError(
                "Connection lost while sending message"
            )

    def close(self):
        self.stop_consuming()
        try:
            self.channel.close()
            self.conn.close()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareCloseError(
                "Connection lost while closing connection"
            )

    def __exit__(self, exc_type, exc, tb):
        self.close()
