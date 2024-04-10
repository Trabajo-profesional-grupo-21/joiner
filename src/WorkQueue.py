import logging
import pika
import asyncio

class WorkQueue():
    def __init__(self, channel, queue_name):
        try:
            self.channel = channel
            self.queue = channel.queue_declare(queue=queue_name, durable=True)
            self.queue_name = self.queue.method.queue
            self.user_callback = None
        except Exception as e:
            logging.error(f"Work Queue: Error creating queue {e}")

    def receive(self, callback, prefetch_count=1):
        try:
            self.user_callback = callback
            self.channel.basic_qos(prefetch_count=prefetch_count)
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=self._callback, auto_ack=True)
        except Exception as e:
            logging.error(f"Work Queue: Error receiving message {e}")


    def _callback(self, ch, method, properties, body):
        try:
            self.user_callback(body, method.delivery_tag)
        except Exception as e:
            logging.error(f"Work Queue: Error on callback {e}")

    def ack(self, ack_element):
        try:
            if isinstance(ack_element, list):
                self.channel.basic_ack(delivery_tag=ack_element[-1], multiple=True)
            elif isinstance(ack_element, int):
                self.channel.basic_ack(delivery_tag=ack_element)
            else:
                raise Exception(f"Not Valid ACK Element {ack_element}")
        except Exception as e:
            logging.error(f"Work Queue: Error sending ack {e}")

    def nack(self, ack_element):
        try:
            if isinstance(ack_element, list):
                self.channel.basic_nack(delivery_tag=ack_element[-1], multiple=True)
            elif isinstance(ack_element, int):
                self.channel.basic_nack(delivery_tag=ack_element)
            else:
                raise Exception(f"Not Valid ACK Element {ack_element}")
        except Exception as e:
            logging.error(f"Work Queue: Error sending nack {e}")


    def send(self, message):
        try:
            self.channel.basic_publish(exchange='',
                        routing_key=self.queue_name,
                        body=message,
                        properties=pika.BasicProperties(delivery_mode=2))
        except Exception as e:
            logging.error(f"Work Queue: Error sending message {e}")