import logging
import pika

PUBLISHER = "pub"
SUBSCRIBER = "sub"

class ExchangeQueue():
    def __init__(self, type, channel, exchange_name, exchange_type, queue_name=None, routing_keys=None):
        try:
            self.type = type
            self.channel = channel
            self.exchange_name = exchange_name
            self.exchange_type = exchange_type
            self.user_callback = None
            self.routing_keys = routing_keys
            channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type)
            if type == SUBSCRIBER:
                if exchange_type == "topic" and routing_keys is None:
                    raise Exception("You Need A Topic To Be Subscribed To")
                self.queue_name = self._declare_queue(exchange_name, queue_name)
                
        except Exception as e:
            logging.error(f"Exchange Queue: Error creating queue {e}")

    def _declare_queue(self, exchange_name, queue_name):
        if not queue_name:
            result = self.channel.queue_declare(queue='', durable=False)
            queue_name = result.method.queue
        else:
            self.channel.queue_declare(queue=queue_name, durable=True)

        if self.exchange_type == "topic":
            for routing_key in self.routing_keys:
                self.channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=routing_key)
        else:
            self.channel.queue_bind(exchange=exchange_name, queue=queue_name)

        return queue_name

    def receive(self, callback, prefetch_count=1):
        try:
            self.user_callback = callback
            self.channel.basic_qos(prefetch_count=prefetch_count)
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=self._callback, auto_ack=True)
        except Exception as e:
            logging.error(f"Work Exchange: Error receiving message -> {e}")

    def _callback(self, ch, method, properties, body):
        try:
            self.user_callback(body, method.delivery_tag)
        except Exception as e:
            logging.error(f"Exchange Queue: Error on callback -> {e}")

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


    def resend_bind_queue(self, message):
        try:
            if self.type == PUBLISHER:
                return
            self.channel.basic_publish(exchange="",
                        routing_key=self.queue_name,
                        body=message,
                        properties=pika.BasicProperties(delivery_mode=2))  # message persistent
        except Exception as e:
            logging.error(f"Exchange Queue: Error resending message to binded queue {e}")

    def send(self, message, routing_key=''):
        try:
            self.channel.basic_publish(exchange=self.exchange_name,
                        routing_key=routing_key,
                        body=message,
                        properties=pika.BasicProperties(delivery_mode=2))  # message persistent
        except Exception as e:
            logging.error(f"Exchange Queue: Error sending message {e}")
