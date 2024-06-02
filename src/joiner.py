from common.connection import Connection
import ujson as json
import signal
import logging
from bisect import insort
import redis
import datetime
import os
from dotenv import load_dotenv
import pymongo
from .crud import update

load_dotenv()


class Joiner():
    def __init__(self):
        self.running = True
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        self.current_batches = {}
        self.current_images = {}

        self.init_conn()

        self.redis = redis.Redis(
            host=os.getenv("REDIS_HOST"),
            port=10756,
            password=os.getenv("REDIS_PASSWORD")
        )

        mongo_client = pymongo.MongoClient(os.getenv("MONGODB_URL"))
        db_name = os.getenv("MONGODB_DB_NAME")
        self.db = mongo_client[db_name]

    def init_conn(self):
        remote_rabbit = os.getenv('REMOTE_RABBIT', False)
        if remote_rabbit:
            self.connection = Connection(host=os.getenv('RABBIT_HOST'), 
                                    port=os.getenv('RABBIT_PORT'),
                                    virtual_host=os.getenv('RABBIT_VHOST'), 
                                    user=os.getenv('RABBIT_USER'), 
                                    password=os.getenv('RABBIT_PASSWORD'))
        else:
            # selfconnection = Connection(host="rabbitmq-0.rabbitmq.default.svc.cluster.local", port=5672)
            self.connection = Connection(host="rabbitmq", port=5672)

        self.input_queue = self.connection.Consumer(queue_name="processed")
        self.output_queue = self.connection.Producer(queue_name="ordered_batches")

    def _handle_sigterm(self, *args):
        """
        Handles SIGTERM signal
        """
        logging.info('SIGTERM received - Shutting server down')
        self.connection.close()

    def process_video(self, body):
        user_id = body['user_id']
        batch_id = body['batch_id']
        file_name = body['file_name']

        batch_key = f"{user_id}-{file_name}-{batch_id}"

        if batch_key not in self.current_batches:
            reply = {"user_id": user_id, "batch_id": batch_id, "batch": body["replies"]}
            self.current_batches[batch_key] = reply
            return

        reply = self.current_batches.pop(batch_key)

        merged_replies = {}

        current_data = reply["batch"]
        for key, value in current_data.items():
            merged_frame_info = value.copy()
            merged_frame_info.update(body['replies'][key])
            merged_replies[key] = merged_frame_info

        reply["batch"] = merged_replies

        self.redis.set(batch_key, json.dumps(reply))
        self.redis.expire(key, 3600)

        update(self.db, user_id, file_name, reply, "video")


    def process_image(self, body):
        origin = body['origin']
        user = body["user_id"]
        data = body["reply"]
        image_id = body['img_name']
        file_name = body['file_name'] # Uno tiene extension y otro no
        upload = body['upload']
        del body['origin']

        key = f'{user}-{image_id}'
        current_image = self.current_images.get(key, {})
        current_image[origin] = data
        self.current_images[key] = current_image
        
        if len(current_image) == 2:
            batch = {
                "0": {
                    "ActionUnit": current_image['arousal']['0']['ActionUnit'],
                    "arousal": current_image['arousal']['0']['arousal'],
                    "valence": current_image['valence']['0']['valence'],
                    "emotions": current_image['valence']['0']['emotions'],
                }
            }
            reply = {"user_id": user, "img_name": image_id, "batch": batch}
            
            self.redis.rpush(key, json.dumps(reply))
            self.redis.expire(key, 3600)
            self.current_images.pop(key)
            if upload:
                update(self.db, user, file_name, reply, "image")


    def _check_batch(self, body: dict):
        body = json.loads(body.decode())
        if "img_name" in body:
            self.process_image(body)
        elif "EOF" in body:
            # TODO: En caso de ser necesitado en el futuro se puede usar este mensaje
            # para acciones de control (borrar users, etc.). Llega cada vez que termina
            # un video. Puede llegar mas de uno al haber mas de un processor.
            pass
        else:
            self.process_video(body)

    def _callback(self, body: dict, ack_tag):     
        self._check_batch(body)

    def run(self):
        self.input_queue.receive(self._callback)
        self.connection.start_consuming()
        self.connection.close()