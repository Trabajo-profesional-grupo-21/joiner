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

        self.counter = 0
        self.last_batches= {}
        self.current_batches_arousal = {}
        self.current_batches_valence = {}
        self.current_batches = {}
        self.current_images = {}
        self.last_amount = {}

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
    
    def remove_user(self, user):
        batches_sent = self.last_batches.get(user, None)
        if batches_sent is None:
            return

        total_batches = self.last_amount.get(user, None)
        if total_batches is None:
            return

        if total_batches == batches_sent:
            logging.info(f"Removing {user}")
            self.current_batches.pop(user, None)
            self.current_batches_arousal.pop(user, None)
            self.current_batches_valence.pop(user, None)
            self.last_amount.pop(user, None)
            self.last_batches.pop(user, None)

    def process_income(self, body):
        origin = body['origin']
        batch_id = body['batch_id'] 
        output = {}
        current = self.current_batches_valence
        other = self.current_batches_arousal
        if origin == "arousal":
            current = self.current_batches_arousal
            other = self.current_batches_valence

        complete_batch = False
        if batch_id in other.keys():
            current_arousal = other[batch_id]
            arousal_replies = current_arousal['replies']
            merged_replies = {}
            for key, value in arousal_replies.items():
                if not value: # OpenFace no detecto caras y devuelve None
                    value = {}
                merged_frame_info = value.copy()
                merged_frame_info.update(body['replies'][key])
                merged_replies[key] = merged_frame_info
            output["user_id"] = body["user_id"]
            output["batch_id"] = body["batch_id"]
            output["replies"] = merged_replies
            output["file_name"] = body['file_name']
            output["upload"] = body['upload']
            complete_batch = True
            del other[batch_id]
        else: 
            del body['origin']
            current[batch_id] = body
        
        return output, complete_batch

    def process_image(self, body):

        origin = body['origin']
        image_id = body['img_name']
        user = body["user_id"]
        data = body["reply"]
        file_name = body['file_name']
        upload = body['upload']
        del body['origin']

        current_image = self.current_images.get(image_id, {})
        current_image[origin] = data
        self.current_images[image_id] = current_image
        
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
            
            key = f'{user}-{image_id}'
            self.redis.rpush(key, json.dumps(reply))
            self.redis.expire(key, 3600)
            self.current_images.pop(image_id)
            if upload:
                update(self.db, user, file_name, reply, "image")
        
        return None, False

    def _check_batch(self, body: dict):
        # puede ser de arousal o valencia
        body = json.loads(body.decode())

        if "EOF" in body:
            amount = body["total"]
            user = body["EOF"]
            self.last_amount[user] = amount
            self.remove_user(user)
            return None, False
        elif "img_name" in body:
            return self.process_image(body)
        else:
            return self.process_income(body)

    def _callback(self, body: dict, ack_tag):     
        batch, complete_batch = self._check_batch(body)
        if not complete_batch:
            return
        # si hubo merge, esto se sigue
        user = batch['user_id']
        batch_id = int(batch['batch_id'])
        file_name = batch['file_name']
        upload = batch['upload']

        # logging.info(f"Recibo batch {batch_id} de {user} -- batch {batch}")
        
        current_batches = self.current_batches.get(user, [])
        insort(current_batches, (batch_id, batch["replies"]))

        last_sent = self.last_batches.get(user, -1)
        first_batch = current_batches[0][0]
        while first_batch == last_sent + 1:
            data = current_batches.pop(0)[1]
            last_sent += 1
            self.last_batches[user] = last_sent
            # Chequear si para tpdp el batch tengo info (arousal y valencia)
            reply = {"user_id": user, "batch_id": first_batch, "batch": data}

            # logging.info(f"Sending batch {first_batch} de {user}")
            # logging.info(f"Len batch {len(json.dumps(reply))}")
            # self.output_queue.send(json.dumps(reply))
            # self.pusher_client.trigger('my-channel', 'my-event', reply)
            logging.info(f"Sending batch {batch_id} de {user}: {datetime.datetime.now()}")

            key = f'{user}-{file_name}-{batch_id}'
            self.redis.set(key, json.dumps(reply))
            self.redis.expire(key, 3600)

            if upload:
                update(self.db, user, file_name, reply, "video")
            
            if len(current_batches) == 0:
                break
            first_batch = current_batches[0][0]
        
        self.current_batches[user] = current_batches
        self.remove_user(user)

    def run(self):
        self.input_queue.receive(self._callback)
        self.connection.start_consuming()
        self.connection.close()