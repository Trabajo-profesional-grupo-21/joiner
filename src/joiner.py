from .Connection import Connection
import ujson as json
import signal
import logging
from bisect import insort




class Joiner():
    def __init__(self):
        self.running = True
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        self.counter = 0
        self.connection = Connection()
        self.input_queue = self.connection.Consumer(queue_name="processed")

        self.output_queue = self.connection.Producer(queue_name="ordered_batches")
        self.last_batches= {}
        self.current_batches_arousal = {}
        self.current_batches_valence = {}
        self.current_batches = {}


    def _handle_sigterm(self, *args):
        """
        Handles SIGTERM signal
        """
        logging.info('SIGTERM received - Shutting server down')
        self.connection.close()
    
    def _check_batch(self, body: dict):
        # puede ser de arousal o valencia
        body = json.loads(body.decode())

        origin = body['origin']
        batch_id = body['batch_id'] 
        output = {}
        complete_batch = False
        logging.info(f"Origin {origin}")
        if origin == "valence":
            if batch_id in self.current_batches_arousal.keys():
                #logging.info(f"Tengo arousal y valencia del batch {batch_id}")
                #logging.info(f"keys del arousal { self.current_batches_arousal}")
                current_arousal = self.current_batches_arousal[batch_id]
                arousal_replies = current_arousal['replies']
                merged_replies = {}
                for key, value in arousal_replies.items():
                    merged_frame_info = value.copy()
                    #logging.info(f"busco key {key} en body {body['replies'].keys()}") 
                    merged_frame_info.update(body['replies'][key])
                    merged_replies[key] = merged_frame_info
                #logging.info(f"merge info {merged_replies}")
                output["user_id"] = body["user_id"]
                output["batch_id"] = body["batch_id"]
                output["replies"] = merged_replies
                complete_batch = True
                del self.current_batches_arousal[batch_id]
            else: 
                del body['origin']
                self.current_batches_valence[batch_id] = body
        elif origin == "arousal":
            if batch_id in self.current_batches_valence.keys():
                #logging.info(f"Tengo arousal y valencia del batch {batch_id}")
                #logging.info(f"keys del valencia { self.current_batches_valence}")
                current_valence = self.current_batches_valence[batch_id]
                valence_replies = current_valence['replies']
                merged_replies = {}
                for key, value in valence_replies.items():
                    merged_frame_info = value.copy()
                    logging.info(f"busco key {key} en body {body['replies'].keys()}") 
                    merged_frame_info.update(body['replies'][key])
                    merged_replies[key] = merged_frame_info
                #logging.info(f"merge info {merged_replies}")
                output["user_id"] = body["user_id"]
                output["batch_id"] = body["batch_id"]
                output["replies"] = merged_replies
                complete_batch = True
                del self.current_batches_valence[batch_id]
            else: 
                del body['origin']
                self.current_batches_arousal[batch_id] = body        
        return output,complete_batch

    def _callback(self, body: dict, ack_tag):
        batch, complete_batch = self._check_batch(body)
        if not complete_batch:
            return
        # si hubo merge, esto se sigue
        user = batch['user_id']
        batch_id = int(batch['batch_id'])

        logging.info(f"Recibo batch {batch_id} de {user} -- batch {batch}")
        
        current_batches = self.current_batches.get(user, [])
        insort(current_batches, (batch_id, batch["replies"]))

        last_sent = self.last_batches.get(user, 0)
        first_batch = current_batches[0][0]
        while first_batch == last_sent + 1:
            data = current_batches.pop(0)[1]
            last_sent += 1
            self.last_batches[user] = last_sent
            # Chequear si para tpdp el batch tengo info (arousal y valencia)
            reply = {"user_id": user, "batch_id": first_batch, "batch": data}

            logging.info(f"Sending batch {first_batch} de {user}")
            self.output_queue.send(json.dumps(reply))
            if len(current_batches) == 0:
                break
            first_batch = current_batches[0][0]
        
        self.current_batches[user] = current_batches

    def run(self):
        self.input_queue.receive(self._callback)
        self.connection.start_consuming()
        self.connection.close()