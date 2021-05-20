from kafka import KafkaProducer
import json
import time


class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic

    #TODO we're generating a dummy data
    def generate_data(self):
        with open(self.input_file) as f:
            json_file  = json.load(f)     # reading the file pointer into 
            for line in json_file:        # reading line by line 
                message = self.dict_to_binary(line)     # converting dictionary to Binary for data transformation
                # TODO send the correct data
                self.send(self.topic,message)  # calling the send method with topicname and current message 
                time.sleep(1)

    # TODO fill this in to return the json dictionary to binary
    def dict_to_binary(self, json_dict):
        return json.dumps(json_dict).encode('utf-8')     # reading dictionary and converting it as a String with  utf-8 encoding
        