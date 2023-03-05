from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

class Df101KafkaProducer:
    """Kafka Producer for DF101 pipeline.
    
    This message producer class sends dict messages serialized as JSON synchronously to a specified topic.
    
    Parameters
    ----------
    bootstrap_servers : str
        URL with port of Kafka bootstrap server. Example: '11.99.22.110:29092'
    """
    
    def __init__(self, bootstrap_servers: str):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )

    def send(self, topic: str, message: dict):
        """Sends message to specified topic.
        
        ----------
        topic : str
            Name of topic.
        message : dict
            Dictionary containing payload that needs to be sent to topic.
        """

        future = self.producer.send(topic, message)

        try:
            record_metadata = future.get(timeout=30)
            print("Succesfully wrote message: "+ record_metadata)
        except KafkaError as e:
            print(e)
