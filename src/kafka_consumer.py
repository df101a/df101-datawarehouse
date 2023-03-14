from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    'github_stars',
     bootstrap_servers=[''],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     value_deserializer=lambda x: loads(x.decode('utf-8')))

for message in consumer:
    message = message.value
    print(message)