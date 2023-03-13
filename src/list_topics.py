from kafka import KafkaConsumer

consumer = KafkaConsumer(bootstrap_servers=[''])
print(consumer.topics())
