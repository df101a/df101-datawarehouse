from kafka import KafkaConsumer

consumer = KafkaConsumer(bootstrap_servers=["13.93.26.75:29092"])
print(consumer.topics())
