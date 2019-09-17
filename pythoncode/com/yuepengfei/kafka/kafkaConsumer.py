from kafka import KafkaConsumer
consumer = KafkaConsumer('test',bootstrap_servers='192.168.240.131:9092')
for msg in consumer:
    print (msg)