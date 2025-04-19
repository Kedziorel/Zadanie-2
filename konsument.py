from kafka import KafkaConsumer
import json

SERVER = "localhost:9092"
TOPIC = "mytopic"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=SERVER,
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    transaction = message.value
    if transaction["values"] > 75:
        print(f"🚨 Wykryto dużą transakcję: {transaction}")
