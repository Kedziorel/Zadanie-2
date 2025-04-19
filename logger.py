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

with open("log.txt", "a") as logfile:
    for message in consumer:
        transaction = message.value
        logfile.write(json.dumps(transaction) + "\n")
        print("✔️ Zapisano transakcję:", transaction)
