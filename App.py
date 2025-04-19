from kafka import KafkaConsumer
import json

SERVER = "broker:9092"
TOPIC = "mytopic"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=SERVER,
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def klasyfikuj_transakcje(transakcja):
    id_ = transakcja["id"]
    value = transakcja["values"]

    if id_ == "a" and value > 50:
        return "Potencjalnie ryzykowna transakcja"
    elif value > 80:
        return "Wysokie ryzyko"
    else:
        return "Transakcja bez ryzyka"

for message in consumer:
    transaction = message.value
    wynik = klasyfikuj_transakcje(transaction)
    print(transaction, "=>", wynik)
