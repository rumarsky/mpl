from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import time

TOPIC = "test-topic"
BOOTSTRAP_SERVERS = ["localhost:9093"]


def produce_messages():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    for i in range(5):
        message = {"number": i, "text": f"Сообщение №{i}"}
        producer.send(TOPIC, message)
        print(f"Отправлено: {message}")
        time.sleep(0.5)
    producer.flush()
    producer.close()


def consume_messages():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest"
    )

    print("\nЧтение сообщений...")
    for msg in consumer:
        print(f"Получено: {msg.value}")
        time.sleep(0.5)
        if msg.value.get("number") == 4:
            break
    consumer.close()


if __name__ == "__main__":
    for _ in range(10):
        try:
            produce_messages()
            break
        except NoBrokersAvailable:
            print("Kafka ещё не готова, жду...")
            time.sleep(2)
    consume_messages()

