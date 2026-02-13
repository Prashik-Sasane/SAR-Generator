from kafka import KafkaConsumer
import json
import time

KAFKA_BROKER = "kafka:9092"
TOPIC = "bankAlert"
GROUP_ID = "bank-alert-consumers"
local_time_struct = time.localtime()
formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", local_time_struct)
def create_consumer():

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,

        auto_offset_reset="earliest",   # read from start if new group
        enable_auto_commit=True,

        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None
    )
    print(" 🚨created_consumer")

    return consumer


def process_alert(message):
# call pipleline here (prutvi and pranvav)
# pipline(message)
    print("🚨 New Bank Alert Received",flush=True)
    print(message)

def start_consumer():
    print("⏳ Waiting for Kafka...")

    consumer = create_consumer()

    print("✅ Connected. Listening on topic:", TOPIC)
    print("Current local time (formatted):", formatted_time)
    for msg in consumer:
        print("Current local time (formatted):", formatted_time)
        process_alert(msg.value)