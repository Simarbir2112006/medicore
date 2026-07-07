import json
import time
import random
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = "patient_vitals"

PATIENT_IDS = [
    "bda59fef-f612-4d83-81d9-ebd22c0520e6"
]

def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer = lambda v: json.dumps(v).encode("utf-8"),
        key_serializer = lambda k: k.encode("utf-8"),
        acks = "all",
        retries = 3
    )

def generate_vital(patient_id: str) -> dict:
    return {
        "message_id": str(uuid.uuid4()),
        "patient_id": patient_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "heart_rate": random.randint(65, 85),
        "oxygen_level": round(random.uniform(95.0, 100.0), 2),
        "blood_pressure_systolic": random.randint(110,130),
        "blood_pressure_diastolic": random.randint(65,85)
    }

def run_producer():
    producer = create_producer()
    print(f"Producer started. Sending to TOPIC: {TOPIC_NAME}")

    try:
        while True:
            for patient_id in PATIENT_IDS:
                vital = generate_vital(patient_id)

                producer.send(
                    TOPIC_NAME,
                    key=patient_id,
                    value=vital
                )

                print(f"Sent vitals for patient: {patient_id}\nHR: {vital["heart_rate"]}\nO2: {vital["oxygen_level"]}")

                producer.flush()
                time.sleep(1)
    
    except KeyboardInterrupt:
        print("Producer stopped")
    
    finally:
        producer.close()


if __name__ == "__main__":
    run_producer()