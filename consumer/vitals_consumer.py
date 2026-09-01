import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import asyncpg
from dotenv import load_dotenv
from kafka import KafkaConsumer

from anomaly.detector import PatientBaseline, detect_anomaly
from redis_engine.alert_engine import create_redis_client, store_alert

load_dotenv()


KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "patient_vitals"
GROUP_ID = "medicore_workers"

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}


async def create_db_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database=DB_CONFIG["database"],
        min_size=2,
        max_size=10
    )

async def process_vital(record: dict, pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        recoreded_at = datetime.fromisoformat(record['timestamp'])

        await conn.execute(
            """
            INSERT INTO vitals (
                patient_id,
                recorded_at,
                heart_rate,
                oxygen_level,
                blood_pressure_systolic,
                blood_pressure_diastolic
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            """,
            record["patient_id"], recoreded_at, record["heart_rate"],
            record["oxygen_level"], record["blood_pressure_systolic"], record["blood_pressure_diastolic"]
        )

    print(f"Stored vitals for patient: {record['patient_id']}\nHR:{record['heart_rate']}\nO2:{record['oxygen_level']}")


patient_baselines: dict[str, PatientBaseline] = {}
executor = ThreadPoolExecutor(max_workers=4)
redis_client = create_redis_client()

async def consume():
    pool = await create_db_pool()
    print("Database pool created")

    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset='latest',
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )
    print(f"Consumer connected, listenting to topic: {TOPIC_NAME}")

    loop = asyncio.get_event_loop()

    try:
        for msg in consumer:
            record = msg.value

            await loop.run_in_executor(None, lambda: None)
            await process_vital(record, pool)

            patient_id = record['patient_id']
            if patient_id not in patient_baselines:
                patient_baselines[patient_id] = PatientBaseline(patient_id=patient_id)

            baseline = patient_baselines[patient_id]

            anomaly = await loop.run_in_executor(
                executor,
                detect_anomaly,
                record,
                baseline,
            )

            if anomaly:
                print(f"!!! ANOMALY DETECTED: {anomaly} !!!")
                store_alert(redis_client, anomaly)
                print(f"Alert stored in Redis for patient {anomaly['patient_id'][:8]}")

            consumer.commit()
    
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("")
        print("="*80)
        print("Shutting down consumer...")
        print("="*80)
    
    finally:
        consumer.close()
        await pool.close()
        print("Consumer shut down cleanly.")

if __name__ == "__main__":
    asyncio.run(consume())