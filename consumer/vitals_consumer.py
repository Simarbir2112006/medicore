import asyncio
import json
import os
from concurrent.futures import ThreadPoolExecutor

import asyncpg
from dotenv import load_dotenv
from kafka import KafkaConsumer

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
        user=DB_CONFIG["host"],
        password=DB_CONFIG["password"],
        database=DB_CONFIG["database"],
        min_size=2,
        max_size=10
    )

async def process_vital(record: dict, pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
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
            record["patient_id"], record["timestamp"], record["heart_rate"],
            record["oxygen_level"], record["blood_pressure_systolic"], record["blood_pressure_diastolic"]
        )

    print(f"Stored vitals for patient: {record['patient_id']}\nHR:{record['heart_rate']}\nO2:{record['oxygen_level']}")