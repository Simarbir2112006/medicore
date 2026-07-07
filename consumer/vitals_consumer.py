import json
import asyncio
import asyncpg
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaConsumer

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "patient_vitals"
GROUP_ID = "medicore_workers"

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": "{DB_USER}",
    "password": "{DB_PASSWORD}",
    "database": "{DB_NAME}",
}