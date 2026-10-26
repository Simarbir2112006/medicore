import json
import time
import os

import redis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = 6379
ALERT_TTL_SECONDS = 300
ACTIVE_ALERTS_KEY = "active_alerts"


def create_redis_client() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
    )


def store_alert(client: redis.Redis, anomaly: dict) -> bool:
    patient_id = anomaly["patient_id"]
    timestamp = time.time()

    for alert in anomaly["anomalies"]:
        metric = alert["metric"]
        dedup_key = f"alert:{patient_id}:{metric}"

        if client.exists(dedup_key):
            continue

        alert_data = {
            "patient_id":patient_id,
            "metric": metric,
            "value": alert["value"],
            "severity": alert["severity"],
            "reason": alert["reason"],
            "timestamp": anomaly["timestamp"],
        }

        client.setex(
            dedup_key,
            ALERT_TTL_SECONDS,
            json.dumps(alert_data),
        )

        client.zadd(
            ACTIVE_ALERTS_KEY,
            {dedup_key: timestamp}
        )

    return True



def get_active_alerts(client: redis.Redis, limit: int=20) -> list[dict]:
    alert_keys = client.zrevrange(ACTIVE_ALERTS_KEY, 0, limit - 1)

    alerts = []

    for key in alert_keys:
        data = client.get(key)
        if data:
            alerts.append(json.loads(data))
        else:
            client.zrem(ACTIVE_ALERTS_KEY, key)

    return alerts