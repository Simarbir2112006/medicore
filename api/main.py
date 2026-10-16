import asyncpg
import os
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from dotenv import load_dotenv

from api.auth import create_access_token, hash_password, verify_password
from api.dependencies import get_current_user, get_db_pool, get_redis_client
from api.models import TokenResponse, PatientCreate, AlertResponse
from redis_engine.alert_engine import get_active_alerts

load_dotenv()

app = FastAPI(title="Medicore API", version="1.0.0")

MOCK_USERS = {
    "doctor01": {
        "username": "doctor01",
        "hashed_password": hash_password("password123"),
        "role": "doctor",
    }
}


@app.post("/auth/login", response_model=TokenResponse)
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = MOCK_USERS.get(form_data.username)

    if not user or not verify_password(form_data.password, user["hashed_password"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Bearer"}
        )

    token = create_access_token({
        "sub": user["username"],
        "role": user["role"],
    })

    return TokenResponse(access_token=token)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "Medicore API"}


@app.get("/alerts", response_model=list[AlertResponse])
async def get_alerts(current_user: dict = Depends(get_current_user)):
    client = get_redis_client()
    alerts = get_active_alerts(client)

    return alerts


@app.post("/patients", status_code=201)
async def create_patient(patient: PatientCreate, current_user: dict = Depends(get_current_user)):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """"
            INSERT INTO patients (name, dob, gender, height_cm, weight_kg)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING patient_id, name, created_at
            """,
            patient.name, patient.dob, patient.gender, patient.height_cm, patient.weight_kg,
        )

    return {"patient_id": str(row["patient_id"]), "name": row["name"]}


@app.get("/patients/{patient_id}/vitals")
async def get_patient_vitals(patient_id: str, hours: int = 24, current_user: dict = Depends(get_current_user)):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT recorded_at, heart_rate, oxygen_level, blood_pressure_systolic, blood_pressure_systolic
            FROM vitals
            WHERE patient_id = $1
            AND recorded_at >= now() - ($2 || ' hours')::interval
            ORDER BY recorded_at DESC
            LIMIT 100
            """,
            patient_id, str(hours)
        )

    return [dict(r) for r in rows]