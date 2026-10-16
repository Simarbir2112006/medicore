from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class VitalReading(BaseModel):
    patient_id: str
    recorded_at: datetime
    heart_rate: int
    oxygen_level: float
    blood_pressure_systolic: int
    blood_pressure_diastolic: int


class AlertResponse(BaseModel):
    patient_id: str
    metric: str
    value: float
    severity: str
    reason: str
    timestamp: str


class PatientCreate(BaseModel):
    name: str
    dob: str
    gender: Optional[str] = None
    height_cm: Optional[float] = None
    weight_kg: Optional[float] = None