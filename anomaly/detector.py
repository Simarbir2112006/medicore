import statistics
from collections import deque
from dataclasses import dataclass, field
from typing import Optional



WINDOW_SIZE = 20
Z_SCORE_THRESHOLD = 3.0

VITAL_THRESHOLDS = {
    "heart_rate":               {"min": 40,    "max": 150},
    "oxygen_level":             {"min": 90.0,  "max": 100.0},
    "blood_pressure_systolic":  {"min": 80,    "max": 180},
    "blood_pressure_diastolic": {"min": 50,    "max": 110},
}

@dataclass
class PatientBaseline:
    patient_id: str
    heart_rate_window:      deque = field(default_factory=lambda: deque(maxlen=WINDOW_SIZE))
    oxygen_window:          deque = field(default_factory=lambda: deque(maxlen=WINDOW_SIZE))
    bp_systolic_window:     deque = field(default_factory=lambda: deque(maxlen=WINDOW_SIZE))
    bp_diastolic_window:    deque = field(default_factory=lambda: deque(maxlen=WINDOW_SIZE))


def calculate_z_score(value: float, window: deque) -> Optional[float]:
    if len(window) < 5:
        return None

    mean = statistics.mean(window)
    std = statistics.stdev(window)

    if std == 0:
        return None

    return abs((value - mean) / std)


def detect_anomaly(vital: dict, baseline: PatientBaseline) -> Optional[dict]:
    anomalies = []

    checks = [
        ('heart_rate',                  vital["heart_rate"],                baseline.heart_rate_window),
        ('oxygen_level',                vital["oxygen_level"],              baseline.oxygen_window),
        ('blood_pressure_systolic',     vital["blood_pressure_systolic"],   baseline.bp_systolic_window),
        ('blood_pressure_diastolic',    vital["blood_pressure_diastolic"],  baseline.bp_diastolic_window),
    ]

    for metric, value, window in checks:
        threshold = VITAL_THRESHOLDS[metric]

        if value < threshold['min'] or value > threshold['max']:
            anomalies.append({
                "metric": metric,
                "value": value,
                "reason": "threshold_breach",
                "severity": "critical",
            })
            continue

        z_score = calculate_z_score(value, window)

        if z_score is not None and z_score > Z_SCORE_THRESHOLD:
            anomalies.append({
                "metric": metric,
                "value": value,
                "z_score": round(z_score, 2),
                "reason": "statistical_anomaly",
                "severity": "warning",
            })

        window.append(value)

    if not anomalies:
        return None

    return {
        "patient_id": vital["patient_id"],
        "timestamp": vital["timestamp"],
        "anomalies": anomalies,
    }

        