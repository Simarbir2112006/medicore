CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS patients (
    patient_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name          VARCHAR(255),
    dob           DATE NOT NULL,
    gender        VARCHAR(8),
    height_cm     NUMERIC(5,2),
    weight_kg     NUMERIC(5,2),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS vitals (
    id                       BIGSERIAL,
    patient_id               UUID NOT NULL REFERENCES patients(patient_id),
    recorded_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    heart_rate               SMALLINT NOT NULL,
    oxygen_level             NUMERIC(5,2) NOT NULL,
    blood_pressure_systolic  SMALLINT NOT NULL,
    blood_pressure_diastolic SMALLINT NOT NULL,

    CONSTRAINT chk_heart_rate
        CHECK (heart_rate BETWEEN 10 AND 300),
    CONSTRAINT chk_oxygen_level
        CHECK (oxygen_level BETWEEN 50.00 AND 100.00),
    CONSTRAINT chk_bp_systolic
        CHECK (blood_pressure_systolic BETWEEN 50 AND 250),
    CONSTRAINT chk_bp_diastolic
        CHECK (blood_pressure_diastolic BETWEEN 30 AND 150)
);

SELECT create_hypertable('vitals', 'recorded_at');