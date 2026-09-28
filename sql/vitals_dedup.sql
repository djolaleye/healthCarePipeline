-- One row per event_id
CREATE OR REPLACE VIEW vitals_dedup AS
SELECT event_id, patient_key, event_timestamp, ingest_ts,
       heart_rate, systolic, diastolic, spo2, temperature_c, dt
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_ts) AS rn
  FROM vitals_clean
)
WHERE rn = 1