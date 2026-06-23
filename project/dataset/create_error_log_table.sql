-- BigQuery table for centralized error logging.
-- Replaces shell error messages for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh
CREATE TABLE IF NOT EXISTS project.dataset.error_log (
    job_id STRING,
    run_id STRING,
    error_code INT64,
    error_message STRING,
    error_arg STRING,
    severity STRING, -- e.g., 'E' for Error, 'W' for Warning
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);