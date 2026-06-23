-- BigQuery table for logging execution details and record counts.
-- Replaces temporary file and implicit job tracking for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh
CREATE TABLE IF NOT EXISTS project.dataset.job_run_audit (
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    status STRING, -- e.g., 'SUCCESS', 'FAILURE'
    records_processed INT64,
    start_timestamp TIMESTAMP,
    end_timestamp TIMESTAMP,
    error_message STRING
);