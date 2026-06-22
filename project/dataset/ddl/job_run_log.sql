-- DDL for job_run_log
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_upgrade.ksh
-- This table logs the start, end, and status of job runs.

CREATE TABLE IF NOT EXISTS project.dataset.job_run_log (
    job_run_id STRING NOT NULL DEFAULT GENERATE_UUID(),
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL,
    start_timestamp TIMESTAMP NOT NULL,
    end_timestamp TIMESTAMP,
    status STRING, -- e.g., 'RUNNING', 'COMPLETED', 'FAILED', 'SKIPPED'
    processed_records INT64,
    log_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
);