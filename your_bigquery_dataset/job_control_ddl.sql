-- BigQuery DDL for job_control table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bigquery_dataset.job_control` (
    job_id STRING NOT NULL,         -- Unique identifier for each job execution (DW_EintragsNr)
    job_name STRING,                -- Name of the job (JobKennung, ProgName)
    job_status STRING,              -- Current status (e.g., 'RUNNING', 'OK', 'ERROR')
    start_time TIMESTAMP,           -- Job start timestamp
    end_time TIMESTAMP,             -- Job end timestamp
    parameter_stichtag DATE,        -- Parameter -s: Stichtag (cutoff date)
    parameter_wiederanlaufwert INT64, -- Parameter -l: Wiederanlaufwert (restart value)
    log_file_path STRING,           -- Path to the log file (or equivalent identifier in BQ)
    sys_date DATE,                  -- System date used during execution (v_sysdate)
    error_code INT64,               -- Error code if an error occurred
    error_message STRING,           -- Detailed error message
    message STRING                  -- General job messages (e.g., "Abarbeitung wurde ohne erkennbare Fehler beendet")
);