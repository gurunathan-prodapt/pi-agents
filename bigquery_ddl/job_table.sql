-- BigQuery DDL for job_table
-- New table for job status and active job management, replacing legacy ksh logic.
-- Part of the migration for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_table`
(
    job_kennung      STRING,
    eintrags_nr      STRING,
    start_time       TIMESTAMP,
    end_time         TIMESTAMP,
    status           STRING, -- e.g., 'RUNNING', 'COMPLETED', 'FAILED', 'DEACTIVATED'
    message          STRING,
    processed_records INT64,
    -- Add other relevant job tracking fields
    last_updated     TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);