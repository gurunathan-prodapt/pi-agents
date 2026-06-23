-- BigQuery DDL for project.job_control.job_table
-- Purpose: To track job status and metadata.
-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs.ksh

CREATE TABLE IF NOT EXISTS `project.job_control.job_table`
(
    job_kennung      STRING NOT NULL,
    eintrags_nr      STRING NOT NULL,
    status           STRING NOT NULL, -- e.g., 'RUNNING', 'COMPLETED', 'FAILED'
    start_time       TIMESTAMP,
    end_time         TIMESTAMP,
    message          STRING
)
OPTIONS(
    description="Table to track job status and metadata."
);