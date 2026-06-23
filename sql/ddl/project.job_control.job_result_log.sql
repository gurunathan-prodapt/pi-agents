-- BigQuery DDL for project.job_control.job_result_log
-- Purpose: To store job execution results like record counts.
-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs.ksh

CREATE TABLE IF NOT EXISTS `project.job_control.job_result_log`
(
    job_kennung      STRING NOT NULL,
    eintrags_nr      STRING NOT NULL,
    result_time      TIMESTAMP NOT NULL,
    record_count     INT64,
    status           STRING -- e.g., 'SUCCESS', 'FAILURE'
)
OPTIONS(
    description="Table to store job execution results like record counts."
);