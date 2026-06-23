-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_carmen.ksh
-- Description: BigQuery DDL for job error logging table.
CREATE TABLE IF NOT EXISTS `project.dataset.job_error_log`
(
    job_name     STRING      NOT NULL,
    entry_nr     STRING,
    error_nr     INT64,
    error_msg    STRING,
    created_at   TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS(
    description = "Table to record job execution errors."
);