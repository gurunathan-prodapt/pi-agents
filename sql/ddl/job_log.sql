--
-- BigQuery DDL for job_log table.
-- Replaces logging functionality from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_vertrag.ksh
--
CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bq_dataset.job_log`
(
    log_id              STRING,      -- Unique identifier for each log entry
    job_id              STRING,      -- Foreign key to job_registry.job_id
    log_timestamp       TIMESTAMP,   -- Timestamp of the log entry
    log_level           STRING,      -- 'INFO', 'WARNING', 'ERROR'
    component           STRING,      -- Which part of the job generated the log, e.g., 'wrapper', 'core_logic'
    message             STRING,      -- Log message content
    details             JSON         -- Optional JSON for additional log details
)
PARTITION BY DATE(log_timestamp)
CLUSTER BY job_id, log_level
OPTIONS(
    description = "Detailed log entries for BigQuery job executions, replacing legacy f_alis_msgerr.ksh."
);