-- BigQuery DDL for logging/control table control_log.job_log
-- Used by vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh

CREATE TABLE IF NOT EXISTS `control_log.job_log`
(
    tab_name     STRING    OPTIONS(description="Name of the table being processed."),
    job_status   STRING    OPTIONS(description="Status of the job execution (e.g., 'A' for active, 'C' for complete, 'E' for error)."),
    record_count INT64     OPTIONS(description="Number of records processed or inserted."),
    stichtag     DATE      OPTIONS(description="Key date for which the job was run."),
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp of the log entry.")
);