-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_msisdn.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_msisdn.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_log` (
    log_id STRING NOT NULL OPTIONS(description="Unique identifier for each log entry"),
    job_run_id STRING NOT NULL OPTIONS(description="Foreign key to job_control.job_run_id"),
    log_time TIMESTAMP NOT NULL OPTIONS(description="Timestamp of the log entry"),
    log_level STRING NOT NULL OPTIONS(description="Severity level of the log (INFO, WARN, ERROR)"),
    message STRING NOT NULL OPTIONS(description="Detailed log message"),
    step STRING OPTIONS(description="Specific step or phase of the job when the log was generated")
);