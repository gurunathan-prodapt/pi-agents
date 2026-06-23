-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_apn_ve.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_apn_ve.ksh

CREATE TABLE IF NOT EXISTS `my-gcp-project.my_dataset.job_log` (
    log_id INT64 OPTIONS(description="Unique identifier for the log entry. Consider using GENERATE_UUID() for unique IDs or BigQuery sequences if available/needed."),
    job_entry_number INT64 NOT NULL OPTIONS(description="Foreign key to job_control.job_entry_number"),
    log_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp of the log entry"),
    log_level STRING OPTIONS(description="Severity level of the log message (e.g., INFO, WARN, ERROR)"),
    message STRING OPTIONS(description="Detailed log message")
);