-- Target BigQuery DDL for job_log table
-- Legacy Source: Detailed logging from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_iccid.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_iccid.ksh

CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_log` (
    run_id STRING OPTIONS(description="Unique identifier for the job execution"),
    log_timestamp TIMESTAMP OPTIONS(description="Timestamp of the log entry"),
    log_level STRING OPTIONS(description="Severity level of the log entry (e.g., 'INFO', 'WARNING', 'ERROR')"),
    procedure_name STRING OPTIONS(description="Name of the stored procedure logging the message"),
    message STRING OPTIONS(description="Detailed log message")
);