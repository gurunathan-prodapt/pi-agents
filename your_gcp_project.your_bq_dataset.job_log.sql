-- DDL for job_log table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_einzeln.ksh
-- This table tracks logs for the job and its components.

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bq_dataset.job_log` (
    log_id STRING NOT NULL OPTIONS(description="Unique identifier for the log entry"),
    job_name STRING NOT NULL OPTIONS(description="Name of the job or component"),
    log_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp of the log entry"),
    log_level STRING NOT NULL OPTIONS(description="Severity level of the log (INFO, WARNING, ERROR)"),
    message STRING NOT NULL OPTIONS(description="Log message details"),
    stichtag STRING OPTIONS(description="Stichtag (snapshot date) parameter used for the job run"),
    wiederanlaufwert STRING OPTIONS(description="Wiederanlaufwert (restart value) parameter used for the job run"),
    error_details STRING OPTIONS(description="Detailed error message if log_level is ERROR")
)
OPTIONS(
    description="Table to store execution logs for various jobs."
);