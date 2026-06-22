-- DDL for error_log_table
-- Replaces error logging functionality from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_aurd_rechstan.ksh
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.error_log`
(
    log_time TIMESTAMP NOT NULL OPTIONS(description="Timestamp of the error"),
    job_id STRING NOT NULL OPTIONS(description="Unique identifier for the job run where the error occurred"),
    procedure_name STRING OPTIONS(description="Name of the BigQuery stored procedure"),
    error_code INT64 OPTIONS(description="Numerical error code, if applicable"),
    error_message STRING NOT NULL OPTIONS(description="Detailed error message"),
    stack_trace STRING OPTIONS(description="Stack trace or location of the error"),
    reference_date DATE OPTIONS(description="Reference date for the job run (p_Stichtag)"),
    additional_info JSON OPTIONS(description="Additional information in JSON format")
)
PARTITION BY
    DATE(log_time)
OPTIONS(
    description="Table to log errors and exceptions during BigQuery job execution."
);