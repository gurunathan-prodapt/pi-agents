-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh

CREATE TABLE IF NOT EXISTS `my_gcp_project.my_bq_dataset.job_log`
(
    job_id STRING NOT NULL OPTIONS(description="Unique identifier for the job run"),
    job_name STRING NOT NULL OPTIONS(description="Name of the job being executed"),
    severity STRING NOT NULL OPTIONS(description="Log level (e.g., 'I' for Info, 'W' for Warning, 'E' for Error)"),
    error_code INT64 OPTIONS(description="Numeric error code if applicable"),
    error_arg STRING OPTIONS(description="Argument related to the error, if any"),
    message STRING NOT NULL OPTIONS(description="Log message or description"),
    created_at TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the log entry was created")
)
OPTIONS(
    description="Centralized logging table for BigQuery job executions"
);