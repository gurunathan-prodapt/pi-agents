-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_apn_ve.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_apn_ve.ksh

CREATE TABLE IF NOT EXISTS `my-gcp-project.my_dataset.job_error_log` (
    error_id INT64 OPTIONS(description="Unique identifier for the error entry. Consider using GENERATE_UUID() for unique IDs or BigQuery sequences if available/needed."),
    job_entry_number INT64 NOT NULL OPTIONS(description="Foreign key to job_control.job_entry_number"),
    error_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the error occurred"),
    script_name STRING OPTIONS(description="Name of the script/procedure where the error occurred"),
    error_code INT64 OPTIONS(description="Numeric error code"),
    error_message STRING OPTIONS(description="Detailed error message"),
    sql_state STRING OPTIONS(description="SQLSTATE if applicable"),
    stack_trace STRING OPTIONS(description="Stack trace or call stack at the time of error")
);