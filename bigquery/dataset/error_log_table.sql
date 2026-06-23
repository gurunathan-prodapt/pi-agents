-- Migrated from: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh

-- DDL for BigQuery Error Logging Table
CREATE TABLE IF NOT EXISTS `dataset.error_log` (
    error_id INT64 OPTIONS(description="Unique identifier for each error entry"),
    job_name STRING NOT NULL OPTIONS(description="Name of the job/script that encountered the error"),
    error_message STRING OPTIONS(description="Detailed error message"),
    error_stack STRING OPTIONS(description="Stack trace or additional error context"),
    error_severity STRING OPTIONS(description="Severity of the error (e.g., ERROR, WARNING)"),
    error_code STRING OPTIONS(description="Custom or system error code"),
    error_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp when the error occurred"),
    additional_info JSON OPTIONS(description="Additional JSON formatted information about the error")
);

-- Note: error_id would typically be an auto-incrementing field. In BigQuery,
-- this usually implies generating it within the INSERT statement using a sequence
-- or a UUID, or using an external system to manage it. For simplicity,
-- we'll assume it's populated during INSERT or managed by a separate process.