-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_einzeln.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_einzeln.ksh
-- Description: DDL for the BigQuery error log table.

CREATE TABLE IF NOT EXISTS `project.dataset.error_log` (
    error_ts TIMESTAMP OPTIONS(description="Timestamp of the error"),
    job_name STRING OPTIONS(description="Name of the job that failed"),
    error_nr INT64 OPTIONS(description="Error number or code"),
    error_arg STRING OPTIONS(description="Argument related to the error (e.g., parameter name)"),
    message STRING OPTIONS(description="Detailed error message")
);