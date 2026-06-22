-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs3.ksh
-- This DDL creates the error logging table for BigQuery stored procedures.
-- Please replace `your_project_id` and `your_dataset_id` with your actual BigQuery project and dataset.
CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.error_log` (
    log_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp of the error event."),
    procedure_name STRING NOT NULL OPTIONS(description="Name of the stored procedure where the error occurred."),
    err_nr INT64 OPTIONS(description="Error number or code, potentially mapped from legacy system."),
    err_arg STRING OPTIONS(description="Specific argument or context related to the error."),
    message STRING OPTIONS(description="Detailed error message.")
)
OPTIONS(
    description="Table to log errors from BigQuery stored procedures, replacing legacy shell script error handling."
);