-- Target: BigQuery
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount.ksh
-- Description: DDL for BigQuery logging table to store detailed error information.

CREATE TABLE IF NOT EXISTS project.dataset.dw_error_log (
    job_kennung STRING NOT NULL OPTIONS(description="Identifier for the specific job type"),
    dw_eintrags_nr INT64 NOT NULL OPTIONS(description="Corresponding entry number from dw_job_log"),
    error_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the error occurred"),
    error_message STRING NOT NULL OPTIONS(description="Detailed error message"),
    error_code STRING OPTIONS(description="Specific error code, if available"),
    stack_trace STRING OPTIONS(description="Stack trace or additional error context")
)
OPTIONS(
    description="Logs for detailed error information during job execution."
);