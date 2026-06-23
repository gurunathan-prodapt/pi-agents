--
-- BigQuery table for logging detailed job errors
-- Replaces error logging functionality from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_def.ksh
--
CREATE TABLE IF NOT EXISTS `project.dataset.job_error_log` (
    job_id STRING NOT NULL OPTIONS(description="Unique identifier for the job run, typically a combination of job key and entry number."),
    job_key STRING NOT NULL OPTIONS(description="Key identifier for the job, e.g., BERT_V_TA_INV_DEF."),
    entry_number STRING NOT NULL OPTIONS(description="Unique entry number for a specific job run."),
    error_time TIMESTAMP OPTIONS(description="Timestamp when the error occurred."),
    error_code INT OPTIONS(description="Numeric error code, if available from source system."),
    error_argument STRING OPTIONS(description="Argument associated with the error code, if available."),
    error_message STRING OPTIONS(description="Detailed error message."),
    stack_trace STRING OPTIONS(description="Stack trace or additional debug information."),
    procedure_name STRING OPTIONS(description="Name of the BigQuery stored procedure where the error occurred."),
    severity STRING OPTIONS(description="Severity level of the error (e.g., 'ERROR', 'WARNING').")
)
PARTITION BY DATE(error_time)
CLUSTER BY job_key, severity
OPTIONS(
    description="Log table for detailed error messages encountered during BigQuery job executions."
);