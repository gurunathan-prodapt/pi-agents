-- BigQuery DDL for job_error_log
-- Replaces error logging functionality of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs2.ksh
-- This table records detailed error information for failed job executions.

CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.YOUR_DATASET_ID.job_error_log` (
    job_id STRING NOT NULL,                  -- Identifier for the specific job
    entry_number INT64,                      -- Corresponds to job_execution_log.entry_number for the failed run
    error_timestamp TIMESTAMP NOT NULL,      -- Timestamp when the error occurred
    error_code INT64,                        -- Numeric error code
    error_message STRING NOT NULL,           -- Detailed error message
    stack_trace STRING,                      -- Optional: BigQuery's @@error.stack_trace
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Recommended: Clustering key if frequently querying by (job_id, entry_number).