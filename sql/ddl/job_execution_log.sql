-- BigQuery DDL for job_execution_log
-- Replaces logging functionality of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs2.ksh
-- This table records the start, end, and status of job executions.

CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.YOUR_DATASET_ID.job_execution_log` (
    job_id STRING NOT NULL,                  -- Identifier for the specific job (e.g., TA_CNTRCT_CRS2)
    entry_number INT64 NOT NULL,             -- Unique identifier for each job execution instance
    start_timestamp TIMESTAMP NOT NULL,      -- Timestamp when the job execution started
    end_timestamp TIMESTAMP,                 -- Timestamp when the job execution ended
    status STRING NOT NULL,                  -- Status of the execution (e.g., 'STARTED', 'OK', 'FAILED')
    message STRING,                          -- Descriptive message about the execution status
    parameters_json JSON,                    -- JSON representation of input parameters for the job run
    stichtag DATE,                           -- Reference date for the job run (like 'Stichtag' in original script)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Recommended: Add a primary key if this were a traditional relational database.
-- In BigQuery, you might enforce uniqueness through application logic or a clustering key
-- if you frequently query by (job_id, entry_number).
-- For example, you could cluster by (job_id, entry_number).