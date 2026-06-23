-- Legacy Source: N/A (new DDL for BigQuery)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs.ksh

-- DDL for project.dataset.job_control table
-- This table stores metadata and status for job executions, replacing aspects of
-- shell script control flow and environment variables.
CREATE OR REPLACE TABLE `project.dataset.job_control` (
    job_id INT64 NOT NULL,
    job_kennung STRING NOT NULL,
    program_name STRING NOT NULL,
    program_version STRING NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status STRING NOT NULL, -- e.g., 'RUNNING', 'OK', 'ERROR'
    log_file_name STRING,   -- Conceptual, could refer to a GCS log path
    reference_date DATE     -- Stichtag (key date for processing)
);

-- DDL for project.dataset.job_log table
-- This table captures detailed log messages from the job execution,
-- replacing stdout/stderr redirection to a log file.
CREATE OR REPLACE TABLE `project.dataset.job_log` (
    job_id INT64 NOT NULL,
    log_timestamp TIMESTAMP NOT NULL,
    log_level STRING NOT NULL, -- e.g., 'INFO', 'WARNING', 'ERROR'
    message STRING NOT NULL
);

-- DDL for project.dataset.job_error_log table
-- This table records specific error details, replacing shell error handling
-- and dedicated error logging utilities.
CREATE OR REPLACE TABLE `project.dataset.job_error_log` (
    job_id INT64 NOT NULL,
    error_timestamp TIMESTAMP NOT NULL,
    error_code STRING,
    error_message STRING NOT NULL,
    error_details STRING
);