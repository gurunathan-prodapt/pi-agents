-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_valid.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_valid.ksh
-- Target: BigQuery Stored Procedures for logging utilities

-- This script provides utility stored procedures for managing entries in the job_log table.
-- Replace 'project' and 'dataset' with your actual GCP project ID and BigQuery dataset name.

-- Procedure to generate a unique job run ID
CREATE OR REPLACE PROCEDURE project.dataset.generate_job_run_id(OUT run_id STRING)
OPTIONS(
  description="Generates a unique UUID for a job execution run."
)
BEGIN
    SET run_id = GENERATE_UUID();
END;

-- Procedure to create a new job log entry with 'RUNNING' status
CREATE OR REPLACE PROCEDURE project.dataset.create_job_log_entry(
    IN p_job_name STRING,         -- The name of the BigQuery job or stored procedure
    IN p_parameters_json JSON,    -- JSON representation of input parameters for the job
    IN p_caller_process STRING,   -- The orchestrator or method that initiated the job
    OUT p_job_run_id STRING       -- Output parameter to return the generated job run ID
)
OPTIONS(
  description="Creates an initial 'RUNNING' entry in the job_log table for a new job execution."
)
BEGIN
    CALL project.dataset.generate_job_run_id(p_job_run_id);

    INSERT INTO project.dataset.job_log (
        job_run_id,
        job_name,
        start_timestamp,
        status,
        parameters_json,
        caller_process
    )
    VALUES (
        p_job_run_id,
        p_job_name,
        CURRENT_TIMESTAMP(),
        'RUNNING',
        p_parameters_json,
        p_caller_process
    );
END;

-- Procedure to update an existing job log entry with its final status
CREATE OR REPLACE PROCEDURE project.dataset.update_job_log_status(
    IN p_job_run_id STRING,       -- The unique identifier of the job run to update
    IN p_status STRING,           -- The final status of the job (e.g., 'SUCCESS', 'FAILED')
    IN p_error_message STRING     -- Detailed error message if the job failed; NULL if successful
)
OPTIONS(
  description="Updates an existing entry in the job_log table with the final status, end timestamp, and any error message."
)
BEGIN
    UPDATE project.dataset.job_log
    SET
        end_timestamp = CURRENT_TIMESTAMP(),
        status = p_status,
        error_message = p_error_message
    WHERE
        job_run_id = p_job_run_id;
END;