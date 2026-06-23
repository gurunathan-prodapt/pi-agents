-- BigQuery DDL for config_job_control
-- Replaces implicit configuration and dynamic script invocation in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs2.ksh
-- This table stores metadata about jobs, including program names and the BigQuery stored procedure names they invoke.

CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.YOUR_DATASET_ID.config_job_control` (
    job_kennung STRING NOT NULL,                  -- Unique identifier for the job (e.g., TA_CNTRCT_CRS2)
    program_name STRING NOT NULL,                 -- Display name for the program (e.g., r_ausd_v_ta_cntrct_crs2)
    kernel_script_name STRING NOT NULL,           -- Name of the BigQuery stored procedure for the core logic (e.g., sp_k_ausd_v_ta_cntrct_crs2)
    description STRING,                           -- Description of the job
    is_active BOOL NOT NULL DEFAULT TRUE,         -- Flag to enable/disable the job
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Example INSERT for the job:
-- INSERT INTO `YOUR_PROJECT_ID.YOUR_DATASET_ID.config_job_control` (job_kennung, program_name, kernel_script_name, description)
-- VALUES ('TA_CNTRCT_CRS2', 'r_ausd_v_ta_cntrct_crs2', 'sp_k_ausd_v_ta_cntrct_crs2', 'Contract reconciliation wrapper job');