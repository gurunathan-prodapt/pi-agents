-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh
-- Purpose: BigQuery Stored Procedure for logging errors and messages, replacing f_alis_msgerr.ksh.

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset.log_message`(
    p_log_level STRING,
    p_job_run_id STRING,
    p_job_name STRING,
    p_job_kennung STRING,
    p_eintrags_nr STRING,
    p_message STRING,
    p_error_code INT64 DEFAULT NULL,
    p_error_argument STRING DEFAULT NULL
)
BEGIN
    INSERT INTO `your_project_id.your_dataset.job_log` (
        log_timestamp, log_level, job_run_id, job_name, job_kennung, eintrags_nr, message, error_code, error_argument
    )
    VALUES (
        CURRENT_TIMESTAMP(), p_log_level, p_job_run_id, p_job_name, p_job_kennung, p_eintrags_nr, p_message, p_error_code, p_error_argument
    );
END;