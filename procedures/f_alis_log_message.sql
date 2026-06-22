-- Legacy Source: Logging functionality similar to DWMSG_ErzeugeEintrag from f_alis_msgerr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh
--
-- Helper procedure to log messages to the BigQuery job_log table.
-- Replace `your_gcp_project.your_bq_dataset` with your actual GCP project ID and BigQuery dataset name.

CREATE OR REPLACE PROCEDURE `your_gcp_project.your_bq_dataset.f_alis_log_message`(
    IN p_job_run_id STRING,
    IN p_job_name STRING,
    IN p_log_level STRING,
    IN p_message STRING,
    IN p_stichtag DATE,
    IN p_wiederanlaufwert INT64,
    IN p_process_id STRING,
    IN p_line_number INT64,
    IN p_error_code STRING,
    IN p_error_arg STRING
)
BEGIN
    INSERT INTO `your_gcp_project.your_bq_dataset.job_log` (
        job_run_id, job_name, log_timestamp, log_level, message, stichtag, wiederanlaufwert, process_id, line_number, error_code, error_arg, log_entry_id
    )
    VALUES (
        p_job_run_id,
        p_job_name,
        CURRENT_TIMESTAMP(),
        p_log_level,
        p_message,
        p_stichtag,
        p_wiederanlaufwert,
        p_process_id,
        p_line_number,
        p_error_code,
        p_error_arg,
        GENERATE_UUID() -- Unique ID for this log entry
    );
END;