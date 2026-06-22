--
-- Helper BigQuery Stored Procedure to log errors.
-- Replaces aspects of f_alis_msgerr.ksh from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh
--
CREATE OR REPLACE PROCEDURE `bq_dataset.sp_log_error`(
    IN p_run_id STRING,
    IN p_job_kennung STRING,
    IN p_eintrags_nr STRING,
    IN p_error_message STRING
)
BEGIN
    INSERT INTO `bq_dataset.job_run_log`
        (run_id, job_kennung, eintrags_nr, start_time, end_time, status, records_processed, error_message)
    VALUES
        (p_run_id, p_job_kennung, p_eintrags_nr, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'FAILED', 0, p_error_message);
END;