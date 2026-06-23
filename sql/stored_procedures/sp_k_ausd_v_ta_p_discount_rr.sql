-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount_rr.ksh
-- Job: DW.BERT_AUSD_V_TA_P_DISCOUNT
CREATE OR REPLACE PROCEDURE `your-gcp-project.your_dataset.sp_k_ausd_v_ta_p_discount_rr`(
    p_job_id STRING,
    p_job_name STRING
)
BEGIN
    -- This procedure orchestrates the SQL transformation for ta_p_discount_rr
    -- and handles basic logging/error reporting.

    -- Log start of core script execution
    INSERT INTO `your-gcp-project.your_dataset.job_log` (job_id, log_level, message)
    VALUES (p_job_id, 'INFO', FORMAT("Starting sp_k_ausd_v_ta_p_discount_rr for job '%s'.", p_job_name));

    BEGIN
        -- Call the SQL transformation stored procedure
        CALL `your-gcp-project.your_dataset.sp_d_ausd_v_ta_p_discount_rr`(p_job_id, p_job_name);

        -- Log successful completion
        INSERT INTO `your-gcp-project.your_dataset.job_log` (job_id, log_level, message)
        VALUES (p_job_id, 'INFO', FORMAT("Finished sp_k_ausd_v_ta_p_discount_rr for job '%s' successfully.", p_job_name));

    EXCEPTION WHEN ERROR THEN
        -- Log error details
        INSERT INTO `your-gcp-project.your_dataset.job_error_log` (job_id, error_message, script_name)
        VALUES (p_job_id, ERROR_MESSAGE(), 'sp_k_ausd_v_ta_p_discount_rr');
        -- Re-raise the error to propagate it up to the calling procedure (r_ script)
        RAISE;
    END;

END;