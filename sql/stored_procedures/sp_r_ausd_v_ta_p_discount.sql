-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount.ksh
-- Job: DW.BERT_AUSD_V_TA_P_DISCOUNT
CREATE OR REPLACE PROCEDURE `your-gcp-project.your_dataset.sp_r_ausd_v_ta_p_discount`(
    p_processing_date DATE -- Corresponds to v_sysdate in KSH (date +%d%m%Y)
)
BEGIN
    DECLARE v_job_name STRING DEFAULT 'DW.BERT_V_TA_P_DISCOUNT';
    DECLARE v_job_id STRING;
    DECLARE v_start_time TIMESTAMP;

    SET v_job_id = GENERATE_UUID();
    SET v_start_time = CURRENT_TIMESTAMP();

    -- Initialize job control entry
    INSERT INTO `your-gcp-project.your_dataset.job_control` (job_id, job_name, start_time, status, processing_date)
    VALUES (v_job_id, v_job_name, v_start_time, 'RUNNING', p_processing_date);

    -- Log start of wrapper script execution
    INSERT INTO `your-gcp-project.your_dataset.job_log` (job_id, log_level, message)
    VALUES (v_job_id, 'INFO', FORMAT("Starting sp_r_ausd_v_ta_p_discount with Job ID: %s, Job Name: %s", v_job_id, v_job_name));

    BEGIN
        -- Call the core script stored procedure
        CALL `your-gcp-project.your_dataset.sp_k_ausd_v_ta_p_discount`(v_job_id, v_job_name);

        -- Update job control to success
        UPDATE `your-gcp-project.your_dataset.job_control`
        SET
            end_time = CURRENT_TIMESTAMP(),
            status = 'COMPLETED'
        WHERE
            job_id = v_job_id;

        -- Log successful completion
        INSERT INTO `your-gcp-project.your_dataset.job_log` (job_id, log_level, message)
        VALUES (v_job_id, 'INFO', "sp_r_ausd_v_ta_p_discount completed successfully.");

    EXCEPTION WHEN ERROR THEN
        -- Update job control to failed
        UPDATE `your-gcp-project.your_dataset.job_control`
        SET
            end_time = CURRENT_TIMESTAMP(),
            status = 'FAILED'
        WHERE
            job_id = v_job_id;

        -- Log error in job_log as well
        INSERT INTO `your-gcp-project.your_dataset.job_log` (job_id, log_level, message)
        VALUES (v_job_id, 'ERROR', FORMAT("sp_r_ausd_v_ta_p_discount failed: %s", ERROR_MESSAGE()));

        -- Re-raise the error to Airflow
        RAISE;
    END;

END;