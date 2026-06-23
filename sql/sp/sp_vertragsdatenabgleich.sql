-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh
-- Description: Migrated wrapper script logic to a BigQuery Stored Procedure for orchestration and logging.

CREATE OR REPLACE PROCEDURE `YOUR_PROJECT_ID.YOUR_DATASET_ID.sp_vertragsdatenabgleich`(
    IN p_s STRING, -- Placeholder, as original ksh did not use this parameter value
    IN p_l STRING  -- Placeholder, as original ksh did not use this parameter value
)
BEGIN
    DECLARE v_prog_name STRING DEFAULT 'Vertragsdatenabgleich';
    DECLARE v_prog_version STRING DEFAULT 'V1.0.0';
    DECLARE v_job_kennung STRING DEFAULT 'BERT_V_TA_CNTRCT_CRS3';
    DECLARE v_sysdate DATE DEFAULT CURRENT_DATE();
    DECLARE v_job_entry_id INT64;
    DECLARE v_log_message STRING;
    DECLARE v_status STRING;

    -- Generate a unique job_entry_id for this execution
    -- In a real production system, consider a more robust ID generation or sequence.
    SET v_job_entry_id = (SELECT COALESCE(MAX(job_entry_id), 0) + 1 FROM `YOUR_PROJECT_ID.YOUR_DATASET_ID.dw_job_log`);

    -- Log job start
    INSERT INTO `YOUR_PROJECT_ID.YOUR_DATASET_ID.dw_job_log` (
        job_entry_id, job_kennung, program_name, program_version, system_date, status, log_message
    ) VALUES (
        v_job_entry_id, v_job_kennung, v_prog_name, v_prog_version, v_sysdate, 'RUNNING', 'Job started.'
    );

    BEGIN
        -- Call the core data processing stored procedure
        CALL `YOUR_PROJECT_ID.YOUR_DATASET_ID.sp_k_ausd_v_ta_cntrct_crs3`(v_job_kennung, v_job_entry_id);

        SET v_status = 'OK';
        SET v_log_message = 'Job completed successfully.';

        -- Update job log for success
        UPDATE `YOUR_PROJECT_ID.YOUR_DATASET_ID.dw_job_log`
        SET
            end_timestamp = CURRENT_TIMESTAMP(),
            status = v_status,
            log_message = v_log_message
        WHERE job_entry_id = v_job_entry_id;

    EXCEPTION WHEN ERROR THEN
        SET v_status = 'ERROR';
        SET v_log_message = FORMAT("Job failed with error: %s", @@error.message);

        -- Log error details
        -- In a real production system, consider a more robust ID generation for error_id.
        INSERT INTO `YOUR_PROJECT_ID.YOUR_DATASET_ID.dw_error_log` (
            error_id, job_entry_id, job_kennung, error_code, error_argument, error_message
        ) VALUES (
            (SELECT COALESCE(MAX(error_id), 0) + 1 FROM `YOUR_PROJECT_ID.YOUR_DATASET_ID.dw_error_log`),
            v_job_entry_id,
            v_job_kennung,
            -- BigQuery's @@error.code provides a system-generated error code
            CAST(SUBSTR(@@error.message, STRPOS(@@error.message, 'Code: ') + 6, STRPOS(@@error.message, ' at') - (STRPOS(@@error.message, 'Code: ') + 6)) AS INT64),
            '', -- No specific argument from original script's context
            @@error.message
        );

        -- Update job log for failure
        UPDATE `YOUR_PROJECT_ID.YOUR_DATASET_ID.dw_job_log`
        SET
            end_timestamp = CURRENT_TIMESTAMP(),
            status = v_status,
            log_message = v_log_message
        WHERE job_entry_id = v_job_entry_id;

        RAISE; -- Re-raise the error to propagate it to the caller (e.g., Cloud Composer)
    END;
END;