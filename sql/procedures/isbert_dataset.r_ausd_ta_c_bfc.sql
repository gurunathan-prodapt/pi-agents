-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh
-- This file defines the BigQuery Stored Procedure for orchestration, replacing the KornShell script.

CREATE OR REPLACE PROCEDURE `isbert_dataset.r_ausd_ta_c_bfc`(
    p_jobkennung STRING,
    p_eintragsnr STRING
)
BEGIN
    DECLARE job_run_id STRING;
    DECLARE records_processed INT64;
    DECLARE job_status STRING;
    DECLARE error_message STRING;

    -- Generate a unique run ID for this execution
    SET job_run_id = GENERATE_UUID();

    -- Parameter Validation
    IF p_jobkennung IS NULL OR p_jobkennung = '' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'ERROR: Parameter p_jobkennung is missing or empty.';
    END IF;

    IF p_eintragsnr IS NULL OR p_eintragsnr = '' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'ERROR: Parameter p_eintragsnr is missing or empty.';
    END IF;

    -- Job Status Management: Check for active jobs (ignoring for this run as per design)
    -- Deactivate any previously 'RUNNING' jobs for this job_kennung before starting a new one.
    UPDATE `isbert_dataset.job_status_log`
    SET
        end_timestamp = CURRENT_TIMESTAMP(),
        status = 'DEACTIVATED',
        message = 'Deactivated by new job run.'
    WHERE
        job_kennung = p_jobkennung
        AND status = 'RUNNING';

    -- Record new job execution as 'RUNNING'
    INSERT INTO `isbert_dataset.job_status_log` (
        job_kennung,
        eintrags_nr,
        start_timestamp,
        status,
        message
    )
    VALUES (
        p_jobkennung,
        p_eintragsnr,
        CURRENT_TIMESTAMP(),
        'RUNNING',
        'Job started successfully.'
    );

    BEGIN
        -- Call the core data processing procedure
        CALL `isbert_dataset.d_ausd_v_ta_c_bfc`(p_eintragsnr, p_jobkennung, records_processed);

        SET job_status = 'COMPLETED';
        SET error_message = 'Job completed successfully.';

    EXCEPTION WHEN ERROR THEN
        SET job_status = 'FAILED';
        SET error_message = @@error.message;
        -- Re-raise the error to propagate it to the caller
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = error_message;
    END;

    -- Update job status log with completion details
    UPDATE `isbert_dataset.job_status_log`
    SET
        end_timestamp = CURRENT_TIMESTAMP(),
        status = job_status,
        record_count = records_processed,
        message = error_message
    WHERE
        job_kennung = p_jobkennung
        AND eintrags_nr = p_eintragsnr
        AND status = 'RUNNING'; -- Only update the job that was marked as RUNNING by this procedure.

END;