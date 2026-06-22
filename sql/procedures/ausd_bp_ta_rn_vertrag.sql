-- BigQuery Stored Procedure for main orchestration
-- Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_vertrag.ksh
CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_rn_vertrag`(
    IN p_stichtag_in STRING, -- DDMMYYYY
    IN p_wiederanlaufWert_in INT64
)
BEGIN
    DECLARE v_job_kennung STRING DEFAULT 'ausd_bp_ta_rn_vertrag';
    DECLARE v_run_id STRING;
    DECLARE v_stichtag STRING;
    DECLARE v_wiederanlaufWert INT64;
    DECLARE v_error_message STRING;

    -- Generate a unique run ID for this execution
    SET v_run_id = GENERATE_UUID();

    -- Set default Stichtag if not provided, otherwise use input
    IF p_stichtag_in IS NULL OR p_stichtag_in = '' THEN
        SET v_stichtag = FORMAT_DATE('%d%m%Y', CURRENT_DATE());
    ELSE
        SET v_stichtag = p_stichtag_in;
    END IF;

    -- Initialize Wiederanlaufwert
    IF p_wiederanlaufWert_in IS NULL THEN
        SET v_wiederanlaufWert = 0;
    ELSE
        SET v_wiederanlaufWert = p_wiederanlaufWert_in;
    END IF;

    BEGIN
        -- Initial logging for the job run
        INSERT INTO `project.dataset.job_audit` (job_kennung, run_id, status, message, created_at)
        VALUES (v_job_kennung, v_run_id, 'STARTED', CONCAT('Job started with Stichtag: ', v_stichtag, ', Wiederanlaufwert: ', v_wiederanlaufWert), CURRENT_TIMESTAMP());

        -- Call the internal control script
        CALL `project.dataset.k_ausd_bp_ta_rn_vertrag`(v_job_kennung, v_run_id, v_stichtag, v_wiederanlaufWert);

        -- Final success logging (if k_ausd_bp_ta_rn_vertrag doesn't re-raise error)
        -- Note: k_ausd_bp_ta_rn_vertrag already logs success, this is for top-level status
        UPDATE `project.dataset.job_audit`
        SET status = 'COMPLETED', message = CONCAT('Job completed successfully. Stichtag: ', v_stichtag)
        WHERE job_kennung = v_job_kennung AND run_id = v_run_id AND status != 'FAILED';

    EXCEPTION WHEN ERROR THEN
        SET v_error_message = @@error.message;
        -- Error handling and logging
        UPDATE `project.dataset.job_audit`
        SET status = 'FAILED', message = CONCAT('Job failed: ', v_error_message)
        WHERE job_kennung = v_job_kennung AND run_id = v_run_id;
        RAISE; -- Re-raise the error to inform the caller/scheduler
    END;
END;