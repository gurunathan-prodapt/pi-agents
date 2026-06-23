-- BigQuery Stored Procedure for Orchestration Logic
-- Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_acc_ref.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_acc_ref.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.sp_k_ausd_v_ta_acc_ref_control`(
    p_JobKennung STRING,
    p_EintragsNr STRING
)
BEGIN
    DECLARE v_TabName STRING DEFAULT 'ta_acc_ref';
    DECLARE records_processed INT64;
    DECLARE error_message STRING;
    DECLARE error_code INT64;

    -- Error handling block
    BEGIN
        -- Parameter Validation (replacing pruefeParameterGesetzt)
        IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
            SET error_code = 193; -- Notwendiges Argument fehlt
            SET error_message = 'FEHLER: p_JobKennung darf nicht leer sein.';
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = error_message;
        END IF;

        IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
            SET error_code = 193; -- Notwendiges Argument fehlt
            SET error_message = 'FEHLER: p_EintragsNr darf nicht leer sein.';
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = error_message;
        END IF;

        -- Job Management (replacing parts of starteSQLSkript)
        -- Deactivate any older active jobs for the same table
        UPDATE `project.dataset.job_table`
        SET status = 'DEACTIVATED', updated_at = CURRENT_TIMESTAMP()
        WHERE tab_name = v_TabName AND status = 'ACTIVE';

        -- Register the current job as active, or update if it exists
        INSERT INTO `project.dataset.job_table` (job_kennung, eintrags_nr, tab_name, status, created_at, updated_at)
        VALUES (p_JobKennung, p_EintragsNr, v_TabName, 'ACTIVE', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
        ON CONFLICT (job_kennung, eintrags_nr) DO UPDATE
        SET status = 'ACTIVE', updated_at = CURRENT_TIMESTAMP();

        -- Execute the data transformation logic
        CALL `project.dataset.sp_d_ausd_v_ta_acc_ref_transform`();

        -- Get the number of records processed
        SELECT COUNT(*)
        INTO records_processed
        FROM `project.dataset.sof_ta_acc_ref`;

        -- Log job completion
        INSERT INTO `project.dataset.job_log` (job_kennung, eintrags_nr, tab_name, record_count, created_at)
        VALUES (p_JobKennung, p_EintragsNr, v_TabName, records_processed, CURRENT_TIMESTAMP());

        -- Update job status to completed
        UPDATE `project.dataset.job_table`
        SET status = 'COMPLETED', updated_at = CURRENT_TIMESTAMP()
        WHERE job_kennung = p_JobKennung AND eintrags_nr = p_EintragsNr;

    EXCEPTION WHEN ERROR THEN
        SET error_message = @@error.message;
        SET error_code = COALESCE(error_code, -1); -- Use specific error code if set, otherwise a generic one

        -- Log the error
        INSERT INTO `project.dataset.error_log` (error_code, error_message, job_kennung, eintrags_nr, created_at)
        VALUES (error_code, error_message, p_JobKennung, p_EintragsNr, CURRENT_TIMESTAMP());

        -- Update job status to failed
        UPDATE `project.dataset.job_table`
        SET status = 'FAILED', updated_at = CURRENT_TIMESTAMP()
        WHERE job_kennung = p_JobKennung AND eintrags_nr = p_EintragsNr;

        -- Re-raise the error for external orchestration systems
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = error_message;
    END;
END;