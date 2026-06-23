--
-- BigQuery Stored Procedure: sp_vertragsdatenabgleich
-- This procedure is the migrated orchestration wrapper for the contract data reconciliation process.
-- It replaces the KornShell script: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_def.ksh
--
CREATE OR REPLACE PROCEDURE `project.dataset.sp_vertragsdatenabgleich`(
    p_help BOOL DEFAULT FALSE,
    p_source_param STRING DEFAULT NULL, -- Corresponds to -s in original script
    p_log_param STRING DEFAULT NULL    -- Corresponds to -l in original script
)
BEGIN
    DECLARE v_job_name STRING DEFAULT 'Vertragsdatenabgleich';
    DECLARE v_job_version STRING DEFAULT 'V1.0.0';
    DECLARE v_job_kennung STRING DEFAULT 'BERT_V_TA_INV_DEF';
    DECLARE v_entry_number STRING;
    DECLARE v_job_id STRING;
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_end_time TIMESTAMP;
    DECLARE v_status STRING;
    DECLARE v_message STRING;
    DECLARE v_stichtag_info DATE;
    DECLARE v_log_params JSON;

    -- Helper procedure for usage/help message (can be expanded)
    IF p_help THEN
        SELECT 'Programm: ' || v_job_name || '\nVersion: ' || v_job_version || '\n\nBeschreibung:\n    Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_inv_def.' ||
               '\n\nParameter:\n    p_help (BOOLEAN): Shows this help message.' ||
               '\n    p_source_param (STRING): Placeholder for -s parameter.' ||
               '\n    p_log_param (STRING): Placeholder for -l parameter.';
        RETURN;
    END IF;

    -- Store input parameters as JSON for auditing
    SET v_log_params = TO_JSON(STRUCT(p_source_param, p_log_param));

    -- Generate a unique entry number for this job run
    SET v_entry_number = GENERATE_UUID();
    SET v_job_id = FORMAT('%s_%s', v_job_kennung, v_entry_number);
    SET v_start_time = CURRENT_TIMESTAMP();
    SET v_stichtag_info = CURRENT_DATE();
    SET v_status = 'RUNNING';
    SET v_message = 'Job started.';

    -- Log job start into job_audit table
    INSERT INTO `project.dataset.job_audit` (
        job_id, job_key, job_name, job_version, entry_number, start_time, status, message, stichtag_info, parameters
    ) VALUES (
        v_job_id, v_job_kennung, v_job_name, v_job_version, v_entry_number, v_start_time, v_status, v_message, v_stichtag_info, v_log_params
    );

    -- Print job details (equivalent to original script's 'print' statements)
    SELECT FORMAT('----------------- Job -----------------------\n Job-Nr    : \'%s\'\n JobKennung: \'%s\'\n Stichtag  : \'%t\'\n---------------------------------------------', v_entry_number, v_job_kennung, v_stichtag_info) AS job_info;

    -- Error handling block
    BEGIN
        -- Call the core logic stored procedure (placeholder for k_ausd_v_ta_inv_def.ksh migration)
        -- This procedure will need to be created separately.
        -- Parameters: job_key, entry_number, and potentially other params from original -s, -l.
        -- Note: The original ksh script passed -j $JobKennung -f ${DW_EintragsNr}
        -- We are assuming sp_k_ausd_v_ta_inv_def will take these as parameters along with p_source_param and p_log_param.
        CALL `project.dataset.sp_k_ausd_v_ta_inv_def`(
            v_job_kennung,
            v_entry_number,
            p_source_param,
            p_log_param
        );

        SET v_end_time = CURRENT_TIMESTAMP();
        SET v_status = 'SUCCESS';
        SET v_message = 'Job completed successfully.';

        -- Update job_audit with success status
        UPDATE `project.dataset.job_audit`
        SET
            end_time = v_end_time,
            status = v_status,
            message = v_message
        WHERE job_id = v_job_id;

        SELECT 'Die Abarbeitung wurde ohne erkennbare Fehler beendet' AS completion_message;

    EXCEPTION WHEN ERROR THEN
        SET v_end_time = CURRENT_TIMESTAMP();
        SET v_status = 'FAILED';
        SET v_message = 'Job failed with an error.';

        -- Log detailed error to job_error_log
        INSERT INTO `project.dataset.job_error_log` (
            job_id, job_key, entry_number, error_time, error_message, stack_trace, procedure_name, severity
        ) VALUES (
            v_job_id,
            v_job_kennung,
            v_entry_number,
            v_end_time,
            @@error.message,
            @@error.stack_trace,
            'sp_vertragsdatenabgleich',
            'ERROR'
        );

        -- Update job_audit with failure status
        UPDATE `project.dataset.job_audit`
        SET
            end_time = v_end_time,
            status = v_status,
            message = v_message
        WHERE job_id = v_job_id;

        -- Re-raise the error to propagate it if needed
        RAISE;
    END;
END;