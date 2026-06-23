-- Main orchestration stored procedure for contract data reconciliation
-- Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_acc.ksh
CREATE OR REPLACE PROCEDURE `project.dataset.sp_vertragsdatenabgleich`(
    IN p_reporting_date_str STRING, -- Expected format: DDMMYYYY, e.g., '26102023'
    IN p_mode STRING,              -- Expected values: 'TEST' or 'PROD'
    IN p_job_kennung_param STRING DEFAULT 'R_AUSD_V_TA_INV_ACC' -- Derived from original script name
)
BEGIN
    DECLARE v_job_id STRING;
    DECLARE v_run_id STRING;
    DECLARE v_reporting_date DATE;
    DECLARE v_error_message STRING;
    DECLARE v_error_stack_trace STRING;

    SET v_run_id = GENERATE_UUID();
    SET v_job_id = UPPER(p_job_kennung_param); -- Ensure uppercase as in original script

    -- Parameter validation: Reporting Date
    IF p_reporting_date_str IS NULL OR p_reporting_date_str = '' THEN
        SET v_error_message = 'ERROR: Reporting date parameter is missing. Expected DDMMYYYY.';
        CALL `project.dataset.sp_dwmsg_fehlerbehandlung`(v_job_id, v_run_id, 'PARAM_ERROR', v_error_message, 'sp_vertragsdatenabgleich', NULL);
    END IF;

    BEGIN
        SET v_reporting_date = PARSE_DATE('%d%m%Y', p_reporting_date_str);
    EXCEPTION WHEN ERROR THEN
        SET v_error_message = 'ERROR: Invalid reporting date format. Expected DDMMYYYY.';
        CALL `project.dataset.sp_dwmsg_fehlerbehandlung`(v_job_id, v_run_id, 'PARAM_ERROR', v_error_message, 'sp_vertragsdatenabgleich', @@error.stack_trace);
    END;

    -- Parameter validation: Mode
    IF p_mode IS NULL OR (p_mode != 'TEST' AND p_mode != 'PROD') THEN
        SET v_error_message = 'ERROR: Invalid or missing mode parameter. Expected ''TEST'' or ''PROD''.';
        CALL `project.dataset.sp_dwmsg_fehlerbehandlung`(v_job_id, v_run_id, 'PARAM_ERROR', v_error_message, 'sp_vertragsdatenabgleich', NULL);
    END IF;

    -- Log job start
    CALL `project.dataset.sp_dwmsg_erzeuge_eintrag`(v_job_id, v_run_id, 'Contract Data Reconciliation', 'Job started', 'RUNNING');

    BEGIN
        -- Call the core reconciliation procedure
        CALL `project.dataset.sp_k_ausd_v_ta_inv_acc`(v_job_id, v_run_id, v_reporting_date, p_mode);

        -- Log successful completion
        CALL `project.dataset.sp_dwmsg_setze_status_ok`(v_job_id, v_run_id, 'Job successfully completed');

    EXCEPTION WHEN ERROR THEN
        SET v_error_message = @@error.message;
        SET v_error_stack_trace = @@error.stack_trace;
        CALL `project.dataset.sp_dwmsg_fehlerbehandlung`(
            v_job_id,
            v_run_id,
            'EXECUTION_ERROR',
            v_error_message,
            'sp_k_ausd_v_ta_inv_acc_call', -- Source component where the error occurred
            v_error_stack_trace
        );
    END;
END;