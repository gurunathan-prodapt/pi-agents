-- BigQuery Stored Procedure
-- Replaces the wrapper script vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier_zusgf.ksh
-- This procedure orchestrates the execution of the kernel script's logic.

CREATE OR REPLACE PROCEDURE `project.dataset.sp_r_ausd_v_ta_barrier_zusgf`(
    IN p_job_kennung STRING,
    IN p_entry_number INT64,
    IN p_debug_mode BOOL,
    IN p_test_mode BOOL
)
BEGIN
    DECLARE v_job_start_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    DECLARE v_sysdate_ddmmyyyy STRING;
    DECLARE v_job_name STRING DEFAULT UPPER(COALESCE(p_job_kennung, 'BERT_V_TA_BARRIER_ZUSGF'));
    DECLARE v_log_entry_number INT64 DEFAULT p_entry_number;
    DECLARE v_status STRING;
    DECLARE v_message STRING;
    DECLARE v_error_code INT64;
    DECLARE v_error_arg STRING;

    -- Initialize v_sysdate_ddmmyyyy
    SET v_sysdate_ddmmyyyy = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

    -- Log job start
    INSERT INTO `project.dataset.job_log` (
        job_name,
        job_number,
        log_level,
        message,
        status,
        script_name,
        created_at,
        stichtag,
        stichtag_format
    )
    VALUES (
        v_job_name,
        v_log_entry_number,
        'INFO',
        'Job started.',
        'RUNNING',
        'sp_r_ausd_v_ta_barrier_zusgf',
        v_job_start_ts,
        v_sysdate_ddmmyyyy,
        'DDMMYYYY'
    );

    IF p_debug_mode THEN
        SELECT FORMAT("----------------- Job -----------------------");
        SELECT FORMAT(" Job-Nr    : '%d'", v_log_entry_number);
        SELECT FORMAT(" JobKennung: '%s'", v_job_name);
        SELECT FORMAT(" Logdatei  : N/A (using BQ logging table)");
        SELECT FORMAT(" ---------------------------------------------");
    END IF;

    BEGIN
        -- Call the kernel script's stored procedure
        -- The parameters passed here should align with the kernel script's expected inputs.
        -- Based on the original shell script passing "-j $JobKennung -f ${DW_EintragsNr}"
        CALL `project.dataset.sp_k_ausd_v_ta_barrier_zusgf`(
            p_job_kennung => v_job_name,
            p_entry_number => v_log_entry_number
        );

        SET v_status = 'SUCCESS';
        SET v_message = 'Die Abarbeitung wurde ohne erkennbare Fehler beendet';

        IF p_debug_mode THEN
            SELECT v_message;
        END IF;

        -- Update job status to success
        UPDATE `project.dataset.job_log`
        SET
            status = v_status,
            message = v_message,
            finished_at = CURRENT_TIMESTAMP()
        WHERE
            job_name = v_job_name
            AND job_number = v_log_entry_number
            AND created_at = v_job_start_ts;

    EXCEPTION WHEN ERROR THEN
        SET v_status = 'FAILED';
        SET v_error_code = CAST(SUBSTR(SQLSTATE(), 1, 5) AS INT64); -- Extract a numeric error code
        SET v_error_arg = ERROR_MESSAGE();
        SET v_message = FORMAT('AppError: Abbruch - %s', v_error_arg);

        IF p_debug_mode THEN
            SELECT v_message;
            SELECT ERROR_MESSAGE();
            SELECT ERROR_STACK_TRACE();
        END IF;

        -- Log the error
        UPDATE `project.dataset.job_log`
        SET
            status = v_status,
            log_level = 'ERROR',
            error_code = v_error_code,
            error_arg = v_error_arg,
            message = v_message,
            finished_at = CURRENT_TIMESTAMP()
        WHERE
            job_name = v_job_name
            AND job_number = v_log_entry_number
            AND created_at = v_job_start_ts;

        RAISE USING MESSAGE = v_message; -- Re-raise the error to signal failure to the caller
    END;
END;