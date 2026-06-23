-- Legacy Source: r_ausd_bp_ta_bpr_basis.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_basis.ksh

CREATE OR REPLACE PROCEDURE project.dataset.ausd_bp_ta_bpr_basis_wrapper(
    IN p_stichtag_in STRING,
    IN p_wiederanlaufWert_in INT64
)
OPTIONS(
    description="Wrapper procedure for r_ausd_bp_ta_bpr_basis.ksh, orchestrating job execution, logging, and calling the core kernel procedure."
)
BEGIN
    DECLARE v_job_kennung STRING;
    DECLARE v_dw_eintrags_nr INT64;
    DECLARE v_stichtag STRING;
    DECLARE v_wiederanlaufWert INT64;
    DECLARE v_sysdate_formatted STRING;
    DECLARE v_log_message STRING;
    DECLARE v_error_message STRING;
    DECLARE v_error_stack_trace STRING;

    -- 1. Initialize Job Kennung (UUID for unique job run ID)
    SET v_job_kennung = GENERATE_UUID();

    -- 2. Determine DW_EintragsNr (sequential job entry number for kernel tracking)
    -- NOTE: This simple MAX + 1 approach can lead to race conditions if multiple jobs start simultaneously.
    -- A more robust solution might involve a dedicated sequence table or external atomic counter as noted in design.
    SELECT IFNULL(MAX(kernel_job_entry_nr), 0) + 1 INTO v_dw_eintrags_nr FROM project.dataset.job_log;

    -- 3. Handle input parameters and defaulting logic
    -- Default p_wiederanlaufWert if not provided (replaces: if [[ -z "$p_wiederanlaufWert" ]])
    SET v_wiederanlaufWert = COALESCE(p_wiederanlaufWert_in, 0);

    -- Derive system date in 'DDMMYYYY' format (replaces DWDate_Gib_Zeitraum)
    SET v_sysdate_formatted = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

    -- Default p_stichtag if not provided or empty (replaces: if [[ -z "$p_stichtag" ]])
    SET v_stichtag = COALESCE(NULLIF(TRIM(p_stichtag_in), ''), v_sysdate_formatted);

    -- 4. Parameter Validation (replaces: pruefeParameterGesetzt Stichtag p_stichtag)
    IF v_stichtag IS NULL OR LENGTH(v_stichtag) != 8 THEN
        SET v_error_message = 'ERROR: Required parameter Stichtag is missing or invalid. Format should be DDMMYYYY.';
        INSERT INTO project.dataset.job_log (
            job_id, job_name, entry_timestamp, log_level, message, status, processing_date, restart_value, error_details, kernel_job_entry_nr
        ) VALUES (
            v_job_kennung, 'ausd_bp_ta_bpr_basis_wrapper', CURRENT_TIMESTAMP(), 'ERROR', v_error_message, 'FAILED_PARAM_VALIDATION', PARSE_DATE('%d%m%Y', '19000101'), v_wiederanlaufWert, v_error_message, v_dw_eintrags_nr
        );
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END IF;

    -- 5. Initial Log Entry (replaces DWMSG_ErzeugeEintrag initial and similar functions)
    SET v_log_message = 'Job started for Stichtag: ' || v_stichtag || ', Wiederanlaufwert: ' || v_wiederanlaufWert || ', DW_EintragsNr: ' || CAST(v_dw_eintrags_nr AS STRING);
    INSERT INTO project.dataset.job_log (
        job_id, job_name, entry_timestamp, log_level, message, status, processing_date, restart_value, kernel_job_entry_nr
    ) VALUES (
        v_job_kennung, 'ausd_bp_ta_bpr_basis_wrapper', CURRENT_TIMESTAMP(), 'INFO', v_log_message, 'STARTED', PARSE_DATE('%d%m%Y', v_stichtag), v_wiederanlaufWert, v_dw_eintrags_nr
    );

    -- 6. Core Processing and Error Handling (replaces calling k_ausd_bp_ta_bpr_basis.ksh and trap functions)
    BEGIN
        -- Log before calling the kernel procedure
        SET v_log_message = 'Calling core kernel procedure project.dataset.k_ausd_bp_ta_bpr_basis...';
        INSERT INTO project.dataset.job_log (
            job_id, job_name, entry_timestamp, log_level, message, status, processing_date, restart_value, kernel_job_entry_nr
        ) VALUES (
            v_job_kennung, 'ausd_bp_ta_bpr_basis_wrapper', CURRENT_TIMESTAMP(), 'INFO', v_log_message, 'RUNNING', PARSE_DATE('%d%m%Y', v_stichtag), v_wiederanlaufWert, v_dw_eintrags_nr
        );

        -- Call the core kernel stored procedure with derived parameters
        CALL project.dataset.k_ausd_bp_ta_bpr_basis(v_job_kennung, v_stichtag, v_dw_eintrags_nr, v_wiederanlaufWert);

        -- If the kernel procedure completes successfully
        SET v_log_message = 'Job completed successfully for Stichtag: ' || v_stichtag || '.';
        INSERT INTO project.dataset.job_log (
            job_id, job_name, entry_timestamp, log_level, message, status, processing_date, restart_value, kernel_job_entry_nr
        ) VALUES (
            v_job_kennung, 'ausd_bp_ta_bpr_basis_wrapper', CURRENT_TIMESTAMP(), 'INFO', v_log_message, 'COMPLETED', PARSE_DATE('%d%m%Y', v_stichtag), v_wiederanlaufWert, v_dw_eintrags_nr
        );

    EXCEPTION WHEN ERROR THEN
        -- Capture error details (replaces shell's $? and error capture)
        SET v_error_message = @@error.message;
        SET v_error_stack_trace = @@error.stack_trace;

        -- Log the error (replaces DWMSG_Fehlerbehandlung)
        SET v_log_message = 'Job failed for Stichtag: ' || v_stichtag || '. Error: ' || v_error_message;
        INSERT INTO project.dataset.job_log (
            job_id, job_name, entry_timestamp, log_level, message, status, processing_date, restart_value, error_details, kernel_job_entry_nr
        ) VALUES (
            v_job_kennung, 'ausd_bp_ta_bpr_basis_wrapper', CURRENT_TIMESTAMP(), 'ERROR', v_log_message, 'FAILED', PARSE_DATE('%d%m%Y', v_stichtag), v_wiederanlaufWert, v_error_message || '\n' || v_error_stack_trace, v_dw_eintrags_nr
        );

        -- Re-raise the error to propagate it to the caller (equivalent of shell exiting with error code)
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Job project.dataset.ausd_bp_ta_bpr_basis_wrapper failed. Check project.dataset.job_log for details. Error: ' || v_error_message;
    END;
END;