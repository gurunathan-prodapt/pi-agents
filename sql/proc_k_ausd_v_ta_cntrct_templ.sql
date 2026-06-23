-- BigQuery Stored Procedure for k_ausd_v_ta_cntrct_templ.ksh
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_templ.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_templ.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.proc_k_ausd_v_ta_cntrct_templ`(
    p_jobkennung STRING,
    p_eintragsnr INT64
)
BEGIN
    -- Declare variables
    DECLARE v_records_processed INT64;
    DECLARE v_error_message STRING;
    DECLARE v_error_stack STRING;

    -- Parameter validation (equivalent to pruefeParameterGesetzt from h_alis_parameter.ksh)
    IF p_jobkennung IS NULL OR p_jobkennung = '' THEN
        RAISE USING MESSAGE = 'FEHLER: Parameter "Jobkennung" (p_jobkennung) must be provided.';
    END IF;
    IF p_eintragsnr IS NULL THEN
        RAISE USING MESSAGE = 'FEHLER: Parameter "EintragsNr" (p_eintragsnr) must be provided.';
    END IF;

    -- Job Control Logic Placeholder:
    -- The original KornShell script had logic to ignore active jobs and deactivate old jobs.
    -- This functionality would typically be handled by an external orchestrator (e.g., Cloud Composer)
    -- or by interacting with a dedicated job control table in BigQuery.
    -- Example (assuming `project.dataset.job_control_table` exists and has columns `job_name`, `entry_number`, `status`):
    /*
    -- Check if job is already active (simplified example)
    IF EXISTS (SELECT 1 FROM `project.dataset.job_control_table` WHERE job_name = p_jobkennung AND status = 'ACTIVE') THEN
        SELECT FORMAT('INFO: Job %s (EintragsNr: %d) is already active. Ignoring this execution.', p_jobkennung, p_eintragsnr);
        RETURN; -- Exit without further processing if already active
    END IF;

    -- Update status for older active jobs (simplified example)
    UPDATE `project.dataset.job_control_table`
    SET status = 'DEACTIVATED', end_time = CURRENT_TIMESTAMP()
    WHERE job_name = p_jobkennung AND status = 'ACTIVE' AND entry_number < p_eintragsnr;

    -- Log job start
    INSERT INTO `project.dataset.job_control_table` (job_name, entry_number, status, start_time)
    VALUES (p_jobkennung, p_eintragsnr, 'ACTIVE', CURRENT_TIMESTAMP());
    */

    -- Exception handling block (equivalent to shell script's error concept)
    BEGIN
        -- Call the core data processing logic
        CALL `project.dataset.proc_d_ausd_v_ta_cntrct_templ`();

        -- Get record count from the target table (replaces reading from tmpFile)
        SELECT COUNT(*) INTO v_records_processed FROM `project.dataset.sof_ta_cntrct_templ`;

        -- Log success (placeholder for DWMSG_MeldeFehler and shell print statements)
        SELECT FORMAT('INFO: Job %s (EintragsNr: %d) completed successfully. %d records processed.', p_jobkennung, p_eintragsnr, v_records_processed) AS log_message;
        /*
        -- Update job control table on success
        UPDATE `project.dataset.job_control_table`
        SET status = 'COMPLETED', records_processed = v_records_processed, end_time = CURRENT_TIMESTAMP()
        WHERE job_name = p_jobkennung AND entry_number = p_eintragsnr AND status = 'ACTIVE';
        */

    EXCEPTION WHEN ERROR THEN
        SET v_error_message = @@error.message;
        SET v_error_stack = @@error.stack_trace;
        -- Log failure (placeholder for DWMSG_MeldeFehler)
        SELECT FORMAT('ERROR: Job %s (EintragsNr: %d) failed. Message: %s. Stack: %s', p_jobkennung, p_eintragsnr, v_error_message, v_error_stack) AS log_message;
        /*
        -- Update job control table on failure
        UPDATE `project.dataset.job_control_table`
        SET status = 'FAILED', error_message = v_error_message, error_stack = v_error_stack, end_time = CURRENT_TIMESTAMP()
        WHERE job_name = p_jobkennung AND entry_number = p_eintragsnr AND status = 'ACTIVE';
        */
        RAISE USING MESSAGE = FORMAT('ERROR: Job %s (EintragsNr: %d) failed. Message: %s. For more details, check logs.', p_jobkennung, p_eintragsnr, v_error_message);
    END;

END;