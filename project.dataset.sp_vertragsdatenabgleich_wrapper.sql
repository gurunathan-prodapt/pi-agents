-- Wrapper stored procedure for contract data reconciliation
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh

CREATE OR REPLACE PROCEDURE project.dataset.sp_vertragsdatenabgleich_wrapper(
    IN p_jobkennung STRING,
    IN p_stichtag DATE OPTIONS(description="Stichtag parameter -s, e.g., 'YYYY-MM-DD'"),
    IN p_log_file_base_name STRING OPTIONS(description="Base name for log file -l, e.g., 'r_ausd_v_ta_p_vertrag'")
)
OPTIONS(
    description="Migrated KornShell wrapper script r_ausd_v_ta_p_vertrag.ksh to BigQuery Stored Procedure."
)
BEGIN
    DECLARE v_prog_name STRING DEFAULT 'r_ausd_v_ta_p_vertrag.ksh';
    DECLARE v_prog_version STRING DEFAULT '1.0';
    DECLARE v_err_nr INT64 DEFAULT 0;
    DECLARE v_err_arg STRING DEFAULT '';
    DECLARE v_eintragsnr INT64;
    DECLARE v_log_name STRING;
    DECLARE v_sysdate DATE;
    DECLARE v_stichtag_info STRING;
    DECLARE v_core_script_name STRING DEFAULT 'k_ausd_v_ta_p_vertrag.ksh'; -- Name of the original core script

    -- --- Error Handling Block (equivalent to 'trap' and DWMSG_Fehlerbehandlung) ---
    BEGIN
        -- Set system date
        SET v_sysdate = CURRENT_DATE();

        -- 1. Generate unique entry number (DWMSG_ErmittleNr replacement)
        -- Uses the provided p_jobkennung to get the next entry number.
        -- If an explicit entry number is needed as a parameter, the signature must be updated.
        SELECT COALESCE(MAX(eintragsnr), 0) + 1
        INTO v_eintragsnr
        FROM project.dataset.job_control
        WHERE job_kennung = p_jobkennung;

        -- 2. Generate log file name (DWMSG_Logdateiname replacement)
        -- The original script generates a log file name; here we store it for reference.
        -- BigQuery itself handles logging to job_runtime_log.
        SET v_log_name = FORMAT('%s_%s_%d.log', p_log_file_base_name, FORMAT_DATE('%Y%m%d', v_sysdate), v_eintragsnr);

        -- 3. Create initial job entry (DWMSG_ErzeugeEintrag replacement)
        INSERT INTO project.dataset.job_control (eintragsnr, job_kennung, script_name, log_name, status, created_ts)
        VALUES (v_eintragsnr, p_jobkennung, v_prog_name, v_log_name, 'RUNNING', CURRENT_TIMESTAMP());

        -- 4. Initial log message
        CALL project.dataset.sp_log_runtime_message(v_eintragsnr, p_jobkennung, 'INFO', FORMAT('Started %s version %s for job_kennung=%s (EintragsNr: %d)', v_prog_name, v_prog_version, p_jobkennung, v_eintragsnr));

        -- 5. Set Stichtag info (DWMSG_SetzeStichtagInfo replacement)
        IF p_stichtag IS NOT NULL THEN
            SET v_stichtag_info = FORMAT('Stichtag=%t', p_stichtag);
            UPDATE project.dataset.job_control
            SET stichtag_info = v_stichtag_info
            WHERE eintragsnr = v_eintragsnr AND job_kennung = p_jobkennung;
            CALL project.dataset.sp_log_runtime_message(v_eintragsnr, p_jobkennung, 'INFO', FORMAT('Stichtag parameter set: %s', v_stichtag_info));
        ELSE
            CALL project.dataset.sp_log_runtime_message(v_eintragsnr, p_jobkennung, 'WARNING', 'No Stichtag parameter provided. Using default logic if applicable in core script.');
        END IF;

        -- 6. Invoke core processing script (equivalent to calling ${Name_Kernskript})
        CALL project.dataset.sp_k_ausd_v_ta_p_vertrag(
            p_jobkennung,
            v_eintragsnr,
            COALESCE(p_stichtag, v_sysdate) -- Pass Stichtag, or current system date if not provided
        );

        -- 7. Log success and update status (DWMSG_SetzeStatusOK replacement)
        CALL project.dataset.sp_log_runtime_message(v_eintragsnr, p_jobkennung, 'INFO', FORMAT('Successfully completed %s for job_kennung=%s', v_prog_name, p_jobkennung));
        UPDATE project.dataset.job_control
        SET status = 'OK', finished_ts = CURRENT_TIMESTAMP()
        WHERE eintragsnr = v_eintragsnr AND job_kennung = p_jobkennung;

    EXCEPTION WHEN ERROR THEN
        -- Catch any error that occurs within the main BEGIN...END block
        SET v_err_nr = -1; -- Generic error number for unexpected BigQuery errors
        SET v_err_arg = CONCAT(@@error.message, ' (State: ', @@error.stateid, ')');

        -- Log the error to job_error_log (DWMSG_MeldeFehler replacement)
        INSERT INTO project.dataset.job_error_log (job_kennung, eintragsnr, error_nr, error_arg, error_message, error_ts, source_proc)
        VALUES (p_jobkennung, v_eintragsnr, v_err_nr, v_err_arg, @@error.message, CURRENT_TIMESTAMP(), 'sp_vertragsdatenabgleich_wrapper');

        -- Log to runtime log as well
        CALL project.dataset.sp_log_runtime_message(v_eintragsnr, p_jobkennung, 'ERROR', FORMAT('Error in %s: %s', v_prog_name, @@error.message));

        -- Update job status to ERROR
        UPDATE project.dataset.job_control
        SET status = 'ERROR', finished_ts = CURRENT_TIMESTAMP()
        WHERE eintragsnr = v_eintragsnr AND job_kennung = p_jobkennung;

        -- Re-raise the error to signal failure to the caller
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = FORMAT('Job %s (EintragsNr: %d) failed: %s', p_jobkennung, v_eintragsnr, @@error.message);
    END;
END;