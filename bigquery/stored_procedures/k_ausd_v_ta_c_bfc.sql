-- Main BigQuery Stored Procedure, replacing k_ausd_v_ta_c_bfc.ksh.
-- This procedure orchestrates the data processing, handles parameters,
-- job control, and error logging.
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_ta_c_bfc`(
    IN p_jobkennung STRING,
    IN p_eintragsnr STRING
)
BEGIN
    DECLARE v_run_id STRING;
    DECLARE v_err_nr INT64 DEFAULT 0;
    DECLARE v_err_arg STRING DEFAULT '';
    DECLARE v_records_processed INT64 DEFAULT 0;
    DECLARE v_status STRING;
    DECLARE v_message STRING;

    -- Generate a unique run_id for this execution
    SET v_run_id = GENERATE_UUID();

    -- Parameter Validation (replacing pruefeParameterGesetzt from h_alis_parameter.ksh)
    IF p_jobkennung IS NULL OR TRIM(p_jobkennung) = '' THEN
        SET v_err_nr = 193; -- Equivalent to "Notwendiges Argument fehlt"
        SET v_err_arg = 'Jobkennung';
        SET v_message = 'FEHLER: 0 E ' || v_err_nr || ' ' || v_err_arg || ' - Jobkennung is missing.';
        SET v_status = 'FAILED';
    END IF;

    IF p_eintragsnr IS NULL OR TRIM(p_eintragsnr) = '' AND v_err_nr = 0 THEN
        SET v_err_nr = 193; -- Equivalent to "Notwendiges Argument fehlt"
        SET v_err_arg = 'EintragsNr';
        SET v_message = 'FEHLER: 0 E ' || v_err_nr || ' ' || v_err_arg || ' - EintragsNr is missing.';
        SET v_status = 'FAILED';
    END IF;

    -- Log initial state and handle validation errors
    IF v_err_nr <> 0 THEN
        INSERT INTO `project.dataset.error_log` (timestamp, run_id, job_kennung, eintrags_nr, error_code, error_message)
        VALUES (CURRENT_TIMESTAMP(), v_run_id, p_jobkennung, p_eintragsnr, v_err_nr, v_message);
        RAISE USING MESSAGE v_message;
        -- The procedure will exit here due to RAISE.
    END IF;

    -- Job Control Logic (replacing starteSQLSkript from h_alis_sqlplus.ksh and associated logic)
    -- 1. Check for active jobs (aktive Jobs werden ignoriert)
    BEGIN
        DECLARE active_job_count INT64;
        SELECT COUNT(1)
        INTO active_job_count
        FROM `project.dataset.job_table`
        WHERE job_kennung = p_jobkennung
          AND eintrags_nr = p_eintragsnr
          AND status = 'RUNNING';

        IF active_job_count > 0 THEN
            SET v_message = 'Job ' || p_jobkennung || '/' || p_eintragsnr || ' is already running. Ignoring.';
            INSERT INTO `project.dataset.job_table` (run_id, job_kennung, eintrags_nr, start_time, end_time, status, message)
            VALUES (v_run_id, p_jobkennung, p_eintragsnr, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'IGNORED', v_message);
            SELECT v_message; -- Output message to console/logs
            RETURN; -- Exit the procedure
        END IF;
    EXCEPTION WHEN ERROR THEN
        SET v_err_nr = -1; -- Internal error code
        SET v_err_arg = 'Job control check failed';
        SET v_message = 'ERROR: Job control check failed: ' || @@error.message;
        SET v_status = 'FAILED';
    END;

    -- Log job start
    INSERT INTO `project.dataset.job_table` (run_id, job_kennung, eintrags_nr, start_time, status, message)
    VALUES (v_run_id, p_jobkennung, p_eintragsnr, CURRENT_TIMESTAMP(), 'RUNNING', 'Job started.');

    -- 2. Deactivate old active jobs (alte aktive Jobs werden einfach dekativiert)
    BEGIN
        UPDATE `project.dataset.job_table`
        SET
            end_time = CURRENT_TIMESTAMP(),
            status = 'DEACTIVATED',
            message = 'Deactivated by new run ' || v_run_id
        WHERE job_kennung = p_jobkennung
          AND eintrags_nr = p_eintragsnr
          AND status = 'RUNNING' -- Only deactivate truly 'active' jobs
          AND run_id <> v_run_id; -- Do not deactivate the current job if it somehow got in RUNNING status early
    EXCEPTION WHEN ERROR THEN
        -- Log this error but do not fail the entire procedure, as it's a cleanup step
        INSERT INTO `project.dataset.error_log` (timestamp, run_id, job_kennung, eintrags_nr, error_code, error_message)
        VALUES (CURRENT_TIMESTAMP(), v_run_id, p_jobkennung, p_eintragsnr, -2, 'Error deactivating old jobs: ' || @@error.message);
    END;

    -- Core Data Processing (Call the migrated SQL logic)
    BEGIN
        CALL `project.dataset.d_ausd_v_ta_c_bfc_core_logic`(v_run_id, p_jobkennung, p_eintragsnr, v_records_processed);
        SET v_status = 'SUCCESS';
        SET v_message = 'Data processing completed successfully. Processed records: ' || v_records_processed;

    EXCEPTION WHEN ERROR THEN
        SET v_err_nr = -3; -- General error for core logic
        SET v_err_arg = 'd_ausd_v_ta_c_bfc_core_logic failed';
        SET v_message = 'ERROR: Data processing failed: ' || @@error.message;
        SET v_status = 'FAILED';

        INSERT INTO `project.dataset.error_log` (timestamp, run_id, job_kennung, eintrags_nr, error_code, error_message)
        VALUES (CURRENT_TIMESTAMP(), v_run_id, p_jobkennung, p_eintragsnr, v_err_nr, v_message);
    END;

    -- Update job status with end time and records processed
    UPDATE `project.dataset.job_table`
    SET
        end_time = CURRENT_TIMESTAMP(),
        status = v_status,
        message = v_message,
        processed_records = v_records_processed
    WHERE run_id = v_run_id;

    -- Final error check to raise if the core logic failed
    IF v_status = 'FAILED' THEN
        RAISE USING MESSAGE v_message;
    END IF;

    SELECT 'Job ' || p_jobkennung || '/' || p_eintragsnr || ' completed with status: ' || v_status || '. ' || v_message;

END;