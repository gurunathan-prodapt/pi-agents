-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount.ksh
-- Purpose: Migrated wrapper logic from k_ausd_v_ta_discount.ksh to BigQuery Stored Procedure.
CREATE OR REPLACE PROCEDURE `your_project.your_dataset.r_ausd_v_ta_discount`(
    IN p_JobKennung STRING,
    IN p_EintragsNr STRING
)
BEGIN
    DECLARE v_script_name STRING DEFAULT 'k_ausd_v_ta_discount.ksh_wrapper'; -- Representing the ksh script itself
    DECLARE v_start_ts TIMESTAMP;
    DECLARE v_records_processed INT64;
    DECLARE v_error_message STRING;
    DECLARE v_error_code INT64;

    -- 1. Parameter Validation
    IF p_JobKennung IS NULL OR LENGTH(TRIM(p_JobKennung)) = 0 THEN
        SET v_error_message = 'ERROR: p_JobKennung cannot be NULL or empty.';
        SET v_error_code = 1001;
        INSERT INTO `your_project.your_dataset.job_error_log` (job_kennung, eintrags_nr, err_nr, err_arg, error_ts, script_name)
        VALUES (COALESCE(p_JobKennung, 'UNKNOWN'), COALESCE(p_EintragsNr, 'UNKNOWN'), v_error_code, v_error_message, CURRENT_TIMESTAMP(), v_script_name);
        RAISE USING MESSAGE v_error_message;
    END IF;

    IF p_EintragsNr IS NULL OR LENGTH(TRIM(p_EintragsNr)) = 0 THEN
        SET v_error_message = 'ERROR: p_EintragsNr cannot be NULL or empty.';
        SET v_error_code = 1002;
        INSERT INTO `your_project.your_dataset.job_error_log` (job_kennung, eintrags_nr, err_nr, err_arg, error_ts, script_name)
        VALUES (COALESCE(p_JobKennung, 'UNKNOWN'), COALESCE(p_EintragsNr, 'UNKNOWN'), v_error_code, v_error_message, CURRENT_TIMESTAMP(), v_script_name);
        RAISE USING MESSAGE v_error_message;
    END IF;

    SET v_start_ts = CURRENT_TIMESTAMP();

    BEGIN TRANSACTION;

    BEGIN
        -- 2. Deactivate older active job entries for the given p_JobKennung
        UPDATE `your_project.your_dataset.job_table`
        SET active_flag = FALSE, end_ts = CURRENT_TIMESTAMP()
        WHERE job_kennung = p_JobKennung AND active_flag = TRUE;

        -- 3. Register current job execution
        INSERT INTO `your_project.your_dataset.job_table` (job_kennung, eintrags_nr, active_flag, start_ts, script_name)
        VALUES (p_JobKennung, p_EintragsNr, TRUE, v_start_ts, v_script_name);

        -- 4. Call the core SQL logic procedure
        CALL `your_project.your_dataset.d_ausd_v_ta_discount`(p_JobKennung, p_EintragsNr, v_records_processed);

        -- 5. Update job_table with end_ts and set active_flag to FALSE upon successful completion
        UPDATE `your_project.your_dataset.job_table`
        SET active_flag = FALSE, end_ts = CURRENT_TIMESTAMP()
        WHERE job_kennung = p_JobKennung AND eintrags_nr = p_EintragsNr AND active_flag = TRUE;

        -- Log the successful run and processed records
        -- Note: d_ausd_v_ta_discount also logs to job_run_control,
        -- this is an additional log from the wrapper for its own execution.
        -- Depending on exact needs, one of these might be sufficient.
        INSERT INTO `your_project.your_dataset.job_run_control` (job_kennung, eintrags_nr, script_name, records_processed, update_ts)
        VALUES (p_JobKennung, p_EintragsNr, v_script_name, v_records_processed, CURRENT_TIMESTAMP());

        COMMIT TRANSACTION;

    EXCEPTION WHEN ERROR THEN
        SET v_error_message = FORMAT("Error during execution of %s for JobKennung: %s, EintragsNr: %s. Details: %s", v_script_name, p_JobKennung, p_EintragsNr, @@error.message);
        SET v_error_code = @@error.code;

        -- Rollback transaction on error
        ROLLBACK TRANSACTION;

        -- Log the error
        INSERT INTO `your_project.your_dataset.job_error_log` (job_kennung, eintrags_nr, err_nr, err_arg, error_ts, script_name)
        VALUES (p_JobKennung, p_EintragsNr, v_error_code, v_error_message, CURRENT_TIMESTAMP(), v_script_name);

        RAISE USING MESSAGE v_error_message;
    END;

END;