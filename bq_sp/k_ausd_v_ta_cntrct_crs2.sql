-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh
-- Description: Main BigQuery Stored Procedure for orchestrating the contract data processing, replacing the ksh script.

CREATE OR REPLACE PROCEDURE project.dataset.k_ausd_v_ta_cntrct_crs2(
    IN p_job_kennung STRING,
    IN p_eintrags_nr STRING
)
BEGIN
    DECLARE v_tab_name STRING DEFAULT 'ta_cntrct_crs2';
    DECLARE v_records INT64;
    DECLARE v_job_status STRING;
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_end_time TIMESTAMP;
    DECLARE v_error_message STRING;

    -- Parameter Validation
    IF p_job_kennung IS NULL OR p_job_kennung = '' THEN
        SET v_error_message = 'FEHLER: Jobkennung (j) ist ein notwendiges Argument und fehlt.';
        INSERT INTO project.dataset.error_log (job_id, entry_number, severity, message, procedure_name)
        VALUES (COALESCE(p_job_kennung, 'UNKNOWN'), COALESCE(p_eintrags_nr, 'UNKNOWN'), 'ERROR', v_error_message, 'k_ausd_v_ta_cntrct_crs2');
        RAISE USING MESSAGE v_error_message;
    END IF;

    IF p_eintrags_nr IS NULL OR p_eintrags_nr = '' THEN
        SET v_error_message = 'FEHLER: EintragsNr (f) ist ein notwendiges Argument und fehlt.';
        INSERT INTO project.dataset.error_log (job_id, entry_number, severity, message, procedure_name)
        VALUES (COALESCE(p_job_kennung, 'UNKNOWN'), COALESCE(p_eintrags_nr, 'UNKNOWN'), 'ERROR', v_error_message, 'k_ausd_v_ta_cntrct_crs2');
        RAISE USING MESSAGE v_error_message;
    END IF;

    -- Job Table Interaction: Check for active jobs (ignore if already running)
    BEGIN
        SELECT status INTO v_job_status
        FROM project.dataset.job_table
        WHERE job_id = p_job_kennung AND entry_number = p_eintrags_nr AND status = 'RUNNING'
        ORDER BY start_timestamp DESC
        LIMIT 1;
    EXCEPTION WHEN NO_DATA_FOUND THEN
        SET v_job_status = NULL; -- No running job found
    END;

    IF v_job_status = 'RUNNING' THEN
        SET v_error_message = CONCAT('Job ', p_job_kennung, ' with entry number ', p_eintrags_nr, ' is already running. Ignoring.');
        INSERT INTO project.dataset.error_log (job_id, entry_number, severity, message, procedure_name)
        VALUES (p_job_kennung, p_eintrags_nr, 'INFO', v_error_message, 'k_ausd_v_ta_cntrct_crs2');
        RETURN; -- Exit procedure without error, as per "ignore active jobs"
    END IF;

    -- Deactivate old active jobs for the same job_id and entry_number
    UPDATE project.dataset.job_table
    SET status = 'DEACTIVATED', end_timestamp = CURRENT_TIMESTAMP()
    WHERE job_id = p_job_kennung AND entry_number = p_eintrags_nr AND status = 'RUNNING';

    -- Start job entry: Record initial state
    SET v_start_time = CURRENT_TIMESTAMP();
    INSERT INTO project.dataset.job_table (job_id, entry_number, start_timestamp, status, table_name)
    VALUES (p_job_kennung, p_eintrags_nr, v_start_time, 'RUNNING', v_tab_name);

    -- Main logic execution within its own block for error handling
    BEGIN
        CALL project.dataset.p_ausd_v_ta_cntrct_crs2_data_logic(p_eintrags_nr, p_job_kennung, v_records);

        -- Update job status to SUCCESS
        SET v_end_time = CURRENT_TIMESTAMP();
        UPDATE project.dataset.job_table
        SET status = 'SUCCESS', end_timestamp = v_end_time, record_count = v_records
        WHERE job_id = p_job_kennung AND entry_number = p_eintrags_nr AND start_timestamp = v_start_time;

        -- Log success
        INSERT INTO project.dataset.error_log (job_id, entry_number, severity, message, procedure_name)
        VALUES (p_job_kennung, p_eintrags_nr, 'INFO', CONCAT('Job completed successfully. Processed records: ', v_records), 'k_ausd_v_ta_cntrct_crs2');

    EXCEPTION WHEN ERROR THEN
        -- Handle errors during data processing
        SET v_error_message = CONCAT('Error during data processing in k_ausd_v_ta_cntrct_crs2: ', @@error.message);
        SET v_end_time = CURRENT_TIMESTAMP();
        UPDATE project.dataset.job_table
        SET status = 'FAILED', end_timestamp = v_end_time, error_message = v_error_message
        WHERE job_id = p_job_kennung AND entry_number = p_eintrags_nr AND start_timestamp = v_start_time;

        INSERT INTO project.dataset.error_log (job_id, entry_number, severity, message, procedure_name)
        VALUES (p_job_kennung, p_eintrags_nr, 'ERROR', v_error_message, 'k_ausd_v_ta_cntrct_crs2');
        RAISE; -- Re-raise the error to propagate it further if needed
    END;

END;