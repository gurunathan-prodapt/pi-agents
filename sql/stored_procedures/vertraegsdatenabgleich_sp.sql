-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs2.ksh
-- This BigQuery Stored Procedure encapsulates the wrapper logic for the Vertragsdatenabgleich job.
-- It handles parameter parsing, logging, error trapping, and calls the core processing logic.

CREATE OR REPLACE PROCEDURE `your_gcp_project_id.your_bigquery_dataset_id.Vertragsdatenabgleich`(
    IN p_help BOOL,
    IN p_s STRING,
    IN p_l STRING
)
BEGIN
    DECLARE v_job_key STRING DEFAULT 'BERT_V_TA_CNTRCT_CRS2';
    DECLARE v_entry_number INT64;
    DECLARE v_start_timestamp TIMESTAMP;
    DECLARE v_sysdate_info DATE;
    DECLARE v_log_message STRING;
    DECLARE v_error_code STRING;
    DECLARE v_error_message STRING;
    DECLARE v_stack_trace STRING;

    -- If help flag is set, provide usage information.
    IF p_help THEN
        SELECT 'Programm: Vertragsdatenabgleich' AS program_name,
               'Version: V1.0.0' AS version,
               'Beschreibung: Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_cntrct_crs2.' AS description,
               'Parameter: -h (zeigt diese Seite an), -s (placeholder parameter), -l (placeholder parameter)' AS parameters;
        RETURN;
    END IF;

    SET v_start_timestamp = CURRENT_TIMESTAMP();
    SET v_sysdate_info = CURRENT_DATE(); -- Equivalent to date +%d%m%Y but storing as DATE type

    -- Simulate DWMSG_ErmittleNr: Generate a unique entry number for this execution
    -- For demonstration, we use a simple incremental approach or a hash of UUID
    -- In a real production system, consider a dedicated sequence generator or more robust method.
    SELECT COALESCE(MAX(entry_number), 0) + 1
    INTO v_entry_number
    FROM `your_gcp_project_id.your_bigquery_dataset_id.job_control`
    WHERE job_key = v_job_key;

    IF v_entry_number IS NULL THEN
        SET v_entry_number = 1; -- First run for this job_key
    END IF;


    -- Initial entry into job_control with RUNNING status
    INSERT INTO `your_gcp_project_id.your_bigquery_dataset_id.job_control` (
        job_key,
        entry_number,
        start_timestamp,
        status,
        parameters,
        program_name,
        sysdate_info
    )
    VALUES (
        v_job_key,
        v_entry_number,
        v_start_timestamp,
        'RUNNING',
        TO_JSON(STRUCT(p_s AS p_s_param, p_l AS p_l_param)),
        'Vertragsdatenabgleich',
        v_sysdate_info
    );

    -- Log initial message (simulating DWMSG_ErzeugeEintrag)
    SET v_log_message = FORMAT("Job start for '%s' with entry number %d. Parameters: p_s='%s', p_l='%s'", v_job_key, v_entry_number, p_s, p_l);
    INSERT INTO `your_gcp_project_id.your_bigquery_dataset_id.job_log` (job_key, entry_number, log_timestamp, message, log_level)
    VALUES (v_job_key, v_entry_number, CURRENT_TIMESTAMP(), v_log_message, 'INFO');

    BEGIN
        -- Call the core processing stored procedure
        CALL `your_gcp_project_id.your_bigquery_dataset_id.k_ausd_v_ta_cntrct_crs2`(v_job_key, v_entry_number);

        -- If core logic completes successfully (no exception)
        -- Log success message
        SET v_log_message = FORMAT("The processing finished without detectable errors for job '%s', entry %d.", v_job_key, v_entry_number);
        INSERT INTO `your_gcp_project_id.your_bigquery_dataset_id.job_log` (job_key, entry_number, log_timestamp, message, log_level)
        VALUES (v_job_key, v_entry_number, CURRENT_TIMESTAMP(), v_log_message, 'INFO');

        -- Update job_control to OK (simulating DWMSG_SetzeStatusOK)
        UPDATE `your_gcp_project_id.your_bigquery_dataset_id.job_control`
        SET
            end_timestamp = CURRENT_TIMESTAMP(),
            status = 'OK'
        WHERE
            job_key = v_job_key AND entry_number = v_entry_number;

    EXCEPTION WHEN ERROR THEN
        -- Handle any error that occurs during the CALL to k_ausd_v_ta_cntrct_crs2 or other statements within the BEGIN block
        SET v_error_code = @@error.code;
        SET v_error_message = @@error.message;
        SET v_stack_trace = @@error.stack_trace;

        -- Log error to job_error_log (simulating DWMSG_MeldeFehler and DWMSG_Fehlerbehandlung)
        INSERT INTO `your_gcp_project_id.your_bigquery_dataset_id.job_error_log` (
            job_key,
            entry_number,
            error_timestamp,
            error_code,
            error_message,
            stack_trace
        )
        VALUES (
            v_job_key,
            v_entry_number,
            CURRENT_TIMESTAMP(),
            v_error_code,
            v_error_message,
            v_stack_trace
        );

        -- Log general error message to job_log
        SET v_log_message = FORMAT("An error occurred for job '%s', entry %d: %s", v_job_key, v_entry_number, v_error_message);
        INSERT INTO `your_gcp_project_id.your_bigquery_dataset_id.job_log` (job_key, entry_number, log_timestamp, message, log_level)
        VALUES (v_job_key, v_entry_number, CURRENT_TIMESTAMP(), v_log_message, 'ERROR');

        -- Update job_control to ERROR
        UPDATE `your_gcp_project_id.your_bigquery_dataset_id.job_control`
        SET
            end_timestamp = CURRENT_TIMESTAMP(),
            status = 'ERROR'
        WHERE
            job_key = v_job_key AND entry_number = v_entry_number;

        RAISE USING MESSAGE v_error_message; -- Re-raise the error for external orchestration if needed

    END;

END;