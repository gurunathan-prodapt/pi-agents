-- Main BigQuery Stored Procedure replacing k_ausd_v_ta_apn_ve.ksh.
-- Handles parameter validation, calls `starte_sql_skript`,
-- retrieves record counts, and logs audit information.
-- Replaces k_ausd_v_ta_apn_ve.ksh for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh
CREATE OR REPLACE PROCEDURE project.dataset.r_ausd_vertrag_control(
    p_JobKennung STRING,
    p_EintragsNr STRING
)
BEGIN
    DECLARE v_run_id STRING DEFAULT GENERATE_UUID(); -- Simulate a unique run ID
    DECLARE v_records_processed INT64;
    DECLARE v_job_status STRING;
    DECLARE v_start_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    DECLARE v_end_timestamp TIMESTAMP;
    DECLARE v_error_message STRING;
    DECLARE v_error_code INT64;

    -- Parameter validation
    IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
        SET v_error_message = 'FEHLER: Notwendiges Argument fehlt - Jobkennung';
        SET v_error_code = 193; -- Based on original script's ErrNr
        INSERT INTO project.dataset.error_log (job_id, run_id, error_code, error_message, error_arg, severity)
        VALUES (p_JobKennung, v_run_id, v_error_code, v_error_message, 'Jobkennung', 'E');
        SELECT ERROR(v_error_message);
    END IF;

    IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
        SET v_error_message = 'FEHLER: Notwendiges Argument fehlt - EintragsNr';
        SET v_error_code = 193;
        INSERT INTO project.dataset.error_log (job_id, run_id, error_code, error_message, error_arg, severity)
        VALUES (p_JobKennung, v_run_id, v_error_code, v_error_message, 'EintragsNr', 'E');
        SELECT ERROR(v_error_message);
    END IF;

    BEGIN
        -- Call the job control wrapper
        CALL project.dataset.starte_sql_skript(p_EintragsNr, p_JobKennung, v_records_processed, v_job_status);
        SET v_end_timestamp = CURRENT_TIMESTAMP();

        IF v_job_status = 'IGNORED' THEN
            -- Log ignored job to audit without an error
            INSERT INTO project.dataset.job_run_audit (job_kennung, eintrags_nr, run_timestamp, status, start_timestamp, end_timestamp, error_message)
            VALUES (p_JobKennung, p_EintragsNr, v_start_timestamp, 'IGNORED', v_start_timestamp, v_end_timestamp, 'Job already active, ignored execution.');
        ELSE
            -- Log successful or failed execution to audit
            INSERT INTO project.dataset.job_run_audit (job_kennung, eintrags_nr, run_timestamp, status, records_processed, start_timestamp, end_timestamp)
            VALUES (p_JobKennung, p_EintragsNr, v_start_timestamp, v_job_status, v_records_processed, v_start_timestamp, v_end_timestamp);
        END IF;

    EXCEPTION WHEN ERROR THEN
        SET v_end_timestamp = CURRENT_TIMESTAMP();
        SET v_error_message = @@error.message;
        SET v_error_code = 500; -- Generic internal error code for BigQuery procedure errors

        -- Log error to error_log table
        INSERT INTO project.dataset.error_log (job_id, run_id, error_code, error_message, error_arg, severity)
        VALUES (p_JobKennung, v_run_id, v_error_code, v_error_message, 'r_ausd_vertrag_control', 'E');

        -- Log failure to audit table
        INSERT INTO project.dataset.job_run_audit (job_kennung, eintrags_nr, run_timestamp, status, start_timestamp, end_timestamp, error_message)
        VALUES (p_JobKennung, p_EintragsNr, v_start_timestamp, 'FAILED', 0, v_start_timestamp, v_end_timestamp, v_error_message);

        -- Re-raise the error to the caller
        RAISE;
    END;

    -- Final message (similar to original script's "ENDE Datenverarbeitung")
    -- In BigQuery SP, this would typically be via logging or status output.
    -- For now, we'll assume the audit log is sufficient.
END;