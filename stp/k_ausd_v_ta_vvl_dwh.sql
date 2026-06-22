-- BigQuery Stored Procedure for k_ausd_v_ta_vvl_dwh
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_dwh.ksh
-- Purpose: Translates the core processing script orchestration. This procedure primarily calls
--          the actual data reconciliation logic in 'd_ausd_v_ta_vvl_dwh'.
-- Note: The actual SQL script 'd_ausd_v_ta_vvl_dwh.sql' is assumed to be migrated to
--       a BigQuery Stored Procedure named `my_project.my_dataset.d_ausd_v_ta_vvl_dwh`.
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.k_ausd_v_ta_vvl_dwh`(
    p_job_kennung STRING,
    p_dw_entry_nr INT64
)
BEGIN
    -- Declare variables for logging
    DECLARE v_message STRING;
    DECLARE v_records_processed INT64;

    -- Log the start of the core script
    SET v_message = 'START: k_ausd_v_ta_vvl_dwh - Core reconciliation script invocation started.';
    INSERT INTO `my_project.my_dataset.dw_job_log` (dw_entry_nr, log_time, message_type, message_text)
    VALUES (p_dw_entry_nr, CURRENT_TIMESTAMP(), 'INFO', v_message);

    BEGIN
        -- Call the actual SQL script for data reconciliation
        -- We pass the job context to the deeper stored procedure if it needs it.
        CALL `my_project.my_dataset.d_ausd_v_ta_vvl_dwh`(p_job_kennung, p_dw_entry_nr, v_records_processed);

        -- Log the successful completion of the core data processing
        SET v_message = FORMAT_BQM('END: k_ausd_v_ta_vvl_dwh - Data processing completed. Records processed: %d', v_records_processed);
        INSERT INTO `my_project.my_dataset.dw_job_log` (dw_entry_nr, log_time, message_type, message_text)
        VALUES (p_dw_entry_nr, CURRENT_TIMESTAMP(), 'INFO', v_message);

    EXCEPTION WHEN ERROR THEN
        -- Log the error
        SET v_message = FORMAT_BQM('ERROR: k_ausd_v_ta_vvl_dwh - Data processing failed: %s', ERROR_MESSAGE());
        INSERT INTO `my_project.my_dataset.dw_error_log` (dw_entry_nr, error_time, error_code, error_message, stack_trace)
        VALUES (p_dw_entry_nr, CURRENT_TIMESTAMP(), 'SQL_ERROR', ERROR_MESSAGE(), ERROR_STACK_TRACE());

        INSERT INTO `my_project.my_dataset.dw_job_log` (dw_entry_nr, log_time, message_type, message_text)
        VALUES (p_dw_entry_nr, CURRENT_TIMESTAMP(), 'ERROR', v_message);

        -- Re-raise the error to the calling procedure
        RAISE;
    END;

    -- Print a completion message to standard output, similar to the original script
    SELECT '---------- ENDE Datenverarbeitung ----------' AS completion_message;

END;