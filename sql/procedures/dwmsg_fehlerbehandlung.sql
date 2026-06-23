-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Target Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh

-- This stored procedure handles generic error processing, similar to DWMSG_Fehlerbehandlung in the KSH script.
-- It logs the error and sets the status of the main entry to 'ABORTED'.
-- Replace `my_project.my_dataset` with your actual BigQuery project and dataset IDs.

CREATE OR REPLACE PROCEDURE `my_project.my_dataset.dwmsg_fehlerbehandlung`(
    IN p_eintrags_nr STRING,
    IN p_job_kennung STRING,
    IN p_programm_name STRING,
    IN p_error_code STRING,
    IN p_error_message STRING
)
BEGIN
    IF p_eintrags_nr IS NULL OR TRIM(p_eintrags_nr) = '' THEN
        -- If eintrags_nr is not available, we cannot log to the main entry, just output
        SELECT FORMAT("Error: EintragsNr is missing. Job: %s, Program: %s, Error Code: %s, Message: %s",
                      p_job_kennung, p_programm_name, p_error_code, p_error_message);
        RETURN;
    END IF;

    -- Log the detailed error
    CALL `my_project.my_dataset.dwmsg_melde_fehler`(
        p_eintrags_nr,
        p_error_code,
        p_error_message,
        p_job_kennung,    -- Using Zusatz1 for job_kennung
        p_programm_name,  -- Using Zusatz2 for programm_name
        NULL              -- No specific variable name from this generic handler
    );

    -- Set the main entry status to ABORTED
    CALL `my_project.my_dataset.dwmsg_setze_status_abbruch`(p_eintrags_nr);

    -- Signal a generic error to the caller, if this procedure is part of a larger transaction
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = CONCAT('Process aborted due to error: ', p_error_message, ' (Code: ', p_error_code, ')');
END;