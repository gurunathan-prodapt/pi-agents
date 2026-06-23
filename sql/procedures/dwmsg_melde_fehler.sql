-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Target Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh

-- This stored procedure records error details in the bert_meldung_fehler table.
-- Replace `my_project.my_dataset` with your actual BigQuery project and dataset IDs.

CREATE OR REPLACE PROCEDURE `my_project.my_dataset.dwmsg_melde_fehler`(
    IN p_eintrags_nr STRING,
    IN p_fehler_nr STRING,
    IN p_fehler_text STRING,
    IN p_zusatz_info_1 STRING,
    IN p_zusatz_info_2 STRING,
    IN p_variable_name STRING
)
BEGIN
    IF p_eintrags_nr IS NULL OR TRIM(p_eintrags_nr) = '' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'p_eintrags_nr cannot be empty or NULL.';
    END IF;

    INSERT INTO `my_project.my_dataset.bert_meldung_fehler` (
        error_id,
        eintrags_nr,
        fehler_nr,
        fehler_text,
        zusatz_info_1,
        zusatz_info_2,
        variable_name,
        error_timestamp
    )
    VALUES (
        GENERATE_UUID(), -- Unique ID for this specific error instance
        p_eintrags_nr,
        p_fehler_nr,
        p_fehler_text,
        p_zusatz_info_1,
        p_zusatz_info_2,
        p_variable_name,
        CURRENT_TIMESTAMP()
    );

    -- Optionally, also update the main entry status to ERROR/ABORTED if not already done by DWMSG_Fehlerbehandlung
    UPDATE `my_project.my_dataset.bert_meldung`
    SET
        status = 'ERROR', -- Or 'ABORTED', depending on convention
        last_update_timestamp = CURRENT_TIMESTAMP()
    WHERE
        eintrags_nr = p_eintrags_nr AND status NOT IN ('OK', 'ABORTED'); -- Only update if not already final
END;