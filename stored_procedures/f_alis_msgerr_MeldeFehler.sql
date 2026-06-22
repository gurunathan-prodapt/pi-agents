-- BigQuery Stored Procedure for DWMSG_MeldeFehler
-- Replaces function in vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Logs a specific error to message_log and updates error details in message_table.

CREATE OR REPLACE PROCEDURE dw_is_error_management.DWMSG_MeldeFehler(
    p_EintragsNr INT64,
    p_Typ STRING,
    p_FehlerNr INT64,
    p_Zusatz1 STRING,
    p_Zusatz2 STRING
)
BEGIN
    IF p_EintragsNr IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'p_EintragsNr cannot be NULL.';
    END IF;
    IF p_Typ IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'p_Typ cannot be NULL.';
    END IF;

    -- Insert into message_log
    INSERT INTO dw_is_error_management.message_log (
        log_id, -- A new log_id would be generated here if using a sequence or UUID
        eintragsnr,
        log_type,
        fehler_nr,
        zusatz1,
        zusatz2,
        logged_at
    )
    VALUES (
        GENERATE_UUID(), -- Placeholder for log_id, consider a sequence for INT64
        p_EintragsNr,
        p_Typ,
        p_FehlerNr,
        p_Zusatz1,
        p_Zusatz2,
        CURRENT_TIMESTAMP()
    );

    -- Update message_table with last error details
    UPDATE dw_is_error_management.message_table
    SET
        last_error_type = p_Typ,
        last_error_nr = p_FehlerNr,
        last_error_zusatz1 = p_Zusatz1,
        last_error_zusatz2 = p_Zusatz2,
        updated_at = CURRENT_TIMESTAMP()
    WHERE eintragsnr = p_EintragsNr;

    IF NOT EXISTS (SELECT 1 FROM dw_is_error_management.message_table WHERE eintragsnr = p_EintragsNr) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = FORMAT("No entry found for EintragsNr: %d", p_EintragsNr);
    END IF;
END;