-- BigQuery Stored Procedure for DWMSG_SetzeStatusOK
-- Replaces function in vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Sets the status of a job entry in message_table to 'OK'.

CREATE OR REPLACE PROCEDURE dw_is_error_management.DWMSG_SetzeStatusOK(
    p_EintragsNr INT64
)
BEGIN
    IF p_EintragsNr IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'p_EintragsNr cannot be NULL.';
    END IF;

    UPDATE dw_is_error_management.message_table
    SET
        status = 'OK',
        updated_at = CURRENT_TIMESTAMP()
    WHERE eintragsnr = p_EintragsNr;

    IF NOT EXISTS (SELECT 1 FROM dw_is_error_management.message_table WHERE eintragsnr = p_EintragsNr) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = FORMAT("No entry found for EintragsNr: %d", p_EintragsNr);
    END IF;
END;