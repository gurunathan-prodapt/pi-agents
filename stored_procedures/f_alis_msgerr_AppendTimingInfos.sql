-- BigQuery Stored Procedure for DWMSG_AppendTimingInfos
-- Replaces function in vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Appends timing information to the 'zusatzinfos_text' column in message_table.

CREATE OR REPLACE PROCEDURE dw_is_error_management.DWMSG_AppendTimingInfos(
    p_EintragsNr INT64,
    p_InfoText STRING,
    p_DateFormat STRING
)
BEGIN
    DECLARE current_time_formatted STRING;

    IF p_EintragsNr IS NULL OR p_InfoText IS NULL OR p_DateFormat IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'All input parameters for DWMSG_AppendTimingInfos cannot be NULL.';
    END IF;

    BEGIN
        SET current_time_formatted = FORMAT_TIMESTAMP(p_DateFormat, CURRENT_TIMESTAMP());
    EXCEPTION WHEN ERROR THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = FORMAT('Invalid date format string provided: "%s". Error: %s', p_DateFormat, @@error.message);
    END;

    UPDATE dw_is_error_management.message_table
    SET
        zusatzinfos_text = CONCAT(
            COALESCE(zusatzinfos_text, ''), -- Append to existing text, or start new if NULL
            p_InfoText, ' ',
            current_time_formatted, ' '
        ),
        updated_at = CURRENT_TIMESTAMP()
    WHERE eintragsnr = p_EintragsNr;

    IF NOT EXISTS (SELECT 1 FROM dw_is_error_management.message_table WHERE eintragsnr = p_EintragsNr) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = FORMAT("No entry found for EintragsNr: %d", p_EintragsNr);
    END IF;
END;