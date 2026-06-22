-- BigQuery Stored Procedure for DWMSG_SetzeStichtagInfo
-- Replaces function in vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Updates the 'zusatzinfos_date' column in message_table with a parsed date.

CREATE OR REPLACE PROCEDURE dw_is_error_management.DWMSG_SetzeStichtagInfo(
    p_EintragsNr INT64,
    p_Stichtag STRING,
    p_StichtagFmt STRING
)
BEGIN
    DECLARE parsed_date DATE;

    IF p_EintragsNr IS NULL OR p_Stichtag IS NULL OR p_StichtagFmt IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'All input parameters for DWMSG_SetzeStichtagInfo cannot be NULL.';
    END IF;

    BEGIN
        SET parsed_date = PARSE_DATE(p_StichtagFmt, p_Stichtag);
    EXCEPTION WHEN ERROR THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = FORMAT('Failed to parse date "%s" with format "%s". Error: %s', p_Stichtag, p_StichtagFmt, @@error.message);
    END;

    UPDATE dw_is_error_management.message_table
    SET
        zusatzinfos_date = parsed_date,
        updated_at = CURRENT_TIMESTAMP()
    WHERE eintragsnr = p_EintragsNr;

    IF NOT EXISTS (SELECT 1 FROM dw_is_error_management.message_table WHERE eintragsnr = p_EintragsNr) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = FORMAT("No entry found for EintragsNr: %d", p_EintragsNr);
    END IF;
END;