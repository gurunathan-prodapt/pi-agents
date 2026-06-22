-- BigQuery Stored Procedure for DWMSG_Fehlerbehandlung
-- Replaces function in vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Central error handling routine. It logs the error and sets the job status to 'ABBRUCH'.
-- This procedure assumes error details (SQLCODE, SQLERRM) are passed from the calling script's
-- EXCEPTION block, as BigQuery's @@error.code and @@error.message are context-specific.

CREATE OR REPLACE PROCEDURE dw_is_error_management.DWMSG_Fehlerbehandlung(
    p_EintragsNr INT64,
    p_SqlCode INT64, -- Corresponds to @@error.code
    p_SqlMessage STRING -- Corresponds to @@error.message
)
BEGIN
    IF p_EintragsNr IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'p_EintragsNr cannot be NULL.';
    END IF;

    -- Log the error
    CALL dw_is_error_management.DWMSG_MeldeFehler(
        p_EintragsNr,
        'FEHLER', -- Default error type for general error handling
        p_SqlCode,
        LEFT(p_SqlMessage, 255), -- Truncate message if too long for Zusatz1
        NULL -- Zusatz2 can be used for more details if needed
    );

    -- Set job status to ABBRUCH
    CALL dw_is_error_management.DWMSG_SetzeStatusAbbruch(p_EintragsNr);

    -- Print a message (for logging/debugging in BigQuery Scripting environment)
    SELECT FORMAT('Error handled for EintragsNr %d. Status set to ABBRUCH.', p_EintragsNr) AS message;
END;