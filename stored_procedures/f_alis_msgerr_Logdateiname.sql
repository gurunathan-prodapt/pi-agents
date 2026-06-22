-- BigQuery Stored Procedure for DWMSG_Logdateiname
-- Replaces function in vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Generates a log file name based on job identifier, timestamp, and entry number.

CREATE OR REPLACE PROCEDURE dw_is_error_management.DWMSG_Logdateiname(
    OUT p_VarName STRING,
    p_JobKennung STRING,
    p_EintragsNr INT64,
    p_LogBasePath STRING -- New parameter for configurable log base path
)
BEGIN
    IF p_JobKennung IS NULL OR p_EintragsNr IS NULL OR p_LogBasePath IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'p_JobKennung, p_EintragsNr, and p_LogBasePath cannot be NULL.';
    END IF;

    -- Construct the log filename. The base path is now a parameter.
    SET p_VarName = CONCAT(
        p_LogBasePath,
        CASE WHEN SUBSTR(p_LogBasePath, LENGTH(p_LogBasePath), 1) <> '/' THEN '/' ELSE '' END, -- Ensure trailing slash
        p_JobKennung,
        '_',
        FORMAT_TIMESTAMP('%Y%m%d_%H%M%S', CURRENT_TIMESTAMP()), -- Added seconds for more uniqueness
        '_',
        CAST(p_EintragsNr AS STRING),
        '.log'
    );
END;