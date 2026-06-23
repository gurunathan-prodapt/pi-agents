-- Legacy Function: DWDate_Datum_Check() from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Description: Checks if a given date string is valid according to the specified format.
-- Returns TRUE if the date is valid, FALSE if invalid, and NULL if input format is invalid or missing.

CREATE OR REPLACE FUNCTION dw_utils.is_valid_date(date_string STRING, date_format STRING)
RETURNS BOOL
AS (
  -- SAFE.PARSE_DATE returns NULL if the string cannot be parsed with the given format.
  -- If it returns non-NULL, the date is valid.
  CASE
    WHEN date_string IS NULL OR date_format IS NULL THEN NULL
    ELSE SAFE.PARSE_DATE(date_format, date_string) IS NOT NULL
  END
);