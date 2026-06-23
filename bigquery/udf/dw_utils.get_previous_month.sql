-- Legacy Function: DWDate_Vormonat() from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Description: Retrieves a date from the previous month relative to the input date.
-- The input date_string is assumed to be in 'YYYYMMDD' format.
-- Returns the date string in the specified output format, or NULL for invalid input.
-- NOTE: The exact logic of the original 'd_alis_vormonat.sql' was not available.
-- This UDF provides a common interpretation: finding the date one month prior,
-- adjusting to the last day of the previous month if the original day exceeds it.

CREATE OR REPLACE FUNCTION dw_utils.get_previous_month(input_date_string STRING, output_format STRING)
RETURNS STRING
AS (
  -- Parse the input date string. SAFE.PARSE_DATE returns NULL if parsing fails.
  CASE
    WHEN SAFE.PARSE_DATE('%Y%m%d', input_date_string) IS NULL THEN NULL
    WHEN output_format IS NULL THEN NULL -- Ensure output format is provided
    ELSE
      FORMAT_DATE(output_format, DATE_SUB(SAFE.PARSE_DATE('%Y%m%d', input_date_string), INTERVAL 1 MONTH))
  END
);