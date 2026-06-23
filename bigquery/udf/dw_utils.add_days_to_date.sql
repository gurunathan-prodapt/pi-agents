-- Legacy Function: AddiereDatum() from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Description: Adds a specified number of days to a date string and returns the new date string.
-- The input date_string is assumed to be in 'YYYYMMDD' format.
-- Returns the new date string in 'YYYYMMDD' format, or NULL for invalid inputs.

CREATE OR REPLACE FUNCTION dw_utils.add_days_to_date(date_string STRING, days_to_add INT64)
RETURNS STRING
AS (
  -- Parse the input date string. SAFE.PARSE_DATE returns NULL if parsing fails.
  CASE
    WHEN SAFE.PARSE_DATE('%Y%m%d', date_string) IS NULL THEN NULL
    ELSE
      -- Add the days and format the result back to 'YYYYMMDD'.
      FORMAT_DATE('%Y%m%d', DATE_ADD(SAFE.PARSE_DATE('%Y%m%d', date_string), INTERVAL days_to_add DAY))
  END
);