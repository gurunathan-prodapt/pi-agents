-- Legacy Function: LetzterTagDesMonat() from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Description: Checks if a given date string represents the last day of its month.
-- Returns TRUE if it's the last day, FALSE otherwise, and NULL for invalid date strings.
-- The input date_string is assumed to be in 'YYYYMMDD' format.

CREATE OR REPLACE FUNCTION dw_utils.is_last_day_of_month(date_string STRING)
RETURNS BOOL
AS (
  -- Parse the input date string. SAFE.PARSE_DATE returns NULL if parsing fails.
  CASE
    WHEN SAFE.PARSE_DATE('%Y%m%d', date_string) IS NULL THEN NULL
    ELSE
      -- Compare the parsed date with the last day of its month
      SAFE.PARSE_DATE('%Y%m%d', date_string) = LAST_DAY(SAFE.PARSE_DATE('%Y%m%d', date_string))
  END
);