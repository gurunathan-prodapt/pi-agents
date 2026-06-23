-- Legacy Function: TageimMonat() from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Description: Calculates the number of days in a given month and year.
-- Returns the number of days, or NULL for invalid year/month combinations.

CREATE OR REPLACE FUNCTION dw_utils.days_in_month(year INT64, month INT64)
RETURNS INT64
AS (
  -- Construct a date for the first day of the given month and year.
  -- SAFE.MAKE_DATE handles invalid year/month by returning NULL.
  CASE
    WHEN SAFE.MAKE_DATE(year, month, 1) IS NULL THEN NULL
    ELSE
      -- Get the last day of that month and extract the day number.
      EXTRACT(DAY FROM LAST_DAY(SAFE.MAKE_DATE(year, month, 1)))
  END
);