-- Legacy Function: DWDate_Datum_LE() from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Description: Compares two date strings (P1 <= P2).
-- The input date strings are assumed to be in 'YYYYMMDD' format.
-- Returns TRUE if date_string1 is less than or equal to date_string2, FALSE otherwise.
-- Returns NULL if any input date string is invalid.
-- The original PL/SQL `raise_application_error` is translated to BigQuery's standard UDF error handling:
-- invalid inputs result in NULL, comparisons proceed with valid DATE types.

CREATE OR REPLACE FUNCTION dw_utils.is_date_le(date_string1 STRING, date_string2 STRING)
RETURNS BOOL
AS (
  -- Parse both date strings. SAFE.PARSE_DATE returns NULL if parsing fails.
  CASE
    WHEN SAFE.PARSE_DATE('%Y%m%d', date_string1) IS NULL
      OR SAFE.PARSE_DATE('%Y%m%d', date_string2) IS NULL THEN NULL
    ELSE
      SAFE.PARSE_DATE('%Y%m%d', date_string1) <= SAFE.PARSE_DATE('%Y%m%d', date_string2)
  END
);