-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Description: Determines if a given year is a leap year.
CREATE OR REPLACE FUNCTION dataset.IsLeapYear(year_input INT64)
RETURNS BOOL
AS (
  (year_input % 4 = 0 AND year_input % 100 != 0) OR (year_input % 400 = 0)
);