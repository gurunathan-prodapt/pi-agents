-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Description: Returns the number of days in a given month and year.
CREATE OR REPLACE FUNCTION dataset.TageimMonat(year_int INT64, month_int INT64)
RETURNS INT64
AS (
  EXTRACT(DAY FROM LAST_DAY(DATE(year_int, month_int, 1)))
);