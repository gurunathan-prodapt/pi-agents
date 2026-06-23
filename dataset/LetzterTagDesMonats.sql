-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Description: Checks if a given date string (YYYYMMDD) is the last day of its month.
CREATE OR REPLACE FUNCTION dataset.LetzterTagDesMonats(date_yyyymmdd STRING)
RETURNS BOOL
AS (
  DATE(PARSE_DATE('%Y%m%d', date_yyyymmdd)) = LAST_DAY(PARSE_DATE('%Y%m%d', date_yyyymmdd))
);