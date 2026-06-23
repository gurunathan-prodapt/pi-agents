-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Description: Adds a specified number of days to a given date string (YYYYMMDD) and returns the result in the same format.
CREATE OR REPLACE PROCEDURE dataset.AddiereDatum(
  IN date_yyyymmdd STRING,
  IN days_to_add INT64,
  OUT result_date STRING
)
BEGIN
  DECLARE d DATE DEFAULT PARSE_DATE('%Y%m%d', date_yyyymmdd);
  SET result_date = FORMAT_DATE('%Y%m%d', DATE_ADD(d, INTERVAL days_to_add DAY));
END;