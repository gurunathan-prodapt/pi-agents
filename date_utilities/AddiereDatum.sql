-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Description: BigQuery stored procedure to add a specified number of days to a given date.
CREATE OR REPLACE PROCEDURE `your_project.date_utilities`.AddiereDatum(
  IN p_datum STRING,
  IN p_tage INT64,
  OUT p_result STRING
)
BEGIN
  DECLARE v_date DATE DEFAULT PARSE_DATE('%Y%m%d', p_datum);
  SET p_result = FORMAT_DATE('%Y%m%d', DATE_ADD(v_date, INTERVAL p_tage DAY));
END;