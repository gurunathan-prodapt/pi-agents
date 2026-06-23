-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Description: BigQuery stored procedure to calculate a date period (start and end) based on an offset and granularity.
CREATE OR REPLACE PROCEDURE `your_project.date_utilities`.DWDate_Gib_Zeitraum(
  IN p_offset INT64,
  IN p_stufe STRING,
  IN p_format STRING,
  OUT p_start STRING,
  OUT p_ende STRING
)
BEGIN
  DECLARE v_today DATE DEFAULT CURRENT_DATE();
  DECLARE v_start DATE;
  DECLARE v_ende DATE;

  IF p_stufe = 'D' THEN
    SET v_start = v_today;
    SET v_ende = DATE_ADD(v_today, INTERVAL p_offset DAY);
  ELSEIF p_stufe = 'M' THEN
    SET v_start = DATE_TRUNC(DATE_ADD(v_today, INTERVAL p_offset MONTH), MONTH);
    SET v_ende = LAST_DAY(DATE_ADD(v_today, INTERVAL p_offset MONTH));
  ELSEIF p_stufe = 'Y' THEN
    SET v_start = DATE_TRUNC(DATE_ADD(v_today, INTERVAL p_offset YEAR), YEAR);
    SET v_ende = LAST_DAY(DATE_ADD(DATE_TRUNC(v_today, YEAR), INTERVAL p_offset YEAR));
  ELSE
    RAISE USING MESSAGE = 'Invalid p_stufe. Must be D, M, or Y.';
  END IF;

  SET p_start = FORMAT_DATE(p_format, v_start);
  SET p_ende = FORMAT_DATE(p_format, v_ende);
END;