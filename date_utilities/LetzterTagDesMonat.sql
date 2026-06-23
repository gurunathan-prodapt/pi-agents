-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Description: BigQuery stored procedure to check if a given date is the last day of its month.
CREATE OR REPLACE PROCEDURE `your_project.date_utilities`.LetzterTagDesMonat(
  IN p_datum STRING,
  OUT p_is_last_day BOOL
)
BEGIN
  DECLARE v_date DATE DEFAULT PARSE_DATE('%Y%m%d', p_datum);
  IF v_date = LAST_DAY(v_date) THEN
    SET p_is_last_day = TRUE;
  ELSE
    SET p_is_last_day = FALSE;
  END IF;
END;