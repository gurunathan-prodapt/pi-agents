-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Description: BigQuery stored procedure to validate if a string is a valid date.
CREATE OR REPLACE PROCEDURE `your_project.date_utilities`.DWDate_Datum_Check(
  IN p_wert STRING,
  IN p_format STRING,
  OUT p_is_valid BOOL
)
BEGIN
  DECLARE v_date DATE DEFAULT NULL;
  SET v_date = SAFE.PARSE_DATE(p_format, p_wert);
  IF v_date IS NULL THEN
    SET p_is_valid = FALSE;
  ELSE
    SET p_is_valid = TRUE;
  END IF;
END;