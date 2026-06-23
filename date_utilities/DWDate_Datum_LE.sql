-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Description: BigQuery stored procedure to compare two dates (P1 <= P2).
CREATE OR REPLACE PROCEDURE `your_project.date_utilities`.DWDate_Datum_LE(
  IN p_datum1 STRING,
  IN p_datum2 STRING,
  OUT p_is_le BOOL
)
BEGIN
  DECLARE v_datum1 DATE;
  DECLARE v_datum2 DATE;
  SET v_datum1 = PARSE_DATE('%Y%m%d', p_datum1);
  SET v_datum2 = PARSE_DATE('%Y%m%d', p_datum2);
  IF v_datum1 > v_datum2 THEN
    SET p_is_le = FALSE;
  ELSE
    SET p_is_le = TRUE;
  END IF;
END;