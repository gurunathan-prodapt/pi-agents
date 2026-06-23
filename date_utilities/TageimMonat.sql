-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Description: BigQuery stored procedure to calculate the number of days in a specific month of a year.
CREATE OR REPLACE PROCEDURE `your_project.date_utilities`.TageimMonat(
  IN p_jahr INT64,
  IN p_monat INT64,
  OUT p_tage INT64
)
BEGIN
  DECLARE v_first DATE DEFAULT DATE(p_jahr, p_monat, 1);
  DECLARE v_last DATE DEFAULT LAST_DAY(v_first);
  SET p_tage = DATE_DIFF(DATE_ADD(v_last, INTERVAL 1 DAY), v_first, DAY);
END;