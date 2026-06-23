-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Description: BigQuery stored procedure to calculate the first day of the previous month.
CREATE OR REPLACE PROCEDURE `your_project.date_utilities`.DWDate_Vormonat(
  IN p_format STRING,
  OUT p_result STRING
)
BEGIN
  SET p_result = FORMAT_DATE(
    p_format,
    DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
  );
END;