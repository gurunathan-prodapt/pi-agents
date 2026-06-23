-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Description: Calculates the previous month's date based on the current date, formatted as specified.
CREATE OR REPLACE PROCEDURE dataset.DWDate_Vormonat(
  IN DWDate_FMT STRING,
  OUT result_value STRING
)
BEGIN
  -- If DWDate_FMT is empty or null, default to 'YYYYMM'
  DECLARE effective_format STRING DEFAULT COALESCE(NULLIF(DWDate_FMT, ''), '%Y%m');

  SET result_value = FORMAT_DATE(effective_format, DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH));
END;