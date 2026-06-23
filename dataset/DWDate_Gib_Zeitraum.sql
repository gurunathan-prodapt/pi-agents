-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Description: Calculates start and end dates for a period (Day, Month, Year) based on an offset.
CREATE OR REPLACE PROCEDURE dataset.DWDate_Gib_Zeitraum(
  IN Offset INT64,
  IN Stufe STRING,
  IN Format STRING,
  OUT Var_Start STRING,
  OUT Var_Ende STRING
)
BEGIN
  DECLARE base_date DATE DEFAULT CURRENT_DATE();
  DECLARE calculated_date DATE;
  DECLARE effective_format STRING DEFAULT COALESCE(NULLIF(Format, ''), '%Y%m%d');

  CASE Stufe
    WHEN 'D' THEN
      SET calculated_date = DATE_ADD(base_date, INTERVAL Offset DAY);
      SET Var_Start = FORMAT_DATE(effective_format, calculated_date);
      SET Var_Ende = FORMAT_DATE(effective_format, calculated_date);
    WHEN 'M' THEN
      SET calculated_date = DATE_ADD(base_date, INTERVAL Offset MONTH);
      SET Var_Start = FORMAT_DATE(effective_format, DATE_TRUNC(calculated_date, MONTH));
      SET Var_Ende = FORMAT_DATE(effective_format, LAST_DAY(calculated_date, MONTH));
    WHEN 'Y' THEN
      SET calculated_date = DATE_ADD(base_date, INTERVAL Offset YEAR);
      SET Var_Start = FORMAT_DATE(effective_format, DATE_TRUNC(calculated_date, YEAR));
      SET Var_Ende = FORMAT_DATE(effective_format, LAST_DAY(calculated_date, YEAR));
    ELSE
      ASSERT FALSE AS 'Invalid Stufe (Level) provided. Must be D, M, or Y.';
  END CASE;
END;