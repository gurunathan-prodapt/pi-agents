-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Description: Validates if a given string represents a valid date according to the provided format.
CREATE OR REPLACE PROCEDURE dataset.DWDate_Datum_Check(
  IN wert STRING,
  IN format STRING,
  OUT is_valid BOOL
)
BEGIN
  DECLARE parsed_date DATE;
  SET is_valid = FALSE; -- Default to false

  BEGIN
    SET parsed_date = PARSE_DATE(format, wert);
    SET is_valid = TRUE;
  EXCEPTION WHEN ERROR THEN
    SET is_valid = FALSE;
  END;
END;