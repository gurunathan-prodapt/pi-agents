-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Description: Compares two dates and asserts if the first is not less than or equal to the second.
-- If the procedure completes without error, datum1 is less than or equal to datum2.
CREATE OR REPLACE PROCEDURE dataset.DWDate_Datum_LE(
  IN datum1 STRING,
  IN datum2 STRING,
  OUT is_less_or_equal BOOL
)
BEGIN
  DECLARE d1 DATE;
  DECLARE d2 DATE;

  SET d1 = PARSE_DATE('%Y%m%d', datum1);
  SET d2 = PARSE_DATE('%Y%m%d', datum2);

  IF d1 > d2 THEN
    ASSERT FALSE AS CONCAT('Datum ', datum1, ' ist groesser als ', datum2);
  END IF;

  SET is_less_or_equal = TRUE;
END;