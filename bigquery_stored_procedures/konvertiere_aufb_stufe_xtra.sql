-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- Original function: konvertiereAufbStufeXtra
-- Purpose: Converts Aufbereitungsstufen (processing stages) to normalized abbreviations.
CREATE OR REPLACE PROCEDURE `konvertiereAufbStufeXtra`(
    INOUT Stufe STRING,
    OUT ErrNr INT64,
    OUT ErrArg STRING
)
BEGIN
  -- Initialize ErrNr and ErrArg if not already set
  IF ErrNr IS NULL THEN
    SET ErrNr = 0;
  END IF;

  IF ErrNr != 0 THEN
    RETURN;
  END IF;

  IF `is_empty`(Stufe) THEN
    SET ErrNr = 196;
    SET ErrArg = 'h_alis_parameter V3.0.9 konvertiereAufbStufeXtra';
    RETURN;
  END IF;

  SET Stufe = `normalize_lower`(Stufe);

  SET Stufe = (
    SELECT
      CASE Stufe
        WHEN 'zusammenfuehrung' THEN 'mrg'
        WHEN 'befuellung' THEN 'fill'
        ELSE NULL
      END
  );

  IF Stufe IS NULL THEN
    SET ErrNr = 195;
    SET ErrArg = CONCAT('Unbekannte Stufenangabe ', Stufe, ' !');
    SET Stufe = '???';
  END IF;
END;