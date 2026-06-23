-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- Original function: pruefeZeitraum
-- Purpose: Validates if two YYYYMMDD formatted dates represent a valid period.
CREATE OR REPLACE PROCEDURE `pruefeZeitraum`(
    IN Anfang STRING,
    IN Ende STRING,
    OUT ErrNr INT64,
    OUT ErrArg STRING
)
BEGIN
  DECLARE v_anfang_date DATE;
  DECLARE v_ende_date DATE;

  -- Initialize ErrNr and ErrArg if not already set
  IF ErrNr IS NULL THEN
    SET ErrNr = 0;
  END IF;

  IF ErrNr != 0 THEN
    RETURN;
  END IF;

  IF `is_empty`(Anfang) OR `is_empty`(Ende) THEN
    SET ErrNr = 196;
    SET ErrArg = 'h_alis_parameter V3.0.9 pruefeZeitraum';
    RETURN;
  END IF;

  -- Try parsing dates. SAFE.PARSE_DATE returns NULL if format is invalid.
  SET v_anfang_date = SAFE.PARSE_DATE('%Y%m%d', Anfang);
  SET v_ende_date = SAFE.PARSE_DATE('%Y%m%d', Ende);

  IF v_anfang_date IS NULL THEN
    SET ErrNr = 195;
    SET ErrArg = CONCAT('Anfangsdatum (', Anfang, ') entspricht nicht dem Format YYYYMMDD');
    RETURN;
  END IF;

  IF v_ende_date IS NULL THEN
    SET ErrNr = 195;
    SET ErrArg = CONCAT('Endedatum (', Ende, ') entspricht nicht dem Format YYYYMMDD');
    RETURN;
  END IF;

  -- Check date order
  IF v_anfang_date > v_ende_date THEN
    SET ErrNr = 195;
    SET ErrArg = 'Anfangsdatum ist nicht kleiner gleich Endedatum';
    RETURN;
  END IF;
END;