-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- Original function: pruefeZahlPositiv
-- Purpose: Checks if a given parameter is a positive numeric value (>= 0).
CREATE OR REPLACE PROCEDURE `pruefeZahlPositiv`(
    IN p_Zahl STRING,
    IN p_ParameterName STRING,
    OUT ErrNr INT64,
    OUT ErrArg STRING
)
BEGIN
  DECLARE v_numeric_val INT64;

  -- Initialize ErrNr and ErrArg if not already set
  IF ErrNr IS NULL THEN
    SET ErrNr = 0;
  END IF;

  IF ErrNr != 0 THEN
    RETURN;
  END IF;

  -- Attempt to cast to INT64. SAFE_CAST returns NULL if not convertible.
  SET v_numeric_val = SAFE_CAST(p_Zahl AS INT64);

  IF v_numeric_val IS NULL THEN
    SET ErrNr = 195;
    SET ErrArg = CONCAT('Parameter ', p_ParameterName, ' ist kein numerischer Wert');
    RETURN;
  END IF;

  IF v_numeric_val < 0 THEN
    SET ErrNr = 195;
    SET ErrArg = CONCAT('Parameter ', p_ParameterName, ' muss groesser gleich 0 sein');
    RETURN;
  END IF;
END;