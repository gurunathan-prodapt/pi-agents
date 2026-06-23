-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- Original function: pruefeZeitParameter
-- Purpose: Validates mutually exclusive input patterns: either date range or timespan.
CREATE OR REPLACE PROCEDURE `pruefeZeitParameter`(
    IN p_Anfangsdatum STRING,
    IN p_Endedatum STRING,
    IN p_ZeitOffset STRING,
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

  -- Case 1: p_ZeitOffset is set
  IF NOT `is_empty`(p_ZeitOffset) THEN
    IF `is_empty`(p_Anfangsdatum) AND `is_empty`(p_Endedatum) THEN
      -- Check if ZeitOffset is a positive number
      CALL `pruefeZahlPositiv`(p_ZeitOffset, 'Zeitspanne', ErrNr, ErrArg);
      RETURN;
    ELSE
      SET ErrNr = 195;
      SET ErrArg = 'Es darf nur eine Zeitspanne oder beide Datumwerte gesetzt werden';
      RETURN;
    END IF;
  ELSE -- Case 2: p_ZeitOffset is empty
    IF NOT `is_empty`(p_Anfangsdatum) AND NOT `is_empty`(p_Endedatum) THEN
      -- Check date semantics
      CALL `pruefeZeitraum`(p_Anfangsdatum, p_Endedatum, ErrNr, ErrArg);
      RETURN;
    ELSE
      SET ErrNr = 195;
      IF `is_empty`(p_Anfangsdatum) AND `is_empty`(p_Endedatum) THEN
        SET ErrArg = 'Datumswerte oder Zeitspanne fehlen';
      ELSE
        SET ErrArg = 'Sowohl Anfang- als auch Endedatum muessen angegeben werden';
      END IF;
      RETURN;
    END IF;
  END IF;
END;