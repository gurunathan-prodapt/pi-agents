-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- Original function: pruefeSystemKennzahl
-- Purpose: Validates whether specific System and Kennzahl combinations are allowed.
CREATE OR REPLACE PROCEDURE `pruefeSystemKennzahl`(
    IN System STRING,
    IN Kennzahl STRING,
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

  IF `is_empty`(System) OR `is_empty`(Kennzahl) THEN
    SET ErrNr = 196;
    SET ErrArg = 'h_alis_parameter V3.0.9 pruefeSystemKennzahl';
    RETURN;
  END IF;

  -- Logic from the original ksh script:
  -- Note: The original ksh script sets ErrArg and then checks if ErrArg is set to set ErrNr.
  -- We'll combine this directly into IF conditions in BigQuery.
  IF (System != 'nnv' AND (Kennzahl = 'tvd' OR Kennzahl = 'lkl')) THEN
    SET ErrNr = 195;
    SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl);
  ELSEIF System = 'carmen' THEN
    IF Kennzahl IN ('twe', 'pln', 'rst', 'srs', 'sgs', 'ust', 'mahn', 'sg_rv', 'sr_rv_dpps', 'bwa') THEN
      SET ErrNr = 195;
      SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl);
    END IF;
  ELSEIF System = 'sap' THEN
    IF Kennzahl IN ('zug', 'abg', 'abz', 'bst', 'twe', 'pln', 'gut', 'auf', 'rst', 'tvd', 'usk', 'ust', 'lkl', 'loe', 'rak', 'ksd', 'bwa') THEN
      SET ErrNr = 195;
      SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl);
    END IF;
  ELSEIF System = 'dpps' THEN
    IF Kennzahl IN ('twe', 'pln', 'loe', 'rak', 'srs', 'sgs', 'mahn', 'sg_rv', 'sr_rv_dpps') THEN
      SET ErrNr = 195;
      SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl);
    END IF;
  ELSEIF System = 'ctel' THEN
    IF Kennzahl NOT IN ('abg', 'bst', 'zug', 'twe') THEN
      SET ErrNr = 195;
      SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl);
    END IF;
  ELSEIF System = 'xtra' THEN
    IF Kennzahl != 'rst' THEN
      SET ErrNr = 195;
      SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl);
    END IF;
  ELSEIF System = 'd1' THEN
    IF Kennzahl IN ('gut', 'auf', 'loe', 'rak', 'sgs', 'srs', 'twe', 'ksd', 'mahn', 'sg_rv', 'sr_rv_dpps', 'bwa') THEN
      SET ErrNr = 195;
      SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl);
    END IF;
  ELSEIF System = 'nnv' THEN
    IF Kennzahl NOT IN ('tvd', 'lkl') THEN
      SET ErrNr = 195;
      SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl);
    END IF;
  ELSEIF System = 'dwh' THEN
    IF Kennzahl != 'mds' THEN
      SET ErrNr = 195;
      SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl);
    END IF;
  ELSEIF System = 'brunet' THEN
    IF Kennzahl NOT IN ('d1n', 'rub', 'lmo') THEN
      SET ErrNr = 195;
      SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl);
    END IF;
  ELSEIF System = 'sigma' THEN
    IF Kennzahl NOT IN ('nnk', 'tvk', 'glv', 'gz', 'zonek', 'zonet', 'nnkt', 'trfa', 'gtyp', 'basisd', 'natint', 'glint') THEN
      SET ErrNr = 195;
      SET ErrArg = CONCAT('Ungueltige Kombination ', System, ' ', Kennzahl);
    END IF;
  END IF;
END;