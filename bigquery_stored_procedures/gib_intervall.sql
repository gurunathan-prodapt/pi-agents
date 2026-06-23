-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- Original function: gibIntervall
-- Purpose: Determines a logical Intervall (interval, 't' for daily, 'm' for monthly) based on the input Kennzahl.
CREATE OR REPLACE PROCEDURE `gibIntervall`(
    IN Kennzahl STRING,
    OUT VarIntervall STRING,
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

  IF `is_empty`(Kennzahl) THEN -- Original ksh had '-z "$Kennzahl" -o -z "$VarIntervall"', but VarIntervall is OUT param.
    SET ErrNr = 196;
    SET ErrArg = 'h_alis_parameter V3.0.9 gibIntervall';
    RETURN;
  END IF;

  SET VarIntervall = (
    SELECT
      CASE
        WHEN Kennzahl IN ('abg', 'abz', 'twe', 'zug', 'gut', 'auf', 'rst', 'ust', 'usk', 'rak', 'loe', 'srs', 'sgs', 'ksd', 'mahn', 'mds', 'tvk', 'sr_rv_dpps', 'gtyp', 'basisd', 'bwa') THEN 't'
        WHEN Kennzahl IN ('bst', 'pln', 'tvd', 'lkl', 'sg_rv', 'd1n', 'rub', 'lmo', 'nnk', 'gz', 'glv', 'zonek', 'zonet', 'nnkt', 'trfa', 'natint', 'glint') THEN 'm'
        ELSE NULL
      END
  );

  IF VarIntervall IS NULL THEN
    SET ErrNr = 196;
    SET ErrArg = CONCAT('h_alis_parameter V3.0.9 gibIntervall - Kuerzel ''', Kennzahl, ''' unbekannt');
  END IF;
END;