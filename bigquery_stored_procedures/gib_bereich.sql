-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- Original function: gibBereich
-- Purpose: Determines a logical Bereich (area/category) based on the input Kennzahl.
CREATE OR REPLACE PROCEDURE `gibBereich`(
    IN Kennzahl STRING,
    OUT VarBereich STRING,
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

  IF `is_empty`(Kennzahl) THEN -- Original ksh had '-z "$Kennzahl" -o -z "$VarBereich"', but VarBereich is OUT param.
    SET ErrNr = 196;
    SET ErrArg = 'h_alis_parameter V3.0.9 gibBereich';
    RETURN;
  END IF;

  SET VarBereich = (
    SELECT
      CASE
        WHEN Kennzahl IN ('abg', 'abz', 'bst', 'pln', 'twe', 'zug', 'loe', 'rak') THEN 'tn'
        WHEN Kennzahl IN ('gut', 'rst', 'auf', 'ust', 'usk', 'srs', 'sgs', 'mahn', 'sg_rv', 'sr_rv_dpps') THEN 'us'
        WHEN Kennzahl IN ('tvd', 'lkl', 'd1n', 'rub', 'lmo', 'nnk', 'tvk', 'gz', 'glv', 'zonek', 'zonet', 'nnkt', 'trfa', 'gtyp', 'basisd', 'natint', 'glint') THEN 'gd'
        WHEN Kennzahl IN ('ksd', 'bwa') THEN 'sd'
        WHEN Kennzahl IN ('mds') THEN 'md'
        ELSE NULL
      END
  );

  IF VarBereich IS NULL THEN
    SET ErrNr = 196;
    SET ErrArg = CONCAT('h_alis_parameter V3.0.9 gibBereich - Kuerzel ''', Kennzahl, ''' unbekannt');
  END IF;
END;