-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- Original function: konvertiereSystem
-- Purpose: Converts descriptive System names to canonical short codes or keeps them as is if already canonical.
CREATE OR REPLACE PROCEDURE `konvertiereSystem`(
    INOUT System STRING,
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

  IF `is_empty`(System) THEN
    SET ErrNr = 196;
    SET ErrArg = 'h_alis_parameter V3.0.9 konvertiereSystem';
    RETURN;
  END IF;

  SET System = `normalize_lower`(System);

  -- In the original ksh, many cases did nothing, implying they were already canonical.
  -- BigQuery CASE WHEN behaves similarly: if a match, it takes that value, otherwise it falls through to ELSE.
  -- Here, we only need to assign if it's an unknown system.
  IF System NOT IN ('sap', 'carmen', 'dpps', 'd1', 'xtra', 'ctel', 'nnv', 'dwh', 'brunet', 'sigma') THEN
    SET ErrNr = 195;
    SET ErrArg = CONCAT('Unbekannte Datenherkunft ', System, ' !');
    SET System = '???';
  END IF;
END;