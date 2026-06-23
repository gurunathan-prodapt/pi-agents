-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- Original function: pruefeParameterGesetzt
-- Purpose: Checks if an input parameter's value is set (not empty).
CREATE OR REPLACE PROCEDURE `pruefeParameterGesetzt`(
    IN param_name STRING,
    IN param_wert STRING, -- Note: Changed from param_var to param_wert directly as we pass value, not var name
    OUT ErrNr INT64,
    OUT ErrArg STRING
)
BEGIN
  -- Initialize ErrNr and ErrArg if not already set, assuming 0 means no error
  IF ErrNr IS NULL THEN
    SET ErrNr = 0;
  END IF;

  IF ErrNr != 0 THEN
    RETURN;
  END IF;

  IF `is_empty`(param_name) THEN -- Original ksh had '-z "$param_name" -o -z "$param_var"', but param_var is the value itself here
    SET ErrNr = 196;
    SET ErrArg = 'h_alis_parameter V3.0.9 pruefeParameterGesetzt';
    RETURN;
  END IF;

  IF `is_empty`(param_wert) THEN
    SET ErrNr = 194;
    SET ErrArg = param_name;
  END IF;
END;