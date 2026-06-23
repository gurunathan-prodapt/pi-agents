-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- Purpose: Helper UDF to check if a string is NULL or empty after trimming.
CREATE OR REPLACE FUNCTION `is_empty`(s STRING) RETURNS BOOL AS (
  s IS NULL OR TRIM(s) = ''
);