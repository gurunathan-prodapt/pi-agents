-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- Purpose: Helper UDF to normalize a string to lowercase.
CREATE OR REPLACE FUNCTION `normalize_lower`(s STRING) RETURNS STRING AS (
  LOWER(s)
);