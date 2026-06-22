-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- This UDF should be created in the `your_project_id.utility_functions` dataset.

CREATE OR REPLACE FUNCTION `your_project_id.utility_functions`.pruefe_zahl_positiv(
  value_str STRING
) RETURNS STRUCT<is_valid BOOL, error_message STRING> AS (
  CASE
    WHEN SAFE_CAST(value_str AS NUMERIC) IS NULL THEN STRUCT(FALSE, 'Value is not a valid number.')
    WHEN SAFE_CAST(value_str AS NUMERIC) < 0 THEN STRUCT(FALSE, 'Value is negative.')
    ELSE STRUCT(TRUE, '')
  END
);