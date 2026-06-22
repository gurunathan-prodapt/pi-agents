-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- This UDF should be created in the `your_project_id.utility_functions` dataset.

CREATE OR REPLACE FUNCTION `your_project_id.utility_functions`.pruefe_parameter_gesetzt(
  param_value STRING
) RETURNS STRUCT<is_valid BOOL, error_message STRING> AS (
  CASE
    WHEN param_value IS NULL OR TRIM(param_value) = '' THEN STRUCT(FALSE, 'Parameter is not set or is empty.')
    ELSE STRUCT(TRUE, '')
  END
);