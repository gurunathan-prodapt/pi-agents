-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- This UDF should be created in the `your_project_id.utility_functions` dataset.

CREATE OR REPLACE FUNCTION `your_project_id.utility_functions`.pruefe_system_kennzahl(
  system STRING,
  kennzahl STRING
) RETURNS STRUCT<is_valid BOOL, error_message STRING> AS (
  CASE
    -- Example 1: SAP system allows 'zug' and 'abg'
    WHEN system = 'sap' AND kennzahl IN ('zug', 'abg') THEN STRUCT(TRUE, '')
    -- Example 2: Carmen system allows 'kfa' but not 'kfb'
    WHEN system = 'carmen' AND kennzahl = 'kfa' THEN STRUCT(TRUE, '')
    WHEN system = 'carmen' AND kennzahl = 'kfb' THEN STRUCT(FALSE, 'Kennzahl "kfb" not allowed for system "carmen".')
    -- Example 3: Sigma system allows 'glint'
    WHEN system = 'sigma' AND kennzahl = 'glint' THEN STRUCT(TRUE, '')
    -- Add more complex if/elif logic from the original KornShell script here
    -- Default for unknown valid combinations or explicit invalid ones
    ELSE STRUCT(FALSE, FORMAT('Invalid combination of system "%s" and kennzahl "%s".', system, kennzahl))
  END
);