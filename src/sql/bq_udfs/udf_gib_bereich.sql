-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- This UDF should be created in the `your_project_id.utility_functions` dataset.

CREATE OR REPLACE FUNCTION `your_project_id.utility_functions`.gib_bereich(
  kennzahl STRING
) RETURNS STRING AS (
  CASE
    -- Assuming list_tn, list_us, list_gd are hardcoded lists in the original script
    WHEN kennzahl IN ('zug', 'abg', 'kfa') THEN 'TN_BEREICH'
    WHEN kennzahl IN ('kfb', 'kfc') THEN 'US_BEREICH'
    WHEN kennzahl IN ('glint') THEN 'GD_BEREICH'
    -- Add more mappings from the original KornShell script's logic here
    ELSE ERROR(FORMAT("Could not determine Bereich for Kennzahl: %s", kennzahl))
  END
);