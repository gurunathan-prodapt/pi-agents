-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- This UDF should be created in the `your_project_id.utility_functions` dataset.

CREATE OR REPLACE FUNCTION `your_project_id.utility_functions`.gib_intervall(
  kennzahl STRING
) RETURNS STRING AS (
  CASE
    -- Assuming hardcoded lists from the original script for daily ('t') or monthly ('m') intervals
    WHEN kennzahl IN ('zug', 'abg', 'kfa', 'kfb') THEN 't' -- Daily interval for these
    WHEN kennzahl IN ('glint', 'bst') THEN 'm' -- Monthly interval for these
    -- Add more mappings from the original KornShell script's logic here
    ELSE ERROR(FORMAT("Could not determine Intervall for Kennzahl: %s", kennzahl))
  END
);