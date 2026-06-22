-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- This UDF should be created in the `your_project_id.utility_functions` dataset.

CREATE OR REPLACE FUNCTION `your_project_id.utility_functions`.konvertiere_kennzahl(
  kennzahl_desc STRING
) RETURNS STRING AS (
  CASE LOWER(kennzahl_desc)
    WHEN 'zugang' THEN 'zug'
    WHEN 'abgang' THEN 'abg'
    -- Add more mappings from the original KornShell script's case statement here
    WHEN 'glaengenintervall' THEN 'glint'
    WHEN 'keyfigurea' THEN 'kfa'
    WHEN 'keyfigureb' THEN 'kfb'
    ELSE ERROR(FORMAT("Invalid Kennzahl description: %s", kennzahl_desc))
  END
);