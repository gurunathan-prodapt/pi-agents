-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- This UDF should be created in the `your_project_id.utility_functions` dataset.

CREATE OR REPLACE FUNCTION `your_project_id.utility_functions`.konvertiere_system(
  system_desc STRING
) RETURNS STRING AS (
  CASE LOWER(system_desc)
    WHEN 'sap' THEN 'sap'
    WHEN 'carmen' THEN 'carmen'
    WHEN 'sigma' THEN 'sigma'
    -- Add more mappings from the original KornShell script's case statement here
    WHEN 'systemx' THEN 'syx'
    WHEN 'systemy' THEN 'syy'
    ELSE ERROR(FORMAT("Invalid System description: %s", system_desc))
  END
);