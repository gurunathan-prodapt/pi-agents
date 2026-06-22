-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- This UDF should be created in the `your_project_id.utility_functions` dataset.

CREATE OR REPLACE FUNCTION `your_project_id.utility_functions`.konvertiere_sdname(
  sdname_desc STRING
) RETURNS STRING AS (
  CASE LOWER(sdname_desc)
    -- Add mappings from the original KornShell script's case statement here
    WHEN 'masterdata1' THEN 'md1'
    WHEN 'masterdata2' THEN 'md2'
    WHEN 'productmd' THEN 'pmd'
    ELSE ERROR(FORMAT("Invalid SDName description: %s", sdname_desc))
  END
);