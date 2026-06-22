-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- This UDF should be created in the `your_project_id.utility_functions` dataset.

CREATE OR REPLACE FUNCTION `your_project_id.utility_functions`.konvertiere_aufbstufextra(
  aufbstufextra_desc STRING
) RETURNS STRING AS (
  CASE LOWER(aufbstufextra_desc)
    WHEN 'zusammenfuehrung' THEN 'mrg'
    WHEN 'befuellung' THEN 'fill'
    -- Add more mappings from the original KornShell script's case statement here
    WHEN 'stagea' THEN 'stga'
    WHEN 'stageb' THEN 'stgb'
    ELSE ERROR(FORMAT("Invalid AufbStufeXtra description: %s", aufbstufextra_desc))
  END
);