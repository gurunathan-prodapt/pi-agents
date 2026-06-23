-- Legacy Source: Commented 'sed' and 'sort' operations on cibasis_fax.dat in k_ausd_bp_ta_apn_vertrag.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh

CREATE OR REPLACE TABLE `project.dataset.cibasis_fax_clean`
OPTIONS(
  description="Cleans and dedupes fax data from cibasis_fax.dat, simulating legacy sed and sort operations."
) AS
SELECT DISTINCT
    TRIM(REPLACE(t.fax_number_column, ' ', '')) AS fax_number_cleaned, -- Assuming this is the fax number field
    t.fax_metadata_column AS original_fax_metadata,
    CURRENT_TIMESTAMP() AS processed_at
FROM
    `project.dataset.cibasis_fax_raw` AS t -- Placeholder for the raw input table
WHERE
    TRUE
ORDER BY
    fax_number_cleaned;