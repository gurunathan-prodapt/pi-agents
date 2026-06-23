-- Legacy Source: Commented 'sed' and 'sort' operations on cibasis_data96.dat in k_ausd_bp_ta_apn_vertrag.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh

CREATE OR REPLACE TABLE `project.dataset.cibasis_data96_clean`
OPTIONS(
  description="Cleans and dedupes data from cibasis_data96.dat, simulating legacy sed and sort operations."
) AS
SELECT DISTINCT
    TRIM(REPLACE(t.column_a, ' ', '')) AS key_column_cleaned, -- Assuming 'column_a' is the key field
    TRIM(REPLACE(t.column_b, ' ', '')) AS data_field_b_cleaned,
    t.column_c AS original_data_field_c,
    CURRENT_TIMESTAMP() AS processed_at
FROM
    `project.dataset.cibasis_data96_raw` AS t -- Placeholder for the raw input table
WHERE
    TRUE
ORDER BY
    key_column_cleaned;