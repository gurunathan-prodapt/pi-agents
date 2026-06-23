-- Legacy Source: Commented 'sed' and 'sort' operations on cibasis_data24.dat in k_ausd_bp_ta_apn_vertrag.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh

CREATE OR REPLACE TABLE `project.dataset.cibasis_data24_clean`
OPTIONS(
  description="Cleans and dedupes data from cibasis_data24.dat, simulating legacy sed and sort operations."
) AS
SELECT DISTINCT
    -- Assuming 'column_1' is the key field from the original file, corresponding to 'sort -u -k 1'
    TRIM(REPLACE(t.column_1, ' ', '')) AS key_column_cleaned, -- Equivalent to sed s/\\ //g and sort -u -k 1
    TRIM(REPLACE(t.column_2, ' ', '')) AS data_field_2_cleaned,
    t.column_3 AS original_data_field_3,
    t.column_4 AS original_data_field_4,
    CURRENT_TIMESTAMP() AS processed_at
FROM
    `project.dataset.cibasis_data24_raw` AS t -- Placeholder for the raw input table
WHERE
    -- Add any specific filtering conditions if identified from the original script
    TRUE
ORDER BY
    key_column_cleaned; -- Simulating sort -k 1