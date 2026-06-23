-- Legacy Source: Commented 'join' operation between cibasis_data24.dat and cibasis_data96.dat in k_ausd_bp_ta_apn_vertrag.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh

CREATE OR REPLACE TABLE `project.dataset.cibasis_24_96`
OPTIONS(
  description="Joins cleaned data from cibasis_data24 and cibasis_data96, simulating legacy join operations."
) AS
SELECT
    t24.key_column_cleaned,
    t24.data_field_2_cleaned,
    t24.original_data_field_3,
    t24.original_data_field_4,
    t96.data_field_b_cleaned,
    t96.original_data_field_c,
    GREATEST(t24.processed_at, t96.processed_at) AS latest_processed_at
FROM
    `project.dataset.cibasis_data24_clean` AS t24
INNER JOIN -- Assuming an inner join for data integrity based on typical join usage
    `project.dataset.cibasis_data96_clean` AS t96
ON
    t24.key_column_cleaned = t96.key_column_cleaned -- Assuming join key based on the first cleaned column
WHERE
    TRUE; -- Add any additional join conditions or filters if known