-- Legacy Source: Final output (cibasisprodukt.csv) generation implied from k_ausd_bp_ta_apn_vertrag.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh

CREATE OR REPLACE TABLE `project.dataset.cibasisprodukt`
OPTIONS(
  description="Final output table, combining relevant cleaned and joined data, replacing cibasisprodukt.csv."
) AS
SELECT
    t2496.key_column_cleaned AS product_identifier,
    t2496.data_field_2_cleaned AS product_short_description,
    t2496.original_data_field_3 AS product_category,
    t2496.original_data_field_4 AS product_attribute_value,
    t2496.data_field_b_cleaned AS product_long_description,
    t2496.original_data_field_c AS product_source_system,
    cf.fax_number_cleaned AS associated_fax_number, -- Example of integrating fax data
    t2496.latest_processed_at AS last_data_update,
    CURRENT_TIMESTAMP() AS record_creation_timestamp
FROM
    `project.dataset.cibasis_24_96` AS t2496
LEFT JOIN
    `project.dataset.cibasis_fax_clean` AS cf
ON
    t2496.key_column_cleaned = cf.fax_number_cleaned -- Example join if the product key relates to a fax number
WHERE
    TRUE; -- Add any final filtering criteria for the output product data