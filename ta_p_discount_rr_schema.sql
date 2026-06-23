-- Schema for table ta_p_discount_rr, used by job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount_rr.ksh
-- This DDL is for Google BigQuery.
-- Replace `your_bigquery_project.your_bigquery_dataset` with your actual BigQuery project and dataset.

CREATE TABLE IF NOT EXISTS `your_bigquery_project.your_bigquery_dataset.ta_p_discount_rr` (
    cntrct_id                       STRING,
    discount_id                     STRING,
    disc_vector_ty                  STRING,
    cntrct_obj_version              STRING,
    cntrct_template_id              STRING,
    disc_invoice_item_id            STRING,
    rabatt                          NUMERIC,
    rabatthoehe                     NUMERIC,
    rabattierte_rech_pos            NUMERIC,
    contract_number                 STRING,
    std_vertrag                     STRING
);

-- Note: Data types are inferred. Please adjust them based on the actual data characteristics
-- and your specific BigQuery schema design requirements.
-- For example, `cntrct_id` might be INT64, `rabatt` might be FLOAT64, etc.
-- If this table is partitioned or clustered, those options should be added to the CREATE TABLE statement.
-- Example for a partitioned table:
-- CREATE TABLE IF NOT EXISTS `your_bigquery_project.your_bigquery_dataset.ta_p_discount_rr` (
--     ...
-- )
-- PARTITION BY _PARTITIONTIME
-- OPTIONS (
--     partition_expiration_days = 30
-- );
--
-- Or if based on a column:
-- CREATE TABLE IF NOT EXISTS `your_bigquery_project.your_bigquery_dataset.ta_p_discount_rr` (
--     ...
--     process_date DATE
-- )
-- PARTITION BY process_date;