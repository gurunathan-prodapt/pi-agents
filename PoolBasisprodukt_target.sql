-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_carmen.ksh
-- Description: BigQuery DDL for the target PoolBasisprodukt table.
-- Schema is generic as the original table schema was not provided.
CREATE TABLE IF NOT EXISTS `project.dataset.PoolBasisprodukt_target`
(
    -- Placeholder for actual schema from source `d_ausd_bp_ta_apn_carmen.sql`
    product_id          STRING,
    process_date        DATE,
    source_system_id    STRING,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    -- Add more relevant columns here based on the actual transformation
    payload             JSON
)
OPTIONS(
    description = "Target table for processed PoolBasisprodukt data."
);