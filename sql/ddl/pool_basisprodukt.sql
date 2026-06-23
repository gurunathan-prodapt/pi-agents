-- DDL for PoolBasisprodukt table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh
-- This table is a placeholder for the output of d_ausd_bp_ta_msisdn.sql.
-- Columns are illustrative as the source SQL was not provided.

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bq_dataset.PoolBasisprodukt`
(
    stichtag DATE,
    msisdn STRING,
    produkt_id STRING,
    aktiv_von DATE,
    aktiv_bis DATE,
    -- Add more columns based on the actual d_ausd_bp_ta_msisdn.sql logic
    _processing_date DATE    -- Partitioning column for daily processing
)
PARTITION BY _processing_date
OPTIONS(
    description="Target table for processed MSISDN basis product data"
);