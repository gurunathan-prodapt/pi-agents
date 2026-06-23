-- Migrated from vobs/dw_source/isrpt/isbert/install_save/k_ausd_bp_ta_tarifoption.ksh
-- DDL for the PoolBasisprodukt table in Google BigQuery.
-- This is a placeholder schema based on the design document's mention of the table.
-- Adjust column names and types as per the actual source system schema and data requirements.

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.PoolBasisprodukt` (
    product_id STRING NOT NULL,
    product_name STRING,
    tariff_option_code STRING,
    effective_start_date DATE NOT NULL,
    effective_end_date DATE,
    value NUMERIC,
    status STRING,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);