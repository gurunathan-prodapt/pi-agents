-- BigQuery DDL for the target table PoolBasisprodukt
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh

-- NOTE: The actual schema for PoolBasisprodukt should be determined from its original definition.
-- This is a placeholder schema.
CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.PoolBasisprodukt` (
    `contract_id` STRING NOT NULL,
    `product_type` STRING,
    `start_date` DATE,
    `end_date` DATE,
    `value` NUMERIC,
    `stichtag` DATE NOT NULL,
    `processing_timestamp` TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);