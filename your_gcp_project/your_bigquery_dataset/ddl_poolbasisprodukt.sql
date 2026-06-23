-- DDL for PoolBasisprodukt table for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh
-- This is a placeholder DDL. The actual schema for PoolBasisprodukt needs to be defined
-- based on its source system schema. This example assumes a simple structure.
CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bigquery_dataset.PoolBasisprodukt` (
    -- Example columns, replace with actual schema from source `PoolBasisprodukt`
    produkt_id STRING,
    basis_datum DATE,
    wert NUMERIC,
    beschreibung STRING,
    last_updated TIMESTAMP
);