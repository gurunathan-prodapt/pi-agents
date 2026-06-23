--
-- BigQuery DDL for fos_contract_cache table.
-- This table serves as the output for "Forderungsscoring", replacing the "FOS-Tabelle".
--
CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bq_dataset.fos_contract_cache`
(
    contract_id         STRING,      -- Unique identifier for the contract
    customer_id         STRING,      -- Customer associated with the contract
    product_type        STRING,      -- Type of product (e.g., 'FAX', 'Data24')
    gueltig_von_date    DATE,        -- Validity start date of the contract entry
    gueltig_bis_date    DATE,        -- Validity end date of the contract entry
    laden_datum         DATE,        -- Load date of the contract entry into DWH
    contract_data_json  JSON,        -- Placeholder for additional contract details
    stichtag_processed  DATE,        -- The Stichtag for which this record was processed
    load_timestamp      TIMESTAMP    -- When this record was loaded into fos_contract_cache
)
PARTITION BY gueltig_von_date -- Or by stichtag_processed, depending on access patterns
CLUSTER BY contract_id, product_type
OPTIONS(
    description = "BigQuery table for 'Forderungsscoring' contract cache, replacing legacy FOS-Tabelle."
);