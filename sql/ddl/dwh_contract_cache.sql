-- Legacy Source: Assumed DWH source for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_einzeln.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_einzeln.ksh

-- This DDL is a placeholder. The actual schema should be derived from the DWH source.
CREATE TABLE IF NOT EXISTS `project.dataset.dwh_contract_cache` (
    dwh_vertrag_id INT64 NOT NULL OPTIONS(description="Unique identifier for the contract in DWH."),
    gueltig_von DATE NOT NULL OPTIONS(description="Contract validity start date."),
    gueltig_bis DATE NOT NULL OPTIONS(description="Contract validity end date."),
    ladedatum DATE NOT NULL OPTIONS(description="Date when the record was loaded into the DWH."),
    product_id STRING OPTIONS(description="Identifier for the product associated with the contract."),
    customer_id STRING OPTIONS(description="Identifier for the customer associated with the contract."),
    -- Add other relevant DWH columns as needed
    _metadata_load_timestamp TIMESTAMP OPTIONS(description="Timestamp of BigQuery load.")
)
PARTITION BY ladedatum -- Partitioning by load date is common for DWH historical data
CLUSTER BY dwh_vertrag_id
OPTIONS(
    description="Migrated DWH contract cache table containing historical contract data."
);