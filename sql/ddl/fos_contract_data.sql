-- Legacy Source: Target for Forderungsscoring (FOS) from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_einzeln.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_einzeln.ksh

-- This DDL is a placeholder. The actual schema should be defined based on FOS requirements.
CREATE TABLE IF NOT EXISTS `project.dataset.fos_contract_data` (
    contract_id INT64 NOT NULL OPTIONS(description="Identifier for the contract."),
    product_id STRING OPTIONS(description="Identifier for the product."),
    customer_id STRING OPTIONS(description="Identifier for the customer."),
    stichtag DATE NOT NULL OPTIONS(description="The processing date for which this data was generated."),
    load_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when this data was loaded into FOS target."),
    -- Add other columns required by the FOS system based on the kernel script's output
    UNIQUE (contract_id, stichtag) NOT ENFORCED -- Assuming unique per contract for a given processing date
)
PARTITION BY stichtag
CLUSTER BY contract_id
OPTIONS(
    description="Target table for credit scoring (FOS) containing extracted contract data."
);