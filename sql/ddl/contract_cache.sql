-- DDL for contract_cache table
-- Represents DWH.TA_C_VERTRAG from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_apn.ksh
-- This table is assumed to be populated by an upstream process.
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.contract_cache` (
    dwh_vertrag_id INT66 NOT NULL OPTIONS(description="Contract identifier"),
    gueltig_von DATE NOT NULL OPTIONS(description="Validity start date"),
    gueltig_bis DATE NOT NULL OPTIONS(description="Validity end date"),
    ladedatum DATE NOT NULL OPTIONS(description="Load date"),
    col_a STRING OPTIONS(description="Example column from source"),
    col_b STRING OPTIONS(description="Example column from source"),
    -- Add other columns from DWH.TA_C_VERTRAG as needed
    PRIMARY KEY (dwh_vertrag_id) NOT ENFORCED
);