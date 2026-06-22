-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_instance.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_instance.ksh

-- Placeholder DDL for the source contract cache table (`DWH$TA_C_VERTRAG` equivalent).
-- The exact schema needs to be defined based on the analysis of `k_ausd_bp_ta_bpr_instance.ksh`.
-- This example includes columns explicitly referenced in the design document.
CREATE TABLE IF NOT EXISTS `project.dataset.source_contract_cache` (
    DWH_VERTRAG_ID INT64 OPTIONS(description="Contract ID, used for matching and restart logic"),
    Gueltig_von DATE OPTIONS(description="Validity start date of the contract"),
    Gueltig_bis DATE OPTIONS(description="Validity end date of the contract"),
    LADEDATUM DATE OPTIONS(description="Load date of the record"),
    -- Add other columns from the actual source schema here
    example_column_1 STRING,
    example_column_2 INT64
);