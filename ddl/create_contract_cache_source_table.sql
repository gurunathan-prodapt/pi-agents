-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_evn.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_evn.ksh

-- Placeholder schema for contract_cache_source. Adjust column types and names as per actual source DWH schema.
CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bigquery_dataset.contract_cache_source` (
    DWH_VERTRAG_ID INT64,
    Gueltig_von DATE,
    Gueltig_bis DATE,
    LADEDATUM DATE,
    -- Add other columns from the source DWH contract cache here
    contract_data_field_1 STRING,
    contract_data_field_2 NUMERIC
);