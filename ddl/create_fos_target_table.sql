-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_evn.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_evn.ksh

-- Placeholder schema for fos_target_table. Should align with the data inserted from contract_cache_source.
CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bigquery_dataset.fos_target_table` (
    DWH_VERTRAG_ID INT64,
    Gueltig_von DATE,
    Gueltig_bis DATE,
    LADEDATUM DATE,
    stichtag DATE, -- Added stichtag to identify data for a specific cutoff date
    -- Add other columns that will be populated from contract_cache_source
    contract_data_field_1 STRING,
    contract_data_field_2 NUMERIC
);