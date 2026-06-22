-- DDL for ta_notice table
-- This table is the target for the data processing logic from d_ausd_v_ta_notice.sql
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.ta_notice` (
    cntrct_id STRING NOT NULL,
    valid_from DATE,
    valid_to DATE,
    entry_date_of_notice DATE,
    -- Assuming these columns might be useful for lineage or future extensions,
    -- mirroring the source table structure for `cds$ta_notice`.
    insert_at TIMESTAMP,
    modified_at TIMESTAMP,
    is_production INT64
);