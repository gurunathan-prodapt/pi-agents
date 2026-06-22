-- BigQuery DDL for the ta_inv_assign table
-- Replaces: Component of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_assign.ksh
-- Based on the schema of cds$ta_inv_assignment from d_ausd_v_ta_inv_assign.sql

CREATE TABLE IF NOT EXISTS `your-gcp-project.isbert_target_data.ta_inv_assign` (
    cntrct_id STRING NOT NULL,
    inv_definition_id STRING NOT NULL,
    insert_at TIMESTAMP,
    modified_at TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_production BOOL
);