-- BigQuery DDL for project.dataset.SOF_TA_APN_VERTRAG
-- Replaces legacy table referenced in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_apn_vertrag.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh

CREATE TABLE IF NOT EXISTS project.dataset.SOF_TA_APN_VERTRAG (
    cntrct_id STRING NOT NULL,
    access_point_names_aggregated STRING, -- Aggregated APNs
    cntrct_id_refs_aggregated STRING,     -- Aggregated Contract ID Refs
    processing_stichtag DATE NOT NULL -- Adding this to track data by stichtag
);