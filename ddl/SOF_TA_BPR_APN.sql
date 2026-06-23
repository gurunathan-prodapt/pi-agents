-- BigQuery DDL for project.dataset.SOF_TA_BPR_APN
-- Replaces legacy table referenced in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_apn_vertrag.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh

CREATE TABLE IF NOT EXISTS project.dataset.SOF_TA_BPR_APN (
    cntrct_id_ref STRING NOT NULL,
    bpr_id INT6 NULL, -- Assuming bpr_id is an integer
    cntrct_id STRING NOT NULL,
    access_point_name STRING NOT NULL,
    -- Add other columns as per actual legacy schema
    effective_date DATE,
    expiration_date DATE
);