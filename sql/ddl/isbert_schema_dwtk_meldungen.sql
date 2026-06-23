-- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh
-- BigQuery DDL for source table isbert_schema.dwtk_meldungen
-- Placeholder DDL based on usage in d_ausd_bp_ta_bcp_msisdn.sql

CREATE TABLE IF NOT EXISTS `dataset.dwtk_meldungen` (
    job_kennung STRING NOT NULL,
    timecreated TIMESTAMP NOT NULL
    -- Add other columns if known from source system
);