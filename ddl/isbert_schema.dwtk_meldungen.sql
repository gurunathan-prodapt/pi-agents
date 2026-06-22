-- DDL for isbert_schema.dwtk_meldungen
-- Migrated from Oracle table referenced by DW.BERT_AUSD_V_TA_CNTRCT_CRS3
-- Job: DW.BERT_AUSD_V_TA_CNTRCT_CRS3

CREATE SCHEMA IF NOT EXISTS `isbert_schema`;

CREATE TABLE IF NOT EXISTS `isbert_schema.dwtk_meldungen` (
    job_kennung STRING,
    timecreated TIMESTAMP
    -- Add other columns as per the full Oracle schema if available
);