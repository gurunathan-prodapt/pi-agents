-- DDL for the metadata BigQuery table isbert_schema.dwtk_meldungen
-- for job DW.BERT_AUSD_V_TA_CNTRCT_TEMPL.
-- This table is assumed to be part of a broader metadata migration.

CREATE SCHEMA IF NOT EXISTS `isbert_schema`;

CREATE TABLE IF NOT EXISTS `isbert_schema.dwtk_meldungen`
(
    timecreated TIMESTAMP,
    job_kennung STRING
);