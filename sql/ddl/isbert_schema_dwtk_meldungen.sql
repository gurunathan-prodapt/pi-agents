-- BigQuery DDL for the staging table isbert_schema.dwtk_meldungen
-- Replaces legacy Oracle table isbert_schema.dwtk_meldungen for job DW.BERT_AUSD_V_TA_P_VERTRAG

CREATE SCHEMA IF NOT EXISTS `project_id.isbert_schema`;

CREATE TABLE IF NOT EXISTS `project_id.isbert_schema.dwtk_meldungen`
(
    timecreated TIMESTAMP,
    job_kennung STRING
)
;