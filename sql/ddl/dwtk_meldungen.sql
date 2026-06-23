-- BigQuery DDL for isbert_schema.dwtk_meldungen
-- Replaces Oracle table isbert_schema.dwtk_meldungen
-- Job: DW.BERT_AUSD_BP_TA_BCP_MSISDN

CREATE TABLE IF NOT EXISTS `your_gcp_project.isbert_schema.dwtk_meldungen`
(
    timecreated  TIMESTAMP,
    job_kennung  STRING
);