-- BigQuery DDL for the metadata/logging table dwtk_meldungen
-- Replaces Oracle table isbert_schema.dwtk_meldungen for DW.BERT_AUSD_V_TA_P_DISCOUNT_RR job.
-- This table is assumed to be populated via an Oracle to BigQuery data ingestion pipeline
-- or a BigQuery-native logging mechanism.

CREATE TABLE IF NOT EXISTS `your-gcp-project-id.your_bigquery_dataset.dwtk_meldungen` (
    timecreated TIMESTAMP,
    job_kennung STRING
);