-- BigQuery DDL for the source table sof_ta_cntrct_templ
-- Required for DW.BERT_AUSD_V_TA_P_DISCOUNT_RR job.
-- This table is assumed to be populated via an Oracle to BigQuery data ingestion pipeline.

CREATE TABLE IF NOT EXISTS `your-gcp-project-id.your_bigquery_dataset.sof_ta_cntrct_templ` (
    cntrct_template_id INT64,
    cds_description STRING
);