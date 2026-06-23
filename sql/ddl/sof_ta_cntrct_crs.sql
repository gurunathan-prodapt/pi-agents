-- BigQuery DDL for the source table sof_ta_cntrct_crs
-- Required for DW.BERT_AUSD_V_TA_P_DISCOUNT_RR job.
-- This table is assumed to be populated via an Oracle to BigQuery data ingestion pipeline.

CREATE TABLE IF NOT EXISTS `your-gcp-project-id.your_bigquery_dataset.sof_ta_cntrct_crs` (
    cntrct_id INT64,
    obj_version INT64,
    contract_number STRING
);