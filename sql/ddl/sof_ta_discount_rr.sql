-- BigQuery DDL for the source table sof_ta_discount_rr
-- Required for DW.BERT_AUSD_V_TA_P_DISCOUNT_RR job.
-- This table is assumed to be populated via an Oracle to BigQuery data ingestion pipeline.

CREATE TABLE IF NOT EXISTS `your-gcp-project-id.your_bigquery_dataset.sof_ta_discount_rr` (
    cntrct_id INT64,
    discount_id INT64,
    disc_vector_ty STRING,
    cntrct_obj_version INT64,
    cntrct_template_id INT64,
    disc_invoice_item_id INT64,
    rabatt NUMERIC,
    rabatthoehe NUMERIC,
    rabattierte_rech_pos NUMERIC,
    zeitstempel DATE
);