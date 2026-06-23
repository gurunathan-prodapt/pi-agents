-- BigQuery DDL for the target table sof_ta_p_discount_rr
-- Replaces object created by DW.BERT_AUSD_V_TA_P_DISCOUNT_RR job.

CREATE TABLE IF NOT EXISTS `your-gcp-project-id.your_bigquery_dataset.sof_ta_p_discount_rr` (
    cntrct_id INT64,
    discount_id INT64,
    disc_vector_ty STRING,
    cntrct_obj_version INT64,
    cntrct_template_id INT64,
    disc_invoice_item_id INT64,
    rabatt NUMERIC,
    rabatthoehe NUMERIC,
    rabattierte_rech_pos NUMERIC,
    contract_number STRING,
    std_vertrag STRING
);