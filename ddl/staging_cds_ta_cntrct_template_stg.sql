-- BigQuery DDL for DW.BERT_AUSD_V_TA_CNTRCT_TEMPL
-- Legacy Source: cds$ta_cntrct_template (Carmen DB via Oracle)
CREATE TABLE IF NOT EXISTS `your_gcp_project_id.staging.cds_ta_cntrct_template_stg` (
    cntrct_template_id INT64,
    cds_description_id INT64,
    insert_at TIMESTAMP,
    modified_at TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_production INT64
);