-- BigQuery DDL for DW.BERT_AUSD_V_TA_CNTRCT_TEMPL
-- Legacy Source: cds$ta_care_description (Carmen DB via Oracle)
CREATE TABLE IF NOT EXISTS `your_gcp_project_id.staging.cds_ta_care_description_stg` (
    cds_description_id INT64,
    cds_description STRING,
    language INT64
);