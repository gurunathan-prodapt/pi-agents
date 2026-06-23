-- BigQuery DDL for sof_ta_bcp_msisdn
-- Replaces Oracle table sof$ta_bcp_msisdn
-- Job: DW.BERT_AUSD_BP_TA_BCP_MSISDN

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bigquery_dataset.sof_ta_bcp_msisdn`
(
    CNTRCT_ID      STRING,
    BPR_ID         STRING,
    CNTRCT_ID_REF  STRING,
    TN_TEL_MSISDN  STRING
);