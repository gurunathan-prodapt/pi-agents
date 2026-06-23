-- BigQuery DDL for sof_ta_bpr_bcp
-- Replaces Oracle table sof$ta_bpr_bcp
-- Job: DW.BERT_AUSD_BP_TA_BCP_MSISDN

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bigquery_dataset.sof_ta_bpr_bcp`
(
    cntrct_id      STRING,
    bpr_id         STRING,
    cntrct_id_ref  STRING
);