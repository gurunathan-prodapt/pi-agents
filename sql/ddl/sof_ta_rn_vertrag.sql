-- BigQuery DDL for sof_ta_rn_vertrag
-- Replaces Oracle table sof$ta_rn_vertrag
-- Job: DW.BERT_AUSD_BP_TA_BCP_MSISDN

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bigquery_dataset.sof_ta_rn_vertrag`
(
    cntrct_id      STRING,
    tn_tel_msisdn  STRING
);