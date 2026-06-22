-- Legacy source table: sof$ta_iccid_einzeln
-- Job: DW.BERT_AUSD_BP_TA_ICCID_VERTRAG

CREATE SCHEMA IF NOT EXISTS `gcp_project.dataset`;

CREATE TABLE IF NOT EXISTS `gcp_project.dataset.sof_ta_iccid_einzeln`
(
    CNTRCT_ID STRING NOT NULL,
    ICCID_TYPE STRING NOT NULL, -- e.g., 'TN', 'TC', 'TB', 'MS1', ..., 'MS10'
    ICCID STRING,
    IMSI_MCC STRING,
    IMSI_MNC STRING,
    IMSI_HLR STRING,
    IMSI_SI STRING,
    STATUS STRING,
    VALID_TO DATE,
    E_ID STRING,
    CARD_TYPE_NAME STRING
);