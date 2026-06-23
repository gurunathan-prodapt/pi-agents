-- DDL for the source staging BigQuery table cds_ta_care_description
-- for job DW.BERT_AUSD_V_TA_CNTRCT_TEMPL.
-- This table is assumed to be populated from the legacy Oracle system.

CREATE TABLE IF NOT EXISTS `cds_ta_care_description`
(
    cds_description_id INT64,
    cds_description STRING,
    language INT64
);