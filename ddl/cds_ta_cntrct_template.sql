-- DDL for the source staging BigQuery table cds_ta_cntrct_template
-- for job DW.BERT_AUSD_V_TA_CNTRCT_TEMPL.
-- This table is assumed to be populated from the legacy Oracle system.

CREATE TABLE IF NOT EXISTS `cds_ta_cntrct_template`
(
    cntrct_template_id INT64,
    cds_description_id INT64,
    insert_at DATE,
    modified_at DATE,
    valid_from DATE,
    valid_to DATE,
    is_production BOOLEAN
);