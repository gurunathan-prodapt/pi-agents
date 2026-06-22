-- DDL for sof.ta_cntrct_crs3
-- Target table for DW.BERT_AUSD_V_TA_CNTRCT_CRS3
-- Job: DW.BERT_AUSD_V_TA_CNTRCT_CRS3

CREATE SCHEMA IF NOT EXISTS `sof`;

CREATE TABLE IF NOT EXISTS `sof.ta_cntrct_crs3` (
    cntrct_id STRING,
    obj_version INT64,
    contract_number STRING,
    cntrct_template_id STRING,
    cntrct_validity_id STRING,
    valid_from DATE,
    com_per_ext_rea_cv STRING,
    billcycle_id STRING,
    vo_code STRING,
    cntrct_start_date DATE,
    cntrct_st STRING,
    cntrct_parent STRING,
    cntrct_ty INT64,
    cost_centre STRING,
    cost_centre_user STRING,
    commitment_reference_date DATE,
    order_number STRING,
    rv_num STRING,
    twinbill STRING,
    twin_vertrag_id STRING
    -- Add other columns if any based on a complete schema.
);