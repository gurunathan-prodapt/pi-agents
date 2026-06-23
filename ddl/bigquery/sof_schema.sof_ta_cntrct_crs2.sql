-- BigQuery DDL for sof_schema.sof_ta_cntrct_crs2
-- Legacy Source: Oracle table sof$ta_cntrct_crs2
-- Job: DW.BERT_AUSD_V_TA_CNTRCT_CRS3
CREATE TABLE IF NOT EXISTS `my-gcp-project.sof_schema.sof_ta_cntrct_crs2` (
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
    rv_num STRING
);