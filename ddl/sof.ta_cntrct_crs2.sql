-- DDL for sof.ta_cntrct_crs2
-- Migrated from Oracle table sof$ta_cntrct_crs2 referenced by DW.BERT_AUSD_V_TA_CNTRCT_CRS3
-- Job: DW.BERT_AUSD_V_TA_CNTRCT_CRS3

CREATE SCHEMA IF NOT EXISTS `sof`;

CREATE TABLE IF NOT EXISTS `sof.ta_cntrct_crs2` (
    cntrct_id STRING, -- Assuming string or int
    obj_version INT64,
    contract_number STRING,
    cntrct_template_id STRING, -- Assuming string or int
    cntrct_validity_id STRING, -- Assuming string or int
    valid_from DATE,
    com_per_ext_rea_cv STRING,
    billcycle_id STRING, -- Assuming string or int
    vo_code STRING,
    cntrct_start_date DATE,
    cntrct_st STRING, -- Assuming string or int
    cntrct_parent STRING, -- Assuming string or int
    cntrct_ty INT64,
    cost_centre STRING,
    cost_centre_user STRING,
    commitment_reference_date DATE,
    order_number STRING,
    rv_num STRING
    -- Add other columns as per the full Oracle schema if available
);