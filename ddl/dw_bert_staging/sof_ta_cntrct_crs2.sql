-- Legacy Source: Implied schema from INSERT statement in d_ausd_v_ta_cntrct_crs2.sql
-- Job: DW.BERT_AUSD_V_TA_CNTRCT_CRS2

-- DDL for the target table sof_ta_cntrct_crs2 in the dw_bert_staging dataset.
-- This script will create the table if it does not already exist.

CREATE TABLE IF NOT EXISTS `dw_bert_staging.sof_ta_cntrct_crs2` (
    cntrct_id INT64,
    obj_version INT64,
    contract_number STRING,
    cntrct_template_id INT64,
    cntrct_validity_id INT64,
    valid_from DATE,
    com_per_ext_rea_cv STRING,
    billcycle_id INT64,
    vo_code STRING,
    cntrct_start_date DATE,
    cntrct_st INT64,
    cntrct_parent INT64,
    cntrct_ty INT64,
    cost_centre STRING,
    cost_centre_user STRING,
    commitment_reference_date DATE,
    order_number STRING,
    rv_num STRING
);