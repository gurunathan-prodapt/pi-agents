-- BigQuery DDL for sof_ta_inv_acc
-- Referenced in d_ausd_v_ta_vertrag_tmp.sql

CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_inv_acc`
(
    inv_definition_id STRING,
    account_reference STRING,
    sales_tax_freed STRING,
    billcycle_id INT64,
    inv_pay_ty_cv INT64,
    inv_media_cv INT64,
    rechn_inh_konfig_text STRING,
    cntrct_id STRING,
    -- Add other relevant columns
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);