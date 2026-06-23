-- BigQuery DDL for SOF$TA_CNTRCT_CRS3
-- Replaces usage in legacy job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh
-- Based on usage in d_ausd_v_ta_vertrag_tmp.sql

CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_cntrct_crs3`
(
    cntrct_id                 STRING,
    rv_num                    STRING,
    vo_code                   STRING,
    order_number              STRING,
    cntrct_start_date         DATE,
    cntrct_st                 INT64,
    twinbill                  STRING,
    commitment_reference_date DATE,
    cntrct_validity_id        INT64,
    cost_centre               STRING,
    cost_centre_user          STRING,
    cntrct_ty                 INT64,
    contract_number           STRING,
    cntrct_template_id        INT64,
    -- Add other columns if they exist in the source table
    loaded_at                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);