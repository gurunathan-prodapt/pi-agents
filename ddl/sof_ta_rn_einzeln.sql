-- BigQuery DDL for the table sof_ta_rn_einzeln
-- Replaces Oracle table sof$ta_rn_einzeln
-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh

CREATE TABLE IF NOT EXISTS my_project.my_dataset.sof_ta_rn_einzeln
(
    contract_id STRING,
    product_code STRING,
    DA_RN_msisdn STRING,
    VDA_RN_msisdn STRING,
    TK_RN_msisdn STRING,
    valid_from_dt DATE,
    valid_to_dt DATE,
    data_value NUMERIC,
    -- Add other relevant columns from the original Oracle table
    -- For example:
    -- additional_info STRING,
    -- amount NUMERIC
);