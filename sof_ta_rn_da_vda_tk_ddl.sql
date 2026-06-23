-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_da_vda_tk.ksh
-- DDL for the target table 'sof$ta_rn_da_vda_tk' (inferred schema).

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.sof$ta_rn_da_vda_tk` (
    CNTRCT_ID STRING,
    DA_RN_MSISDN STRING,
    DA_RN_STATUS STRING,
    DA_RN_VALID_TO DATE,
    VDA_RN_MSISDN STRING,
    VDA_RN_STATUS STRING,
    VDA_RN_VALID_TO DATE,
    TK_RN_MSISDN STRING,
    TK_RN_STATUS STRING,
    TK_RN_VALID_TO DATE
)
OPTIONS(
  description="Target table for Basisprodukt RN data, schema inferred from SQL script."
);