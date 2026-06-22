-- DDL for project.dataset.sof_ta_bpr_basis
-- Legacy Source: Oracle SOF$TA_BPR_BASIS table referenced by vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh
CREATE OR REPLACE TABLE `project.dataset.sof_ta_bpr_basis` (
    cntrct_id STRING,
    bpr_id INT64,
    slave_number INT64,
    iccid STRING,
    imsi_mcc STRING,
    imsi_mnc STRING,
    imsi_hlr STRING,
    imsi_si STRING,
    valid_to DATE,
    E_ID STRING,
    CARD_TYPE_NAME STRING
);