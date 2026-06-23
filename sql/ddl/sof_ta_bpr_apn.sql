-- DDL for BigQuery table sof_ta_bpr_apn (target table)
-- Replaces data from legacy Oracle target.
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.sof_ta_bpr_apn`
(
    CNTRCT_ID STRING,
    BPR_ID INT64,
    CNTRCT_ID_REF STRING,
    ACCESS_POINT_NAME STRING
);