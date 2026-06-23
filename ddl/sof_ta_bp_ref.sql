-- DDL for BigQuery table sof_ta_bp_ref (renamed from SOF$TA_BP_REF)
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh

CREATE TABLE IF NOT EXISTS `my_project.my_dataset.sof_ta_bp_ref` (
    cntrct_cp2_id INT64,
    bp_id INT64
);