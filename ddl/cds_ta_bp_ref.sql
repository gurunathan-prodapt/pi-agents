-- DDL for BigQuery table cds_ta_bp_ref (renamed from CDS$TA_BP_REF)
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh

CREATE TABLE IF NOT EXISTS `my_project.my_dataset.cds_ta_bp_ref` (
    cntrct_cp2_id INT64,
    bp_id INT64,
    insert_at TIMESTAMP,
    modified_at TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_production INT64,
    bp_ref_ty INT64
);