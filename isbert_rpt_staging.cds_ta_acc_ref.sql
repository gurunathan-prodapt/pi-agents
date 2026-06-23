-- BigQuery DDL for the staging table cds_ta_acc_ref
-- Replica of Oracle cds$ta_acc_ref@pcrs1 used by job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_acc_ref.ksh
CREATE TABLE IF NOT EXISTS `isbert_rpt_staging.cds_ta_acc_ref`
(
    insert_at               DATE,
    modified_at             DATE,
    valid_from              DATE,
    valid_to                DATE,
    is_production           INT64,
    ta_acc_ref_key          STRING,
    ta_acc_ref_id           STRING,
    ta_acc_id               STRING,
    ta_acc_code             STRING,
    ta_acc_bezeichnung      STRING,
    ta_acc_gueltigkeit_von  DATE,
    ta_acc_gueltigkeit_bis  DATE
    -- Add other columns from the source Oracle table as needed for a complete replica
);