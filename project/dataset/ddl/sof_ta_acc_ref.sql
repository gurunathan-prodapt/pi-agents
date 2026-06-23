-- DDL for project.dataset.sof_ta_acc_ref
-- Legacy source: sof$ta_acc_ref (Oracle)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_acc_ref.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_acc_ref`
(
    acc_ref_id        INT64,
    account_reference STRING
);