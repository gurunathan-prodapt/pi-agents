-- DDL for project.dataset.cds_ta_acc_ref
-- Legacy source: cds$ta_acc_ref (Oracle)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_acc_ref.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.cds_ta_acc_ref`
(
    acc_ref_id        INT64,
    account_reference STRING,
    insert_at         TIMESTAMP,
    modified_at       TIMESTAMP,
    valid_from        TIMESTAMP,
    valid_to          TIMESTAMP,
    is_production     INT64
);