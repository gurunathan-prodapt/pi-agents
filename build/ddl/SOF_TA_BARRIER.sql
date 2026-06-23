-- BigQuery DDL for legacy source table SOF$TA_BARRIER
-- Original job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh
CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.SOF_TA_BARRIER` (
    cntrct_id INT64,
    sperrart STRING,
    sperrgrund STRING,
    ist_stillegung INT64,
    sperr_ende TIMESTAMP,
    sperr_beginn TIMESTAMP,
    barrier_reason_cv INT64
);