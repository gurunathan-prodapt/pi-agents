-- Target table DDL for BigQuery, replacing Oracle table sof$ta_barrier
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier.ksh
CREATE TABLE IF NOT EXISTS `my-project.data_warehouse.sof_ta_barrier` (
    cntrct_id STRING,
    barrier_kind_id STRING,
    barrier_init_cv STRING,
    barrier_reason_cv STRING,
    bfc_age TIMESTAMP,
    sperrart STRING,
    sperr_beginn DATE,
    sperr_ende DATE,
    sperrgrund STRING,
    ist_stillegung BOOL
);