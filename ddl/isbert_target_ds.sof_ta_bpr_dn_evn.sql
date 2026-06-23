-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_geschaeftspartner.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_geschaeftspartner.ksh

CREATE TABLE IF NOT EXISTS isbert_target_ds.sof_ta_bpr_dn_evn (
    CNTRCT_ID STRING,
    BPR_ID INT64,
    BPR_INSTANCE_ID INT64,
    CNTRCT_ID_REF STRING,
    COLUMN_5VALID_TO DATE
);