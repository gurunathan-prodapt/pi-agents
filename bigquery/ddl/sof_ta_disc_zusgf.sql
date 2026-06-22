-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_disc_zusgf.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh

CREATE TABLE IF NOT EXISTS `isbert_ds.sof_ta_disc_zusgf`
(
    cntrct_id           INT64,
    cntrct_obj_version  INT64,
    disc_vector_ty      STRING,
    rabatt_alle         STRING
);