--
-- BigQuery DDL for source table sof_ta_disc_zusgf
-- Replaces Oracle table sof$ta_disc_zusgf from job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount.ksh
--
CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_disc_zusgf`
(
    cntrct_id           STRING,
    disc_vector_ty      STRING,
    cntrct_obj_version  STRING,
    rabatt_alle         STRING
);