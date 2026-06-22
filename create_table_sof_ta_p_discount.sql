--
-- BigQuery DDL for target table sof_ta_p_discount
-- Replaces Oracle table sof$ta_p_discount from job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount.ksh
--
CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_p_discount`
(
    cntrct_id           STRING,
    disc_vector_ty      STRING,
    cntrct_obj_version  STRING,
    rabatt_alle         STRING,
    contract_number     STRING
);