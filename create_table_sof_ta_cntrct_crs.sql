--
-- BigQuery DDL for source table sof_ta_cntrct_crs
-- Replaces Oracle table sof$ta_cntrct_crs from job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount.ksh
--
CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_cntrct_crs`
(
    cntrct_id           STRING,
    obj_version         STRING,
    contract_number     STRING
);