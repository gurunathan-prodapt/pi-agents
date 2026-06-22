--
-- BigQuery DDL for sof_dataset.ta_apn_ve
-- Legacy Source: sof$ta_apn_ve (Oracle)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh
--
CREATE SCHEMA IF NOT EXISTS `sof_dataset`;

CREATE TABLE IF NOT EXISTS `sof_dataset.ta_apn_ve`
(
    `cntrct_id` INT64 NOT NULL,
    `access_point_name` STRING
)
OPTIONS(
    description="BigQuery equivalent of Oracle table sof$ta_apn_ve"
);