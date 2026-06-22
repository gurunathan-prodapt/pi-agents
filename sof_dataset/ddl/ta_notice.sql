--
-- BigQuery DDL for sof_dataset.ta_notice
-- Legacy Source: sof$ta_notice (Oracle)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh
--
CREATE SCHEMA IF NOT EXISTS `sof_dataset`;

CREATE TABLE IF NOT EXISTS `sof_dataset.ta_notice`
(
    `cntrct_id` INT64 NOT NULL,
    `valid_from` DATE,
    `entry_date_of_notice` DATE
)
OPTIONS(
    description="BigQuery equivalent of Oracle table sof$ta_notice"
);