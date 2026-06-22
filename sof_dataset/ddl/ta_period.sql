--
-- BigQuery DDL for sof_dataset.ta_period
-- Legacy Source: sof$ta_period (Oracle)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh
--
CREATE SCHEMA IF NOT EXISTS `sof_dataset`;

CREATE TABLE IF NOT EXISTS `sof_dataset.ta_period`
(
    `period_id` INT64 NOT NULL,
    `number_time_measurement` INT64,
    `einheit` STRING
)
OPTIONS(
    description="BigQuery equivalent of Oracle table sof$ta_period"
);