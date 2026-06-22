--
-- BigQuery DDL for sof_dataset.ta_cntrct_valid
-- Legacy Source: sof$ta_cntrct_valid (Oracle)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh
--
CREATE SCHEMA IF NOT EXISTS `sof_dataset`;

CREATE TABLE IF NOT EXISTS `sof_dataset.ta_cntrct_valid`
(
    `cntrct_validity_id` INT64 NOT NULL,
    `first_period_id` INT64
)
OPTIONS(
    description="BigQuery equivalent of Oracle table sof$ta_cntrct_valid"
);