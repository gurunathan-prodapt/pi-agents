--
-- BigQuery DDL for sof_dataset.ta_action_assoc
-- Legacy Source: sof$ta_action_assoc (Oracle)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh
--
CREATE SCHEMA IF NOT EXISTS `sof_dataset`;

CREATE TABLE IF NOT EXISTS `sof_dataset.ta_action_assoc`
(
    `cntrct_id` INT64 NOT NULL,
    `rv_action_id` INT64
)
OPTIONS(
    description="BigQuery equivalent of Oracle table sof$ta_action_assoc"
);