--
-- BigQuery DDL for sof_dataset.ta_bp_ref
-- Legacy Source: sof$ta_bp_ref (Oracle)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh
--
CREATE SCHEMA IF NOT EXISTS `sof_dataset`;

CREATE TABLE IF NOT EXISTS `sof_dataset.ta_bp_ref`
(
    `cntrct_cp2_id` INT64 NOT NULL,
    `bp_id` INT64
)
OPTIONS(
    description="BigQuery equivalent of Oracle table sof$ta_bp_ref"
);