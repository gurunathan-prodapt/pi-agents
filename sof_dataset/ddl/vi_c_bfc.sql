--
-- BigQuery DDL for sof_dataset.vi_c_bfc
-- Legacy Source: sof$vi_c_bfc (Oracle)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh
--
CREATE SCHEMA IF NOT EXISTS `sof_dataset`;

CREATE TABLE IF NOT EXISTS `sof_dataset.vi_c_bfc`
(
    `cntrct_id` INT64 NOT NULL,
    `bindefrist` INT64
)
OPTIONS(
    description="BigQuery equivalent of Oracle view sof$vi_c_bfc"
);