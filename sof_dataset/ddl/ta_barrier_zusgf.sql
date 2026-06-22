--
-- BigQuery DDL for sof_dataset.ta_barrier_zusgf
-- Legacy Source: sof$ta_barrier_zusgf (Oracle)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh
--
CREATE SCHEMA IF NOT EXISTS `sof_dataset`;

CREATE TABLE IF NOT EXISTS `sof_dataset.ta_barrier_zusgf`
(
    `cntrct_id` INT64 NOT NULL,
    `sperrart_alle` STRING,
    `sperrgrund_alle` STRING,
    `stilllegungszeitraum_alle` STRING,
    `sperrgrund_zusgf` INT64
)
OPTIONS(
    description="BigQuery equivalent of Oracle table sof$ta_barrier_zusgf"
);