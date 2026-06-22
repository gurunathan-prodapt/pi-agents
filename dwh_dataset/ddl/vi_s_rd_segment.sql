--
-- BigQuery DDL for dwh_dataset.vi_s_rd_segment
-- Legacy Source: dwh$vi_s_rd_segment (Oracle)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh
--
CREATE SCHEMA IF NOT EXISTS `dwh_dataset`;

CREATE TABLE IF NOT EXISTS `dwh_dataset.vi_s_rd_segment`
(
    `rechdef_id_carmen` INT64 NOT NULL,
    `segment_id` INT64
)
OPTIONS(
    description="BigQuery equivalent of Oracle view dwh$vi_s_rd_segment"
);