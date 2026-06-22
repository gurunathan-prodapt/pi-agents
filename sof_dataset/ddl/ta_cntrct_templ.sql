--
-- BigQuery DDL for sof_dataset.ta_cntrct_templ
-- Legacy Source: sof$ta_cntrct_templ (Oracle)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh
--
CREATE SCHEMA IF NOT EXISTS `sof_dataset`;

CREATE TABLE IF NOT EXISTS `sof_dataset.ta_cntrct_templ`
(
    `cntrct_template_id` INT64 NOT NULL,
    `cds_description` STRING
)
OPTIONS(
    description="BigQuery equivalent of Oracle table sof$ta_cntrct_templ"
);