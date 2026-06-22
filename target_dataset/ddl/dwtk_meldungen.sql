--
-- BigQuery DDL for isbert_dataset.dwtk_meldungen
-- Legacy Source: isbert_schema.dwtk_meldungen (Oracle)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh
--
CREATE SCHEMA IF NOT EXISTS `isbert_dataset`;

CREATE TABLE IF NOT EXISTS `isbert_dataset.dwtk_meldungen`
(
    `job_kennung` STRING NOT NULL,
    `timecreated` TIMESTAMP NOT NULL
)
OPTIONS(
    description="BigQuery equivalent of Oracle table isbert_schema.dwtk_meldungen"
);