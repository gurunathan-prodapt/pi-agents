-- BigQuery DDL for isbert_schema dataset
-- Replaces parts of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_rechempf.ksh

CREATE SCHEMA IF NOT EXISTS `isbert_schema`;

CREATE TABLE IF NOT EXISTS `isbert_schema.dwtk_meldungen`
(
  job_kennung STRING,
  timecreated TIMESTAMP
);