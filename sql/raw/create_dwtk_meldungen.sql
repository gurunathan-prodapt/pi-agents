-- DDL for BigQuery table raw.dwtk_meldungen
-- Replaces Oracle table isbert_schema.dwtk_meldungen from k_ausd_adressen.ksh
CREATE TABLE IF NOT EXISTS `PROJECT_ID.raw.dwtk_meldungen`
(
    timecreated                 TIMESTAMP,
    job_kennung                 STRING
);