-- DDL for project.dataset.sof_ta_bank
-- Replaces: N/A (intermediate table from d_ausd_rechempf.sql)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_rechempf.ksh

CREATE TABLE IF NOT EXISTS project.dataset.sof_ta_bank (
  BANK_ID STRING,
  INSERT_AT TIMESTAMP,
  COUNTRY_CODE STRING,
  BANK_SORT_NAME STRING,
  BANK_NAME STRING,
  INSERT_BY STRING,
  MODIFIED_AT TIMESTAMP,
  MODIFIED_BY STRING,
  MODIFY_REASON STRING,
  IS_IN_ARCHIVE INT64,
  ROW_VERSION INT64,
  BIC STRING,
  BANK_INTERNATIONAL_ID STRING
);