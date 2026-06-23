-- DDL for project.dataset.sof_ta_bank_verb
-- Replaces: N/A (intermediate table from d_ausd_rechempf.sql)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_rechempf.ksh

CREATE TABLE IF NOT EXISTS project.dataset.sof_ta_bank_verb (
  MEANS_OF_PAYMENT_ID STRING,
  BP_ID STRING,
  ACCOUNT_NUMBER_ACC STRING,
  BANK_NAME STRING,
  BANK_SORT_NAME STRING,
  IBAN STRING,
  BIC STRING
);