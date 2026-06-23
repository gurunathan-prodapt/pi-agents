-- BigQuery DDL for logging and control tables
-- Replaces logging functionality from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_aurd_rechstan.ksh

CREATE SCHEMA IF NOT EXISTS `project.dataset`;

CREATE TABLE IF NOT EXISTS `project.dataset.job_control` (
  eintragsnr INT64 NOT NULL,
  job_kennung STRING NOT NULL,
  script_name STRING,
  logdateiname STRING,
  stichtag STRING,
  status STRING,
  start_ts TIMESTAMP,
  end_ts TIMESTAMP,
  error_message STRING,
  sysdate STRING
);

CREATE TABLE IF NOT EXISTS `project.dataset.job_run_log` (
  eintragsnr INT64 NOT NULL,
  job_kennung STRING NOT NULL,
  log_ts TIMESTAMP NOT NULL,
  message STRING,
  status STRING
);

CREATE TABLE IF NOT EXISTS `project.dataset.job_error_log` (
  eintragsnr INT64 NOT NULL,
  job_kennung STRING NOT NULL,
  log_ts TIMESTAMP NOT NULL,
  error_code STRING,
  error_message STRING,
  stichtag STRING,
  wiederanlaufwert INT64
);