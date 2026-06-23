-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Purpose: Create BigQuery message table.
CREATE TABLE IF NOT EXISTS `project_id.dataset_name.message_table` (
  entry_nr STRING,
  job_kennung STRING,
  programmname STRING,
  logdatei STRING,
  status STRING,
  fehler_typ STRING,
  fehler_nr STRING,
  zusatz1 STRING,
  zusatz2 STRING,
  zusatzinfos STRING,
  created_ts TIMESTAMP,
  updated_ts TIMESTAMP
);