-- Target BigQuery table for script metadata.
-- Replaces functionality related to file existence and readability checks in the legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh
CREATE TABLE `project.dataset.script_registry` (
  script_name STRING,
  script_sql STRING,
  is_readable BOOLEAN,
  original_source_path STRING
);