-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Purpose: Create BigQuery dataset for logging and error management.
CREATE SCHEMA IF NOT EXISTS `project_id.dataset_name`
OPTIONS (
  location = 'US'
);