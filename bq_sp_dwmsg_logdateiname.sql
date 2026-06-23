-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Purpose: BigQuery Stored Procedure for DWMSG_Logdateiname.
CREATE OR REPLACE PROCEDURE `project_id.dataset_name.DWMSG_Logdateiname`(OUT var_name STRING, job_kennung STRING, entry_nr STRING)
BEGIN
  SET var_name = CONCAT(
    '/protocol/', -- Re-evaluate this path for Cloud Storage integration
    job_kennung,
    '_',
    FORMAT_TIMESTAMP('%Y%m%d_%H%M', CURRENT_TIMESTAMP()),
    '_',
    entry_nr,
    '.log'
  );
END;