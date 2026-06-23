-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Purpose: BigQuery Stored Procedure for DWMSG_ErmittleNr.
CREATE OR REPLACE PROCEDURE `project_id.dataset_name.DWMSG_ErmittleNr`(OUT var_name STRING)
BEGIN
  SET var_name = GENERATE_UUID(); -- Or use a sequence generator if specific numbering is needed
END;