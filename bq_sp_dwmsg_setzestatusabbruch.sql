-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Purpose: BigQuery Stored Procedure for DWMSG_SetzeStatusAbbruch.
CREATE OR REPLACE PROCEDURE `project_id.dataset_name.DWMSG_SetzeStatusAbbruch`(entry_nr STRING)
BEGIN
  IF entry_nr IS NULL OR entry_nr = '' THEN
    RAISE USING MESSAGE = 'Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben';
  END IF;
  UPDATE `project_id.dataset_name.message_table` SET status = 'ABBRUCH', updated_ts = CURRENT_TIMESTAMP() WHERE entry_nr = entry_nr;
END;