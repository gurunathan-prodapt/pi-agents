-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Purpose: BigQuery Stored Procedure for DWMSG_SetzeStatusOK.
CREATE OR REPLACE PROCEDURE `project_id.dataset_name.DWMSG_SetzeStatusOK`(entry_nr STRING)
BEGIN
  IF entry_nr IS NULL OR entry_nr = '' THEN
    RAISE USING MESSAGE = 'Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben';
  END IF;
  UPDATE `project_id.dataset_name.message_table` SET status = 'OK', updated_ts = CURRENT_TIMESTAMP() WHERE entry_nr = entry_nr;
END;