-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Purpose: BigQuery Stored Procedure for DWMSG_AppendTimingInfos.
CREATE OR REPLACE PROCEDURE `project_id.dataset_name.DWMSG_AppendTimingInfos`(entry_nr STRING, info_text STRING, date_format STRING)
BEGIN
  IF entry_nr IS NULL OR entry_nr = '' THEN RAISE USING MESSAGE = 'Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben'; END IF;
  IF date_format IS NULL OR date_format = '' THEN RAISE USING MESSAGE = 'Argh!, Formatangabe erforderlich!'; END IF;
  UPDATE `project_id.dataset_name.message_table`
  SET zusatzinfos = CONCAT(COALESCE(zusatzinfos, ''), info_text, ' ', FORMAT_TIMESTAMP(date_format, CURRENT_TIMESTAMP()), ' '),
      updated_ts = CURRENT_TIMESTAMP()
  WHERE entry_nr = entry_nr;
END;