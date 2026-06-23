-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Purpose: BigQuery Stored Procedure for DWMSG_MeldeFehler.
CREATE OR REPLACE PROCEDURE `project_id.dataset_name.DWMSG_MeldeFehler`(entry_nr STRING, typ STRING, fehler_nr STRING, zusatz1 STRING, zusatz2 STRING)
BEGIN
  IF entry_nr IS NULL OR entry_nr = '' THEN
    RAISE USING MESSAGE = 'Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben';
  END IF;
  UPDATE `project_id.dataset_name.message_table`
  SET fehler_typ = typ, fehler_nr = fehler_nr, zusatz1 = zusatz1, zusatz2 = zusatz2, updated_ts = CURRENT_TIMESTAMP()
  WHERE entry_nr = entry_nr;
END;