-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Purpose: BigQuery Stored Procedure for DWMSG_SetzeStichtagInfo.
CREATE OR REPLACE PROCEDURE `project_id.dataset_name.DWMSG_SetzeStichtagInfo`(entry_nr STRING, stichtag STRING, stichtag_fmt STRING)
BEGIN
  IF entry_nr IS NULL OR entry_nr = '' THEN RAISE USING MESSAGE = 'Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben'; END IF;
  IF stichtag IS NULL OR stichtag = '' THEN RAISE USING MESSAGE = 'Argh!, keinen Stichtag angegeben!'; END IF;
  IF stichtag_fmt IS NULL OR stichtag_fmt = '' THEN RAISE USING MESSAGE = 'Argh!, Stichtagsangaben ohne Formatangaben knnen nicht verarbeitet werden!'; END IF;
  UPDATE `project_id.dataset_name.message_table` SET zusatzinfos = CAST(PARSE_TIMESTAMP(stichtag_fmt, stichtag) AS STRING), updated_ts = CURRENT_TIMESTAMP() WHERE entry_nr = entry_nr;
END;