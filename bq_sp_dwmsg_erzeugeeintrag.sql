-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Purpose: BigQuery Stored Procedure for DWMSG_ErzeugeEintrag.
CREATE OR REPLACE PROCEDURE `project_id.dataset_name.DWMSG_ErzeugeEintrag`(entry_nr STRING, job_kennung STRING, programmname STRING, logdatei STRING)
BEGIN
  IF entry_nr IS NULL OR entry_nr = '' THEN
    RAISE USING MESSAGE = 'Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben';
  END IF;
  INSERT INTO `project_id.dataset_name.message_table` (entry_nr, job_kennung, programmname, logdatei, status, created_ts, updated_ts)
  VALUES (entry_nr, job_kennung, programmname, logdatei, 'NEW', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
END;