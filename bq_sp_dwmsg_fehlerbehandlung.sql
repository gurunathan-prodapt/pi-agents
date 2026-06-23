-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Purpose: BigQuery Stored Procedure for DWMSG_Fehlerbehandlung.
CREATE OR REPLACE PROCEDURE `project_id.dataset_name.DWMSG_Fehlerbehandlung`(entry_nr STRING)
BEGIN
  -- Simulating error code capture and handling
  DECLARE fehler_nr INT64 DEFAULT 1; -- Placeholder, actual error will come from EXCEPTION
  DECLARE kUnerwFehler INT64 DEFAULT 10;
  CALL `project_id.dataset_name.DWMSG_MeldeFehler`(entry_nr, 'F', CAST(kUnerwFehler AS STRING), CONCAT('ErrorCode ist: ', CAST(fehler_nr AS STRING)), NULL);
  CALL `project_id.dataset_name.DWMSG_SetzeStatusAbbruch`(entry_nr);
END;