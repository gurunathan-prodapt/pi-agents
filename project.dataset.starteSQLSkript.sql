-- Target BigQuery stored procedure.
-- Replaces the KornShell script: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh
CREATE OR REPLACE PROCEDURE `project.dataset.starteSQLSkript`(
  p_Eintragsnr STRING,
  p_Skript STRING,
  p_Params ARRAY<STRING>
)
BEGIN
  DECLARE errcode INT64 DEFAULT 0;
  DECLARE ModulName STRING DEFAULT 'h_alis_sqlplus.ksh'; -- Original script name
  DECLARE ModulVersion STRING DEFAULT '1.0'; -- Placeholder, update as needed
  DECLARE v_script_sql STRING;
  DECLARE v_is_readable BOOLEAN;

  -- Log invocation (optional, as per design doc)
  -- INSERT INTO `project.dataset.invocation_log` (entry_number, script_name, params, timestamp)
  -- VALUES (p_Eintragsnr, p_Skript, p_Params, CURRENT_TIMESTAMP());

  -- Parameter Validation
  IF p_Eintragsnr IS NULL OR p_Skript IS NULL THEN
    CALL `project.dataset.DWMSG_MeldeFehler`(
      p_Eintragsnr,
      'ERROR',
      196, -- Custom error code for missing parameters
      CONCAT('FEHLER: Eintragsnr oder Skriptname ist NULL. Modul: ', ModulName, ' Version: ', ModulVersion)
    );
    -- In a BigQuery SP, we can't directly "exit" with an error code like a shell script.
    -- The procedure will complete, and the caller needs to check for error logs.
    RETURN;
  END IF;

  -- Script Readability Check
  SELECT script_sql, is_readable
  INTO v_script_sql, v_is_readable
  FROM `project.dataset.script_registry`
  WHERE script_name = p_Skript
  LIMIT 1;

  IF v_script_sql IS NULL OR NOT v_is_readable THEN
    CALL `project.dataset.DWMSG_MeldeFehler`(
      p_Eintragsnr,
      'ERROR',
      201, -- Custom error code for script not found/readable
      CONCAT('FEHLER: Skript ', p_Skript, ' nicht gefunden oder nicht lesbar in script_registry. Modul: ', ModulName, ' Version: ', ModulVersion)
    );
    RETURN;
  END IF;

  -- Informational Output (using a SELECT statement for logging, or insert into invocation_log)
  SELECT CONCAT('INFO: Rufe SQL-Skript auf mit folgenden Einstellungen: Skript: ', p_Skript, ', Parameter: ', IF(ARRAY_LENGTH(p_Params) > 0, ARRAY_TO_STRING(p_Params, ' '), 'Keine'), '. Modul: ', ModulName, ' Version: ', ModulVersion);

  -- Dynamic SQL Execution
  BEGIN
    EXECUTE IMMEDIATE v_script_sql;
    SET errcode = 0;
  EXCEPTION WHEN ERROR THEN
    SET errcode = 1; -- Capture BigQuery execution error
    CALL `project.dataset.DWMSG_MeldeFehler`(
      p_Eintragsnr,
      'ERROR',
      errcode, -- BigQuery error code (or a mapped custom one if more specific)
      CONCAT('FEHLER: Fehler beim Ausführen des Skripts ', p_Skript, '. BigQuery Fehlermeldung: ', @@error.message, '. Modul: ', ModulName, ' Version: ', ModulVersion)
    );
  END;

  -- Return equivalent (implicit return by completion or error logging)
  -- The original script returned the exit code. In BigQuery SP, the success/failure
  -- is determined by whether an unhandled exception occurs or by checking the error_log table.
  -- For a direct return value, the procedure signature would need an OUT parameter.
END;