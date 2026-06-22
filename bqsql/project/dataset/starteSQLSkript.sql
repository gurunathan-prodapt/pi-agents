-- Target code for legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh
-- This BigQuery Stored Procedure is the re-engineered version of the h_alis_sqlplus.ksh script.

CREATE OR REPLACE PROCEDURE `project.dataset.starteSQLSkript`(
  p_Eintragsnr STRING,
  p_Skript STRING,
  p_Parameter ARRAY<STRING>
)
BEGIN
  DECLARE Modul_Name STRING DEFAULT 'alis_sqlplus';
  DECLARE Modul_Version STRING DEFAULT 'V1.1.3';
  DECLARE errcode INT64 DEFAULT 0;
  DECLARE p_Parameter_String STRING DEFAULT '';
  DECLARE script_exists BOOL DEFAULT FALSE;
  DECLARE sql_to_execute STRING;

  SET p_Parameter_String = ARRAY_TO_STRING(p_Parameter, ' ');

  IF p_Eintragsnr IS NULL OR p_Eintragsnr = '' OR p_Skript IS NULL OR p_Skript = '' THEN
    CALL `project.dataset.DWMSG_MeldeFehler`(
      p_Eintragsnr,
      'E',
      196,
      CONCAT(Modul_Name, ' ', Modul_Version, ' starteSQLSkript')
    );
    -- Using SELECT to return the error code, as BigQuery procedures do not have explicit return statements
    SELECT 196 AS return_code;
    LEAVE;
  END IF;

  SET script_exists = (
    SELECT COUNT(1) > 0
    FROM `project.dataset.sql_script_registry`
    WHERE script_name = p_Skript
      AND is_readable = TRUE
  );

  IF NOT script_exists THEN
    CALL `project.dataset.DWMSG_MeldeFehler`(
      p_Eintragsnr,
      'E',
      201,
      p_Skript
    );
    SELECT 201 AS return_code;
    LEAVE;
  END IF;

  -- Logging equivalent of 'echo' statements
  SELECT 'Rufe SQL*PLUS auf mit folgenden Einstellungen' AS message_info;
  SELECT CONCAT('Skript-Pfad  : ', p_Skript) AS message_info;
  SELECT CONCAT('Skript-Parameter: ', p_Parameter_String) AS message_info;

  -- Retrieve the BigQuery SQL content
  SET sql_to_execute = (
    SELECT script_sql
    FROM `project.dataset.sql_script_registry`
    WHERE script_name = p_Skript
    LIMIT 1
  );

  BEGIN
    -- Execute the migrated BigQuery SQL logic dynamically
    -- Note: This assumes the `script_sql` does not contain parameters that need direct substitution,
    -- but rather is a self-contained BigQuery SQL statement or a CALL to another stored procedure.
    -- If `p_Parameter` need to be passed, `sql_to_execute` would need to be constructed with them.
    EXECUTE IMMEDIATE sql_to_execute;

    SET errcode = 0;
  EXCEPTION WHEN ERROR THEN
    SET errcode = 1;
    -- Log the actual error that occurred during EXECUTE IMMEDIATE
    CALL `project.dataset.DWMSG_MeldeFehler`(
      p_Eintragsnr,
      'E',
      -1, -- Placeholder for dynamic SQL execution error. Consider using specific BigQuery error codes if available.
      CONCAT('Error executing script ', p_Skript, ': ', @@error.message)
    );
  END;

  SELECT errcode AS return_code;
END;