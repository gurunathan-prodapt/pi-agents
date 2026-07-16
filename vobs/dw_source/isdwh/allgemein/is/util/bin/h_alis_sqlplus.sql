-- Create the target dataset if it does not already exist
CREATE SCHEMA IF NOT EXISTS dwh;

-- -------------------------------------------------------------------
-- TABLE: dwh.ta_k_meldungen (Mock/Reference Tracking Table)
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.ta_k_meldungen (
  entrynr INT64 NOT NULL,
  job_kennung STRING,
  status STRING,
  updated_at TIMESTAMP
);

-- -------------------------------------------------------------------
-- TABLE: dwh.sql_execution_file_logs (Audit Destination)
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.sql_execution_file_logs (
  entrynr INT64,
  script_name STRING,
  directory STRING,
  file_name STRING,
  job_kennung STRING,
  execution_timestamp TIMESTAMP,
  status STRING,
  message STRING
);

-- -------------------------------------------------------------------
-- PROCEDURE: dwh.dwmsg_meldefehler (Standard Error Handler)
-- -------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE dwh.dwmsg_meldefehler(
  IN p_entrynr INT64,
  IN p_err_type STRING,
  IN p_err_code INT64,
  IN p_context STRING
)
BEGIN
  -- Fallback logic preserving the requested literal string checks if triggered
  IF p_context LIKE '%ErmittleNr%' THEN
    INSERT INTO dwh.sql_execution_file_logs (entrynr, script_name, execution_timestamp, status, message)
    VALUES (p_entrynr, 'SYSTEM_ERR', CURRENT_TIMESTAMP(), 'ERROR', "Argh!, keinen Variablennamen bei ErmittleNr angegeben");
  ELSEIF p_context LIKE '%Fehlerbehandlung%' THEN
    INSERT INTO dwh.sql_execution_file_logs (entrynr, script_name, execution_timestamp, status, message)
    VALUES (p_entrynr, 'SYSTEM_ERR', CURRENT_TIMESTAMP(), 'ERROR', "Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus");
  ELSE
    -- Generic audit insert to emulate error message registration
    INSERT INTO dwh.sql_execution_file_logs (entrynr, script_name, execution_timestamp, status, message)
    VALUES (p_entrynr, p_context, CURRENT_TIMESTAMP(), 'ERROR', CONCAT('Type: ', p_err_type, ' | Code: ', CAST(p_err_code AS STRING)));
  END IF;
END;

-- -------------------------------------------------------------------
-- PROCEDURE: dwh.gensqlscript
-- Replicates metadata registration logic by looking up the job indicator
-- and returning structured metadata execution context.
-- -------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE dwh.gensqlscript(
  IN p_entrynr INT64,
  IN p_script STRING,
  OUT r_job_kennung STRING,
  OUT r_app_info STRING
)
BEGIN
  DECLARE l_job_kennung STRING DEFAULT 'JOB???';

  BEGIN
    -- Query the status tracking table
    SET l_job_kennung = (
      SELECT job_kennung 
      FROM dwh.ta_k_meldungen 
      WHERE entrynr = p_entrynr 
      LIMIT 1
    );
  EXCEPTION WHEN OTHERS THEN
    -- Fallback handler
    SET l_job_kennung = 'JOB???';
  END;

  IF l_job_kennung IS NULL THEN
    SET l_job_kennung = 'JOB???';
  END IF;

  SET r_job_kennung = l_job_kennung;
  SET r_app_info = SUBSTR(p_script, -32);
END;

-- -------------------------------------------------------------------
-- PROCEDURE: dwh.checkSyntaxDBConnect
-- Validates that the input string matches the expected database connection structure
-- -------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE dwh.checkSyntaxDBConnect(
  IN p_Eintragsnr INT64,
  IN p_Connect STRING,
  OUT r_status INT64
)
BEGIN
  -- Pattern matches <user>/<pass>@<instanz> without nested slashes or @ symbols
  IF REGEXP_CONTAINS(p_Connect, r'^[^/@]+/[^/@]+@[^/@]+$') THEN
    SET r_status = 0;
  ELSE
    CALL dwh.dwmsg_meldefehler(p_Eintragsnr, 'E', 198, 'alis_sqlplus V8.0.6 checkSyntaxDBConnect');
    SET r_status = 198;
  END IF;
END;

-- -------------------------------------------------------------------
-- PROCEDURE: dwh.tryDBConnect
-- Validates database connectivity (Staged check returning status)
-- -------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE dwh.tryDBConnect(
  IN p_Eintragsnr INT64,
  IN p_Connect STRING,
  OUT r_status INT64
)
BEGIN
  -- BigQuery relies on pre-established IAM/Resource Connections. 
  -- Syntax validation is verified here before execution.
  DECLARE l_syntax_status INT64;
  
  CALL dwh.checkSyntaxDBConnect(p_Eintragsnr, p_Connect, l_syntax_status);
  
  IF l_syntax_status != 0 THEN
    SET r_status = l_syntax_status;
  ELSE
    SET r_status = 0;
  END IF;
END;

-- -------------------------------------------------------------------
-- PROCEDURE: dwh.starteSQLSkript
-- Dynamically executes SQL commands, setting up environment metadata
-- -------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE dwh.starteSQLSkript(
  IN p_Eintragsnr INT64,
  IN p_Skript STRING,
  IN p_SQL_Statement STRING, -- The actual SQL statement payload to execute
  OUT r_status INT64
)
BEGIN
  DECLARE l_job_kennung STRING;
  DECLARE l_app_info STRING;

  -- Validate arguments
  IF p_Eintragsnr IS NULL OR p_Skript = '' OR p_Skript IS NULL THEN
    CALL dwh.dwmsg_meldefehler(p_Eintragsnr, 'E', 196, 'alis_sqlplus V8.0.6 starteSQLSkript');
    SET r_status = 196;
    RETURN;
  END IF;

  -- Read execution metadata context
  CALL dwh.gensqlscript(p_Eintragsnr, p_Skript, l_job_kennung, l_app_info);

  -- Execute SQL command dynamically
  BEGIN
    EXECUTE IMMEDIATE p_SQL_Statement;
    SET r_status = 0;
  EXCEPTION WHEN OTHERS THEN
    -- Fallback error output handler
    CALL dwh.dwmsg_meldefehler(p_Eintragsnr, 'E', 201, p_Skript);
    SET r_status = 201;
  END;
END;

-- -------------------------------------------------------------------
-- PROCEDURE: dwh.starteSQLSkriptStrict
-- Strictly checks arguments and parameter mappings during runtime execution
-- -------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE dwh.starteSQLSkriptStrict(
  IN p_Eintragsnr INT64,
  IN p_Skript STRING,
  IN p_SQL_Statement STRING,
  OUT r_status INT64
)
BEGIN
  IF p_Eintragsnr IS NULL OR p_Skript = '' OR p_Skript IS NULL OR p_SQL_Statement IS NULL OR p_SQL_Statement = '' THEN
    CALL dwh.dwmsg_meldefehler(p_Eintragsnr, 'E', 196, 'alis_sqlplus V8.0.6 starteSQLSkriptStrict');
    SET r_status = 196;
    RETURN;
  END IF;

  CALL dwh.starteSQLSkript(p_Eintragsnr, p_Skript, p_SQL_Statement, r_status);
END;

-- -------------------------------------------------------------------
-- PROCEDURE: dwh.starteSQLSkriptSilent
-- Executes dynamic SQL commands without verbose console emissions.
-- -------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE dwh.starteSQLSkriptSilent(
  IN p_Eintragsnr INT64,
  IN p_Skript STRING,
  IN p_SQL_Statement STRING,
  OUT r_status INT64
)
BEGIN
  -- Re-uses core execution path silently
  CALL dwh.starteSQLSkript(p_Eintragsnr, p_Skript, p_SQL_Statement, r_status);
END;

-- -------------------------------------------------------------------
-- PROCEDURE: dwh.starteSQLSkriptUser
-- Dynamically routes SQL execution utilizing specific connection schemas
-- -------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE dwh.starteSQLSkriptUser(
  IN p_Eintragsnr INT64,
  IN p_Skript STRING,
  IN p_Connect STRING,
  IN p_SQL_Statement STRING,
  OUT r_status INT64
)
BEGIN
  -- Perform connection syntax checks on target schema
  CALL dwh.checkSyntaxDBConnect(p_Eintragsnr, p_Connect, r_status);
  
  IF r_status != 0 THEN
    RETURN;
  END IF;

  -- Execute using dynamic SQL engine helper
  CALL dwh.starteSQLSkript(p_Eintragsnr, p_Skript, p_SQL_Statement, r_status);
END;

-- -------------------------------------------------------------------
-- PROCEDURE: dwh.starteSQLSkriptSilentFile
-- Executes dynamic SQL statements and logs output into a BigQuery audit destination.
-- Includes directory verification warning flags mapping back to design targets.
-- -------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE dwh.starteSQLSkriptSilentFile(
  IN p_Eintragsnr INT64,
  IN p_Skript STRING,
  IN p_Workdir STRING,
  IN p_Filename STRING,
  IN p_SQL_Statement STRING,
  OUT r_status INT64
)
BEGIN
  DECLARE l_job_kennung STRING;
  DECLARE l_app_info STRING;
  DECLARE l_warning_msg STRING;

  -- Parameter validation
  IF p_Eintragsnr IS NULL OR p_Skript = '' OR p_Skript IS NULL OR p_Workdir = '' OR p_Workdir IS NULL OR p_Filename = '' OR p_Filename IS NULL THEN
    CALL dwh.dwmsg_meldefehler(p_Eintragsnr, 'E', 196, 'alis_sqlplus V8.0.6 starteSQLSkriptSilentFile');
    SET r_status = 196;
    RETURN;
  END IF;

  -- Rule Preservation: Emulate directory existence warning in cloud execution
  -- If directory structure parameter contains a missing directory flag, populate warning log
  IF p_Workdir LIKE '%NOT_EXIST%' OR p_Workdir = '/' THEN
    SET l_warning_msg = CONCAT("Directory ", p_Workdir, " exitiert nicht");
    CALL dwh.dwmsg_meldefehler(p_Eintragsnr, 'W', 197, l_warning_msg);
  END IF;

  CALL dwh.gensqlscript(p_Eintragsnr, p_Skript, l_job_kennung, l_app_info);

  BEGIN
    -- Execute dynamic workload
    EXECUTE IMMEDIATE p_SQL_Statement;

    -- Track transaction results in logging destination
    INSERT INTO dwh.sql_execution_file_logs (
      entrynr, 
      script_name, 
      directory, 
      file_name, 
      job_kennung, 
      execution_timestamp, 
      status,
      message
    )
    VALUES (
      p_Eintragsnr, 
      p_Skript, 
      p_Workdir, 
      p_Filename, 
      l_job_kennung, 
      CURRENT_TIMESTAMP(), 
      'SUCCESS',
      COALESCE(l_warning_msg, 'Executed successfully')
    );

    SET r_status = 0;

  EXCEPTION WHEN OTHERS THEN
    CALL dwh.dwmsg_meldefehler(p_Eintragsnr, 'E', 201, p_Skript);
    
    INSERT INTO dwh.sql_execution_file_logs (
      entrynr, 
      script_name, 
      directory, 
      file_name, 
      job_kennung, 
      execution_timestamp, 
      status,
      message
    )
    VALUES (
      p_Eintragsnr, 
      p_Skript, 
      p_Workdir, 
      p_Filename, 
      l_job_kennung, 
      CURRENT_TIMESTAMP(), 
      'FAILED',
      @@error.message
    );
    
    SET r_status = 201;
  END;
END;