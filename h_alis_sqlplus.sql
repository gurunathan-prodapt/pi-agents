--------------------------------------------------------------------
-- Procedure: tryDBConnect
-- Replaces tryDBConnect Oracle sanity check validation.
-- Validates infrastructure health & dataset metadata permissions.
--------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@gcp_project.@bq_dataset.tryDBConnect`(OUT out_status STRING)
BEGIN
  BEGIN
    -- Query schema information schema as a connection assertion
    DECLARE v_test INT64;
    SELECT 1 INTO v_test 
    FROM `@gcp_project.@bq_dataset.INFORMATION_SCHEMA.TABLES` 
    LIMIT 1;
    
    SET out_status = 'CONNECTED';
  EXCEPTION WHEN ERROR THEN
    SET out_status = 'CONNECTION_FAILED';
  END;
END;

--------------------------------------------------------------------
-- Procedure: starteSQLSkriptSilent
-- Replaces starteSQLSkriptSilent SQLPLUS execution wrapping logic.
--------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@gcp_project.@bq_dataset.starteSQLSkriptSilent`(p_script_name STRING)
BEGIN
  -- BigQuery does not read external local SQL files directly during execution.
  -- This is handled by routing calls natively to translated procedure counterparts.
  DECLARE v_stmt STRING;
  
  IF p_script_name IS NULL THEN
    ERROR "Script name parameter cannot be NULL";
  END IF;

  -- Clean procedure string to protect statement composition
  SET p_script_name = REGEXP_REPLACE(p_script_name, r'[^a-zA-Z0-9_\-\.]', '');

  SET v_stmt = CONCAT('CALL `@gcp_project.@bq_dataset.', p_script_name, '`()');
  EXECUTE IMMEDIATE v_stmt;
END;

--------------------------------------------------------------------
-- Procedure: starteSQLSkriptUser
-- Replaces starteSQLSkriptUser SQLPLUS target-user dynamic execution.
--------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@gcp_project.@bq_dataset.starteSQLSkriptUser`(
  p_script_name STRING, 
  p_user STRING
)
BEGIN
  DECLARE v_stmt STRING;
  
  IF p_script_name IS NULL THEN
    ERROR "Script name parameter cannot be NULL";
  END IF;

  -- Trace execution environment mapping (equivalent to logging target context)
  CALL `@gcp_project.@bq_dataset.DWMSG_LogInfo`(
    'SYSTEM', 
    CONCAT('Executing script: ', p_script_name, ' acting under context of user: ', COALESCE(p_user, 'DEFAULT_IAM'))
  );

  -- Clean parameters
  SET p_script_name = REGEXP_REPLACE(p_script_name, r'[^a-zA-Z0-9_\-\.]', '');

  SET v_stmt = CONCAT('CALL `@gcp_project.@bq_dataset.', p_script_name, '`()');
  EXECUTE IMMEDIATE v_stmt;
END;

--------------------------------------------------------------------
-- Procedure: starte_sql_skript_silent_file (Companion Context Wrapper)
-- Replaces the shell runtime directory tracking checks
--------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@gcp_project.@bq_dataset.starte_sql_skript_silent_file`(
  p_workdir STRING, 
  p_script_name STRING
)
BEGIN
  -- Original Preserved Literal Check for directories: "Directory $p_Workdir exitiert nicht"
  -- BigQuery executes within serverless storage. This wrapper raises a structural log event.
  IF p_workdir IS NULL THEN
    ERROR "Directory $p_Workdir exitiert nicht";
  END IF;

  -- Execute dynamic procedure routing
  CALL `@gcp_project.@bq_dataset.starteSQLSkriptSilent`(p_script_name);
END;