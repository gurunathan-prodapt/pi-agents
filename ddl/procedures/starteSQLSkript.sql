-- BigQuery Native Migration Design for h_alis_sqlplus.ksh
-- Target: Google BigQuery
-- Environment: Development/UAT (`gcp-is-dw-dev.dw_utility_dev`)
-- Purpose: Reusable helper procedures to validate, log, resolve, and execute registered SQL scripts

-- ============================================================================
-- 1) CONFIGURATION / REGISTRY TABLE
-- ============================================================================

CREATE OR REPLACE TABLE `gcp-is-dw-dev.dw_utility_dev.sql_script_registry` (
  script_name STRING OPTIONS(description="Logical script name"),
  script_sql STRING OPTIONS(description="BigQuery SQL or CALL statement to execute"),
  is_active BOOL OPTIONS(description="Active execution flag"),
  last_modified TIMESTAMP OPTIONS(description="Last modification timestamp")
);

-- Audit logging table (Replaces DWMSG_MeldeFehler)
CREATE OR REPLACE TABLE `gcp-is-dw-dev.dw_audit_dev.sql_execution_audit` (
  audit_ts TIMESTAMP OPTIONS(description="Audit timestamp"),
  module_name STRING OPTIONS(description="Module name"),
  module_version STRING OPTIONS(description="Module version"),
  entry_nr INT64 OPTIONS(description="Error/Status entry number"),
  script_name STRING OPTIONS(description="Requested script name"),
  severity STRING OPTIONS(description="INFO/WARN/ERROR"),
  message STRING OPTIONS(description="Log message"),
  return_code INT64 OPTIONS(description="Return code")
);

-- ============================================================================
-- 2) REUSABLE LOGGING PROCEDURE
-- ============================================================================

CREATE OR REPLACE PROCEDURE `gcp-is-dw-dev.dw_utility_dev.log_execution_event`(
  p_module_name STRING,
  p_module_version STRING,
  p_entry_nr INT64,
  p_script_name STRING,
  p_severity STRING,
  p_message STRING,
  p_return_code INT64
)
BEGIN
  INSERT INTO `gcp-is-dw-dev.dw_audit_dev.sql_execution_audit`
  (audit_ts, module_name, module_version, entry_nr, script_name, severity, message, return_code)
  VALUES
  (CURRENT_TIMESTAMP(), p_module_name, p_module_version, p_entry_nr, p_script_name, p_severity, p_message, p_return_code);

  SELECT
    CURRENT_TIMESTAMP() AS audit_ts,
    p_severity AS severity,
    p_message AS message,
    p_return_code AS return_code;
END;

-- ============================================================================
-- 3) REUSABLE VALIDATION PROCEDURE
-- ============================================================================

CREATE OR REPLACE PROCEDURE `gcp-is-dw-dev.dw_utility_dev.validate_sql_request`(
  p_entry_nr INT64,
  p_script_name STRING,
  OUT o_return_code INT64,
  OUT o_error_message STRING
)
BEGIN
  SET o_return_code = 0;
  SET o_error_message = NULL;

  -- Validation: missing entry number or script name => 196
  IF p_entry_nr IS NULL OR p_script_name IS NULL OR TRIM(p_script_name) = '' THEN
    SET o_return_code = 196;
    SET o_error_message = 'Missing required arguments for starteSQLSkript';
    RETURN;
  END IF;

  -- Validation: script exists and is active => 201
  IF NOT EXISTS (
    SELECT 1
    FROM `gcp-is-dw-dev.dw_utility_dev.sql_script_registry`
    WHERE script_name = p_script_name
      AND is_active = TRUE
  ) THEN
    SET o_return_code = 201;
    SET o_error_message = CONCAT('Script not found or inactive in registry: ', p_script_name);
    RETURN;
  END IF;
END;

-- ============================================================================
-- 4) REUSABLE SCRIPT RESOLUTION PROCEDURE
-- ============================================================================

CREATE OR REPLACE PROCEDURE `gcp-is-dw-dev.dw_utility_dev.resolve_sql_script`(
  p_script_name STRING,
  OUT o_script_sql STRING,
  OUT o_return_code INT64,
  OUT o_error_message STRING
)
BEGIN
  SET o_script_sql = NULL;
  SET o_return_code = 0;
  SET o_error_message = NULL;

  SET o_script_sql = (
    SELECT script_sql
    FROM `gcp-is-dw-dev.dw_utility_dev.sql_script_registry`
    WHERE script_name = p_script_name
      AND is_active = TRUE
    ORDER BY last_modified DESC
    LIMIT 1
  );

  IF o_script_sql IS NULL THEN
    SET o_return_code = 201;
    SET o_error_message = CONCAT('No active SQL found for script: ', p_script_name);
  END IF;
END;

-- ============================================================================
-- 5) REUSABLE PARAMETER RENDERING PROCEDURE
--    Supports positional replacement tokens: ${1}, ${2}, ${3}, ...
-- ============================================================================

CREATE OR REPLACE PROCEDURE `gcp-is-dw-dev.dw_utility_dev.render_sql_parameters`(
  p_sql_template STRING,
  p_parameters ARRAY<STRING>,
  OUT o_rendered_sql STRING
)
BEGIN
  DECLARE v_sql STRING DEFAULT p_sql_template;
  DECLARE i INT64 DEFAULT 0;
  DECLARE n INT64 DEFAULT ARRAY_LENGTH(p_parameters);

  WHILE i < n DO
    SET v_sql = REPLACE(
      v_sql,
      CONCAT('${', CAST(i + 1 AS STRING), '}'),
      IFNULL(p_parameters[OFFSET(i)], '')
    );
    SET i = i + 1;
  END WHILE;

  SET o_rendered_sql = v_sql;
END;

-- ============================================================================
-- 6) MAIN PROCEDURE: STARTE SQL SCRIPT
--    BigQuery-native equivalent of starteSQLSkript
-- ============================================================================

CREATE OR REPLACE PROCEDURE `gcp-is-dw-dev.dw_utility_dev.starteSQLSkript`(
  p_Eintragsnr INT64,
  p_Skript STRING,
  p_Parameter ARRAY<STRING>
)
BEGIN
  DECLARE v_return_code INT64 DEFAULT 0;
  DECLARE v_error_message STRING DEFAULT NULL;
  DECLARE v_script_sql STRING DEFAULT NULL;
  DECLARE v_rendered_sql STRING DEFAULT NULL;
  DECLARE v_module_name STRING DEFAULT 'h_alis_sqlplus';
  DECLARE v_module_version STRING DEFAULT 'V1.1.3';

  -- Validate input request
  CALL `gcp-is-dw-dev.dw_utility_dev.validate_sql_request`(
    p_Eintragsnr,
    p_Skript,
    v_return_code,
    v_error_message
  );

  IF v_return_code <> 0 THEN
    CALL `gcp-is-dw-dev.dw_utility_dev.log_execution_event`(
      v_module_name,
      v_module_version,
      p_Eintragsnr,
      p_Skript,
      'ERROR',
      v_error_message,
      v_return_code
    );
    SELECT v_return_code AS return_code, v_error_message AS error_message;
    RETURN;
  END IF;

  -- Resolve SQL script text from metadata registry
  CALL `gcp-is-dw-dev.dw_utility_dev.resolve_sql_script`(
    p_Skript,
    v_script_sql,
    v_return_code,
    v_error_message
  );

  IF v_return_code <> 0 THEN
    CALL `gcp-is-dw-dev.dw_utility_dev.log_execution_event`(
      v_module_name,
      v_module_version,
      p_Eintragsnr,
      p_Skript,
      'ERROR',
      v_error_message,
      v_return_code
    );
    SELECT v_return_code AS return_code, v_error_message AS error_message;
    RETURN;
  END IF;

  -- Audit invocation start
  CALL `gcp-is-dw-dev.dw_utility_dev.log_execution_event`(
    v_module_name,
    v_module_version,
    p_Eintragsnr,
    p_Skript,
    'INFO',
    CONCAT(
      'Rufe SQL auf mit Skript: ', p_Skript,
      ' | Parameteranzahl: ', CAST(ARRAY_LENGTH(p_Parameter) AS STRING)
    ),
    0
  );

  -- Render parameter inputs into dynamic template placeholders
  CALL `gcp-is-dw-dev.dw_utility_dev.render_sql_parameters`(
    v_script_sql,
    p_Parameter,
    v_rendered_sql
  );

  -- Execute dynamic SQL
  BEGIN
    EXECUTE IMMEDIATE v_rendered_sql;
    SET v_return_code = 0;

    CALL `gcp-is-dw-dev.dw_utility_dev.log_execution_event`(
      v_module_name,
      v_module_version,
      p_Eintragsnr,
      p_Skript,
      'INFO',
      CONCAT('Execution successful for script: ', p_Skript),
      v_return_code
    );

  EXCEPTION WHEN ERROR THEN
    SET v_return_code = 1;
    SET v_error_message = CONCAT('Execution failed for script: ', p_Skript);

    CALL `gcp-is-dw-dev.dw_utility_dev.log_execution_event`(
      v_module_name,
      v_module_version,
      p_Eintragsnr,
      p_Skript,
      'ERROR',
      v_error_message,
      v_return_code
    );
  END;

  SELECT v_return_code AS return_code;
END;

-- ============================================================================
-- 7) OPTIONAL WRAPPER FOR DIRECT CALLS WITH NAMED PARAMETERS
-- ============================================================================

CREATE OR REPLACE PROCEDURE `gcp-is-dw-dev.dw_utility_dev.starteSQLSkript_named`(
  p_Eintragsnr INT64,
  p_Skript STRING,
  p_param1 STRING,
  p_param2 STRING,
  p_param3 STRING
)
BEGIN
  DECLARE v_params ARRAY<STRING> DEFAULT [p_param1, p_param2, p_param3];

  CALL `gcp-is-dw-dev.dw_utility_dev.starteSQLSkript`(
    p_Eintragsnr,
    p_Skript,
    v_params
  );
END;

-- ============================================================================
-- 8) EXAMPLE REGISTRY SEED DATA
-- ============================================================================

INSERT INTO `gcp-is-dw-dev.dw_utility_dev.sql_script_registry`
(script_name, script_sql, is_active, last_modified)
VALUES
(
  'example_script',
  '''
  SELECT
    '${1}' AS param1,
    '${2}' AS param2,
    CURRENT_TIMESTAMP() AS executed_at
  ''',
  TRUE,
  CURRENT_TIMESTAMP()
);

-- ============================================================================
-- 9) EXAMPLE USAGE
-- ============================================================================

-- CALL `gcp-is-dw-dev.dw_utility_dev.starteSQLSkript`(
--   1001,
--   'example_script',
--   ['alpha', 'beta']
-- );