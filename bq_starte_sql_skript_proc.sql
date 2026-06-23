--
-- BigQuery Stored Procedure for starteSQLSkript
-- Replaces the core logic of h_alis_sqlplus.ksh
-- JOB: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh
--

CREATE OR REPLACE PROCEDURE `your_gcp_project.your_bq_dataset.starteSQLSkript`(
    p_Eintragsnr STRING,
    p_Skript STRING,
    p_Params ARRAY<STRING>
)
OPTIONS(
  description="Replaces h_alis_sqlplus.ksh. Validates parameters, logs execution, and calls a migrated BigQuery stored procedure."
)
BEGIN
    DECLARE script_registry_entry STRUCT<
        script_name STRING,
        is_readable BOOLEAN,
        target_procedure_name STRING
    >;
    DECLARE log_message STRING;
    DECLARE error_code STRING;
    DECLARE error_message STRING;
    DECLARE dynamic_sql STRING;

    -- Logging module and version for logs
    DECLARE MODULE_NAME STRING DEFAULT 'starteSQLSkript';
    DECLARE MODULE_VERSION STRING DEFAULT '1.0';

    -- Initial log entry for execution start
    SET log_message = 'Starting execution of BigQuery stored procedure. Parameters: p_Eintragsnr = ' || IFNULL(p_Eintragsnr, 'NULL') || ', p_Skript = ' || IFNULL(p_Skript, 'NULL') || ', p_Params = [' || ARRAY_TO_STRING(p_Params, ', ') || ']';
    INSERT INTO `your_gcp_project.your_bq_dataset.execution_log` (module_name, module_version, entry_nr, script_name, script_params, log_message)
    VALUES (MODULE_NAME, MODULE_VERSION, p_Eintragsnr, p_Skript, p_Params, log_message);

    -- Parameter Validation (replaces: if [ -z "$p_Eintragsnr" -o -z "$p_Skript" ])
    IF p_Eintragsnr IS NULL OR p_Eintragsnr = '' OR p_Skript IS NULL OR p_Skript = '' THEN
        SET error_code = '196';
        SET error_message = 'ERROR ' || error_code || ': Missing required parameters (Eintragsnr or Skript).';
        INSERT INTO `your_gcp_project.your_bq_dataset.error_log` (entry_nr, severity, error_code, message, module_name, module_version)
        VALUES (p_Eintragsnr, 'E', error_code, error_message, MODULE_NAME, MODULE_VERSION);
        RAISE BQ.EXCEPTION(error_message);
    END IF;

    -- Script Existence/Readability Check (replaces: if [ ! -r $p_Skript ])
    SELECT AS STRUCT script_name, is_readable, target_procedure_name
    FROM `your_gcp_project.your_bq_dataset.sql_script_registry`
    WHERE script_name = p_Skript
    INTO script_registry_entry;

    IF script_registry_entry IS NULL THEN
        SET error_code = '200'; -- Custom error code for script not found
        SET error_message = 'ERROR ' || error_code || ': Script "' || p_Skript || '" not found in registry.';
        INSERT INTO `your_gcp_project.your_bq_dataset.error_log` (entry_nr, severity, error_code, message, module_name, module_version)
        VALUES (p_Eintragsnr, 'E', error_code, error_message, MODULE_NAME, MODULE_VERSION);
        RAISE BQ.EXCEPTION(error_message);
    END IF;

    IF NOT script_registry_entry.is_readable THEN
        SET error_code = '201'; -- Custom error code for script not readable
        SET error_message = 'ERROR ' || error_code || ': Script "' || p_Skript || '" is not marked as readable/executable in the registry.';
        INSERT INTO `your_gcp_project.your_bq_dataset.error_log` (entry_nr, severity, error_code, message, module_name, module_version)
        VALUES (p_Eintragsnr, 'E', error_code, error_message, MODULE_NAME, MODULE_VERSION);
        RAISE BQ.EXCEPTION(error_message);
    END IF;

    -- Dynamic CALL to the target BigQuery procedure
    -- The target procedure is expected to accept an ARRAY<STRING> for its parameters
    SET dynamic_sql = 'CALL ' || script_registry_entry.target_procedure_name || '(' ||
                      CASE WHEN ARRAY_LENGTH(p_Params) > 0 THEN 'ARRAY[' || (SELECT STRING_AGG('''' || REPLACE(param, '''', '\'\'') || '''') FROM UNNEST(p_Params) AS param) || ']' ELSE 'ARRAY<STRING>[]' END
                      || ');';

    BEGIN
        EXECUTE IMMEDIATE dynamic_sql;
        SET log_message = 'Successfully executed target procedure: ' || script_registry_entry.target_procedure_name;
        INSERT INTO `your_gcp_project.your_bq_dataset.execution_log` (module_name, module_version, entry_nr, script_name, script_params, log_message)
        VALUES (MODULE_NAME, MODULE_VERSION, p_Eintragsnr, p_Skript, p_Params, log_message);
    EXCEPTION WHEN ERROR THEN
        SET error_code = BQ.EXCEPTION_ERROR_CODE();
        SET error_message = 'ERROR ' || error_code || ': Execution of target procedure failed. ' || BQ.EXCEPTION_MESSAGE();
        INSERT INTO `your_gcp_project.your_bq_dataset.error_log` (entry_nr, severity, error_code, message, module_name, module_version)
        VALUES (p_Eintragsnr, 'E', error_code, error_message, MODULE_NAME, MODULE_VERSION);
        RAISE BQ.EXCEPTION(error_message);
    END;

END;