-- Create Metadata Dataset if not exists (Adjust project name accordingly)
CREATE SCHEMA IF NOT EXISTS `metadata`;

-- Create an Audit/Log table to capture environment execution metadata or validation failures
CREATE TABLE IF NOT EXISTS `metadata.dw_environment_log` (
  log_id STRING DEFAULT GENERATE_UUID(),
  log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  log_level STRING,
  procedure_name STRING,
  message STRING,
  missing_variable STRING
);

-- Stored Procedure to initialize and validate the global DWH environment variables
CREATE OR REPLACE PROCEDURE `metadata.sp_dw_global`(
  -- Input parameters representing the system environment variables
  IN p_DW_DIR_ROOT STRING,
  IN p_DW_DIR_PROT STRING,
  IN p_DW_DIR_CUBES STRING,
  IN p_DW_DIR_IMP_D1 STRING,
  IN p_DW_DIR_IMP_XTRA STRING,
  IN p_DW_DIR_IMP_CTEL STRING,
  IN p_DW_DIR_IMP_VO STRING,
  IN p_DW_DIR_IMP_RV STRING,
  IN p_DW_DIR_IMP_IF STRING,
  IN p_DW_DIR_IMP_NNV STRING,
  IN p_ORACLE_HOME STRING,
  -- Outputs returning standard session settings
  OUT out_NLS_LANG STRING,
  OUT out_NLS_DATE_FORMAT STRING,
  OUT out_NLS_DATE_LANGUAGE STRING,
  OUT out_LANG STRING
)
BEGIN
  -- Declarations for validation
  DECLARE v_fehler ARRAY<STRING> DEFAULT [];
  DECLARE v_idx INT64 DEFAULT 0;
  DECLARE v_error_count INT64 DEFAULT 0;
  
  -- Perform checks analogous to shell script conditional checks
  IF COALESCE(p_DW_DIR_ROOT, '') = '' THEN
    SET v_fehler = ARRAY_CONCAT(v_fehler, ['DW_DIR_ROOT']);
  END IF;
  
  IF COALESCE(p_DW_DIR_PROT, '') = '' THEN
    SET v_fehler = ARRAY_CONCAT(v_fehler, ['DW_DIR_PROT']);
  END IF;

  IF COALESCE(p_DW_DIR_CUBES, '') = '' THEN
    SET v_fehler = ARRAY_CONCAT(v_fehler, ['DW_DIR_CUBES']);
  END IF;

  IF COALESCE(p_DW_DIR_IMP_D1, '') = '' THEN
    SET v_fehler = ARRAY_CONCAT(v_fehler, ['DW_DIR_IMP_D1']);
  END IF;

  IF COALESCE(p_DW_DIR_IMP_XTRA, '') = '' THEN
    SET v_fehler = ARRAY_CONCAT(v_fehler, ['DW_DIR_IMP_XTRA']);
  END IF;

  IF COALESCE(p_DW_DIR_IMP_CTEL, '') = '' THEN
    SET v_fehler = ARRAY_CONCAT(v_fehler, ['DW_DIR_IMP_CTEL']);
  END IF;

  IF COALESCE(p_DW_DIR_IMP_VO, '') = '' THEN
    SET v_fehler = ARRAY_CONCAT(v_fehler, ['DW_DIR_IMP_VO']);
  END IF;

  IF COALESCE(p_DW_DIR_IMP_RV, '') = '' THEN
    SET v_fehler = ARRAY_CONCAT(v_fehler, ['DW_DIR_IMP_RV']);
  END IF;

  IF COALESCE(p_DW_DIR_IMP_IF, '') = '' THEN
    SET v_fehler = ARRAY_CONCAT(v_fehler, ['DW_DIR_IMP_IF']);
  END IF;

  IF COALESCE(p_DW_DIR_IMP_NNV, '') = '' THEN
    SET v_fehler = ARRAY_CONCAT(v_fehler, ['DW_DIR_IMP_NNV']);
  END IF;

  IF COALESCE(p_ORACLE_HOME, '') = '' THEN
    SET v_fehler = ARRAY_CONCAT(v_fehler, ['ORACLE_HOME']);
  END IF;

  -- Verify if any missing variables were captured
  SET v_error_count = ARRAY_LENGTH(v_fehler);
  
  IF v_error_count > 0 THEN
    -- Loop through the errors and log each missing variable
    WHILE v_idx < v_error_count DO
      INSERT INTO `metadata.dw_environment_log` (log_level, procedure_name, message, missing_variable)
      VALUES ('ERROR', 'sp_dw_global', 'Umgebungsvariable ist nicht gesetzt !', v_fehler[OFFSET(v_idx)]);
      
      SET v_idx = v_idx + 1;
    END WHILE;
    
    -- Raise an execution error for missing dependencies using valid BigQuery syntax
    RAISE USING MESSAGE = CONCAT('Fehler in .dw_global: ', CAST(v_error_count AS STRING), ' required global environment variables are missing.');
  END IF;

  -- Define global/session mappings for downstream activities
  SET out_NLS_LANG = 'GERMAN_GERMANY.WE8ISO8859P1';
  SET out_NLS_DATE_FORMAT = 'DD.MM.YY';
  SET out_NLS_DATE_LANGUAGE = 'GERMAN_GERMANY.WE8ISO8859P1';
  SET out_LANG = 'de';

END;