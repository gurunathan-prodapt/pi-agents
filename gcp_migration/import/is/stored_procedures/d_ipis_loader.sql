-- Create Logging and Audit Tables
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.dw_execution_log` (
  log_id STRING DEFAULT GENERATE_UUID(),
  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  procedure_name STRING,
  message STRING
);

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.dw_error_log` (
  error_id STRING DEFAULT GENERATE_UUID(),
  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  procedure_name STRING,
  level STRING,
  error_code INT64,
  message STRING,
  error_statement STRING
);

-- Reusable Informational Log Helper
CREATE OR REPLACE PROCEDURE `your_project.your_dataset.sp_log_info`(
  IN p_proc_name STRING,
  IN p_message STRING
)
BEGIN
  INSERT INTO `your_project.your_dataset.dw_execution_log` (procedure_name, message)
  VALUES (p_proc_name, p_message);
END;

-- Reusable Error Log Helper
CREATE OR REPLACE PROCEDURE `your_project.your_dataset.sp_log_error`(
  IN p_proc_name STRING,
  IN p_level STRING,
  IN p_error_code INT64,
  IN p_message STRING,
  IN p_error_statement STRING
)
BEGIN
  INSERT INTO `your_project.your_dataset.dw_error_log` (procedure_name, level, error_code, message, error_statement)
  VALUES (p_proc_name, p_level, p_error_code, p_message, p_error_statement);
END;

-- Core Migrated Stored Procedure
CREATE OR REPLACE PROCEDURE `your_project.your_dataset.d_ipis_loader`(
  IN p_control_file_name STRING,
  IN p_data_file_uri STRING,
  IN p_land STRING,
  OUT p_err_nr INT64
)
BEGIN
  DECLARE v_proc_name STRING DEFAULT 'd_ipis_loader';
  DECLARE v_target_table STRING;
  DECLARE v_log_message STRING;
  DECLARE v_sql STRING;

  -- 1. Argument Validation
  IF p_control_file_name IS NULL OR p_control_file_name = '' THEN
    SET p_err_nr = 1;
    CALL `your_project.your_dataset.sp_log_error`(
      v_proc_name, 'E', p_err_nr, 'Validation failed: CONTROLFILE (-c) is empty', NULL
    );
    RETURN;
  END IF;

  IF p_data_file_uri IS NULL OR p_data_file_uri = '' THEN
    SET p_err_nr = 1;
    CALL `your_project.your_dataset.sp_log_error`(
      v_proc_name, 'E', p_err_nr, 'Validation failed: DATAFILE URI (-d) is empty', NULL
    );
    RETURN;
  END IF;

  -- 2. Map Control File Names to Target Tables
  SET v_target_table = CASE 
    WHEN LOWER(p_control_file_name) LIKE '%customer%' THEN '`your_project.your_dataset.t_customer`'
    WHEN LOWER(p_control_file_name) LIKE '%orders%'   THEN '`your_project.your_dataset.t_orders`'
    ELSE NULL
  END;

  IF v_target_table IS NULL THEN
    SET p_err_nr = 2;
    CALL `your_project.your_dataset.sp_log_error`(
      v_proc_name, 'F', p_err_nr, CONCAT('Unknown control file layout: ', p_control_file_name), NULL
    );
    RETURN;
  END IF;

  -- 3. Ingestion Transaction Block
  BEGIN
    BEGIN TRANSACTION;

    SET v_sql = FORMAT("""
      LOAD DATA OVERWRITE %s
      FROM FILES (
        format = 'CSV',
        uris = ['%s'],
        skip_header = 1,
        field_delimiter = ';',
        quote = '"',
        max_bad_records = 0
      )
    """, v_target_table, p_data_file_uri);

    EXECUTE IMMEDIATE v_sql;

    COMMIT TRANSACTION;

    SET p_err_nr = 0;
    SET v_log_message = CONCAT('SUCCESS: Loaded GCS uri: ', p_data_file_uri, ' into target table: ', v_target_table);
    CALL `your_project.your_dataset.sp_log_info`(v_proc_name, v_log_message);

  EXCEPTION WHEN ERROR THEN
    ROLLBACK TRANSACTION;

    SET p_err_nr = 200;
    SET v_log_message = CONCAT('FAILURE: SQL*Loader equivalent aborted. File: ', p_data_file_uri, ' | Error Details: ', @@error.message);
    
    CALL `your_project.your_dataset.sp_log_error`(
      v_proc_name, 'F', p_err_nr, v_log_message, @@error.statement
    );
  END;

END;