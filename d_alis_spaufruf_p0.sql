/* ---------------------------------------------------------------------
-- File Name       : d_alis_spaufruf_p0.sql
-- Project         : Information Services
-- Target Engine   : Google BigQuery (GoogleSQL)
--
-- Description     : Dynamic procedure execution wrapper. Simulates the 
--                   legacy Oracle SQL*Plus execution framework.
--
-- Parameters (Declared via Orchestrator / Query Parameters):
--   @target_dataset   : Target schema/dataset where the procedure resides.
--   @procedure_name   : Name of the stored procedure to execute (legacy &1).
--   @arguments        : Optional comma-separated procedure argument string.
-------------------------------------------------------------------------*/

DECLARE target_dataset STRING DEFAULT @target_dataset;
DECLARE procedure_name STRING DEFAULT @procedure_name;
DECLARE procedure_args STRING DEFAULT SAFE_CAST(@arguments AS STRING);

-- Safe system identifier sanitization (guards against SQL injection)
DECLARE v_clean_dataset STRING;
DECLARE v_clean_procedure STRING;
DECLARE v_sql STRING;

IF procedure_name IS NULL OR TRIM(procedure_name) = '' THEN
  RAISE USING MESSAGE = 'Error: The Stored Procedure name (procedure_name) cannot be empty.';
END IF;

SET v_clean_dataset = REGEXP_REPLACE(target_dataset, r'[^a-zA-Z0-9_]', '');
SET v_clean_procedure = REGEXP_REPLACE(procedure_name, r'[^a-zA-Z0-9_]', '');

-- Generate dynamic call script
-- Handles procedures both with and without input arguments
IF procedure_args IS NULL OR TRIM(procedure_args) = '' THEN
  SET v_sql = FORMAT("CALL `%s.%s`()", v_clean_dataset, v_clean_procedure);
ELSE
  SET v_sql = FORMAT("CALL `%s.%s`(%s)", v_clean_dataset, v_clean_procedure, procedure_args);
END IF;

-- Transaction block to ensure transactional safety equivalent to Oracle COMMIT/ROLLBACK
BEGIN
  BEGIN TRANSACTION;
    
    -- Execute the generated dynamic procedure call
    EXECUTE IMMEDIATE v_sql;
    
  COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
  -- Equivalent behavior to WHENEVER OSERROR EXIT FAILURE ROLLBACK
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = FORMAT('SP_Execution_Error: %s. SQL State: %s', @@error.message, @@error.statement_text);
END;