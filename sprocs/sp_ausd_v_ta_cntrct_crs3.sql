-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs3.ksh
-- This BigQuery Stored Procedure re-implements the control and orchestration logic
-- of the ksh script `k_ausd_v_ta_cntrct_crs3.ksh`.
-- It handles parameter validation, job activation/deactivation, error logging,
-- execution logging, and orchestrates the core SQL logic.
-- Please replace `your_project_id` and `your_dataset_id` with your actual BigQuery project and dataset.
CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.sp_ausd_v_ta_cntrct_crs3`(
  p_JobKennung STRING OPTIONS(description="Job identifier (corresponds to legacy 'j' parameter)."),
  p_EintragsNr STRING OPTIONS(description="Entry number or instance identifier (corresponds to legacy 'f' parameter).")
)
BEGIN
  -- Declare variables for job state and logging
  DECLARE v_records_processed_total INT64 DEFAULT 0;
  DECLARE v_job_name STRING;
  DECLARE v_tab_name STRING DEFAULT 'ta_cntrct_crs3'; -- Derived from legacy `v_TabName`
  DECLARE v_procedure_name STRING DEFAULT 'sp_ausd_v_ta_cntrct_crs3';
  DECLARE v_error_message STRING;
  DECLARE v_error_number INT64;
  DECLARE v_job_active BOOL;

  -- Assign job_name from parameter
  SET v_job_name = p_JobKennung;

  -- 1. Parameter Validation (replaces legacy getopts and checks)
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    SET v_error_message = 'ERROR: Parameter p_JobKennung (Job ID) must be provided.';
    SET v_error_number = 192; -- Based on legacy script's error numbers
    CALL `your_project_id.your_dataset_id.log_error`(v_procedure_name, v_error_number, 'p_JobKennung', v_error_message);
    RAISE USING MESSAGE v_error_message;
  END IF;

  IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN
    SET v_error_message = 'ERROR: Parameter p_EintragsNr (Entry Number) must be provided.';
    SET v_error_number = 193; -- Based on legacy script's error numbers
    CALL `your_project_id.your_dataset_id.log_error`(v_procedure_name, v_error_number, 'p_EintragsNr', v_error_message);
    RAISE USING MESSAGE v_error_message;
  END IF;

  -- Error Handling Block for the main business logic
  BEGIN
    -- 2. Check if job is already active (replaces legacy implicit check)
    SET v_job_active = (
      SELECT active_flag
      FROM `your_project_id.your_dataset_id.job_table`
      WHERE job_name = v_job_name AND entry_nr = p_EintragsNr AND tab_name = v_tab_name
      LIMIT 1
    );

    IF v_job_active THEN
      -- Job is already active, log and exit as per original script's behavior
      CALL `your_project_id.your_dataset_id.log_execution`(
          v_procedure_name,
          v_job_name,
          p_EintragsNr,
          v_tab_name,
          0,
          'SKIPPED_ALREADY_ACTIVE'
      );
      SELECT CONCAT('Job (', v_job_name, ', Entry: ', p_EintragsNr, ') for table ', v_tab_name, ' is already active. Exiting.');
      RETURN;
    END IF;

    -- 3. Activate the job in the job_table (replaces legacy job activation logic)
    MERGE INTO `your_project_id.your_dataset_id.job_table` T
    USING (SELECT v_job_name AS job_name, p_EintragsNr AS entry_nr, v_tab_name AS tab_name) S
    ON T.job_name = S.job_name AND T.entry_nr = S.entry_nr AND T.tab_name = S.tab_name
    WHEN MATCHED THEN
      UPDATE SET active_flag = TRUE, updated_ts = CURRENT_TIMESTAMP(), completed_ts = NULL
    WHEN NOT MATCHED THEN
      INSERT (job_name, entry_nr, tab_name, active_flag, created_ts, updated_ts)
      VALUES (S.job_name, S.entry_nr, S.tab_name, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

    -- 4. Execute Core SQL Logic (replaces `d_ausd_v_ta_cntrct_crs3.sql` execution via `starteSQLSkript`)
    -- IMPORTANT: The actual SQL content from `d_ausd_v_ta_cntrct_crs3.sql` was NOT PROVIDED
    -- in the migration design document. You MUST replace the following placeholder block
    -- with the BigQuery Standard SQL translation of your core business logic.
    --
    -- This section should perform the primary data manipulation (INSERT, UPDATE, DELETE)
    -- and capture the total number of records processed into `v_records_processed_total`.
    -- If your SQL is complex, consider creating a separate BigQuery script or another
    -- stored procedure and calling it here.
    --
    -- Example of capturing row counts from DML operations:
    --
    -- DELETE FROM `your_project_id.your_dataset_id.ta_cntrct_crs3` WHERE some_condition;
    -- SET v_records_processed_total = v_records_processed_total + @@row_count;
    --
    -- INSERT INTO `your_project_id.your_dataset_id.ta_cntrct_crs3` (...) SELECT ...;
    -- SET v_records_processed_total = v_records_processed_total + @@row_count;
    --
    -- UPDATE `your_project_id.your_dataset_id.ta_cntrct_crs3` SET ... WHERE some_condition;
    -- SET v_records_processed_total = v_records_processed_total + @@row_count;

    -- *** PLACEHOLDER FOR ACTUAL CORE SQL LOGIC. REPLACE THIS BLOCK. ***
    -- For demonstration, setting v_records_processed_total to a dummy value.
    -- This section corresponds to the logic in `sql/d_ausd_v_ta_cntrct_crs3_placeholder.sql`
    -- if you choose to inline it, or call a routine here.
    SET v_records_processed_total = 100; -- Replace with actual logic to calculate records processed.
    -- Example call to an external script (if you choose that architecture):
    -- CALL `your_project_id.your_dataset_id.sp_execute_core_logic`(v_job_name, p_EintragsNr, v_tab_name, v_records_processed_total);
    -- ^ this would require `sp_execute_core_logic` to output `records_processed`
    -- END OF CORE SQL LOGIC PLACEHOLDER

    -- 5. Deactivate job and log successful execution
    UPDATE `your_project_id.your_dataset_id.job_table`
    SET active_flag = FALSE, updated_ts = CURRENT_TIMESTAMP(), completed_ts = CURRENT_TIMESTAMP()
    WHERE job_name = v_job_name AND entry_nr = p_EintragsNr AND tab_name = v_tab_name;

    CALL `your_project_id.your_dataset_id.log_execution`(
        v_procedure_name,
        v_job_name,
        p_EintragsNr,
        v_tab_name,
        v_records_processed_total,
        'SUCCESS'
    );
    SELECT CONCAT('Job (', v_job_name, ', Entry: ', p_EintragsNr, ') for table ', v_tab_name, ' completed successfully. Records processed: ', v_records_processed_total);

  EXCEPTION WHEN ERROR THEN
    -- Capture error details
    SET v_error_message = @@error.message;
    -- Attempt to extract a numeric error code from the message, otherwise use a default
    SET v_error_number = COALESCE(SAFE_CAST(REGEXP_EXTRACT(@@error.message, r'Error code: (\d+)') AS INT64), -1);

    -- Log the error
    CALL `your_project_id.your_dataset_id.log_error`(v_procedure_name, v_error_number, 'CORE_EXECUTION_FAILURE', v_error_message);

    -- Update job_table to mark as failed (only if it was set active by this run)
    UPDATE `your_project_id.your_dataset_id.job_table`
    SET active_flag = FALSE, updated_ts = CURRENT_TIMESTAMP(), completed_ts = CURRENT_TIMESTAMP() -- Mark completion timestamp even on failure
    WHERE job_name = v_job_name AND entry_nr = p_EintragsNr AND tab_name = v_tab_name
      AND active_flag = TRUE; -- Only deactivate if it was marked active

    -- Log execution as failed
    CALL `your_project_id.your_dataset_id.log_execution`(
        v_procedure_name,
        v_job_name,
        p_EintragsNr,
        v_tab_name,
        v_records_processed_total,
        'FAILED'
    );

    -- Re-raise the error to signal failure to the caller/orchestrator
    RAISE USING MESSAGE CONCAT('Procedure `', v_procedure_name, '` failed for Job: ', v_job_name, ', Entry: ', p_EintragsNr, '. Error: ', v_error_message);
  END;
END;