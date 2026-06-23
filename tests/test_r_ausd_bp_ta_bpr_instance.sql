-- Test script for BigQuery Stored Procedure r_ausd_bp_ta_bpr_instance
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh
-- This script provides example calls for testing the migrated BigQuery stored procedures.

-- Define test parameters
DECLARE test_job_kennung STRING DEFAULT 'TEST_JOB_001';
DECLARE test_eintrags_nr STRING DEFAULT '001';
DECLARE test_stichtag_valid STRING DEFAULT '25122023'; -- DDMMYYYY
DECLARE test_stichtag_invalid STRING DEFAULT '2023-12-25';
DECLARE test_wiederanlauf_wert STRING DEFAULT NULL; -- Or '1' for a restart scenario

-- 1. Test case: Successful run with valid parameters
-- This call should populate sof_ta_bpr_instance and log to job_run_log.
SELECT '--- Running Test Case 1: Successful execution ---' AS TEST_CASE;
CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_bpr_instance`(
  p_JobKennung => test_job_kennung,
  p_EintragsNr => test_eintrags_nr,
  p_Stichtag => test_stichtag_valid,
  p_wiederanlaufWert => test_wiederanlauf_wert
);

-- Verify results for Test Case 1
SELECT '--- Verification for Test Case 1 ---' AS VERIFICATION;
SELECT * FROM `your_project_id.your_dataset_id.job_run_log` WHERE job_name = 'r_ausd_bp_ta_bpr_instance' ORDER BY created_at DESC LIMIT 1;
SELECT COUNT(*) FROM `your_project_id.your_dataset_id.sof_ta_bpr_instance`; -- Check if data was inserted (adjust WHERE clause if needed for specific test data)

-- 2. Test case: Invalid Stichtag format
-- This call should log an error to job_error_log and gracefully exit.
SELECT '--- Running Test Case 2: Invalid Stichtag format ---' AS TEST_CASE;
-- Using a BEGIN...EXCEPTION block to catch and report the error without stopping the script
BEGIN
  CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_bpr_instance`(
    p_JobKennung => test_job_kennung,
    p_EintragsNr => test_eintrags_nr,
    p_Stichtag => test_stichtag_invalid, -- Invalid format
    p_wiederanlaufWert => test_wiederanlauf_wert
  );
EXCEPTION WHEN ERROR THEN
  SELECT FORMAT('Caught expected error for invalid Stichtag: %s', ERROR_MESSAGE()) AS ErrorDetails;
END;

-- Verify results for Test Case 2
SELECT '--- Verification for Test Case 2 ---' AS VERIFICATION;
SELECT * FROM `your_project_id.your_dataset_id.job_error_log` WHERE job_name = 'r_ausd_bp_ta_bpr_instance' AND error_code = 193 ORDER BY created_at DESC LIMIT 1;

-- 3. Test case: Missing JobKennung (example of parameter validation error)
SELECT '--- Running Test Case 3: Missing JobKennung ---' AS TEST_CASE;
BEGIN
  CALL `your_project_id.your_dataset_id.r_ausd_bp_ta_bpr_instance`(
    p_JobKennung => NULL, -- Missing parameter
    p_EintragsNr => test_eintrags_nr,
    p_Stichtag => test_stichtag_valid,
    p_wiederanlaufWert => test_wiederanlauf_wert
  );
EXCEPTION WHEN ERROR THEN
  SELECT FORMAT('Caught expected error for missing JobKennung: %s', ERROR_MESSAGE()) AS ErrorDetails;
END;

-- Verify results for Test Case 3
SELECT '--- Verification for Test Case 3 ---' AS VERIFICATION;
SELECT * FROM `your_project_id.your_dataset_id.job_error_log` WHERE job_name = 'r_ausd_bp_ta_bpr_instance' AND error_code = 1 ORDER BY created_at DESC LIMIT 1;

-- Note: For a complete test, you would need to mock or pre-populate the source tables
-- `your_project_id.your_dataset_id.cds_ta_cntrct` and
-- `your_project_id.your_dataset_id.pds_ta_bpri_com` with test data.
-- Also, the `sof_ta_bpr_instance` table would need to exist prior to running these tests.
-- Ensure the dataset and tables are created before executing these tests.