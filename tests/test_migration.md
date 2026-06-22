The following migration validation tests are designed to ensure the BigQuery Stored Procedure `sp_ausd_v_ta_cntrct_crs3` is behaviourally equivalent to the legacy KornShell script `k_ausd_v_ta_cntrct_crs3.ksh`.

**Important Note on Core SQL Logic (`d_ausd_v_ta_cntrct_crs3.sql`):**
The migration design document explicitly states that the content of `d_ausd_v_ta_cntrct_crs3.sql` was not provided. For the purpose of these tests, we will assume a simplified core logic that inserts new records into `ta_cntrct_crs3` from a temporary staging table and returns the count of inserted rows. This assumption is crucial for testing `records_processed` accuracy and overall flow. In a real migration, the actual `d_ausd_v_ta_cntrct_crs3.sql` content would need to be translated to BigQuery SQL and thoroughly tested for its specific transformation correctness (joins, aggregations, filters, type handling, NULL handling, and any edge cases).

To make the tests runnable, the placeholder for the core SQL logic within `sprocs/sp_ausd_v_ta_cntrct_crs3.sql` will be replaced with the following assumed logic:

```sql
-- Modified section within sprocs/sp_ausd_v_ta_cntrct_crs3.sql for testing purposes
-- This simulates the core business logic from d_ausd_v_ta_cntrct_crs3.sql
-- It inserts new contracts into ta_cntrct_crs3 from a temporary staging table.

-- Create a temporary table to simulate staging data for this run
CREATE TEMPORARY TABLE temp_staging_contracts AS
SELECT 'CONTRACT_A' AS contract_id, CURRENT_DATE() AS contract_date, 'PENDING' AS status UNION ALL
SELECT 'CONTRACT_B' AS contract_id, DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS contract_date, 'PENDING' AS status UNION ALL
SELECT 'CONTRACT_C' AS contract_id, DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AS contract_date, 'PENDING' AS status;

-- Insert new contracts into ta_cntrct_crs3
INSERT INTO `your_project_id.your_dataset_id.ta_cntrct_crs3` (contract_id, contract_date, status, last_update_ts)
SELECT
    stg.contract_id,
    stg.contract_date,
    stg.status,
    CURRENT_TIMESTAMP()
FROM
    temp_staging_contracts stg
WHERE NOT EXISTS (
    SELECT 1 FROM `your_project_id.your_dataset_id.ta_cntrct_crs3`
    WHERE contract_id = stg.contract_id
);

SET v_records_processed_total = @@row_count;

-- Clean up temporary table
DROP TEMPORARY TABLE temp_staging_contracts;
```

---

## Test Case 1: Successful Execution - New Job

*   **Purpose**: Verify the stored procedure executes successfully when the job is not yet in `job_table`, correctly activates the job, runs the assumed core logic, deactivates the job, and logs execution details. This covers output parity and transformation correctness for a successful run.
*   **Setup**:
    1.  Ensure `your_project_id.your_dataset_id.job_table`, `your_project_id.your_dataset_id.error_log`, `your_project_id.your_dataset_id.execution_log`, and `your_project_id.your_dataset_id.ta_cntrct_crs3` are empty.
    2.  The `sp_ausd_v_ta_cntrct_crs3` procedure is deployed with the assumed core logic (inserting 3 unique records).
*   **Action**:
    Execute the BigQuery stored procedure with valid parameters:
    ```sql
    CALL `your_project_id.your_dataset_id.sp_ausd_v_ta_cntrct_crs3`('JOB_TEST_1', 'ENTRY_001');
    ```
*   **Pass/Fail Criterion**:
    1.  **`job_table`**: Contains exactly one entry for `job_name='JOB_TEST_1'`, `entry_nr='ENTRY_001'`, `tab_name='ta_cntrct_crs3'`. The `active_flag` must be `FALSE`, `created_ts` and `updated_ts` should be recent, and `completed_ts` must be populated.
    2.  **`execution_log`**: Contains exactly one entry for `procedure_name='sp_ausd_v_ta_cntrct_crs3'`, `job_name='JOB_TEST_1'`, `entry_nr='ENTRY_001'`, `tab_name='ta_cntrct_crs3'`. The `status` must be `'SUCCESS'`, and `records_processed` must be `3` (based on the assumed core logic).
    3.  **`error_log`**: Must be empty.
    4.  **`ta_cntrct_crs3`**: Contains exactly 3 new records (`CONTRACT_A`, `CONTRACT_B`, `CONTRACT_C`) inserted by the procedure.
    ```sql
    -- Pytest assertion example
    def test_successful_execution_new_job(bq_client):
        # Setup: Clear tables
        bq_client.query("TRUNCATE TABLE `your_project_id.your_dataset_id.job_table`").result()
        bq_client.query("TRUNCATE TABLE `your_project_id.your_dataset_id.execution_log`").result()
        bq_client.query("TRUNCATE TABLE `your_project_id.your_dataset_id.error_log`").result()
        bq_client.query("TRUNCATE TABLE `your_project_id.your_dataset_id.ta_cntrct_crs3`").result()

        # Action: Call SP
        bq_client.query("CALL `your_project_id.your_dataset_id.sp_ausd_v_ta_cntrct_crs3`('JOB_TEST_1', 'ENTRY_001')").result()

        # Assertions
        job_entry = bq_client.query("""
            SELECT job_name, entry_nr, tab_name, active_flag, completed_ts
            FROM `your_project_id.your_dataset_id.job_table`
            WHERE job_name = 'JOB_TEST_1' AND entry_nr = 'ENTRY_001'
        """).to_dataframe()
        assert len(job_entry) == 1
        assert job_entry.iloc[0]['active_flag'] == False
        assert job_entry.iloc[0]['completed_ts'] is not None

        exec_entry = bq_client.query("""
            SELECT job_name, entry_nr, tab_name, records_processed, status
            FROM `your_project_id.your_dataset_id.execution_log`
            WHERE job_name = 'JOB_TEST_1' AND entry_nr = 'ENTRY_001'
        """).to_dataframe()
        assert len(exec_entry) == 1
        assert exec_entry.iloc[0]['status'] == 'SUCCESS'
        assert exec_entry.iloc[0]['records_processed'] == 3

        error_count = bq_client.query("SELECT COUNT(*) FROM `your_project_id.your_dataset_id.error_log`").to_dataframe().iloc[0,0]
        assert error_count == 0

        ta_cntrct_count = bq_client.query("SELECT COUNT(*) FROM `your_project_id.your_dataset_id.ta_cntrct_crs3`").to_dataframe().iloc[0,0]
        assert ta_cntrct_count == 3
    ```

## Test Case 2: Successful Execution - Existing Inactive Job

*   **Purpose**: Verify the stored procedure executes successfully when the job exists in `job_table` but is inactive, correctly activates, runs core logic, deactivates, and logs. This covers transformation correctness for job control.
*   **Setup**:
    1.  `your_project_id.your_dataset_id.job_table` contains an entry for `job_name='JOB_TEST_2'`, `entry_nr='ENTRY_002'`, `tab_name='ta_cntrct_crs3'` with `active_flag = FALSE`.
    2.  `your_project_id.your_dataset_id.error_log` and `your_project_id.your_dataset_id.execution_log` are empty.
    3.  `your_project_id.your_dataset_id.ta_cntrct_crs3` contains some initial data (e.g., `CONTRACT_A` from a previous run) but not all 3 records from the assumed core logic.
*   **Action**:
    Execute the BigQuery stored procedure with the existing job parameters:
    ```sql
    CALL `your_project_id.your_dataset_id.sp_ausd_v_ta_cntrct_crs3`('JOB_TEST_2', 'ENTRY_002');
    ```
*   **Pass/Fail Criterion**:
    1.  **`job_table`**: The entry for `job_name='JOB_TEST_2'` has `active_flag = FALSE`, and `updated_ts` and `completed_ts` are updated to reflect the current run.
    2.  **`execution_log`**: Contains exactly one entry for `job_name='JOB_TEST_2'`, `entry_nr='ENTRY_002'`. The `status` must be `'SUCCESS'`, and `records_processed` must be `2` (if `CONTRACT_A` already exists, only `CONTRACT_B` and `CONTRACT_C` are inserted by the assumed core logic).
    3.  **`error_log`**: Must be empty.
    4.  **`ta_cntrct_crs3`**: Contains the expected total number of records (e.g., 3 records if `CONTRACT_A` was already there, then `CONTRACT_B` and `CONTRACT_C` are added).
    ```sql
    -- Pytest assertion example
    def test_successful_execution_existing_inactive_job(bq_client):
        # Setup: Clear logs, insert initial job_table entry, and some data into ta_cntrct_crs3
        bq_client.query("TRUNCATE TABLE `your_project_id.your_dataset_id.execution_log`").result()
        bq_client.query("TRUNCATE TABLE `your_project_id.your_dataset_id.error_log`").result()
        bq_client.query("TRUNCATE TABLE `your_project_id.your_dataset_id.job_table`").result()
        bq_client.query("TRUNCATE TABLE `your_project_id.your_dataset_id.ta_cntrct_crs3`").result()

        bq_client.query("""
            INSERT INTO `your_project_id.your_dataset_id.job_table`
            (job_name, entry_nr, tab_name, active_flag, created_ts, updated_ts)
            VALUES ('JOB_TEST_2', 'ENTRY_002', 'ta_cntrct_crs3', FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
        """).result()
        bq_client.query("""
            INSERT INTO `your_project_id.your_dataset_id.ta_cntrct_crs3`
            (contract_id, contract_date, status, last_update_ts)
            VALUES ('CONTRACT_A', CURRENT_DATE(), 'EXISTING', CURRENT_TIMESTAMP())
        """).result()

        # Action: Call SP
        bq_client.query("CALL `your_project_id.your_dataset_id.sp_ausd_v_ta_cntrct_crs3`('JOB_TEST_2', 'ENTRY_002')").result()

        # Assertions
        job_entry = bq_client.query("""
            SELECT job_name, entry_nr, tab_name, active_flag, completed_ts
            FROM `your_project_id.your_dataset_id.job_table`
            WHERE job_name = 'JOB_TEST_2' AND entry_nr = 'ENTRY_002'
        """).to_dataframe()
        assert len(job_entry) == 1
        assert job_entry.iloc[0]['active_flag'] == False
        assert job_entry.iloc[0]['completed_ts'] is not None

        exec_entry = bq_client.query("""
            SELECT job_name, entry_nr, tab_name, records_processed, status
            FROM `your_project_id.your_dataset_id.execution_log`
            WHERE job_name = 'JOB_TEST_2' AND entry_nr = 'ENTRY_002'
        """).to_dataframe()
        assert len(exec_entry) == 1
        assert exec_entry.iloc[0]['status'] == 'SUCCESS'
        assert exec_entry.iloc[0]['records_processed'] == 2 # Only B and C are new

        error_count = bq_client.query("SELECT COUNT(*) FROM `your_project_id.your_dataset_id.error_log`").to_dataframe().iloc[0,0]
        assert error_count == 0

        ta_cntrct_count = bq_client.query("SELECT COUNT(*) FROM `your_project_id.your_dataset_id.ta_cntrct_crs3`").to_dataframe().iloc[0,0]
        assert ta_cntrct_count == 3 # A (existing) + B + C (new)
    ```

## Test Case 3: Job Already Active - Skip Behavior

*   **Purpose**: Verify the stored procedure correctly identifies an already active job and skips execution, logging the skip event, mirroring the legacy script's "aktive Jobs werden ignoriert" behavior. This covers transformation correctness and external system replacement for job control.
*   **Setup**:
    1.  `your_project_id.your_dataset_id.job_table` contains an entry for `job_name='JOB_TEST_3'`, `entry_nr='ENTRY_003'`, `tab_name='ta_cntrct_crs3'` with `active_flag = TRUE`.
    2.  `your_project_id.your_dataset_id.error_log` and `your_project_id.your_dataset_id.execution_log` are empty.
    3.  `your_project_id.your_dataset_id.ta_cntrct_crs3` contains some initial data.
*   **Action**:
    Execute the BigQuery stored procedure with the active job parameters:
    ```sql
    CALL `your_project_id.your_dataset_id.sp_ausd_v_ta_cntrct_crs3`('JOB_TEST_3', 'ENTRY_003');
    ```
*   **Pass/Fail Criterion**:
    1.  **`job_table`**: The entry for `job_name='JOB_TEST_3'` remains `active_flag = TRUE`. The `updated_ts` and `completed_ts` for this entry must NOT be updated by this call.
    2.  **`execution_log`**: Contains exactly one entry for `job_name='JOB_TEST_3'`, `entry_nr='ENTRY_003'`. The `status` must be `'SKIPPED_ALREADY_ACTIVE'`, and `records_processed` must be `0`.
    3.  **`error_log`**: Must be empty.
    4.  **`ta_cntrct_crs3`**: Must remain unchanged.
    5.  The procedure should complete without raising an error.
    ```sql
    -- Pytest assertion example
    def test_job_already_active_skip_behavior(bq_client):
        # Setup: Clear logs, insert initial active job_table entry
        bq_client.query("TRUNCATE TABLE `your_project_id.your_dataset_id.execution_log`").result()
        bq_client.query("TRUNCATE TABLE `your_project_id.your_dataset_id.error_log`").result()
        bq_client.query("TRUNCATE TABLE `your_project_id.your_dataset_id.job_table`").result()
        bq_client.query("TRUNCATE TABLE `your_project_id.your_dataset_id.ta_cntrct_crs3`").result()

        bq_client.query("""
            INSERT INTO `your_project_id.your_dataset_id.job_table`
            (job_name, entry_nr, tab_name, active_flag, created_ts, updated_ts)
            VALUES ('JOB_TEST_3', 'ENTRY_003', 'ta_cntrct_crs3', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
        """).result()
        initial_job_entry = bq_client.query("""
            SELECT updated_ts, completed_ts FROM `your_project_id.your_dataset_id.job_table`
            WHERE job_name = 'JOB_TEST_3' AND entry_nr = 'ENTRY_003'
        """).to_dataframe().iloc[0]

        # Action: Call SP
        bq_client.query("CALL `your_project_id.your_dataset_id.sp_ausd_v_ta_cntrct_crs3`('JOB_TEST_3', 'ENTRY_003')").result()

        # Assertions
        job_entry_after = bq_client.query("""
            SELECT active_flag, updated_ts, completed_ts
            FROM `your_project_id.your_dataset_id.job_table`
            WHERE job_name = 'JOB_TEST_3' AND entry_nr = 'ENTRY_003'
        """).to_dataframe().iloc[0]
        assert job_entry_after['active_flag'] == True
        assert job_entry_after['updated_ts'] == initial_job_entry['updated_ts'] # Should not be updated
        assert job_entry_after['completed_ts'] == initial_job_entry['completed_ts'] # Should not be updated

        exec_entry = bq_client.query("""
            SELECT job_name, entry_nr, records_processed, status
            FROM `your_project_id.your_dataset_id.execution_log`
            WHERE job_name = 'JOB_TEST_3' AND entry_nr = 'ENTRY_003'
        """).to_dataframe()
        assert len(exec_entry) == 1
        assert exec_entry.iloc[0]['status'] == 'SKIPPED_ALREADY_ACTIVE'
        assert exec_entry.iloc[0]['records_processed'] == 0

        error_count = bq_client.query("SELECT COUNT(*) FROM `your_project_id.your_dataset_id.error_log`").to_dataframe().iloc[0,0]
        assert error_count == 0

        ta_cntrct_count = bq_client.query("SELECT COUNT(*) FROM `your_project_id.your_dataset_id.ta_cntrct_crs3`").to_dataframe().iloc[0,0]
        assert ta_cntrct_count == 0 # Should remain unchanged
    ```

## Test Case 4: Missing `p_JobKennung` Parameter

*   **Purpose**: Verify parameter validation for a missing `p_JobKennung` (legacy `j`) and correct error logging/raising, matching the legacy script's error handling (`ErrNr=192`). This covers transformation correctness and external system replacement for error handling.
*   **Setup**:
    1.  Ensure `your_project_id.your_dataset_id.job_table`, `your_project_id.your_dataset_id.error_log`, `your_project_id.your_dataset_id.execution_log` are empty.
*   **Action**:
    Attempt to execute the BigQuery stored procedure with `p_JobKennung` as `NULL` or an empty string:
    ```sql
    -- Using NULL
    CALL `your_project_id.your_dataset_id.sp_ausd_v_ta_cntrct_crs3`(NULL, 'ENTRY_004');
    -- Or using empty string
    -- CALL `your_project_id.your_dataset_id.sp_ausd_v_ta_cntrct_crs3`('', 'ENTRY_004');
    ```
*   **Pass/Fail Criterion**:
    1.  The procedure must `RAISE` an error. The error message should indicate `p_JobKennung` is missing.
    2.  **`error_log`**: Contains exactly one entry with `procedure_name='sp_ausd_v_ta_cntrct_crs3'`, `err_nr = 192`, `err_arg = 'p_JobKennung'`, and a message similar to 'ERROR: Parameter p_JobKennung (Job ID) must be provided.'.
    3.  **`job_table`** and **`execution_log`**: Must be empty.
    ```sql
    -- Pytest assertion example
    import pytest
    from google.cloud import bigquery

    def test_missing_jobkennung_parameter(bq_client):
        # Setup: Clear tables
        bq_client.query("TRUNCATE TABLE `your_project_id.your_dataset_id.job_table`").result()
        bq_client.query("TRUNCATE TABLE `your_project_id.your_dataset_id.execution_log`").result()
        bq_client.query("TRUNCATE TABLE `your_project_id.your_dataset_id.error_log`").result()

        # Action & Assertions
        with pytest.raises(bigquery.exceptions.GoogleAPICallError) as excinfo:
            bq_client.query("CALL `your_project_id.your_dataset_id.sp_ausd_v_ta_cntrct_crs3`(NULL, 'ENTRY_004')").result()

        assert "Parameter p_JobKennung (Job ID) must be provided" in str(excinfo.value)

        error_entry = bq_client.query("""
            SELECT procedure_name, err_nr, err_arg, message
            FROM `your_project_id.your_dataset_id.error_log`
        """).to_dataframe()
        assert len(error_entry) == 1
        assert error_entry.iloc[0]['procedure_name'] == 'sp_ausd_v_ta_cntrct_crs3'
        assert error_entry.iloc[0]['err_nr'] == 192
        assert error_entry.iloc[0]['err_arg'] == 'p_JobKennung'
        assert "Parameter p_JobKennung (Job ID) must be provided" in error_entry.iloc[0]['message']

        job_count = bq_client.query("SELECT COUNT(*) FROM `your_project_id.your_dataset_id.job_table`").to_dataframe().iloc[0,0]
        assert job_count == 0
        exec_count = bq_client.query("SELECT COUNT(*) FROM `your_project_id.your_dataset_id.execution_log`").to_dataframe().iloc[0,0]
        assert exec_count == 0
    ```

## Test Case 5: Missing `p_EintragsNr` Parameter

*   **Purpose**: Verify parameter validation for a missing `p_EintragsNr` (legacy `f`) and correct error logging/raising, matching the legacy script's error handling (`ErrNr=193`). This covers transformation correctness and external system replacement for error handling.
*   **Setup**:
    1.  Ensure `your_project_id.your_dataset_id.job_table`, `your_project_id.your_dataset_id.error_log`, `your_project_id.your_dataset_id.execution_log` are empty.
*   **Action**:
    Attempt to execute the BigQuery stored procedure with `p_EintragsNr` as `NULL` or an empty string:
    ```sql
    -- Using NULL
    CALL `your_project_id.your_dataset_id.sp_ausd_v_ta_cntrct_crs3`('JOB_TEST_5', NULL);
    -- Or using empty string
    -- CALL `your_project_id.your_dataset_id.sp_ausd_v_ta_cntrct_crs3`('JOB_TEST_5', '');
    ```
*   **Pass/Fail Criterion**:
    1.  The procedure must `RAISE` an error. The error message should indicate `p_EintragsNr` is missing.
    2.  **`error_log`**: Contains exactly one entry with `procedure_name='sp_ausd_v_ta_cntrct_crs3'`, `err_nr = 193`, `err_arg = 'p_EintragsNr'`, and a message similar to 'ERROR: Parameter p_EintragsNr (Entry Number) must be provided.'.
    3.  **`job_table`** and **`execution_log`**: Must be empty.
    ```sql
    -- Pytest assertion example
    import pytest
    from google.cloud import bigquery

    def test_missing_eintragsnr_parameter(bq_client):
        # Setup: Clear tables
        bq_client.query("TRUNCATE TABLE `your_project_id.your_dataset_id.job_table`").result()
        bq_client.query("TRUNCATE TABLE `your_project_id.your_dataset_id.execution_log`").result()
        bq_client.query("TRUNCATE TABLE `your_project_id.your_dataset_id.error_log`").result()

        # Action & Assertions
        with pytest.raises(bigquery.exceptions.GoogleAPICallError) as excinfo:
            bq_client.query("CALL `your_project_id.your_dataset_id.sp_ausd_v_ta_cntrct_crs3`('JOB_TEST_5', NULL)").result()

        assert "Parameter p_EintragsNr (Entry Number) must be provided" in str(excinfo.value)

        error_entry = bq_client.query("""
            SELECT procedure_name, err_nr, err_arg, message
            FROM `your_project_id.your_dataset_id.error_log`
        """).to_dataframe()
        assert len(error_entry) == 1
        assert error_entry.iloc[0]['procedure_name'] == 'sp_ausd_v_ta_cntrct_crs3'
        assert error_entry.iloc[0]['err_nr'] == 193
        assert error_entry.iloc[0]['err_arg'] == 'p_EintragsNr'
        assert "Parameter p_EintragsNr (Entry Number) must be provided" in error_entry.iloc[0]['message']

        job_count = bq_client.query("SELECT COUNT(*) FROM `your_project_id.your_dataset_id.job_table`").to_dataframe().iloc[0,0]
        assert job_count == 0
        exec_count = bq_client.query("SELECT COUNT(*) FROM `your_project_id.your_dataset_id.execution_log`").to_dataframe().iloc[0,0]
        assert exec_count == 0
    ```

## Test Case 6: Core SQL Logic Failure

*   **Purpose**: Verify that if the core SQL logic fails, the job is marked as failed in `job_table`, an error is logged in `error_log`, and execution details are recorded in `execution_log` with a `FAILED` status. This covers transformation correctness and external system replacement for error handling.
*   **Setup**:
    1.  `your_project_id.your_dataset_id.job_table` contains an entry for `job_name='JOB_TEST_6'`, `entry_nr='ENTRY_006'` with `active_flag = FALSE`.
    2.  `your_project_id.your_dataset_id.error_log` and `your_project_id.your_dataset_id.execution_log` are empty.
    3.  **Crucially**: Temporarily modify the core SQL logic within `sp_ausd_v_ta_cntrct_crs3` to intentionally cause an error (e.g., `SELECT 1/0;` or `INSERT INTO non_existent_table ...`).
*   **Action**:
    Execute the BigQuery stored procedure with valid parameters:
    ```sql
    CALL `your_project_id.your_dataset_id.sp_ausd_v_ta_cntrct_crs3`('JOB_TEST_6', 'ENTRY_006');
    ```
*   **Pass/Fail Criterion**:
    1.  The procedure must `RAISE` an error.
    2.  **`job_table`**: The entry for `job_name='JOB_TEST_6'` has `active_flag = FALSE`, and `updated_ts` and `completed_ts` are updated.
    3.  **`error_log`**: Contains exactly one entry with `procedure_name='sp_ausd_v_ta_cntrct_crs3'`, an appropriate `err_nr` (e.g., -1 or a BigQuery-specific error code), `err_arg = 'CORE_EXECUTION_FAILURE'`, and the specific error message from the SQL failure.
    4.  **`execution_log`**: Contains exactly one entry for `job_name='JOB_TEST_6'`, `entry_nr='ENTRY_006'`. The `status` must be `'FAILED'`, and `records_processed` should reflect the state before the error (e.g., 0 if the error occurs early, or a partial count if some DML succeeded before the error).
    5.  **`ta_cntrct_crs3`**: Should reflect only partial or no changes, depending on where the error occurred and BigQuery's transactionality.
    ```sql
    -- Pytest assertion example (requires temporary modification of the SP)
    import pytest
    from google.cloud import bigquery

    def test_core_sql_logic_failure(bq_client):
        # Setup: Clear tables, insert initial job_table entry
        bq_client.query("TRUNCATE TABLE `your_project_id.your_dataset_id.job_table`").result()
        bq_client.query("TRUNCATE TABLE `your_project_id.your_dataset_id.execution_log`").result()
        bq_client.query("TRUNCATE TABLE `your_project_id.your_dataset_id.error_log`").result()
        bq_client.query("TRUNCATE TABLE `your_project_id.your_dataset_id.ta_cntrct_crs3`").result()

        bq_client.query("""
            INSERT INTO `your_project_id.your_dataset_id.job_table`
            (job_name, entry_nr, tab_name, active_flag, created_ts, updated_ts)
            VALUES ('JOB_TEST_6', 'ENTRY_006', 'ta_cntrct_crs3', FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
        """).result()

        # IMPORTANT: Temporarily deploy a version of sp_ausd_v_ta_cntrct_crs3
        # that includes a deliberate error in its core logic section, e.g.:
        # SET v_records_processed_total = (SELECT 1/0); -- This will cause a division by zero error
        # Or: INSERT INTO `non_existent_table` VALUES (1);

        # Action & Assertions
        with pytest.raises(bigquery.exceptions.GoogleAPICallError) as excinfo:
            bq_client.query("CALL `your_project_id.your_dataset_id.sp_ausd_v_ta_cntrct_crs3`('JOB_TEST_6', 'ENTRY_006')").result()

        assert "division by zero" in str(excinfo.value) or "non_existent_table" in str(excinfo.value) # Adjust based on injected error

        job_entry = bq_client.query("""
            SELECT active_flag, completed_ts
            FROM `your_project_id.your_dataset_id.job_table`
            WHERE job_name = 'JOB_TEST_6' AND entry_nr = 'ENTRY_006'
        """).to_dataframe()
        assert len(job_entry) == 1
        assert job_entry.iloc[0]['active_flag'] == False
        assert job_entry.iloc[0]['completed_ts'] is not None

        error_entry = bq_client.query("""
            SELECT procedure_name, err_nr, err_arg, message
            FROM `your_project_id.your_dataset_id.error_log`
            WHERE err_arg = 'CORE_EXECUTION_FAILURE'
        """).to_dataframe()
        assert len(error_entry) == 1
        assert error_entry.iloc[0]['procedure_name'] == 'sp_ausd_v_ta_cntrct_crs3'
        assert error_entry.iloc[0]['err_arg'] == 'CORE_EXECUTION_FAILURE'
        assert "division by zero" in error_entry.iloc[0]['message'] or "non_existent_table" in error_entry.iloc[0]['message']

        exec_entry = bq_client.query("""
            SELECT status, records_processed
            FROM `your_project_id.your_dataset_id.execution_log`
            WHERE job_name = 'JOB_TEST_6' AND entry_nr = 'ENTRY_006'
        """).to_dataframe()
        assert len(exec_entry) == 1
        assert exec_entry.iloc[0]['status'] == 'FAILED'
        # records_processed might be 0 or partial depending on where the error was injected
        # For a division by zero, it would likely be 0.
        assert exec_entry.iloc[0]['records_processed'] == 0

        ta_cntrct_count = bq_client.query("SELECT COUNT(*) FROM `your_project_id.your_dataset_id.ta_cntrct_crs3`").to_dataframe().iloc[0,0]
        assert ta_cntrct_count == 0 # No records should be inserted due to error
    ```

## Test Case 7: Data Quality - `records_processed` Accuracy

*   **Purpose**: Verify that `v_records_processed_total` accurately reflects the number of rows affected by the core SQL logic, matching the legacy script's capture of `v_records`. This covers data quality and row-count assertions.
*   **Setup**:
    1.  Ensure `your_project_id.your_dataset_id.job_table`, `your_project_id.your_dataset_id.error_log`, `your_project_id.your_dataset_id.execution_log` are empty.
    2.  `your_project_id.your_dataset_id.ta_cntrct_crs3` is empty.
    3.  The `sp_ausd_v_ta_cntrct_crs3` procedure is deployed with the assumed core logic.
*   **Action**:
    1.  Call `sp_ausd_v_ta_cntrct_crs3` with `JOB_TEST_7_A`, `ENTRY_007A`. (Expected 3 records)
    2.  Modify the assumed core SQL logic *temporarily* to insert only 1 record (e.g., filter `WHERE contract_id = 'CONTRACT_A'`). Call `sp_ausd_v_ta_cntrct_crs3` with `JOB_TEST_7_B`, `ENTRY_007B`.
    3.  Modify the assumed core SQL logic *temporarily* to insert 0 records (e.g., filter `WHERE 1=0`). Call `sp_ausd_v_ta_cntrct_crs3` with `JOB_TEST_7_C`, `ENTRY_007C`.
*   **Pass/Fail Criterion**:
    1.  For each run, the `records_processed` in the `execution_log` entry for `SUCCESS` matches the actual number of rows inserted into `ta_cntrct_crs3` by that specific call.
    2.  `ta_cntrct_crs3` contains the correct cumulative number of records after all runs.
    ```sql
    -- Pytest assertion example (requires temporary modification of the SP for runs B and C)
    def test_records_processed_accuracy(bq_client):
        # Setup: Clear tables
        bq_client.query("TRUNCATE TABLE `your_project_id.your_dataset_id.job_table`").result()
        bq_client.query("TRUNCATE TABLE `your_project_id.your_dataset_id.execution_log`").result()
        bq_client.query("TRUNCATE TABLE `your_project_id.your_dataset_id.error_log`").result()
        bq_client.query("TRUNCATE TABLE `your_project_id.your_dataset_id.ta_cntrct_crs3`").result()

        # Run 1: Default assumed logic (3 records)
        bq_client.query("CALL `your_project_id.your_dataset_id.sp_ausd_v_ta_cntrct_crs3`('JOB_TEST_7_A', 'ENTRY_007A')").result()
        exec_entry_a = bq_client.query("SELECT records_processed FROM `your_project_id.your_dataset_id.execution_log` WHERE entry_nr = 'ENTRY_007A'").to_dataframe()
        assert exec_entry_a.iloc[0]['records_processed'] == 3
        ta_cntrct_count_a = bq_client.query("SELECT COUNT(*) FROM `your_project_id.your_dataset_id.ta_cntrct_crs3`").to_dataframe().iloc[0,0]
        assert ta_cntrct_count_a == 3

        # Run 2: Modify SP to insert 1 record (e.g., filter for 'CONTRACT_A' only)
        # This would involve redeploying the SP with modified core logic:
        # ... WHERE contract_id = 'CONTRACT_A';
        # For testing purposes, we'll simulate this by manually inserting 1 record and asserting.
        # In a real test, you'd redeploy the SP.
        bq_client.query("""
            INSERT INTO `your_project_id.your_dataset_id.ta_cntrct_crs3` (contract_id, contract_date, status, last_update_ts)
            VALUES ('CONTRACT_D', CURRENT_DATE(), 'PENDING', CURRENT_TIMESTAMP())
        """).result()
        bq_client.query("CALL `your_project_id.your_dataset_id.sp_ausd_v_ta_cntrct_crs3`('JOB_TEST_7_B', 'ENTRY_007B')").result()
        exec_entry_b = bq_client.query("SELECT records_processed FROM `your_project_id.your_dataset_id.execution_log` WHERE entry_nr = 'ENTRY_007B'").to_dataframe()
        # Assuming the modified SP would insert 1 new record (e.g., 'CONTRACT_D' if it wasn't there, or a new one)
        # For this example, let's assume the SP is modified to insert only 'CONTRACT_D'
        assert exec_entry_b.iloc[0]['records_processed'] == 1
        ta_cntrct_count_b = bq_client.query("SELECT COUNT(*) FROM `your_project_id.your_dataset_id.ta_cntrct_crs3`").to_dataframe().iloc[0,0]
        assert ta_cntrct_count_b == 4 # 3 from A + 1 from B

        # Run 3: Modify SP to insert 0 records (e.g., filter WHERE 1=0)
        # This would involve redeploying the SP with modified core logic:
        # ... WHERE 1=0;
        bq_client.query("CALL `your_project_id.your_dataset_id.sp_ausd_v_ta_cntrct_crs3`('JOB_TEST_7_C', 'ENTRY_007C')").result()
        exec_entry_c = bq_client.query("SELECT records_processed FROM `your_project_id.your_dataset_id.execution_log` WHERE entry_nr = 'ENTRY_007C'").to_dataframe()
        assert exec_entry_c.iloc[0]['records_processed'] == 0
        ta_cntrct_count_c = bq_client.query("SELECT COUNT(*) FROM `your_project_id.your_dataset_id.ta_cntrct_crs3`").to_dataframe().iloc[0,0]
        assert ta_cntrct_count_c == 4 # No new records
    ```

## Test Case 8: Schema and DDL Compliance

*   **Purpose**: Verify that the DDLs for `job_table`, `error_log`, `execution_log`, and `ta_cntrct_crs3` are correctly applied and match the expected structure, including data types, nullability, and default values. This covers schema assertions.
*   **Setup**:
    1.  Ensure all DDLs (`ddl/job_table.sql`, `ddl/error_log.sql`, `ddl/execution_log.sql`, `ddl/ta_cntrct_crs3.sql`) have been executed in the target BigQuery dataset.
*   **Action**:
    Query BigQuery's `INFORMATION_SCHEMA.COLUMNS` and `INFORMATION_SCHEMA.TABLES` for the details of the created tables.
*   **Pass/Fail Criterion**:
    1.  All specified tables (`job_table`, `error_log`, `execution_log`, `ta_cntrct_crs3`) exist in `your_project_id.your_dataset_id`.
    2.  Each table has the exact set of columns, with correct `DATA_TYPE`, `IS_NULLABLE` (nullability), and `COLUMN_DEFAULT` (default values) as defined in their respective DDLs.
        *   **`job_table`**:
            *   `job_name` (STRING, NOT NULL)
            *   `entry_nr` (STRING, NOT NULL)
            *   `tab_name` (STRING, NOT NULL)
            *   `active_flag` (BOOL, NOT NULL, DEFAULT FALSE)
            *   `created_ts` (TIMESTAMP, NOT NULL, DEFAULT CURRENT_TIMESTAMP())
            *   `updated_ts` (TIMESTAMP, NOT NULL, DEFAULT CURRENT_TIMESTAMP())
            *   `completed_ts` (TIMESTAMP, NULLABLE)
        *   **`error_log`**:
            *   `log_ts` (TIMESTAMP, NOT NULL, DEFAULT CURRENT_TIMESTAMP())
            *   `procedure_name` (STRING, NOT NULL)
            *   `err_nr` (INT64, NULLABLE)
            *   `err_arg` (STRING, NULLABLE)
            *   `message` (STRING, NULLABLE)
        *   **`execution_log`**:
            *   `log_ts` (TIMESTAMP, NOT NULL, DEFAULT CURRENT_TIMESTAMP())
            *   `procedure_name` (STRING, NOT NULL)
            *   `job_name` (STRING, NOT NULL)
            *   `entry_nr` (STRING, NOT NULL)
            *   `tab_name` (STRING, NOT NULL)
            *   `records_processed` (INT64, NULLABLE)
            *   `status` (STRING, NOT NULL)
        *   **`ta_cntrct_crs3`**: (Based on placeholder DDL)
            *   `contract_id` (STRING, NOT NULL)
            *   `contract_date` (DATE, NULLABLE)
            *   `status` (STRING, NULLABLE)
            *   `last_update_ts` (TIMESTAMP, NULLABLE)
    ```sql
    -- SQL Assertion Example for job_table schema
    SELECT
        column_name,
        data_type,
        is_nullable,
        column_default
    FROM
        `your_project_id.your_dataset_id.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = 'job_table'
    ORDER BY
        ordinal_position;

    -- Expected result for job_table:
    -- column_name    data_type   is_nullable column_default
    -- job_name       STRING      NO          NULL
    -- entry_nr       STRING      NO          NULL
    -- tab_name       STRING      NO          NULL
    -- active_flag    BOOL        NO          FALSE
    -- created_ts     TIMESTAMP   NO          CURRENT_TIMESTAMP()
    -- updated_ts     TIMESTAMP   NO          CURRENT_TIMESTAMP()
    -- completed_ts   TIMESTAMP   YES         NULL

    -- Pytest assertion example
    def test_schema_compliance(bq_client):
        dataset_id = "your_dataset_id"
        project_id = "your_project_id"

        expected_schemas = {
            "job_table": [
                ("job_name", "STRING", "NO", None),
                ("entry_nr", "STRING", "NO", None),
                ("tab_name", "STRING", "NO", None),
                ("active_flag", "BOOL", "NO", "FALSE"),
                ("created_ts", "TIMESTAMP", "NO", "CURRENT_TIMESTAMP()"),
                ("updated_ts", "TIMESTAMP", "NO", "CURRENT_TIMESTAMP()"),
                ("completed_ts", "TIMESTAMP", "YES", None),
            ],
            "error_log": [
                ("log_ts", "TIMESTAMP", "NO", "CURRENT_TIMESTAMP()"),
                ("procedure_name", "STRING", "NO", None),
                ("err_nr", "INT64", "YES", None),
                ("err_arg", "STRING", "YES", None),
                ("message", "STRING", "YES", None),
            ],
            "execution_log": [
                ("log_ts", "TIMESTAMP", "NO", "CURRENT_TIMESTAMP()"),
                ("procedure_name", "STRING", "NO", None),
                ("job_name", "STRING", "NO", None),
                ("entry_nr", "STRING", "NO", None),
                ("tab_name", "STRING", "NO", None),
                ("records_processed", "INT64", "YES", None),
                ("status", "STRING", "NO", None),
            ],
            "ta_cntrct_crs3": [ # Based on the placeholder DDL
                ("contract_id", "STRING", "NO", None),
                ("contract_date", "DATE", "YES", None),
                ("status", "STRING", "YES", None),
                ("last_update_ts", "TIMESTAMP", "YES", None),
            ],
        }

        for table_name, expected_cols in expected_schemas.items():
            query = f"""
                SELECT
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM
                    `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
                WHERE
                    table_name = '{table_name}'
                ORDER BY
                    ordinal_position
            """
            rows = bq_client.query(query).result()
            actual_cols = [(row.column_name, row.data_type, row.is_nullable, row.column_default) for row in rows]
            
            # Normalize column_default for comparison (e.g., remove parentheses for functions)
            normalized_actual_cols = []
            for col_name, data_type, is_nullable, col_default in actual_cols:
                if col_default and col_default.endswith("()"):
                    col_default = col_default[:-2] # Remove () for comparison with 'CURRENT_TIMESTAMP'
                normalized_actual_cols.append((col_name, data_type, is_nullable, col_default))

            normalized_expected_cols = []
            for col_name, data_type, is_nullable, col_default in expected_cols:
                if col_default and col_default.endswith("()"):
                    col_default = col_default[:-2]
                normalized_expected_cols.append((col_name, data_type, is_nullable, col_default))

            assert sorted(normalized_actual_cols) == sorted(normalized_expected_cols), f"Schema mismatch for table {table_name}"
    ```