The migration of `k_ausd_bp_ta_bpr_basis_his.ksh` primarily involves transforming an orchestration shell script into a BigQuery Stored Procedure. The core data transformation logic, originally in `d_ausd_bp_ta_bpr_basis_his.sql`, was provided as a placeholder in the migration design. Therefore, tests for deep transformation correctness (joins, aggregations, etc.) will be conceptual and rely on the assumption that the placeholder SQL will be replaced with functionally equivalent BigQuery SQL.

The tests below focus on validating the orchestration logic, parameter handling, error management, and audit logging, which are directly implemented in the BigQuery Stored Procedure.

---

## Migration Validation Tests for `k_ausd_bp_ta_bpr_basis_his.ksh`

### 1. Output Parity

#### Test Case 1.1: Data Content Parity in `PoolBasisprodukt`

*   **Purpose:** To verify that the final data loaded into the `PoolBasisprodukt` table by the BigQuery stored procedure is identical to the data loaded by the legacy KornShell script, given the same initial source data and parameters. This test implicitly validates the correctness of the embedded SQL transformation logic.
*   **Setup:**
    1.  **Legacy Environment:**
        *   Prepare a controlled, representative set of input data for all source tables referenced by the original `d_ausd_bp_ta_bpr_basis_his.sql`.
        *   Ensure the legacy `PoolBasisprodukt` table (or its equivalent) is empty.
        *   Execute the legacy `k_ausd_bp_ta_bpr_basis_his.ksh` script with a specific set of valid parameters (e.g., `p_JobKennung='TEST_JOB'`, `p_EintragsNr='1'`, `p_Stichtag='01012023'`, `p_wiederanlaufWert=0`).
        *   Extract the entire content of the legacy `PoolBasisprodukt` table into a canonical reference format (e.g., sorted CSV, JSON array of objects).
    2.  **BigQuery Environment:**
        *   Load the *exact same* controlled input data into the corresponding BigQuery source tables (e.g., `project.dataset.source_pool_basisprodukt_data`).
        *   Ensure the target `project.dataset.PoolBasisprodukt` table is empty.
*   **Action:**
    1.  Call the BigQuery stored procedure:
        ```sql
        CALL `project.dataset.proc_k_ausd_bp_ta_bpr_basis_his`(
            p_JobKennung => 'TEST_JOB',
            p_EintragsNr => '1',
            p_Stichtag => '01012023',
            p_wiederanlaufWert => 0
        );
        ```
    2.  After the procedure completes, extract the entire content of `project.dataset.PoolBasisprodukt` into a comparable format.
*   **Pass/Fail Criterion:**
    *   The extracted data from `project.dataset.PoolBasisprodukt` must be identical to the reference data extracted from the legacy `PoolBasisprodukt` table. This includes row counts, column values, and data types (allowing for BigQuery's native types where appropriate, e.g., `NUMERIC` vs. `DECIMAL`).
*   **Runnable Test Code (Conceptual Pytest with Pandas):**
    ```python
    import pandas as pd
    from google.cloud import bigquery
    import pytest

    # Assume bigquery_client is a pytest fixture providing an authenticated BigQuery client
    # Assume legacy_data_reference_df is a pandas DataFrame loaded from the legacy system's output

    def test_poolbasisprodukt_data_parity(bigquery_client, legacy_data_reference_df):
        # --- Setup (pre-test, typically in a fixture or setup function) ---
        # 1. Load source data into BigQuery (mocked or actual)
        #    Example: bigquery_client.query("INSERT INTO `project.dataset.source_pool_basisprodukt_data` ...")
        # 2. Ensure target table is empty
        bigquery_client.query("TRUNCATE TABLE `project.dataset.PoolBasisprodukt`").result()

        # --- Action ---
        # Call the BigQuery Stored Procedure
        sp_call_query = """
        CALL `project.dataset.proc_k_ausd_bp_ta_bpr_basis_his`(
            p_JobKennung => 'TEST_JOB',
            p_EintragsNr => '1',
            p_Stichtag => '01012023',
            p_wiederanlaufWert => 0
        );
        """
        bigquery_client.query(sp_call_query).result()

        # Extract data from BigQuery target table
        bq_data_query = "SELECT * FROM `project.dataset.PoolBasisprodukt` ORDER BY product_key, processing_date"
        bq_df = bigquery_client.query(bq_data_query).to_dataframe()

        # --- Pass/Fail Criterion ---
        # Sort both DataFrames consistently for comparison
        bq_df_sorted = bq_df.sort_values(by=['product_key', 'processing_date']).reset_index(drop=True)
        legacy_df_sorted = legacy_data_reference_df.sort_values(by=['product_key', 'processing_date']).reset_index(drop=True)

        # Compare DataFrames
        pd.testing.assert_frame_equal(
            bq_df_sorted,
            legacy_df_sorted,
            check_dtype=True,
            check_exact=False, # Use False for floating point comparisons if needed
            atol=1e-6 # Absolute tolerance for numeric comparisons
        )

        # Also check record count parity
        assert len(bq_df_sorted) == len(legacy_df_sorted), "Record count mismatch in PoolBasisprodukt"

    ```

### 2. Transformation Correctness (Orchestration Logic)

#### Test Case 2.1: Parameter Validation - Missing Required Parameter (`p_JobKennung`)

*   **Purpose:** Verify that the BigQuery stored procedure correctly identifies and handles a missing required parameter (`p_JobKennung`), logs an error, and terminates execution. This mirrors the `pruefeParameterGesetzt` and `DWMSG_MeldeFehler` behavior.
*   **Setup:**
    1.  Ensure `project.dataset.error_log` and `project.dataset.job_audit` tables are empty.
*   **Action:**
    1.  **Legacy:** Attempt to execute `k_ausd_bp_ta_bpr_basis_his.ksh` without the `-j` parameter:
        ```bash
        ./k_ausd_bp_ta_bpr_basis_his.ksh -f 1 -s 01012023
        ```
    2.  **BigQuery:** Call the stored procedure with `p_JobKennung` as `NULL` or an empty string:
        ```sql
        CALL `project.dataset.proc_k_ausd_bp_ta_bpr_basis_his`(
            p_JobKennung => NULL, -- or ''
            p_EintragsNr => '1',
            p_Stichtag => '01012023',
            p_wiederanlaufWert => 0
        );
        ```
*   **Pass/Fail Criterion:**
    1.  **Legacy:** The script exits with a non-zero status code (e.g., `193`) and prints an error message indicating a missing `Jobkennung`.
    2.  **BigQuery:**
        *   The `CALL` statement fails and raises an error.
        *   A new entry exists in `project.dataset.error_log` with `job_id='k_ausd_bp_ta_bpr_basis_his'`, `component='Parameter Validation'`, and `error_message` containing "Parameter p_JobKennung must be provided".
        *   A new entry exists in `project.dataset.job_audit` with `status='FAILED'` for the corresponding `run_id`.
*   **Runnable Test Code (SQL Assertions):**
    ```sql
    -- Setup: Clear logs
    TRUNCATE TABLE `project.dataset.error_log`;
    TRUNCATE TABLE `project.dataset.job_audit`;

    -- Action: Call SP with missing parameter (this will raise an error)
    BEGIN
        CALL `project.dataset.proc_k_ausd_bp_ta_bpr_basis_his`(
            p_JobKennung => NULL,
            p_EintragsNr => '1',
            p_Stichtag => '01012023',
            p_wiederanlaufWert => 0
        );
    EXCEPTION WHEN ERROR THEN
        -- Expected error, do nothing or log for test framework
        SELECT 'Procedure failed as expected' AS status;
    END;

    -- Pass/Fail: Assertions
    SELECT
        COUNT(*) = 1 AS error_log_entry_exists,
        (SELECT error_message FROM `project.dataset.error_log` WHERE component = 'Parameter Validation' AND error_message LIKE '%p_JobKennung%') IS NOT NULL AS correct_error_message,
        (SELECT status FROM `project.dataset.job_audit` ORDER BY start_timestamp DESC LIMIT 1) = 'FAILED' AS job_audit_failed
    FROM `project.dataset.error_log`
    WHERE component = 'Parameter Validation' AND error_message LIKE '%p_JobKennung%';
    ```

#### Test Case 2.2: Date Validation - Invalid `p_Stichtag` Format

*   **Purpose:** Verify that the BigQuery stored procedure correctly validates the `p_Stichtag` format (DDMMYYYY), logs an error for invalid formats, and terminates. This replaces the `DWDate_Datum_Check` functionality.
*   **Setup:**
    1.  Ensure `project.dataset.error_log` and `project.dataset.job_audit` tables are empty.
*   **Action:**
    1.  **Legacy:** Execute `k_ausd_bp_ta_bpr_basis_his.ksh` with an invalid date format:
        ```bash
        ./k_ausd_bp_ta_bpr_basis_his.ksh -j TEST_JOB -f 1 -s 2023-01-01
        ```
    2.  **BigQuery:** Call the stored procedure with an invalid `p_Stichtag` format:
        ```sql
        CALL `project.dataset.proc_k_ausd_bp_ta_bpr_basis_his`(
            p_JobKennung => 'TEST_JOB',
            p_EintragsNr => '1',
            p_Stichtag => '2023-01-01', -- Invalid format
            p_wiederanlaufWert => 0
        );
        ```
*   **Pass/Fail Criterion:**
    1.  **Legacy:** The script exits with a non-zero status code and an error message from `DWDate_Datum_Check` indicating an invalid date format.
    2.  **BigQuery:**
        *   The `CALL` statement fails and raises an error.
        *   A new entry exists in `project.dataset.error_log` with `job_id='k_ausd_bp_ta_bpr_basis_his'`, `component='Date Validation'`, and `error_message` indicating an invalid date format (e.g., "Expected DDMMYYYY").
        *   A new entry exists in `project.dataset.job_audit` with `status='FAILED'` for the corresponding `run_id`.
*   **Runnable Test Code (SQL Assertions):**
    ```sql
    -- Setup: Clear logs
    TRUNCATE TABLE `project.dataset.error_log`;
    TRUNCATE TABLE `project.dataset.job_audit`;

    -- Action: Call SP with invalid date format
    BEGIN
        CALL `project.dataset.proc_k_ausd_bp_ta_bpr_basis_his`(
            p_JobKennung => 'TEST_JOB',
            p_EintragsNr => '1',
            p_Stichtag => '2023-01-01', -- Invalid format
            p_wiederanlaufWert => 0
        );
    EXCEPTION WHEN ERROR THEN
        SELECT 'Procedure failed as expected' AS status;
    END;

    -- Pass/Fail: Assertions
    SELECT
        COUNT(*) = 1 AS error_log_entry_exists,
        (SELECT error_message FROM `project.dataset.error_log` WHERE component = 'Date Validation' AND error_message LIKE '%invalid format%') IS NOT NULL AS correct_error_message,
        (SELECT status FROM `project.dataset.job_audit` ORDER BY start_timestamp DESC LIMIT 1) = 'FAILED' AS job_audit_failed
    FROM `project.dataset.error_log`
    WHERE component = 'Date Validation' AND error_message LIKE '%invalid format%';
    ```

#### Test Case 2.3: `p_wiederanlaufWert` Default Handling

*   **Purpose:** Verify that if `p_wiederanlaufWert` is not provided (or `NULL`), the BigQuery stored procedure correctly defaults its internal value to `0`, mimicking the shell script's `if [[ -z "$p_wiederanlaufWert" ]] then p_wiederanlaufWert=0 fi` logic. This default should then be used by the embedded SQL.
*   **Setup:**
    1.  Populate `project.dataset.source_pool_basisprodukt_data` with test data, including records where `version` is `0` and `version` is greater than `0`.
    2.  Ensure `project.dataset.PoolBasisprodukt` is empty.
    3.  Ensure `project.dataset.job_audit` is empty.
*   **Action:**
    1.  **Legacy:** Execute `k_ausd_bp_ta_bpr_basis_his.ksh` without the `-l` parameter:
        ```bash
        ./k_ausd_bp_ta_bpr_basis_his.ksh -j TEST_JOB -f 1 -s 01012023
        ```
    2.  **BigQuery:** Call the stored procedure with `p_wiederanlaufWert => NULL`:
        ```sql
        CALL `project.dataset.proc_k_ausd_bp_ta_bpr_basis_his`(
            p_JobKennung => 'TEST_JOB',
            p_EintragsNr => '1',
            p_Stichtag => '01012023',
            p_wiederanlaufWert => NULL
        );
        ```
*   **Pass/Fail Criterion:**
    1.  **Legacy:** The `d_ausd_bp_ta_bpr_basis_his.sql` (if instrumented for testing) would receive `p_wiederanlaufWert=0`. The `PoolBasisprodukt` table would contain all records from the source where `src.version >= 0`.
    2.  **BigQuery:**
        *   The `job_audit` table's `input_params` JSON for the successful run should show `wiederanlaufWert: 0`.
        *   The `project.dataset.PoolBasisprodukt` table should contain all records from `project.dataset.source_pool_basisprodukt_data` that satisfy `src.version >= 0` (i.e., all records, as `0` is the minimum).
*   **Runnable Test Code (SQL Assertions):**
    ```sql
    -- Setup: Clear target and audit tables, populate source
    TRUNCATE TABLE `project.dataset.PoolBasisprodukt`;
    TRUNCATE TABLE `project.dataset.job_audit`;
    -- Example source data setup:
    -- INSERT INTO `project.dataset.source_pool_basisprodukt_data` (product_key, product_name, start_date, end_date, status, record_date, version) VALUES
    -- ('P1', 'Product A', '2023-01-01', '2023-12-31', 'ACTIVE', '20230101', 0),
    -- ('P2', 'Product B', '2023-01-01', '2023-12-31', 'ACTIVE', '20230101', 1),
    -- ('P3', 'Product C', '2023-01-01', '2023-12-31', 'ACTIVE', '20230101', 5);

    -- Action: Call SP with NULL wiederanlaufWert
    CALL `project.dataset.proc_k_ausd_bp_ta_bpr_basis_his`(
        p_JobKennung => 'TEST_JOB_DEFAULT',
        p_EintragsNr => '1',
        p_Stichtag => '01012023',
        p_wiederanlaufWert => NULL
    );

    -- Pass/Fail: Assertions
    SELECT
        (SELECT JSON_EXTRACT_SCALAR(input_params, '$.wiederanlaufWert') FROM `project.dataset.job_audit` WHERE job_id = 'k_ausd_bp_ta_bpr_basis_his' ORDER BY start_timestamp DESC LIMIT 1) = '0' AS wiederanlaufWert_default_correct,
        (SELECT COUNT(*) FROM `project.dataset.PoolBasisprodukt` WHERE processing_date = '2023-01-01') = (SELECT COUNT(*) FROM `project.dataset.source_pool_basisprodukt_data` WHERE CAST(FORMAT_DATE('%Y%m%d', record_date) AS STRING) = '01012023') AS all_records_processed_when_default_0
    ;
    ```

#### Test Case 2.4: Date Derivation Correctness (`v_heute`, `v_gestern`)

*   **Purpose:** Verify that the BigQuery stored procedure correctly derives `v_heute` and `v_gestern` using native BigQuery functions, matching the behavior of `gestern.ksh`.
*   **Setup:**
    1.  Ensure `project.dataset.job_audit` is empty.
    2.  (Optional) Modify the stored procedure temporarily to include `v_heute` and `v_gestern` in the `notes` field of `job_audit` for direct assertion.
*   **Action:**
    1.  **Legacy:** Execute `gestern.ksh` directly and capture its output.
        ```bash
        TODAY_LEGACY=$(date +%Y-%m-%d)
        YESTERDAY_LEGACY=$(date -d "yesterday" +%Y-%m-%d)
        ```
    2.  **BigQuery:** Call `proc_k_ausd_bp_ta_bpr_basis_his` with valid parameters.
        ```sql
        CALL `project.dataset.proc_k_ausd_bp_ta_bpr_basis_his`(
            p_JobKennung => 'TEST_DATE_DERIVATION',
            p_EintragsNr => '1',
            p_Stichtag => '01012023',
            p_wiederanlaufWert => 0
        );
        ```
*   **Pass/Fail Criterion:**
    *   The `v_heute` and `v_gestern` values used internally by the BigQuery procedure (and ideally logged in `job_audit` for verification) must correspond to `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` respectively, matching the output of the legacy `gestern.ksh` for the execution date.
*   **Runnable Test Code (SQL Assertions, assuming dates are logged in `notes`):**
    ```sql
    -- Setup: Clear audit table
    TRUNCATE TABLE `project.dataset.job_audit`;

    -- Action: Call SP
    CALL `project.dataset.proc_k_ausd_bp_ta_bpr_basis_his`(
        p_JobKennung => 'TEST_DATE_DERIVATION',
        p_EintragsNr => '1',
        p_Stichtag => '01012023',
        p_wiederanlaufWert => 0
    );

    -- Pass/Fail: Assertions (assuming notes contain derived dates for testing)
    -- This requires a temporary modification to the SP to log v_heute and v_gestern
    -- Example: notes = '... Heute: ' || CAST(v_heute AS STRING) || ', Gestern: ' || CAST(v_gestern AS STRING)
    SELECT
        (SELECT notes FROM `project.dataset.job_audit` WHERE job_id = 'k_ausd_bp_ta_bpr_basis_his' ORDER BY start_timestamp DESC LIMIT 1) LIKE '%Heute: ' || CAST(CURRENT_DATE() AS STRING) || '%' AS heute_correct,
        (SELECT notes FROM `project.dataset.job_audit` WHERE job_id = 'k_ausd_bp_ta_bpr_basis_his' ORDER BY start_timestamp DESC LIMIT 1) LIKE '%Gestern: ' || CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS STRING) || '%' AS gestern_correct
    ;
    ```

### 3. External-System Replacements

#### Test Case 3.1: Error Logging Mechanism (`error_log` table)

*   **Purpose:** Verify that the BigQuery `error_log` table correctly captures error details, replacing the shell script's `DWMSG_MeldeFehler` mechanism.
*   **Setup:**
    1.  Ensure `project.dataset.error_log` is empty.
*   **Action:**
    1.  Trigger an error in `proc_k_ausd_bp_ta_bpr_basis_his` (e.g., by providing an invalid `p_Stichtag`).
        ```sql
        CALL `project.dataset.proc_k_ausd_bp_ta_bpr_basis_his`(
            p_JobKennung => 'ERROR_TEST',
            p_EintragsNr => '1',
            p_Stichtag => 'INVALID_DATE',
            p_wiederanlaufWert => 0
        );
        ```
*   **Pass/Fail Criterion:**
    *   A single entry exists in `project.dataset.error_log` with:
        *   `job_id = 'k_ausd_bp_ta_bpr_basis_his'`
        *   `run_id` (a UUID)
        *   `timestamp` (close to execution time)
        *   `error_message` (describing the date parsing error)
        *   `component = 'Date Validation'`
        *   `stack_trace` (not NULL)
*   **Runnable Test Code (SQL Assertions):**
    ```sql
    -- Setup: Clear error log
    TRUNCATE TABLE `project.dataset.error_log`;

    -- Action: Call SP to trigger error
    BEGIN
        CALL `project.dataset.proc_k_ausd_bp_ta_bpr_basis_his`(
            p_JobKennung => 'ERROR_TEST',
            p_EintragsNr => '1',
            p_Stichtag => 'INVALID_DATE',
            p_wiederanlaufWert => 0
        );
    EXCEPTION WHEN ERROR THEN
        SELECT 'Procedure failed as expected' AS status;
    END;

    -- Pass/Fail: Assertions on error_log content
    SELECT
        COUNT(*) = 1 AS single_error_entry,
        MAX(job_id) = 'k_ausd_bp_ta_bpr_basis_his' AS correct_job_id,
        MAX(component) = 'Date Validation' AS correct_component,
        MAX(error_message) LIKE '%invalid format%' AS correct_error_message_content,
        MAX(stack_trace) IS NOT NULL AS stack_trace_present
    FROM `project.dataset.error_log`;
    ```

#### Test Case 3.2: Job Audit Logging Mechanism (`job_audit` table)

*   **Purpose:** Verify that the BigQuery `job_audit` table correctly captures job execution details for both successful and failed runs, replacing the commented-out `FOSJobErzeugeEintrag` functionality.
*   **Setup:**
    1.  Ensure `project.dataset.job_audit` is empty.
    2.  Populate `project.dataset.source_pool_basisprodukt_data` with some test data.
*   **Action:**
    1.  **Successful Run:** Call `proc_k_ausd_bp_ta_bpr_basis_his` with valid parameters.
        ```sql
        CALL `project.dataset.proc_k_ausd_bp_ta_bpr_basis_his`(
            p_JobKennung => 'AUDIT_SUCCESS',
            p_EintragsNr => '1',
            p_Stichtag => '01012023',
            p_wiederanlaufWert => 0
        );
        ```
    2.  **Failed Run:** Call `proc_k_ausd_bp_ta_bpr_basis_his` with invalid parameters (e.g., invalid `p_Stichtag`).
        ```sql
        BEGIN
            CALL `project.dataset.proc_k_ausd_bp_ta_bpr_basis_his`(
                p_JobKennung => 'AUDIT_FAIL',
                p_EintragsNr => '1',
                p_Stichtag => 'BAD_DATE',
                p_wiederanlaufWert => 0
            );
        EXCEPTION WHEN ERROR THEN
            SELECT 'Expected failure for audit test' AS status;
        END;
        ```
*   **Pass/Fail Criterion:**
    *   Two entries exist in `project.dataset.job_audit`:
        *   One for `job_id='AUDIT_SUCCESS'` with `status='SUCCESS'`, `records_processed` > 0, and `input_params` containing the correct JSON.
        *   One for `job_id='AUDIT_FAIL'` with `status='FAILED'`, `records_processed=0` (or NULL), and `input_params` containing the correct JSON.
        *   `start_timestamp` and `end_timestamp` are populated correctly for both.
*   **Runnable Test Code (SQL Assertions):**
    ```sql
    -- Setup: Clear audit table
    TRUNCATE TABLE `project.dataset.job_audit`;
    TRUNCATE TABLE `project.dataset.error_log`; -- Clear error log too, as failed run will log there

    -- Action 1: Successful Run
    CALL `project.dataset.proc_k_ausd_bp_ta_bpr_basis_his`(
        p_JobKennung => 'AUDIT_SUCCESS',
        p_EintragsNr => '1',
        p_Stichtag => '01012023',
        p_wiederanlaufWert => 0
    );

    -- Action 2: Failed Run
    BEGIN
        CALL `project.dataset.proc_k_ausd_bp_ta_bpr_basis_his`(
            p_JobKennung => 'AUDIT_FAIL',
            p_EintragsNr => '1',
            p_Stichtag => 'BAD_DATE',
            p_wiederanlaufWert => 0
        );
    EXCEPTION WHEN ERROR THEN
        SELECT 'Expected failure for audit test' AS status;
    END;

    -- Pass/Fail: Assertions
    SELECT
        (SELECT COUNT(*) FROM `project.dataset.job_audit` WHERE job_id = 'AUDIT_SUCCESS' AND status = 'SUCCESS' AND records_processed > 0) = 1 AS success_audit_entry_correct,
        (SELECT COUNT(*) FROM `project.dataset.job_audit` WHERE job_id = 'AUDIT_FAIL' AND status = 'FAILED') = 1 AS failed_audit_entry_correct,
        (SELECT JSON_EXTRACT_SCALAR(input_params, '$.p_JobKennung') FROM `project.dataset.job_audit` WHERE job_id = 'AUDIT_SUCCESS') = 'AUDIT_SUCCESS' AS success_params_correct,
        (SELECT JSON_EXTRACT_SCALAR(input_params, '$.p_JobKennung') FROM `project.dataset.job_audit` WHERE job_id = 'AUDIT_FAIL') = 'AUDIT_FAIL' AS failed_params_correct
    ;
    ```

#### Test Case 3.3: Optional Post-Processing to Cloud Storage (if activated)

*   **Purpose:** If the commented-out file post-processing logic in the legacy script is activated and migrated to `proc_k_ausd_bp_ta_bpr_basis_his_postprocess`, verify that this procedure correctly processes data and exports it to Cloud Storage as CSV files.
*   **Setup:**
    1.  Populate `project.dataset.PoolBasisprodukt` and any other necessary BigQuery tables (e.g., `some_other_table` from the placeholder) with test data.
    2.  Ensure the specified GCS bucket and path exist and are writable.
    3.  Define the expected content of the output CSV file(s) based on the legacy `sed`, `sort`, `join` logic.
*   **Action:**
    1.  Call the optional post-processing procedure:
        ```sql
        CALL `project.dataset.proc_k_ausd_bp_ta_bpr_basis_his_postprocess`(
            p_processing_date => '2023-01-01',
            p_output_gcs_path => 'gs://your-gcs-bucket/path/to/exports/'
        );
        ```
    2.  Download the generated CSV file(s) from Cloud Storage.
*   **Pass/Fail Criterion:**
    *   The CSV file(s) exist at the specified GCS path.
    *   The content of the downloaded CSV file(s) matches the expected output, including headers, delimiters, and data order (if `sort` was used).
*   **Runnable Test Code (Conceptual Pytest with GCS client):**
    ```python
    import pytest
    from google.cloud import storage
    import pandas as pd

    # Assume bigquery_client and gcs_client are pytest fixtures

    def test_gcs_export_postprocessing(bigquery_client, gcs_client):
        # --- Setup ---
        # 1. Populate BigQuery tables with test data
        #    Example: bigquery_client.query("INSERT INTO `project.dataset.PoolBasisprodukt` ...")
        # 2. Define expected output path and content
        output_gcs_path = "gs://your-test-bucket/exports/k_ausd_bp_ta_bpr_basis_his/"
        expected_csv_content = pd.DataFrame({
            'product_key': ['P1', 'P2'],
            'product_name': ['Product A', 'Product B'],
            'additional_info': ['Info A', 'Info B']
        }) # This DataFrame should reflect the expected output after joins/sorts

        # --- Action ---
        # Call the post-processing SP
        sp_call_query = f"""
        CALL `project.dataset.proc_k_ausd_bp_ta_bpr_basis_his_postprocess`(
            p_processing_date => '2023-01-01',
            p_output_gcs_path => '{output_gcs_path}'
        );
        """
        bigquery_client.query(sp_call_query).result()

        # Download the exported CSV from GCS
        bucket_name = output_gcs_path.split('/')[2]
        blob_prefix = '/'.join(output_gcs_path.split('/')[3:]) + 'cibasisprodukt_' # Assuming wildcard output
        bucket = gcs_client.get_bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=blob_prefix))
        assert len(blobs) > 0, "No CSV file exported to GCS."

        # Read the first exported file
        blob = blobs[0]
        downloaded_content = blob.download_as_text()
        actual_df = pd.read_csv(pd.io.common.StringIO(downloaded_content))

        # --- Pass/Fail Criterion ---
        pd.testing.assert_frame_equal(
            actual_df.sort_values(by=list(actual_df.columns)).reset_index(drop=True),
            expected_csv_content.sort_values(by=list(expected_csv_content.columns)).reset_index(drop=True),
            check_dtype=True
        )
    ```

### 4. Data Quality / Row Count / Schema Assertions

#### Test Case 4.1: `error_log` Table Schema and Partitioning

*   **Purpose:** Verify that the `project.dataset.error_log` table exists with the correct schema, data types, partitioning, and clustering as defined in `error_log.ddl.sql`.
*   **Setup:** None. The table should be created as part of the migration.
*   **Action:** Query BigQuery's `INFORMATION_SCHEMA` for the table details.
*   **Pass/Fail Criterion:**
    *   Table `project.dataset.error_log` exists.
    *   Columns match: `job_id` (STRING), `run_id` (STRING), `timestamp` (TIMESTAMP), `error_message` (STRING), `error_code` (STRING), `component` (STRING), `stack_trace` (STRING).
    *   `timestamp` column is used for `PARTITION BY DATE(timestamp)`.
    *   `job_id` column is used for `CLUSTER BY job_id`.
*   **Runnable Test Code (SQL Assertions):**
    ```sql
    SELECT
        table_name = 'error_log' AS table_exists,
        (SELECT COUNT(*) FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'error_log' AND column_name = 'job_id' AND data_type = 'STRING') = 1 AS job_id_col_correct,
        (SELECT COUNT(*) FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'error_log' AND column_name = 'timestamp' AND data_type = 'TIMESTAMP') = 1 AS timestamp_col_correct,
        (SELECT COUNT(*) FROM `project.dataset.INFORMATION_SCHEMA.PARTITIONS` WHERE table_name = 'error_log' AND partition_id IS NOT NULL) > 0 AS is_partitioned, -- Checks if any partitions exist
        (SELECT partitioning_column FROM `project.dataset.INFORMATION_SCHEMA.TABLE_OPTIONS` WHERE table_name = 'error_log' AND option_name = 'partitioning_column') = 'timestamp' AS correct_partition_column,
        (SELECT ARRAY_TO_STRING(clustering_columns, ',') FROM `project.dataset.INFORMATION_SCHEMA.TABLE_OPTIONS` WHERE table_name = 'error_log' AND option_name = 'clustering_columns') = 'job_id' AS correct_cluster_column
    FROM `project.dataset.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = 'error_log';
    ```

#### Test Case 4.2: `job_audit` Table Schema and Partitioning

*   **Purpose:** Verify that the `project.dataset.job_audit` table exists with the correct schema, data types, partitioning, and clustering as defined in `job_audit.ddl.sql`.
*   **Setup:** None. The table should be created as part of the migration.
*   **Action:** Query BigQuery's `INFORMATION_SCHEMA` for the table details.
*   **Pass/Fail Criterion:**
    *   Table `project.dataset.job_audit` exists.
    *   Columns match: `job_id` (STRING), `run_id` (STRING), `start_timestamp` (TIMESTAMP), `end_timestamp` (TIMESTAMP), `status` (STRING), `input_params` (JSON), `records_processed` (INT64), `notes` (STRING).
    *   `start_timestamp` column is used for `PARTITION BY DATE(start_timestamp)`.
    *   `job_id` column is used for `CLUSTER BY job_id`.
*   **Runnable Test Code (SQL Assertions):**
    ```sql
    SELECT
        table_name = 'job_audit' AS table_exists,
        (SELECT COUNT(*) FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'job_audit' AND column_name = 'start_timestamp' AND data_type = 'TIMESTAMP') = 1 AS start_timestamp_col_correct,
        (SELECT COUNT(*) FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'job_audit' AND column_name = 'input_params' AND data_type = 'JSON') = 1 AS input_params_col_correct,
        (SELECT COUNT(*) FROM `project.dataset.INFORMATION_SCHEMA.PARTITIONS` WHERE table_name = 'job_audit' AND partition_id IS NOT NULL) > 0 AS is_partitioned,
        (SELECT partitioning_column FROM `project.dataset.INFORMATION_SCHEMA.TABLE_OPTIONS` WHERE table_name = 'job_audit' AND option_name = 'partitioning_column') = 'start_timestamp' AS correct_partition_column,
        (SELECT ARRAY_TO_STRING(clustering_columns, ',') FROM `project.dataset.INFORMATION_SCHEMA.TABLE_OPTIONS` WHERE table_name = 'job_audit' AND option_name = 'clustering_columns') = 'job_id' AS correct_cluster_column
    FROM `project.dataset.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = 'job_audit';
    ```

#### Test Case 4.3: `PoolBasisprodukt` Target Table Schema Compatibility

*   **Purpose:** Verify that the schema of the target `project.dataset.PoolBasisprodukt` table is compatible with the `MERGE` statement (or equivalent DML) embedded within `proc_k_ausd_bp_ta_bpr_basis_his`. This is a design-time check given the placeholder nature of the SQL.
*   **Setup:** The `project.dataset.PoolBasisprodukt` table and `project.dataset.source_pool_basisprodukt_data` table should exist with their defined schemas.
*   **Action:** Manually review the `MERGE` statement in `k_ausd_bp_ta_bpr_basis_his.bq.sql` (or the actual embedded SQL) and compare the columns and data types being inserted/updated against the actual schema of `project.dataset.PoolBasisprodukt`.
*   **Pass/Fail Criterion:** All columns referenced in the `INSERT` and `UPDATE` clauses of the embedded SQL must exist in `project.dataset.PoolBasisprodukt` with compatible data types.

#### Test Case 4.4: Record Count in `PoolBasisprodukt` (Post-Execution)

*   **Purpose:** Verify that the number of records in the `project.dataset.PoolBasisprodukt` table after a successful execution of the BigQuery stored procedure matches the expected count based on the source data and the (assumed correct) transformation logic.
*   **Setup:**
    1.  Populate `project.dataset.source_pool_basisprodukt_data` with a known, controlled number of records that would be processed by the SQL logic.
    2.  Ensure `project.dataset.PoolBasisprodukt` is empty.
*   **Action:**
    1.  Call `proc_k_ausd_bp_ta_bpr_basis_his` with valid parameters.
    2.  Query the record count from `project.dataset.PoolBasisprodukt`.
*   **Pass/Fail Criterion:** The `COUNT(*)` from `project.dataset.PoolBasisprodukt` equals the expected number of records that should have been processed and loaded/updated by the embedded SQL. This count should also match the `records_processed` value in the `job_audit` table for the corresponding run.
*   **Runnable Test Code (SQL Assertions):**
    ```sql
    -- Setup: Clear target table, populate source with known data
    TRUNCATE TABLE `project.dataset.PoolBasisprodukt`;
    TRUNCATE TABLE `project.dataset.job_audit`;
    -- Example: Insert 5 records into source_pool_basisprodukt_data for '01012023'
    -- (Assuming the MERGE logic processes all 5 for this Stichtag)

    -- Action: Call SP
    CALL `project.dataset.proc_k_ausd_bp_ta_bpr_basis_his`(
        p_JobKennung => 'RECORD_COUNT_TEST',
        p_EintragsNr => '1',
        p_Stichtag => '01012023',
        p_wiederanlaufWert => 0
    );

    -- Pass/Fail: Assertions
    SELECT
        (SELECT COUNT(*) FROM `project.dataset.PoolBasisprodukt` WHERE processing_date = '2023-01-01') = 5 AS target_table_count_correct,
        (SELECT records_processed FROM `project.dataset.job_audit` WHERE job_id = 'k_ausd_bp_ta_bpr_basis_his' ORDER BY start_timestamp DESC LIMIT 1) = 5 AS audit_record_count_correct
    ;
    ```