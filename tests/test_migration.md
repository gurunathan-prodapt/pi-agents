As a senior data-migration QA engineer, I've reviewed the migration design for `k_ausd_v_ta_notice.ksh` to Google Cloud BigQuery. The migration involves transforming a KornShell orchestration script and its invoked Oracle SQL into a BigQuery Stored Procedure, along with dedicated BigQuery tables for job control and error logging.

Below are the migration validation tests designed to ensure behavioral equivalence, data integrity, and correct functionality of the migrated components.

---

## Migration Validation Tests for `k_ausd_v_ta_notice.ksh` to BigQuery

### Test Case 1: Successful Execution - Output Parity (Happy Path)

*   **Purpose:** Verify that with valid inputs and a representative dataset, the migrated BigQuery Stored Procedure produces the exact same output data in `mydataset.SOF_TA_NOTICE` and `mydataset.VIA` as the legacy KornShell script executing the Oracle SQL. This is the primary test for output parity and transformation correctness.
*   **Setup:**
    1.  **Legacy Environment:**
        *   Populate Oracle `DWTK_MELDUNGEN` and `CDS_TA_NOTICE` with a diverse, representative dataset (e.g., 100-1000 rows, covering various data types, some NULLs, some edge values, and data that specifically triggers the logic in `d_ausd_v_ta_notice.sql`).
        *   Ensure Oracle `SOF$TA_NOTICE` and `VIA` are empty.
        *   Record the initial state of any legacy job control or error logs.
    2.  **Migrated Environment:**
        *   Populate BigQuery `mydataset.DWTK_MELDUNGEN` and `mydataset.CDS_TA_NOTICE` with the *exact same data* as their Oracle counterparts. Ensure data types are correctly mapped from Oracle to BigQuery.
        *   Ensure BigQuery `mydataset.SOF_TA_NOTICE` and `mydataset.VIA` are empty.
        *   Ensure `mydataset.job_table` and `mydataset.error_log` are empty or in a known clean state.
        *   Deploy the BigQuery Stored Procedure `mydataset.r_ausd_vertrag_control` and `mydataset.log_error`, along with any necessary UDFs (e.g., `mydataset.dwpa_util_skript_get_date_formatted`).
*   **Action:**
    1.  **Legacy:** Execute the legacy `k_ausd_v_ta_notice.ksh` script with valid `JobKennung` and `EintragsNr` parameters.
        ```bash
        ./k_ausd_v_ta_notice.ksh -j "TEST_JOB_HAPPY_PATH" -f "20231026"
        ```
    2.  **Migrated:** Execute the BigQuery Stored Procedure `mydataset.r_ausd_vertrag_control` with the *same* `JobKennung` and `EintragsNr`. This can be done directly in BigQuery or via the Cloud Composer DAG.
        ```sql
        CALL `mydataset.r_ausd_vertrag_control`('TEST_JOB_HAPPY_PATH', '20231026');
        ```
    3.  After both executions, export the data from Oracle `SOF$TA_NOTICE` and `VIA` into temporary BigQuery tables (e.g., `temp_legacy_sof_ta_notice`, `temp_legacy_via`) for comparison.
*   **Pass/Fail Criterion:**
    1.  **Output Data Parity:**
        *   The data in BigQuery `mydataset.SOF_TA_NOTICE` must be *identical* to the data in `temp_legacy_sof_ta_notice` (row count, column values, data types).
        *   The data in BigQuery `mydataset.VIA` must be *identical* to the data in `temp_legacy_via` (row count, column values, data types).
        *   *Note:* "Identical" implies semantic equivalence. Minor differences in floating-point precision or timestamp representation (e.g., timezone handling) should be evaluated for business impact.
    2.  **Record Count Parity:** The `record_count` stored in `mydataset.job_table` for `TEST_JOB_HAPPY_PATH` must match the record count reported by the legacy script (e.g., from its logs or by querying the Oracle target tables).
    3.  **Job Status:** The `job_table` entry for `TEST_JOB_HAPPY_PATH` in BigQuery must have `status = 'COMPLETED'`.
    4.  **Error Log:** `mydataset.error_log` must contain no entries for this job run.

    ```sql
    -- Example SQL assertions for SOF_TA_NOTICE (assuming 'id' is a unique key)
    -- Run this after exporting legacy data to `temp_legacy_sof_ta_notice`
    SELECT
        'SOF_TA_NOTICE Row Count Check' AS Test,
        (SELECT COUNT(*) FROM `mydataset.SOF_TA_NOTICE`) AS BQ_Count,
        (SELECT COUNT(*) FROM `mydataset.temp_legacy_sof_ta_notice`) AS Legacy_Count,
        CASE
            WHEN (SELECT COUNT(*) FROM `mydataset.SOF_TA_NOTICE`) = (SELECT COUNT(*) FROM `mydataset.temp_legacy_sof_ta_notice`) THEN 'PASS'
            ELSE 'FAIL'
        END AS Status;

    SELECT
        'SOF_TA_NOTICE Missing in BQ' AS Test,
        COUNT(*) AS MissingRows
    FROM `mydataset.temp_legacy_sof_ta_notice` AS T1
    LEFT JOIN `mydataset.SOF_TA_NOTICE` AS T2 ON T1.id = T2.id
    WHERE T2.id IS NULL;

    SELECT
        'SOF_TA_NOTICE Extra in BQ' AS Test,
        COUNT(*) AS ExtraRows
    FROM `mydataset.SOF_TA_NOTICE` AS T1
    LEFT JOIN `mydataset.temp_legacy_sof_ta_notice` AS T2 ON T1.id = T2.id
    WHERE T2.id IS NULL;

    -- Detailed column comparison (adjust column names and types as per actual schema)
    SELECT
        'SOF_TA_NOTICE Data Differences' AS Test,
        T1.id,
        T1.job_kennung AS Legacy_JobKennung, T2.job_kennung AS BQ_JobKennung,
        T1.eintrags_nr AS Legacy_EintragsNr, T2.eintrags_nr AS BQ_EintragsNr,
        T1.data AS Legacy_Data, T2.data AS BQ_Data,
        -- Add more columns as needed
        CASE
            WHEN (T1.job_kennung = T2.job_kennung OR (T1.job_kennung IS NULL AND T2.job_kennung IS NULL))
             AND (T1.eintrags_nr = T2.eintrags_nr OR (T1.eintrags_nr IS NULL AND T2.eintrags_nr IS NULL))
             AND (T1.data = T2.data OR (T1.data IS NULL AND T2.data IS NULL))
            THEN 'MATCH' ELSE 'MISMATCH'
        END AS Row_Comparison_Status
    FROM `mydataset.temp_legacy_sof_ta_notice` AS T1
    JOIN `mydataset.SOF_TA_NOTICE` AS T2 ON T1.id = T2.id
    WHERE NOT (
        (T1.job_kennung = T2.job_kennung OR (T1.job_kennung IS NULL AND T2.job_kennung IS NULL))
        AND (T1.eintrags_nr = T2.eintrags_nr OR (T1.eintrags_nr IS NULL AND T2.eintrags_nr IS NULL))
        AND (T1.data = T2.data OR (T1.data IS NULL AND T2.data IS NULL))
        -- Add all other column comparisons here
    );
    -- Repeat similar checks for `VIA` table.
    ```

### Test Case 2: Parameter Validation - Missing JobKennung

*   **Purpose:** Verify that the BigQuery Stored Procedure correctly handles missing or empty `p_JobKennung` parameters, mirroring the legacy script's error handling and exit behavior.
*   **Setup:**
    1.  **Legacy Environment:** Ensure no active jobs are running and error logs are clean.
    2.  **Migrated Environment:** Ensure `mydataset.job_table` and `mydataset.error_log` are clean.
*   **Action:**
    1.  **Legacy:** Execute `k_ausd_v_ta_notice.ksh` without the `-j` parameter.
        ```bash
        ./k_ausd_v_ta_notice.ksh -f "20231026"
        ```
    2.  **Migrated:** Execute `mydataset.r_ausd_vertrag_control` with `p_JobKennung` as `NULL`.
        ```sql
        CALL `mydataset.r_ausd_vertrag_control`(NULL, '20231026');
        -- Also test with an empty string:
        -- CALL `mydataset.r_ausd_vertrag_control`('', '20231026');
        ```
*   **Pass/Fail Criterion:**
    1.  **Legacy:** The script must exit with a non-zero status code (e.g., `ErrNr=193`) and print an error message to `stderr` (e.g., "FEHLER: 0 E 193 Jobkennung").
    2.  **Migrated:**
        *   The BigQuery Stored Procedure execution must fail and raise an error with a message similar to "Parameter p_JobKennung cannot be null or empty."
        *   An entry must be present in `mydataset.error_log` with `severity = 'ERROR'`, `error_message` indicating the missing `JobKennung`, and `job_kennung` as `NULL` or empty.
        *   No new entries should be created in `mydataset.SOF_TA_NOTICE` or `mydataset.VIA`.
        *   No `ACTIVE` entry should be present in `mydataset.job_table` for this failed attempt. If an entry is created, its `status` should be 'FAILED'.

### Test Case 3: Parameter Validation - Missing EintragsNr

*   **Purpose:** Verify that the BigQuery Stored Procedure correctly handles missing or empty `p_EintragsNr` parameters, mirroring the legacy script's error handling.
*   **Setup:** Same as Test Case 2.
*   **Action:**
    1.  **Legacy:** Execute `k_ausd_v_ta_notice.ksh` without the `-f` parameter.
        ```bash
        ./k_ausd_v_ta_notice.ksh -j "TEST_JOB_MISSING_F"
        ```
    2.  **Migrated:** Execute `mydataset.r_ausd_vertrag_control` with `p_EintragsNr` as `NULL`.
        ```sql
        CALL `mydataset.r_ausd_vertrag_control`('TEST_JOB_MISSING_F', NULL);
        -- Also test with an empty string:
        -- CALL `mydataset.r_ausd_vertrag_control`('TEST_JOB_MISSING_F', '');
        ```
*   **Pass/Fail Criterion:**
    1.  **Legacy:** The script must exit with a non-zero status code (e.g., `ErrNr=193`) and print an error message (e.g., "FEHLER: 0 E 193 EintragsNr").
    2.  **Migrated:**
        *   The BigQuery Stored Procedure execution must fail and raise an error with a message similar to "Parameter p_EintragsNr cannot be null or empty."
        *   An entry must be present in `mydataset.error_log` with `severity = 'ERROR'`, `error_message` indicating the missing `EintragsNr`, and `eintrags_nr` as `NULL` or empty.
        *   No new entries should be created in `mydataset.SOF_TA_NOTICE` or `mydataset.VIA`.
        *   No `ACTIVE` entry should be present in `mydataset.job_table` for this failed attempt. If an entry is created, its `status` should be 'FAILED'.

### Test Case 4: Job State Management - Deactivation of Old Active Jobs

*   **Purpose:** Verify that the BigQuery Stored Procedure correctly deactivates previously active jobs for the same `table_name` (`ta_notice`) when a new job starts, as specified in the legacy script's purpose and the migration design.
*   **Setup:**
    1.  **Migrated Environment:**
        *   Insert records into `mydataset.job_table` representing 'ACTIVE' jobs:
            *   One for `table_name = 'ta_notice'` (this should be deactivated).
            *   One for a *different* `table_name` (this should remain active).
            *   One for `table_name = 'ta_notice'` but already `COMPLETED` (this should remain completed).
        *   Ensure `mydataset.SOF_TA_NOTICE`, `mydataset.VIA`, and `mydataset.error_log` are clean.
        ```sql
        INSERT INTO `mydataset.job_table` (job_id, job_kennung, eintrags_nr, table_name, status, start_time, message)
        VALUES
            ('old_job_id_1', 'OLD_JOB_A', '20231025', 'ta_notice', 'ACTIVE', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), 'Old job running for ta_notice.'),
            ('old_job_id_2', 'OLD_JOB_B', '20231024', 'other_table', 'ACTIVE', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR), 'Another old job running for other_table.'),
            ('old_job_id_3', 'OLD_JOB_C', '20231023', 'ta_notice', 'COMPLETED', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR), 'Old job completed for ta_notice.');
        ```
*   **Action:**
    1.  Execute the BigQuery Stored Procedure `mydataset.r_ausd_vertrag_control` with valid parameters.
        ```sql
        CALL `mydataset.r_ausd_vertrag_control`('NEW_JOB_D', '20231027');
        ```
*   **Pass/Fail Criterion:**
    1.  Query `mydataset.job_table` after the new job completes.
    2.  The `job_table` entry for `old_job_id_1` must be updated to `status = 'DEACTIVATED'`, `end_time` populated, and `message = 'Deactivated by a new job run.'`.
    3.  The `job_table` entry for `old_job_id_2` (for `other_table`) must remain `status = 'ACTIVE'`.
    4.  The `job_table` entry for `old_job_id_3` must remain `status = 'COMPLETED'`.
    5.  A new entry for `NEW_JOB_D` must be present with `status = 'COMPLETED'` (assuming successful data processing).
    6.  No errors in `mydataset.error_log`.

### Test Case 5: Data Transformation - Edge Cases (NULLs, Empty Sources)

*   **Purpose:** Verify the robustness and correctness of the BigQuery SQL transformation logic when source tables contain NULL values, empty strings, or are entirely empty. This covers transformation correctness and NULL handling.
*   **Setup:**
    1.  **Legacy Environment:**
        *   Populate Oracle `DWTK_MELDUNGEN` and `CDS_TA_NOTICE` with specific datasets for each scenario:
            *   **Scenario A (All NULLs):** All nullable columns contain `NULL`.
            *   **Scenario B (Empty Strings):** All string columns contain empty strings (`''`).
            *   **Scenario C (Empty Sources):** Both `DWTK_MELDUNGEN` and `CDS_TA_NOTICE` are completely empty.
            *   **Scenario D (One Empty Source):** One source table is empty, the other has data.
        *   Ensure target tables are empty for each scenario.
    2.  **Migrated Environment:**
        *   Populate BigQuery `mydataset.DWTK_MELDUNGEN` and `mydataset.CDS_TA_NOTICE` with the *exact same data* as Oracle for each scenario.
        *   Ensure target tables are empty for each scenario.
*   **Action:**
    1.  **Legacy:** Execute `k_ausd_v_ta_notice.ksh` for each scenario with unique `JobKennung`/`EintragsNr`.
    2.  **Migrated:** Execute `mydataset.r_ausd_vertrag_control` for each scenario with corresponding unique `JobKennung`/`EintragsNr`.
    3.  Export legacy results to temporary BigQuery tables for comparison.
*   **Pass/Fail Criterion:**
    1.  **Output Parity:** For each scenario, the data in BigQuery `mydataset.SOF_TA_NOTICE` and `mydataset.VIA` must be *identical* to the data in their Oracle counterparts (including zero rows if sources were empty).
    2.  **Record Count Parity:** The `record_count` in `mydataset.job_table` must match the legacy output for each scenario.
    3.  **Error Handling:** No unexpected errors should occur in either system. If the legacy system produces specific warnings or errors for these edge cases, the migrated system should replicate that behavior (e.g., log to `error_log`).

### Test Case 6: Data Transformation - Specific Function Mapping (DWPA_UTIL_SKRIPT / Date Handling)

*   **Purpose:** Verify that functions from the legacy `PACKAGE:DWPA_UTIL_SKRIPT` (or other utility scripts like `h_alis_date.ksh`) are correctly translated and behave identically in BigQuery (e.g., as UDFs or inlined logic). This covers transformation correctness for specific function calls.
*   **Setup:**
    1.  **Identify specific functions:** Review the actual `d_ausd_v_ta_notice.sql` for calls to `DWPA_UTIL_SKRIPT` functions or complex date manipulations from `h_alis_date.ksh`. For this test, we'll use `mydataset.dwpa_util_skript_get_date_formatted` as an example.
    2.  **Create test data:** Prepare source data in `DWTK_MELDUNGEN` and `CDS_TA_NOTICE` that specifically exercises these functions with various inputs (e.g., different dates, NULL dates, different format strings, boundary dates like year start/end).
*   **Action:**
    1.  **Legacy:** Execute `k_ausd_v_ta_notice.ksh` with the prepared test data.
    2.  **Migrated:** Execute `mydataset.r_ausd_vertrag_control` with the same prepared test data.
    3.  Export legacy results to temporary BigQuery tables for comparison.
*   **Pass/Fail Criterion:**
    1.  The output data in `SOF_TA_NOTICE` and `VIA` (specifically the columns affected by these functions) must be *identical* between legacy and migrated systems.
    2.  If possible, create isolated unit tests for the BigQuery UDFs/logic that mimic the Oracle function's behavior directly.
    ```sql
    -- Example assertion for dwpa_util_skript_get_date_formatted
    SELECT
        CASE WHEN `mydataset.dwpa_util_skript_get_date_formatted`(DATE '2023-10-26', '%Y-%m-%d') = '2023-10-26' THEN 'PASS' ELSE 'FAIL' END AS test_case_1_format,
        CASE WHEN `mydataset.dwpa_util_skript_get_date_formatted`(NULL, '%Y-%m-%d') IS NULL THEN 'PASS' ELSE 'FAIL' END AS test_case_2_null_input,
        CASE WHEN `mydataset.dwpa_util_skript_get_date_formatted`(DATE '2023-01-01', '%d.%m.%Y') = '01.01.2023' THEN 'PASS' ELSE 'FAIL' END AS test_case_3_custom_format,
        CASE WHEN `mydataset.dwpa_util_skript_get_date_formatted`(DATE '1900-01-01', '%Y') = '1900' THEN 'PASS' ELSE 'FAIL' END AS test_case_4_boundary_date;
    ```

### Test Case 7: Error Handling - SQL Execution Failure

*   **Purpose:** Verify that if the core BigQuery SQL logic fails (e.g., due to data type mismatch, constraint violation, or a deliberately introduced error), the Stored Procedure correctly logs the error to `mydataset.error_log` and marks the job as 'FAILED' in `mydataset.job_table`. This covers error handling.
*   **Setup:**
    1.  **Migrated Environment:**
        *   Ensure `mydataset.job_table` and `mydataset.error_log` are clean.
        *   **Introduce a controlled error:** Temporarily modify the BigQuery SQL logic within `r_ausd_vertrag_control` (or the underlying SQL it calls) to deliberately cause a failure. For example, attempt to insert a string into an `INT64` column, or introduce a `SELECT 1 / 0;` statement.
*   **Action:**
    1.  Execute `mydataset.r_ausd_vertrag_control` with valid parameters.
        ```sql
        CALL `mydataset.r_ausd_vertrag_control`('FAIL_JOB_SQL_ERROR', '20231028');
        ```
*   **Pass/Fail Criterion:**
    1.  The BigQuery Stored Procedure execution must fail and raise an error.
    2.  An entry must be present in `mydataset.error_log` with:
        *   `job_id` corresponding to the failed run.
        *   `job_kennung = 'FAIL_JOB_SQL_ERROR'`.
        *   `severity = 'FATAL'`.
        *   `error_message` clearly describing the SQL error (e.g., "Division by zero" or "Cannot convert string to INT64").
        *   `stack_trace` should be populated.
    3.  The `job_table` entry for `FAIL_JOB_SQL_ERROR` must have `status = 'FAILED'`, `end_time` populated, and `message = 'Job failed.'`.
    4.  No data should be committed to `mydataset.SOF_TA_NOTICE` or `mydataset.VIA` from this failed run (transactional integrity).

### Test Case 8: Idempotency and Concurrent Runs (Job State Management)

*   **Purpose:** Verify that running the job multiple times, especially in quick succession, correctly manages job states in `mydataset.job_table` and prevents unintended side effects, specifically regarding the deactivation logic. This covers job state management and robustness.
*   **Setup:**
    1.  **Migrated Environment:** Ensure `mydataset.job_table`, `mydataset.SOF_TA_NOTICE`, `mydataset.VIA`, and `mydataset.error_log` are clean.
    2.  Populate source tables with a small, consistent dataset.
*   **Action:**
    1.  Execute `mydataset.r_ausd_vertrag_control` with `JobKennung = 'IDEMPOTENCY_TEST'` and `EintragsNr = '20231029_1'`.
    2.  Immediately (or shortly after) execute it again with `JobKennung = 'IDEMPOTENCY_TEST_2'` and `EintragsNr = '20231029_2'`.
    3.  Execute it a third time with `JobKennung = 'IDEMPOTENCY_TEST_3'` and `EintragsNr = '20231029_3'`.
*   **Pass/Fail Criterion:**
    1.  After all runs complete, query `mydataset.job_table`.
        *   The first job (`IDEMPOTENCY_TEST`) should have `status = 'DEACTIVATED'`.
        *   The second job (`IDEMPOTENCY_TEST_2`) should have `status = 'DEACTIVATED'`.
        *   The third job (`IDEMPOTENCY_TEST_3`) should have `status = 'COMPLETED'`.
        *   All `DEACTIVATED` entries should have `end_time` and `message = 'Deactivated by a new job run.'`.
    2.  The final state of `mydataset.SOF_TA_NOTICE` and `mydataset.VIA` should reflect only the data processed by the *last successfully completed* job (`IDEMPOTENCY_TEST_3`), assuming the SQL logic is designed to replace or update data for a given `JobKennung`/`EintragsNr` combination or `table_name`. If the SQL logic appends, then all three runs should have contributed, but the job status management should still be correct.
    3.  No unexpected errors in `mydataset.error_log`.

### Test Case 9: Data Quality - Schema and Data Type Validation

*   **Purpose:** Verify that the BigQuery target tables (`mydataset.SOF_TA_NOTICE`, `mydataset.VIA`) have the correct schema (column names, data types, nullability) and that data is correctly mapped to these types without loss, truncation, or corruption. This covers schema assertions and data quality.
*   **Setup:**
    1.  **Legacy Environment:** Obtain the exact schema (column names, Oracle data types, nullability, precision/scale) of Oracle `SOF$TA_NOTICE` and `VIA`.
    2.  **Migrated Environment:** Ensure the BigQuery DDLs for `mydataset.SOF_TA_NOTICE` and `mydataset.VIA` are deployed.
    3.  Populate source tables with a diverse dataset covering all data types and potential edge values (e.g., maximum length strings, largest/smallest numbers, various date formats).
*   **Action:**
    1.  Execute the migration job (legacy and migrated) with the diverse dataset.
    2.  **Schema Comparison:** Compare the BigQuery table schemas (using `INFORMATION_SCHEMA.COLUMNS`) with the documented Oracle schemas.
    3.  **Data Type Validation:** Query data from BigQuery target tables and verify that values are stored in the expected BigQuery data types and retain their original precision/scale.
*   **Pass/Fail Criterion:**
    1.  The BigQuery target table schemas must match the Oracle schemas, with appropriate BigQuery type conversions (e.g., `NUMBER` to `INT64`/`BIGNUMERIC`/`FLOAT64`, `VARCHAR2` to `STRING`, `DATE`/`TIMESTAMP` to `DATE`/`TIMESTAMP`).
    2.  No data truncation, unexpected type coercion, or data corruption should be observed in the BigQuery target tables.
    3.  All columns defined as `NOT NULL` in Oracle should be `NOT NULL` in BigQuery, and the job should fail if a `NULL` is attempted to be inserted into such a column.
    ```sql
    -- Example SQL for schema validation (run after DDL deployment)
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM `mydataset.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'SOF_TA_NOTICE'
    ORDER BY ordinal_position;
    -- Compare this output with the documented Oracle schema.
    ```

### Test Case 10: External System Replacement - Composer Orchestration

*   **Purpose:** Verify that the Cloud Composer DAG correctly triggers the BigQuery Stored Procedure and passes parameters as intended, effectively replacing the upstream shell script (`r_ausd_vertrag.ksh`) that previously invoked `k_ausd_v_ta_notice.ksh`. This covers external system replacements.
*   **Setup:**
    1.  **Composer Environment:**
        *   Deploy the `composer/dags/r_ausd_vertrag_control_dag.py` to Cloud Composer.
        *   Ensure the `google_cloud_default` Airflow connection is configured correctly with BigQuery access.
        *   Ensure the BigQuery Stored Procedure `mydataset.r_ausd_vertrag_control` is deployed and functional.
        *   Ensure source tables are populated with test data.
        *   Ensure target tables (`mydataset.SOF_TA_NOTICE`, `mydataset.VIA`) are empty.
        *   Ensure `mydataset.job_table` and `mydataset.error_log` are clean.
*   **Action:**
    1.  Trigger the `r_ausd_vertrag_control_dag` in Cloud Composer (either manually or by its defined schedule).
*   **Pass/Fail Criterion:**
    1.  The Cloud Composer DAG run must complete successfully.
    2.  The `execute_bq_r_ausd_vertrag_control` task within the DAG must succeed.
    3.  A new entry must be present in `mydataset.job_table` with `status = 'COMPLETED'`, `job_kennung = 'TA_NOTICE_DAILY'`, and `eintrags_nr` matching the Airflow execution date (e.g., `{{ ds_nodash }}`).
    4.  Data should be present in `mydataset.SOF_TA_NOTICE` and `mydataset.VIA` as expected from a successful run (verify against Test Case 1's output parity).
    5.  No errors in `mydataset.error_log`.