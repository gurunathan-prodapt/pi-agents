As a senior data-migration QA engineer, I've analyzed the migration design for `k_ausd_v_ta_p_vertrag.ksh` to BigQuery. The migration involves re-implementing KornShell orchestration and Oracle SQL data transformation into BigQuery Stored Procedures, along with dedicated BigQuery tables for job management and error logging.

The following test cases are designed to ensure behavioral equivalence, data integrity, and correct functionality of the migrated BigQuery components.

---

## Migration Validation Tests for `k_ausd_v_ta_p_vertrag.ksh`

### Test Case 1: Orchestration - Happy Path (Successful End-to-End Execution)

*   **Purpose:** Verify the complete end-to-end execution of the migrated job under normal conditions. This includes correct parameter handling, successful data transformation, accurate job status updates, and correct record count reporting.
*   **Setup:**
    1.  Ensure `project.dataset.DWTK_MELDUNGEN` and `project.dataset.SOF_TA_VERTRAG_TMP` are populated with a representative set of valid input data that will result in records being processed and inserted into the target tables.
    2.  Ensure `project.dataset.SOF_TA_P_VERTRAG` and `project.dataset.VIA` are empty.
    3.  Ensure `project.dataset.job_table` and `project.dataset.error_log` are empty or in a clean state.
    4.  Define expected output data for `project.dataset.SOF_TA_P_VERTRAG` and `project.dataset.VIA` based on the legacy job's execution with the same input data.
*   **Action:**
    Execute the main orchestration BigQuery Stored Procedure `project.dataset.r_ausd_vertrag` with valid `p_JobKennung` and `p_EintragsNr` parameters.

    ```sql
    CALL project.dataset.r_ausd_vertrag(
        p_JobKennung => 'TEST_JOB_001',
        p_EintragsNr => 'ENTRY_001'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  **Job Status:** The `project.dataset.job_table` contains a single entry for `job_kennung = 'TEST_JOB_001'` and `tab_name = 'ta_p_vertrag'` with `status = 'COMPLETED'`, `start_time` and `end_time` populated, and `record_count` reflecting the number of records processed.
    2.  **Error Logging:** The `project.dataset.error_log` table is empty.
    3.  **Output Parity:** The data in `project.dataset.SOF_TA_P_VERTRAG` and `project.dataset.VIA` exactly matches the expected output from the legacy system (same row count, same column values).
    4.  **Record Count:** The `record_count` in `job_table` matches `SELECT COUNT(*) FROM project.dataset.SOF_TA_P_VERTRAG`.

    ```sql
    -- Pass/Fail Check 1: Job Status
    SELECT
        job_kennung,
        eintrags_nr,
        tab_name,
        status,
        record_count
    FROM
        project.dataset.job_table
    WHERE
        job_kennung = 'TEST_JOB_001'
        AND tab_name = 'ta_p_vertrag';
    -- Expected: status = 'COMPLETED', record_count > 0

    -- Pass/Fail Check 2: Error Logging
    SELECT COUNT(*) FROM project.dataset.error_log;
    -- Expected: 0

    -- Pass/Fail Check 3 & 4: Output Parity and Record Count
    -- This requires a comparison with a baseline. Assuming 'legacy_sof_ta_p_vertrag' and 'legacy_via' are baseline tables.
    SELECT
        (SELECT COUNT(*) FROM project.dataset.SOF_TA_P_VERTRAG) = (SELECT COUNT(*) FROM project.dataset.legacy_sof_ta_p_vertrag) AS sof_count_match,
        (SELECT COUNT(*) FROM project.dataset.VIA) = (SELECT COUNT(*) FROM project.dataset.legacy_via) AS via_count_match,
        (SELECT COUNT(*) FROM project.dataset.SOF_TA_P_VERTRAG EXCEPT DISTINCT SELECT * FROM project.dataset.legacy_sof_ta_p_vertrag) = 0 AS sof_data_match,
        (SELECT COUNT(*) FROM project.dataset.legacy_sof_ta_p_vertrag EXCEPT DISTINCT SELECT * FROM project.dataset.SOF_TA_P_VERTRAG) = 0 AS sof_data_reverse_match,
        (SELECT COUNT(*) FROM project.dataset.VIA EXCEPT DISTINCT SELECT * FROM project.dataset.legacy_via) = 0 AS via_data_match,
        (SELECT COUNT(*) FROM project.dataset.legacy_via EXCEPT DISTINCT SELECT * FROM project.dataset.VIA) = 0 AS via_data_reverse_match;
    -- Expected: All boolean columns are TRUE.
    ```

### Test Case 2: Orchestration - Parameter Validation (Missing Required Parameter)

*   **Purpose:** Verify that the migrated orchestration procedure correctly identifies and logs an error when a required parameter (`p_JobKennung` or `p_EintragsNr`) is missing, mimicking the legacy `pruefeParameterGesetzt` and `DWMSG_MeldeFehler` behavior.
*   **Setup:**
    1.  Ensure `project.dataset.job_table` and `project.dataset.error_log` are empty.
*   **Action:**
    Attempt to execute `project.dataset.r_ausd_vertrag` with `p_JobKennung` missing.

    ```sql
    CALL project.dataset.r_ausd_vertrag(
        p_EintragsNr => 'ENTRY_002'
        -- p_JobKennung is intentionally omitted
    );
    ```
*   **Pass/Fail Criterion:**
    1.  **Job Status:** The `project.dataset.job_table` either contains no entry for this execution attempt, or an entry with `status = 'FAILED'` and an appropriate error message.
    2.  **Error Logging:** The `project.dataset.error_log` contains an entry with `procedure_name = 'r_ausd_vertrag'`, `error_message` indicating a missing parameter (e.g., "Missing required parameter: p_JobKennung"), and `job_kennung` (if captured before failure) or `NULL`.
    3.  **No Data Transformation:** `project.dataset.SOF_TA_P_VERTRAG` and `project.dataset.VIA` remain unchanged.

    ```sql
    -- Pass/Fail Check 1 & 2: Job Status and Error Logging
    SELECT
        error_timestamp,
        procedure_name,
        error_message,
        job_kennung,
        eintrags_nr
    FROM
        project.dataset.error_log
    WHERE
        procedure_name = 'r_ausd_vertrag'
        AND error_message LIKE '%p_JobKennung%';
    -- Expected: One row, error_message indicating missing p_JobKennung.

    SELECT
        status,
        error_message
    FROM
        project.dataset.job_table
    WHERE
        eintrags_nr = 'ENTRY_002';
    -- Expected: status = 'FAILED' (if an entry was created), error_message indicating parameter error.

    -- Pass/Fail Check 3: No Data Transformation
    SELECT COUNT(*) FROM project.dataset.SOF_TA_P_VERTRAG;
    SELECT COUNT(*) FROM project.dataset.VIA;
    -- Expected: 0 (or original count if not empty before test)
    ```

### Test Case 3: Orchestration - Job Management (Ignoring Currently Active Job)

*   **Purpose:** Verify that the migrated orchestration procedure correctly identifies and ignores an attempt to run a job that is already marked as 'ACTIVE' for the same `p_JobKennung` and `v_TabName` (which is 'ta_p_vertrag'). This replicates the "aktive Jobs werden ignoriert" logic.
*   **Setup:**
    1.  Insert an 'ACTIVE' entry into `project.dataset.job_table` for a specific `job_kennung` and `tab_name`.
    2.  Ensure `project.dataset.error_log` is empty.

    ```sql
    INSERT INTO project.dataset.job_table (job_id, job_kennung, eintrags_nr, tab_name, status, start_time, end_time, record_count, error_message)
    VALUES ('ACTIVE_JOB_ID_1', 'ACTIVE_TEST_JOB', 'ACTIVE_ENTRY_1', 'ta_p_vertrag', 'ACTIVE', CURRENT_TIMESTAMP(), NULL, NULL, NULL);
    ```
*   **Action:**
    Execute `project.dataset.r_ausd_vertrag` with the same `p_JobKennung` and `p_EintragsNr` as the active job.

    ```sql
    CALL project.dataset.r_ausd_vertrag(
        p_JobKennung => 'ACTIVE_TEST_JOB',
        p_EintragsNr => 'ACTIVE_ENTRY_1'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  **Job Status:** The `project.dataset.job_table` still contains only *one* entry for `job_kennung = 'ACTIVE_TEST_JOB'` and `tab_name = 'ta_p_vertrag'`, and its `status` remains 'ACTIVE'. No new entry is created, and the existing entry is not updated to 'COMPLETED' or 'FAILED' by this second call.
    2.  **Error Logging:** The `project.dataset.error_log` contains an informational message indicating that the job was ignored because it was already active.
    3.  **No Data Transformation:** `project.dataset.SOF_TA_P_VERTRAG` and `project.dataset.VIA` remain unchanged (no new data inserted/updated).

    ```sql
    -- Pass/Fail Check 1: Job Status
    SELECT
        job_kennung,
        eintrags_nr,
        tab_name,
        status,
        COUNT(*) AS num_entries
    FROM
        project.dataset.job_table
    WHERE
        job_kennung = 'ACTIVE_TEST_JOB'
        AND tab_name = 'ta_p_vertrag'
    GROUP BY 1,2,3,4;
    -- Expected: One row with status = 'ACTIVE', num_entries = 1.

    -- Pass/Fail Check 2: Error Logging (or informational log)
    SELECT
        error_message
    FROM
        project.dataset.error_log
    WHERE
        job_kennung = 'ACTIVE_TEST_JOB'
        AND error_message LIKE '%already active%';
    -- Expected: One row with a message like "Job 'ACTIVE_TEST_JOB' for 'ta_p_vertrag' is already active, ignoring."
    ```

### Test Case 4: Orchestration - Job Management (Deactivating Older Active Jobs)

*   **Purpose:** Verify the logic for deactivating older active jobs, as specified by "alte aktive Jobs werden einfach dekativiert". This implies a cleanup mechanism for stale active jobs related to the same `v_TabName`.
*   **Setup:**
    1.  Insert multiple 'ACTIVE' entries into `project.dataset.job_table` for `tab_name = 'ta_p_vertrag'`, with varying `job_kennung` and `start_time` values. One should be significantly older.

    ```sql
    INSERT INTO project.dataset.job_table (job_id, job_kennung, eintrags_nr, tab_name, status, start_time, end_time, record_count, error_message) VALUES
    ('OLD_ACTIVE_JOB_1', 'OLD_JOB_A', 'ENTRY_OLD_A', 'ta_p_vertrag', 'ACTIVE', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR), NULL, NULL, NULL),
    ('OLD_ACTIVE_JOB_2', 'OLD_JOB_B', 'ENTRY_OLD_B', 'ta_p_vertrag', 'ACTIVE', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), NULL, NULL, NULL),
    ('OTHER_TAB_ACTIVE', 'OTHER_JOB_C', 'ENTRY_OTHER_C', 'other_table', 'ACTIVE', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE), NULL, NULL, NULL);
    ```
*   **Action:**
    Execute `project.dataset.r_ausd_vertrag` with new `p_JobKennung` and `p_EintragsNr`.

    ```sql
    CALL project.dataset.r_ausd_vertrag(
        p_JobKennung => 'NEW_JOB_D',
        p_EintragsNr => 'ENTRY_NEW_D'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  **Job Status:**
        *   The newly executed job (`NEW_JOB_D`) is marked 'COMPLETED'.
        *   All *other* previously 'ACTIVE' jobs for `tab_name = 'ta_p_vertrag'` (i.e., `OLD_JOB_A`, `OLD_JOB_B`) are updated to `status = 'DEACTIVATED'` (or 'COMPLETED' if that's the chosen status for deactivation).
        *   The job for `tab_name = 'other_table'` (`OTHER_JOB_C`) remains 'ACTIVE'.
    2.  **Error Logging:** The `project.dataset.error_log` is empty (assuming successful execution).

    ```sql
    -- Pass/Fail Check 1: Job Status
    SELECT
        job_kennung,
        tab_name,
        status,
        start_time,
        end_time
    FROM
        project.dataset.job_table
    WHERE
        tab_name = 'ta_p_vertrag'
    ORDER BY start_time;
    -- Expected:
    -- 'OLD_JOB_A', 'ta_p_vertrag', 'DEACTIVATED' (or 'COMPLETED'), <old_time>, <updated_time>
    -- 'OLD_JOB_B', 'ta_p_vertrag', 'DEACTIVATED' (or 'COMPLETED'), <old_time>, <updated_time>
    -- 'NEW_JOB_D', 'ta_p_vertrag', 'COMPLETED', <current_time>, <current_time>

    SELECT
        job_kennung,
        tab_name,
        status
    FROM
        project.dataset.job_table
    WHERE
        job_kennung = 'OTHER_JOB_C';
    -- Expected: 'OTHER_JOB_C', 'other_table', 'ACTIVE'
    ```

### Test Case 5: Data Transformation - Output Parity (Specific Column Values & NULL Handling)

*   **Purpose:** Verify that specific data transformation logic, including joins, filters, data type conversions, NULL handling, and logic previously encapsulated in Oracle packages (`DWPA_UTIL_SKRIPT`, `PV`), is correctly replicated in BigQuery. This test focuses on the `d_ausd_v_ta_p_vertrag` procedure.
*   **Setup:**
    1.  Populate `project.dataset.DWTK_MELDUNGEN` and `project.dataset.SOF_TA_VERTRAG_TMP` with a diverse dataset, including:
        *   Rows that should be filtered out.
        *   Rows with NULLs in critical columns.
        *   Rows with boundary values for dates/numbers.
        *   Rows that would trigger specific logic within the legacy Oracle packages (e.g., `vertrag_id_carmen` logic).
        *   Rows that should result in updates vs. inserts (if `MERGE` is used).
    2.  Run the *legacy* job with this specific dataset and capture the exact output in `SOF$TA_P_VERTRAG` and `VIA` as a baseline.
    3.  Ensure `project.dataset.SOF_TA_P_VERTRAG` and `project.dataset.VIA` are empty before the test.
*   **Action:**
    Execute the main orchestration BigQuery Stored Procedure `project.dataset.r_ausd_vertrag` with valid parameters.

    ```sql
    CALL project.dataset.r_ausd_vertrag(
        p_JobKennung => 'TRANSFORM_TEST_001',
        p_EintragsNr => 'ENTRY_TRANSFORM_001'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  **Full Data Match:** Every column and every row in `project.dataset.SOF_TA_P_VERTRAG` and `project.dataset.VIA` must exactly match the corresponding baseline tables from the legacy system. This includes data types, NULL presence, and specific calculated values.
    2.  **Row Counts:** The total number of rows in `project.dataset.SOF_TA_P_VERTRAG` and `project.dataset.VIA` must match the legacy baseline.

    ```sql
    -- Pass/Fail Check: Comprehensive Data Comparison
    -- Assuming 'legacy_sof_ta_p_vertrag' and 'legacy_via' are baseline tables with identical schema.
    SELECT
        (SELECT COUNT(*) FROM project.dataset.SOF_TA_P_VERTRAG) = (SELECT COUNT(*) FROM project.dataset.legacy_sof_ta_p_vertrag) AS sof_row_count_match,
        (SELECT COUNT(*) FROM project.dataset.VIA) = (SELECT COUNT(*) FROM project.dataset.legacy_via) AS via_row_count_match,
        (SELECT COUNT(*) FROM project.dataset.SOF_TA_P_VERTRAG EXCEPT DISTINCT SELECT * FROM project.dataset.legacy_sof_ta_p_vertrag) = 0 AS sof_data_exact_match_forward,
        (SELECT COUNT(*) FROM project.dataset.legacy_sof_ta_p_vertrag EXCEPT DISTINCT SELECT * FROM project.dataset.SOF_TA_P_VERTRAG) = 0 AS sof_data_exact_match_reverse,
        (SELECT COUNT(*) FROM project.dataset.VIA EXCEPT DISTINCT SELECT * FROM project.dataset.legacy_via) = 0 AS via_data_exact_match_forward,
        (SELECT COUNT(*) FROM project.dataset.legacy_via EXCEPT DISTINCT SELECT * FROM project.dataset.VIA) = 0 AS via_data_exact_match_reverse;
    -- Expected: All boolean columns are TRUE.

    -- Example for specific NULL handling or type conversion check (requires knowledge of actual SQL)
    -- If legacy SQL had: NVL(column_a, 'DEFAULT')
    -- Check:
    SELECT COUNT(*) FROM project.dataset.SOF_TA_P_VERTRAG WHERE column_a IS NULL;
    -- Expected: 0 if NVL was applied correctly.

    -- If legacy SQL had: TO_DATE(date_string, 'YYYYMMDD')
    -- Check:
    SELECT COUNT(*) FROM project.dataset.SOF_TA_P_VERTRAG WHERE NOT SAFE.PARSE_DATE('%Y%m%d', date_string_column) IS NOT NULL;
    -- Expected: 0 (no invalid dates after conversion)
    ```

### Test Case 6: Data Transformation - Empty Source Tables

*   **Purpose:** Verify that the data transformation procedure handles cases where source tables are empty gracefully, resulting in empty target tables and correct record counts.
*   **Setup:**
    1.  Ensure `project.dataset.DWTK_MELDUNGEN` and `project.dataset.SOF_TA_VERTRAG_TMP` are completely empty.
    2.  Ensure `project.dataset.SOF_TA_P_VERTRAG` and `project.dataset.VIA` are empty.
    3.  Ensure `project.dataset.job_table` and `project.dataset.error_log` are empty.
*   **Action:**
    Execute the main orchestration BigQuery Stored Procedure `project.dataset.r_ausd_vertrag` with valid parameters.

    ```sql
    CALL project.dataset.r_ausd_vertrag(
        p_JobKennung => 'EMPTY_SOURCE_TEST',
        p_EintragsNr => 'ENTRY_EMPTY_001'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  **Job Status:** The `project.dataset.job_table` contains an entry for `job_kennung = 'EMPTY_SOURCE_TEST'` with `status = 'COMPLETED'` and `record_count = 0`.
    2.  **Error Logging:** The `project.dataset.error_log` table is empty.
    3.  **Target Tables:** `project.dataset.SOF_TA_P_VERTRAG` and `project.dataset.VIA` remain empty.

    ```sql
    -- Pass/Fail Check 1 & 3: Job Status and Target Tables
    SELECT
        job_kennung,
        status,
        record_count
    FROM
        project.dataset.job_table
    WHERE
        job_kennung = 'EMPTY_SOURCE_TEST';
    -- Expected: status = 'COMPLETED', record_count = 0

    SELECT COUNT(*) FROM project.dataset.SOF_TA_P_VERTRAG;
    -- Expected: 0
    SELECT COUNT(*) FROM project.dataset.VIA;
    -- Expected: 0
    ```

### Test Case 7: External System Replacement - Oracle Packages (`DWPA_UTIL_SKRIPT`, `PV`)

*   **Purpose:** Verify that the logic previously contained within Oracle packages (`DWPA_UTIL_SKRIPT`, `PV`) has been correctly migrated and integrated into BigQuery SQL or UDFs/Stored Procedures, producing identical results. This is a specific instance of Transformation Correctness.
*   **Setup:**
    1.  Identify specific input scenarios that would trigger distinct logic paths or return specific values from the legacy Oracle packages.
    2.  Populate `project.dataset.DWTK_MELDUNGEN` and `project.dataset.SOF_TA_VERTRAG_TMP` with data designed to test these specific package behaviors.
    3.  Run the *legacy* job with this data and capture the output in `SOF$TA_P_VERTRAG` and `VIA` as a baseline.
    4.  Ensure `project.dataset.SOF_TA_P_VERTRAG` and `project.dataset.VIA` are empty.
*   **Action:**
    Execute the main orchestration BigQuery Stored Procedure `project.dataset.r_ausd_vertrag` with valid parameters.

    ```sql
    CALL project.dataset.r_ausd_vertrag(
        p_JobKennung => 'PACKAGE_LOGIC_TEST',
        p_EintragsNr => 'ENTRY_PACKAGE_001'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  **Output Parity:** The data in `project.dataset.SOF_TA_P_VERTRAG` and `project.dataset.VIA` must exactly match the baseline output from the legacy system, specifically verifying the columns and rows affected by the migrated package logic.
    2.  **Specific Assertions:** If possible, assert specific values in the target tables that are direct results of the package logic.

    ```sql
    -- Pass/Fail Check: Data comparison (similar to Test Case 5)
    SELECT
        (SELECT COUNT(*) FROM project.dataset.SOF_TA_P_VERTRAG EXCEPT DISTINCT SELECT * FROM project.dataset.legacy_sof_ta_p_vertrag) = 0 AS sof_data_match,
        (SELECT COUNT(*) FROM project.dataset.legacy_sof_ta_p_vertrag EXCEPT DISTINCT SELECT * FROM project.dataset.SOF_TA_P_VERTRAG) = 0 AS sof_data_reverse_match;
    -- Expected: Both boolean columns are TRUE.

    -- Example: If PV.vertrag_id_carmen transformed a 'contract_id_raw' to 'contract_id_processed'
    SELECT
        COUNT(*)
    FROM
        project.dataset.SOF_TA_P_VERTRAG AS bq
    JOIN
        project.dataset.legacy_sof_ta_p_vertrag AS legacy
    ON
        bq.primary_key_col = legacy.primary_key_col
    WHERE
        bq.contract_id_processed <> legacy.contract_id_processed;
    -- Expected: 0 (all processed contract IDs match)
    ```

### Test Case 8: Data Quality - Schema Assertions

*   **Purpose:** Verify that the schema of the target tables (`SOF_TA_P_VERTRAG`, `VIA`) in BigQuery matches the expected schema, including column names, data types, and nullability, as derived from the legacy Oracle tables.
*   **Setup:**
    1.  Obtain the definitive schema for the legacy Oracle tables `SOF$TA_P_VERTRAG` and `VIA`.
*   **Action:**
    Query the BigQuery `INFORMATION_SCHEMA` for the target tables.
*   **Pass/Fail Criterion:**
    The BigQuery schema (column names, data types, nullability) for `project.dataset.SOF_TA_P_VERTRAG` and `project.dataset.VIA` must precisely match the legacy Oracle schema.

    ```sql
    -- Pass/Fail Check: Schema comparison for SOF_TA_P_VERTRAG
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM
        project.dataset.INFORMATION_SCHEMA.COLUMNS
    WHERE
        table_name = 'SOF_TA_P_VERTRAG'
    ORDER BY
        ordinal_position;
    /*
    Expected Output (example, based on legacy Oracle schema):
    column_name | data_type | is_nullable
    ------------|-----------|------------
    ID          | INT64     | NO
    CONTRACT_NUM| STRING    | YES
    START_DATE  | DATE      | NO
    ...
    */

    -- Pass/Fail Check: Schema comparison for VIA
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM
        project.dataset.INFORMATION_SCHEMA.COLUMNS
    WHERE
        table_name = 'VIA'
    ORDER BY
        ordinal_position;
    /*
    Expected Output (example, based on legacy Oracle schema):
    column_name | data_type | is_nullable
    ------------|-----------|------------
    VIA_ID      | INT64     | NO
    DESCRIPTION | STRING    | YES
    ...
    */
    ```

### Test Case 9: Idempotency

*   **Purpose:** Verify that running the migrated job multiple times with the same inputs produces the same final state in the target tables, ensuring the job is idempotent. This is crucial for recovery and re-run scenarios.
*   **Setup:**
    1.  Populate `project.dataset.DWTK_MELDUNGEN` and `project.dataset.SOF_TA_VERTRAG_TMP` with a consistent dataset.
    2.  Ensure `project.dataset.SOF_TA_P_VERTRAG` and `project.dataset.VIA` are empty.
    3.  Ensure `project.dataset.job_table` and `project.dataset.error_log` are empty.
*   **Action:**
    1.  Execute `project.dataset.r_ausd_vertrag` for the first time.
    2.  Immediately execute `project.dataset.r_ausd_vertrag` a second time with the *exact same* parameters.

    ```sql
    -- First run
    CALL project.dataset.r_ausd_vertrag(
        p_JobKennung => 'IDEMPOTENCY_TEST',
        p_EintragsNr => 'ENTRY_IDEMPOTENT_001'
    );

    -- Second run
    CALL project.dataset.r_ausd_vertrag(
        p_JobKennung => 'IDEMPOTENCY_TEST',
        p_EintragsNr => 'ENTRY_IDEMPOTENT_001'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  **Job Table:** The `job_table` should reflect one successful run (`status = 'COMPLETED'`) and one ignored run (due to "ignoring currently active jobs" logic, see Test Case 3).
    2.  **Target Tables:** The data in `project.dataset.SOF_TA_P_VERTRAG` and `project.dataset.VIA` after the second execution is identical to the state after the first execution. No duplicate rows, no unexpected updates, no changes in row counts.

    ```sql
    -- Pass/Fail Check 1: Job Table
    SELECT
        job_kennung,
        status,
        COUNT(*) AS num_entries
    FROM
        project.dataset.job_table
    WHERE
        job_kennung = 'IDEMPOTENCY_TEST'
    GROUP BY 1,2;
    -- Expected: One row for 'COMPLETED' (num_entries=1), and one row for 'IGNORED' (num_entries=1) or similar status indicating the second run was skipped.

    -- Pass/Fail Check 2: Target Tables (assuming 'snapshot_sof_ta_p_vertrag' and 'snapshot_via' are taken after the first run)
    SELECT
        (SELECT COUNT(*) FROM project.dataset.SOF_TA_P_VERTRAG EXCEPT DISTINCT SELECT * FROM project.dataset.snapshot_sof_ta_p_vertrag) = 0 AS sof_data_match,
        (SELECT COUNT(*) FROM project.dataset.snapshot_sof_ta_p_vertrag EXCEPT DISTINCT SELECT * FROM project.dataset.SOF_TA_P_VERTRAG) = 0 AS sof_data_reverse_match,
        (SELECT COUNT(*) FROM project.dataset.VIA EXCEPT DISTINCT SELECT * FROM project.dataset.snapshot_via) = 0 AS via_data_match,
        (SELECT COUNT(*) FROM project.dataset.snapshot_via EXCEPT DISTINCT SELECT * FROM project.dataset.VIA) = 0 AS via_data_reverse_match;
    -- Expected: All boolean columns are TRUE.
    ```