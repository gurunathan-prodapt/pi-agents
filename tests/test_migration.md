The migration of `r_ausd_bp_ta_bpr_evn.ksh` to BigQuery involves translating KornShell orchestration, parameter handling, and error management into BigQuery Stored Procedures, along with migrating the core data transformation logic. The following test cases are designed to ensure behavioral equivalence, data integrity, and correct functionality of the migrated BigQuery solution.

---

## Migration Validation Tests for `r_ausd_bp_ta_bpr_evn.ksh`

### Test Setup Prerequisites

Before running any tests, ensure the following:

1.  **BigQuery Environment:**
    *   A BigQuery project and dataset (`project.dataset`) are configured.
    *   The `job_audit` table is created using `bq_ddl/job_audit_table.sql`.
    *   The `sp_k_ausd_bp_ta_bpr_evn` and `sp_r_ausd_bp_ta_bpr_evn` stored procedures are deployed.
    *   `source_contract_cache` and `fos_target_table` exist with appropriate schemas. For testing, assume `source_contract_cache` has at least `dwh_vertrag_id INT64`, `gueltig_von DATE`, `gueltig_bis DATE`, `ladedatum DATE`, and other relevant columns that `SELECT *` would pick up. `fos_target_table` should have a compatible schema.
2.  **Legacy Environment Access:**
    *   Access to the legacy system to run `r_ausd_bp_ta_bpr_evn.ksh` and inspect its output (log files, target database).
3.  **Test Data:**
    *   Prepare a set of diverse test data for `source_contract_cache` that covers various date ranges, `dwh_vertrag_id` values, and NULL scenarios. This data should be loadable into both the legacy DWH source and BigQuery `source_contract_cache`.

---

### Test Case 1: Happy Path - Full Load (Stichtag Provided, No Restart)

*   **Purpose:** Verify the basic functionality of the migrated job when a `Stichtag` is explicitly provided and no restart value is used. This tests parameter passing, date parsing, core filtering logic, and successful job completion logging.
*   **Setup:**
    1.  Populate `project.dataset.source_contract_cache` with a comprehensive set of test data.
    2.  Ensure `project.dataset.fos_target_table` is empty.
    3.  Ensure `project.dataset.job_audit` is empty.
    4.  Choose a `Stichtag` (e.g., '15032023') that will result in a non-empty `fos_target_table` after filtering.
*   **Action:**
    1.  Execute the BigQuery orchestration stored procedure:
        ```sql
        CALL `project.dataset.sp_r_ausd_bp_ta_bpr_evn`('15032023', 0);
        ```
    2.  (For output parity) Execute the legacy KSH script with equivalent parameters:
        ```bash
        ./r_ausd_bp_ta_bpr_evn.ksh -s 15032023 -l 0
        ```
*   **Pass/Fail Criterion:**
    1.  **Job Audit:** A single entry exists in `project.dataset.job_audit` with `status = 'SUCCESS'`, `stichtag_param = '15032023'`, `restart_value_param = 0`, and `stichtag_processed = '2023-03-15'`.
    2.  **Row Count:** The number of rows in `project.dataset.fos_target_table` matches the number of rows in the legacy target table after its execution.
    3.  **Data Parity:** The data content of `project.dataset.fos_target_table` is identical to the legacy target table. This can be verified by comparing hashes of sorted data or a row-by-row comparison.
    4.  **Transformation Correctness:** All records in `fos_target_table` satisfy the conditions: `gueltig_von <= '2023-03-15'` AND `'2023-03-15' < gueltig_bis` AND `ladedatum < '2023-03-15'`.

*   **Test Code (SQL Assertions):**
    ```sql
    -- 1. Verify job_audit entry
    SELECT
        COUNT(1) AS audit_entry_count,
        MAX(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS is_success,
        MAX(CASE WHEN stichtag_param = '15032023' THEN 1 ELSE 0 END) AS stichtag_param_correct,
        MAX(CASE WHEN restart_value_param = 0 THEN 1 ELSE 0 END) AS restart_param_correct,
        MAX(CASE WHEN stichtag_processed = '2023-03-15' THEN 1 ELSE 0 END) AS stichtag_processed_correct
    FROM `project.dataset.job_audit`
    WHERE job_name = 'sp_r_ausd_bp_ta_bpr_evn'
      AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE); -- Adjust time window as needed

    -- Expected: audit_entry_count = 1, is_success = 1, stichtag_param_correct = 1, restart_param_correct = 1, stichtag_processed_correct = 1

    -- 2. Verify row count (assuming a legacy_target_table exists for comparison)
    SELECT COUNT(1) FROM `project.dataset.fos_target_table`;
    -- Compare this count with the count from the legacy system's target table.

    -- 3. Verify data content (example for a few columns, full comparison requires more advanced tools)
    SELECT
        TO_HEX(MD5(ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t) ORDER BY dwh_vertrag_id), '')))
    FROM `project.dataset.fos_target_table` AS t;
    -- Compare this hash with a hash generated from the legacy target table.

    -- 4. Verify transformation correctness (filters)
    SELECT
        COUNT(1) AS invalid_records_count
    FROM `project.dataset.fos_target_table`
    WHERE NOT (
        gueltig_von <= PARSE_DATE('%d%m%Y', '15032023')
        AND PARSE_DATE('%d%m%Y', '15032023') < gueltig_bis
        AND ladedatum < PARSE_DATE('%d%m%Y', '15032023')
    );
    -- Expected: invalid_records_count = 0
    ```

---

### Test Case 2: Happy Path - Full Load (Stichtag Defaulted, No Restart)

*   **Purpose:** Verify that `Stichtag` correctly defaults to the current system date when not provided, and the job completes successfully.
*   **Setup:**
    1.  Populate `project.dataset.source_contract_cache` with test data, including records that would be selected by `CURRENT_DATE()`.
    2.  Ensure `project.dataset.fos_target_table` is empty.
    3.  Ensure `project.dataset.job_audit` is empty.
*   **Action:**
    1.  Execute the BigQuery orchestration stored procedure without `p_stichtag_in`:
        ```sql
        CALL `project.dataset.sp_r_ausd_bp_ta_bpr_evn`(NULL, 0);
        -- Or with an empty string: CALL `project.dataset.sp_r_ausd_bp_ta_bpr_evn`('', 0);
        ```
    2.  (For output parity) Execute the legacy KSH script without `-s`:
        ```bash
        ./r_ausd_bp_ta_bpr_evn.ksh -l 0
        ```
*   **Pass/Fail Criterion:**
    1.  **Job Audit:** A single entry exists in `project.dataset.job_audit` with `status = 'SUCCESS'`, `stichtag_param` being `NULL` or `''`, `restart_value_param = 0`, and `stichtag_processed` matching `CURRENT_DATE()` (formatted as 'YYYY-MM-DD').
    2.  **Row Count & Data Parity:** The row count and data content of `project.dataset.fos_target_table` match the legacy target table, both based on `CURRENT_DATE()`.
    3.  **Transformation Correctness:** All records in `fos_target_table` satisfy the filtering conditions using `CURRENT_DATE()` as the `Stichtag`.

*   **Test Code (SQL Assertions):**
    ```sql
    -- 1. Verify job_audit entry
    SELECT
        COUNT(1) AS audit_entry_count,
        MAX(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS is_success,
        MAX(CASE WHEN stichtag_param IS NULL OR stichtag_param = '' THEN 1 ELSE 0 END) AS stichtag_param_correct,
        MAX(CASE WHEN restart_value_param = 0 THEN 1 ELSE 0 END) AS restart_param_correct,
        MAX(CASE WHEN stichtag_processed = CURRENT_DATE() THEN 1 ELSE 0 END) AS stichtag_processed_correct
    FROM `project.dataset.job_audit`
    WHERE job_name = 'sp_r_ausd_bp_ta_bpr_evn'
      AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE);

    -- Expected: audit_entry_count = 1, is_success = 1, stichtag_param_correct = 1, restart_param_correct = 1, stichtag_processed_correct = 1

    -- 2. Verify transformation correctness (filters with CURRENT_DATE)
    SELECT
        COUNT(1) AS invalid_records_count
    FROM `project.dataset.fos_target_table`
    WHERE NOT (
        gueltig_von <= CURRENT_DATE()
        AND CURRENT_DATE() < gueltig_bis
        AND ladedatum < CURRENT_DATE()
    );
    -- Expected: invalid_records_count = 0
    ```

---

### Test Case 3: Restart Logic - `Wiederanlaufwert > 0` (Initial Run)

*   **Purpose:** Verify the `Wiederanlaufwert` logic for conditional insertion when the target table is initially empty. This tests that only records with `dwh_vertrag_id > p_wiederanlaufWert` are inserted.
*   **Setup:**
    1.  Populate `project.dataset.source_contract_cache` with test data, including `dwh_vertrag_id` values both above and below the chosen `Wiederanlaufwert`.
    2.  Ensure `project.dataset.fos_target_table` is empty.
    3.  Ensure `project.dataset.job_audit` is empty.
    4.  Choose `Stichtag = '15032023'` and `Wiederanlaufwert = 100`.
*   **Action:**
    1.  Execute the BigQuery orchestration stored procedure:
        ```sql
        CALL `project.dataset.sp_r_ausd_bp_ta_bpr_evn`('15032023', 100);
        ```
    2.  (For output parity) Execute the legacy KSH script:
        ```bash
        ./r_ausd_bp_ta_bpr_evn.ksh -s 15032023 -l 100
        ```
*   **Pass/Fail Criterion:**
    1.  **Job Audit:** A single entry exists in `project.dataset.job_audit` with `status = 'SUCCESS'`, `restart_value_param = 100`, and `restart_value_processed = 100`.
    2.  **Row Count & Data Parity:** The row count and data content of `project.dataset.fos_target_table` match the legacy target table.
    3.  **Transformation Correctness:** All records in `fos_target_table` must have `dwh_vertrag_id > 100` AND satisfy the date filtering conditions. No records with `dwh_vertrag_id <= 100` should be present.

*   **Test Code (SQL Assertions):**
    ```sql
    -- 1. Verify job_audit entry (similar to Test Case 1)

    -- 2. Verify transformation correctness (dwh_vertrag_id filter)
    SELECT
        COUNT(1) AS invalid_records_count
    FROM `project.dataset.fos_target_table`
    WHERE dwh_vertrag_id <= 100;
    -- Expected: invalid_records_count = 0

    -- 3. Verify transformation correctness (all filters combined)
    SELECT
        COUNT(1) AS invalid_records_count
    FROM `project.dataset.fos_target_table`
    WHERE NOT (
        gueltig_von <= PARSE_DATE('%d%m%Y', '15032023')
        AND PARSE_DATE('%d%m%Y', '15032023') < gueltig_bis
        AND ladedatum < PARSE_DATE('%d%m%Y', '15032023')
        AND dwh_vertrag_id > 100
    );
    -- Expected: invalid_records_count = 0
    ```

---

### Test Case 4: Restart Logic - `Wiederanlaufwert > 0` (Re-run Scenario)

*   **Purpose:** Verify the idempotent behavior and correct handling of existing data when `Wiederanlaufwert` is used in a re-run scenario. This tests the conditional `DELETE` and `INSERT` logic.
*   **Setup:**
    1.  Populate `project.dataset.source_contract_cache` with test data.
    2.  Pre-populate `project.dataset.fos_target_table` with data:
        *   Records with `dwh_vertrag_id < 100` (these should remain untouched).
        *   Records with `dwh_vertrag_id = 100` (these should be deleted and NOT re-inserted).
        *   Records with `dwh_vertrag_id > 100` (these should be deleted and then re-inserted from source if they meet other filters).
        *   Include some records in `fos_target_table` that *would not* be selected by the `Stichtag` and `dwh_vertrag_id > 100` filters, but *do* have `dwh_vertrag_id >= 100`. These should be deleted.
    3.  Ensure `project.dataset.job_audit` is empty.
    4.  Choose `Stichtag = '15032023'` and `Wiederanlaufwert = 100`.
*   **Action:**
    1.  Execute the BigQuery orchestration stored procedure:
        ```sql
        CALL `project.dataset.sp_r_ausd_bp_ta_bpr_evn`('15032023', 100);
        ```
    2.  (For output parity) Execute the legacy KSH script:
        ```bash
        ./r_ausd_bp_ta_bpr_evn.ksh -s 15032023 -l 100
        ```
*   **Pass/Fail Criterion:**
    1.  **Job Audit:** A single entry exists in `project.dataset.job_audit` with `status = 'SUCCESS'`.
    2.  **Row Count & Data Parity:** The row count and data content of `project.dataset.fos_target_table` match the legacy target table.
    3.  **Transformation Correctness:**
        *   All records with `dwh_vertrag_id < 100` that were initially present in `fos_target_table` are still there.
        *   No records with `dwh_vertrag_id = 100` are present in `fos_target_table`.
        *   All records with `dwh_vertrag_id > 100` in `fos_target_table` satisfy the date filtering conditions and originate from `source_contract_cache`.

*   **Test Code (SQL Assertions):**
    ```sql
    -- Assume initial_fos_target_table_state is a temporary table or snapshot of fos_target_table before the run.

    -- 1. Verify job_audit entry (similar to Test Case 1)

    -- 2. Verify records with dwh_vertrag_id < 100 are untouched
    SELECT
        COUNT(1) AS mismatch_count
    FROM `project.dataset.fos_target_table` AS current_state
    FULL OUTER JOIN `initial_fos_target_table_state` AS initial_state
        ON current_state.dwh_vertrag_id = initial_state.dwh_vertrag_id
        -- Add other primary key columns for a robust join
    WHERE
        (current_state.dwh_vertrag_id < 100 AND initial_state.dwh_vertrag_id IS NULL) OR -- Missing records
        (initial_state.dwh_vertrag_id < 100 AND current_state.dwh_vertrag_id IS NULL) OR -- Extra records
        (current_state.dwh_vertrag_id < 100 AND initial_state.dwh_vertrag_id < 100 AND NOT (current_state.col1 = initial_state.col1 AND ...)); -- Data mismatch
    -- Expected: mismatch_count = 0

    -- 3. Verify no records with dwh_vertrag_id = 100
    SELECT
        COUNT(1) AS records_at_boundary_count
    FROM `project.dataset.fos_target_table`
    WHERE dwh_vertrag_id = 100;
    -- Expected: records_at_boundary_count = 0

    -- 4. Verify all records with dwh_vertrag_id > 100 meet all criteria and match source
    SELECT
        COUNT(1) AS invalid_records_count
    FROM `project.dataset.fos_target_table` AS t
    LEFT JOIN `project.dataset.source_contract_cache` AS s
        ON t.dwh_vertrag_id = s.dwh_vertrag_id -- Add other join conditions for full record match
    WHERE
        t.dwh_vertrag_id > 100 AND (
            s.dwh_vertrag_id IS NULL OR -- Record exists in target but not in source
            NOT (
                t.gueltig_von = s.gueltig_von AND t.gueltig_bis = s.gueltig_bis AND t.ladedatum = s.ladedatum AND -- Compare other columns
                s.gueltig_von <= PARSE_DATE('%d%m%Y', '15032023')
                AND PARSE_DATE('%d%m%Y', '15032023') < s.gueltig_bis
                AND s.ladedatum < PARSE_DATE('%d%m%Y', '15032023')
                AND s.dwh_vertrag_id > 100
            )
        );
    -- Expected: invalid_records_count = 0
    ```

---

### Test Case 5: Error Handling - Invalid `Stichtag` Format

*   **Purpose:** Verify that the job correctly handles an invalid `Stichtag` format, logs the error, and fails gracefully without processing data.
*   **Setup:**
    1.  Ensure `project.dataset.fos_target_table` is empty.
    2.  Ensure `project.dataset.job_audit` is empty.
*   **Action:**
    1.  Execute the BigQuery orchestration stored procedure with a malformed `Stichtag`:
        ```sql
        CALL `project.dataset.sp_r_ausd_bp_ta_bpr_evn`('2023-03-15', 0);
        ```
    2.  (For output parity) Execute the legacy KSH script with a malformed `Stichtag` (expecting it to fail with ErrNr 193):
        ```bash
        ./r_ausd_bp_ta_bpr_evn.ksh -s 2023-03-15 -l 0
        ```
*   **Pass/Fail Criterion:**
    1.  **Job Execution:** The BigQuery stored procedure call should raise an error (e.g., `Invalid date: '2023-03-15'`).
    2.  **Job Audit:** A single entry exists in `project.dataset.job_audit` with `status = 'FAILED'`, `stichtag_param = '2023-03-15'`, `error_code = 193`, and `error_arg` containing a message about invalid date format.
    3.  **Data Integrity:** `project.dataset.fos_target_table` remains empty (unchanged).
    4.  **Legacy Parity:** The legacy script should exit with error code 193, and its log file should reflect the parameter error.

*   **Test Code (Pytest with BigQuery client):**
    ```python
    import pytest
    from google.cloud import bigquery

    def test_invalid_stichtag_format(bigquery_client: bigquery.Client, project_id, dataset_id):
        sp_name = f"{project_id}.{dataset_id}.sp_r_ausd_bp_ta_bpr_evn"
        audit_table = f"{project_id}.{dataset_id}.job_audit"
        target_table = f"{project_id}.{dataset_id}.fos_target_table"

        # Clear audit and target tables
        bigquery_client.query(f"TRUNCATE TABLE {audit_table}").result()
        bigquery_client.query(f"TRUNCATE TABLE {target_table}").result()

        # Action: Call SP with invalid Stichtag
        with pytest.raises(Exception) as excinfo:
            bigquery_client.query(f"CALL {sp_name}('2023-03-15', 0)").result()

        # Pass/Fail: Verify error message (BigQuery specific)
        assert "Invalid date: '2023-03-15'" in str(excinfo.value)

        # Pass/Fail: Verify job_audit entry
        query_job_audit = f"""
        SELECT status, error_code, error_arg, stichtag_param
        FROM {audit_table}
        WHERE job_name = 'sp_r_ausd_bp_ta_bpr_evn'
        ORDER BY created_at DESC LIMIT 1
        """
        audit_results = bigquery_client.query(query_job_audit).result()
        audit_row = next(audit_results)

        assert audit_row.status == 'FAILED'
        assert audit_row.error_code == 193
        assert "Invalid Stichtag format" in audit_row.error_arg
        assert audit_row.stichtag_param == '2023-03-15'

        # Pass/Fail: Verify target table is empty
        query_target_count = f"SELECT COUNT(1) FROM {target_table}"
        target_count_results = bigquery_client.query(query_target_count).result()
        assert next(target_count_results)[0] == 0
    ```

---

### Test Case 6: Error Handling - Core Logic Failure

*   **Purpose:** Verify that errors originating from the core data transformation logic (`sp_k_ausd_bp_ta_bpr_evn`) are caught, logged, and re-raised by the orchestration procedure.
*   **Setup:**
    1.  Ensure `project.dataset.fos_target_table` is empty.
    2.  Ensure `project.dataset.job_audit` is empty.
    3.  **Temporarily modify `sp_k_ausd_bp_ta_bpr_evn` to force an error.** For example, change the `INSERT` statement to reference a non-existent column or table, or add `SELECT 1/0;` at the beginning.
*   **Action:**
    1.  Execute the BigQuery orchestration stored procedure with valid parameters:
        ```sql
        CALL `project.dataset.sp_r_ausd_bp_ta_bpr_evn`('15032023', 0);
        ```
*   **Pass/Fail Criterion:**
    1.  **Job Execution:** The BigQuery stored procedure call should raise an error, indicating a failure in `sp_k_ausd_bp_ta_bpr_evn`.
    2.  **Job Audit:** A single entry exists in `project.dataset.job_audit` with `status = 'FAILED'`, `error_code` and `error_arg` reflecting the specific error from `sp_k_ausd_bp_ta_bpr_evn`.
    3.  **Data Integrity:** `project.dataset.fos_target_table` remains empty (unchanged).
*   **Cleanup:** Revert the temporary modification to `sp_k_ausd_bp_ta_bpr_evn`.

*   **Test Code (Pytest with BigQuery client):**
    ```python
    import pytest
    from google.cloud import bigquery

    # Assume a fixture `deploy_faulty_k_sp` that temporarily deploys a version
    # of sp_k_ausd_bp_ta_bpr_evn that will fail (e.g., by dividing by zero).
    # And a fixture `deploy_original_k_sp` to revert it.

    def test_core_logic_failure(bigquery_client: bigquery.Client, project_id, dataset_id, deploy_faulty_k_sp, deploy_original_k_sp):
        sp_r_name = f"{project_id}.{dataset_id}.sp_r_ausd_bp_ta_bpr_evn"
        audit_table = f"{project_id}.{dataset_id}.job_audit"
        target_table = f"{project_id}.{dataset_id}.fos_target_table"

        # Clear audit and target tables
        bigquery_client.query(f"TRUNCATE TABLE {audit_table}").result()
        bigquery_client.query(f"TRUNCATE TABLE {target_table}").result()

        # Action: Call SP with valid parameters, expecting core SP to fail
        with pytest.raises(Exception) as excinfo:
            bigquery_client.query(f"CALL {sp_r_name}('15032023', 0)").result()

        # Pass/Fail: Verify error message (BigQuery specific, depends on forced error)
        assert "Division by zero" in str(excinfo.value) # If forced error was 1/0

        # Pass/Fail: Verify job_audit entry
        query_job_audit = f"""
        SELECT status, error_code, error_arg, message
        FROM {audit_table}
        WHERE job_name = 'sp_r_ausd_bp_ta_bpr_evn'
        ORDER BY created_at DESC LIMIT 1
        """
        audit_results = bigquery_client.query(query_job_audit).result()
        audit_row = next(audit_results)

        assert audit_row.status == 'FAILED'
        assert audit_row.error_code is not None # Should be a BigQuery error code
        assert "Job failed due to an error during core processing." in audit_row.message
        assert "Division by zero" in audit_row.error_arg # Or other specific error detail

        # Pass/Fail: Verify target table is empty
        query_target_count = f"SELECT COUNT(1) FROM {target_table}"
        target_count_results = bigquery_client.query(query_target_count).result()
        assert next(target_count_results)[0] == 0
    ```

---

### Test Case 7: Data Filtering Edge Cases and NULL Handling

*   **Purpose:** Verify the precise behavior of date filtering (`Gueltig_von`, `Gueltig_bis`, `LADEDATUM`) and how NULL values in these columns are handled.
*   **Setup:**
    1.  Populate `project.dataset.source_contract_cache` with specific test data for `Stichtag = '10012023'`:
        *   **Include:**
            *   `dwh_vertrag_id = 1`, `gueltig_von = '2023-01-10'`, `gueltig_bis = '2023-01-11'`, `ladedatum = '2023-01-09'`
            *   `dwh_vertrag_id = 2`, `gueltig_von = '2023-01-01'`, `gueltig_bis = '2023-01-15'`, `ladedatum = '2023-01-05'`
        *   **Exclude (due to `Gueltig_von <= Stichtag`):**
            *   `dwh_vertrag_id = 3`, `gueltig_von = '2023-01-11'`, `gueltig_bis = '2023-01-12'`, `ladedatum = '2023-01-09'`
            *   `dwh_vertrag_id = 4`, `gueltig_von = NULL`, `gueltig_bis = '2023-01-12'`, `ladedatum = '2023-01-09'`
        *   **Exclude (due to `Stichtag < Gueltig_bis`):**
            *   `dwh_vertrag_id = 5`, `gueltig_von = '2023-01-09'`, `gueltig_bis = '2023-01-10'`, `ladedatum = '2023-01-08'`
            *   `dwh_vertrag_id = 6`, `gueltig_von = '2023-01-09'`, `gueltig_bis = NULL`, `ladedatum = '2023-01-08'`
        *   **Exclude (due to `LADEDATUM < Stichtag`):**
            *   `dwh_vertrag_id = 7`, `gueltig_von = '2023-01-01'`, `gueltig_bis = '2023-01-15'`, `ladedatum = '2023-01-10'`
            *   `dwh_vertrag_id = 8`, `gueltig_von = '2023-01-01'`, `gueltig_bis = '2023-01-15'`, `ladedatum = '2023-01-11'`
            *   `dwh_vertrag_id = 9`, `gueltig_von = '2023-01-01'`, `gueltig_bis = '2023-01-15'`, `ladedatum = NULL`
    2.  Ensure `project.dataset.fos_target_table` is empty.
    3.  Ensure `project.dataset.job_audit` is empty.
*   **Action:**
    1.  Execute the BigQuery orchestration stored procedure:
        ```sql
        CALL `project.dataset.sp_r_ausd_bp_ta_bpr_evn`('10012023', 0);
        ```
*   **Pass/Fail Criterion:**
    1.  **Job Audit:** A single entry exists in `project.dataset.job_audit` with `status = 'SUCCESS'`.
    2.  **Data Content:** `project.dataset.fos_target_table` contains exactly two records, with `dwh_vertrag_id = 1` and `dwh_vertrag_id = 2`, and no other records.
    3.  **NULL Handling:** Records with NULL values in `gueltig_von`, `gueltig_bis`, or `ladedatum` are correctly excluded.

*   **Test Code (SQL Assertions):**
    ```sql
    -- 1. Verify job_audit entry (similar to Test Case 1)

    -- 2. Verify expected records are present and only expected records
    SELECT
        ARRAY_AGG(dwh_vertrag_id ORDER BY dwh_vertrag_id) AS actual_ids
    FROM `project.dataset.fos_target_table`;
    -- Expected: actual_ids = [1, 2]

    -- 3. Verify no unexpected records (e.g., those with NULLs or boundary conditions)
    SELECT
        COUNT(1) AS unexpected_records_count
    FROM `project.dataset.fos_target_table`
    WHERE dwh_vertrag_id NOT IN (1, 2);
    -- Expected: unexpected_records_count = 0
    ```

---

### Test Case 8: External System Replacement - `job_audit` Table Schema

*   **Purpose:** Verify that the `job_audit` table schema is correctly defined and captures all necessary information as specified in the migration design, replacing the legacy file-based logging.
*   **Setup:**
    1.  Ensure `project.dataset.job_audit` table is created.
*   **Action:**
    1.  Inspect the schema of `project.dataset.job_audit`.
*   **Pass/Fail Criterion:**
    1.  The `job_audit` table schema matches the definition in `bq_ddl/job_audit_table.sql`, including column names, data types, and nullability constraints.
    2.  Specifically, columns like `job_id`, `job_name`, `status`, `stichtag_param`, `restart_value_param`, `stichtag_processed`, `restart_value_processed`, `error_code`, `error_arg`, `message`, `created_at`, `updated_at` are present with correct types.

*   **Test Code (Pytest with BigQuery client):**
    ```python
    import pytest
    from google.cloud import bigquery

    def test_job_audit_table_schema(bigquery_client: bigquery.Client, project_id, dataset_id):
        table_id = f"{project_id}.{dataset_id}.job_audit"
        table = bigquery_client.get_table(table_id)

        expected_schema = {
            "job_id": ("STRING", "REQUIRED"),
            "job_name": ("STRING", "REQUIRED"),
            "status": ("STRING", "REQUIRED"),
            "stichtag_param": ("STRING", "NULLABLE"),
            "restart_value_param": ("INT64", "NULLABLE"),
            "stichtag_processed": ("DATE", "NULLABLE"),
            "restart_value_processed": ("INT64", "NULLABLE"),
            "error_code": ("INT64", "NULLABLE"),
            "error_arg": ("STRING", "NULLABLE"),
            "message": ("STRING", "NULLABLE"),
            "created_at": ("TIMESTAMP", "REQUIRED"),
            "updated_at": ("TIMESTAMP", "REQUIRED"),
        }

        actual_schema = {field.name: (field.field_type, field.mode) for field in table.schema}

        for col_name, (col_type, col_mode) in expected_schema.items():
            assert col_name in actual_schema, f"Column {col_name} missing from job_audit table."
            assert actual_schema[col_name][0] == col_type, f"Column {col_name} has wrong type. Expected {col_type}, got {actual_schema[col_name][0]}."
            assert actual_schema[col_name][1] == col_mode, f"Column {col_name} has wrong mode. Expected {col_mode}, got {actual_schema[col_name][1]}."

        assert len(actual_schema) == len(expected_schema), "job_audit table has unexpected columns."
    ```