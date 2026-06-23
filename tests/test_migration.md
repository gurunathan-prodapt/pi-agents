As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the migration of `k_ausd_v_ta_c_bfc.ksh` to BigQuery. These tests aim to ensure behavioral equivalence, data integrity, and correct functionality of the migrated components.

---

## Migration Validation Tests for `k_ausd_v_ta_c_bfc.ksh`

**Assumptions:**
*   The `project.dataset` placeholder refers to the actual BigQuery project and dataset where the migrated code and tables reside.
*   Source tables (`sof_ta_cntrct_crs`, `sof_ta_barrier`, `sof_ta_cntrct_valid`, `sof_ta_period`) in BigQuery contain data that is an exact replica of the legacy Oracle source data for the test scenarios.
*   Target tables (`sof_ta_c_bfc_akt`, `sof_ta_c_bfc`) are available in BigQuery.
*   Control tables (`job_table`, `error_log`) are created as per the provided DDLs.
*   The `project.dataset.bfc_get_bindefrist` function has been fully implemented and verified separately to accurately replicate the Oracle `Cds$vr_Bindefrist.GetBindeFrist` logic. For these tests, we assume its correctness.
*   Access to legacy Oracle data (or a reliable snapshot/dump) is available for comparison.

---

### Test Case 1: Parameter Validation - Missing Jobkennung

*   **Purpose:** Verify that the migrated BigQuery Stored Procedure `r_ausd_ta_c_bfc` correctly handles missing `p_jobkennung` parameters, logs the error, and exits gracefully without processing data. This replicates the `pruefeParameterGesetzt Jobkennung p_JobKennung` and `DWMSG_MeldeFehler` behavior.
*   **Setup:**
    1.  Ensure `project.dataset.job_table` and `project.dataset.error_log` are empty.
*   **Action:**
    1.  Execute the BigQuery Stored Procedure `project.dataset.r_ausd_ta_c_bfc` with a `NULL` or empty `p_jobkennung` and a valid `p_eintragsnr`.
        ```sql
        CALL `project.dataset.r_ausd_ta_c_bfc`(NULL, '12345');
        -- OR
        CALL `project.dataset.r_ausd_ta_c_bfc`('', '12345');
        ```
*   **Pass/Fail Criteria:**
    1.  The procedure call should fail with an error message indicating a missing `Jobkennung`.
    2.  An entry should be present in `project.dataset.error_log` with:
        *   `error_code = 193`
        *   `error_message` containing "Jobkennung is missing."
        *   `job_kennung` as `NULL` or empty, and `eintrags_nr` as '12345'.
    3.  No entries should be created in `project.dataset.job_table`.
    4.  No data processing should occur in `project.dataset.sof_ta_c_bfc` or `project.dataset.sof_ta_c_bfc_akt`.

    ```python
    # pytest assertion example
    def test_missing_jobkennung_parameter(bigquery_client):
        # Setup: Clear logs and job table
        bigquery_client.query("TRUNCATE TABLE `project.dataset.error_log`").result()
        bigquery_client.query("TRUNCATE TABLE `project.dataset.job_table`").result()

        # Action: Call procedure with missing jobkennung
        try:
            bigquery_client.query("CALL `project.dataset.r_ausd_ta_c_bfc`(NULL, '12345')").result()
            assert False, "Procedure should have failed for missing Jobkennung"
        except Exception as e:
            assert "Jobkennung is missing" in str(e)

        # Pass/Fail: Check error_log
        error_log_query = """
            SELECT error_code, error_message, job_kennung, eintrags_nr
            FROM `project.dataset.error_log`
            WHERE error_code = 193
            AND error_message LIKE '%Jobkennung is missing%'
            AND eintrags_nr = '12345'
        """
        errors = list(bigquery_client.query(error_log_query).result())
        assert len(errors) == 1, "Expected exactly one error log entry for missing Jobkennung"
        assert errors[0].error_code == 193
        assert errors[0].job_kennung is None or errors[0].job_kennung == ''

        # Pass/Fail: Check job_table (should be empty)
        job_table_count = bigquery_client.query("SELECT COUNT(1) FROM `project.dataset.job_table`").result().total_rows
        assert job_table_count == 0, "No entries expected in job_table for failed parameter validation"
    ```

---

### Test Case 2: Parameter Validation - Missing EintragsNr

*   **Purpose:** Verify that the migrated BigQuery Stored Procedure `r_ausd_ta_c_bfc` correctly handles missing `p_eintragsnr` parameters, logs the error, and exits gracefully. This replicates the `pruefeParameterGesetzt EintragsNr p_EintragsNr` behavior.
*   **Setup:**
    1.  Ensure `project.dataset.job_table` and `project.dataset.error_log` are empty.
*   **Action:**
    1.  Execute the BigQuery Stored Procedure `project.dataset.r_ausd_ta_c_bfc` with a valid `p_jobkennung` and a `NULL` or empty `p_eintragsnr`.
        ```sql
        CALL `project.dataset.r_ausd_ta_c_bfc`('TEST_JOB', NULL);
        -- OR
        CALL `project.dataset.r_ausd_ta_c_bfc`('TEST_JOB', '');
        ```
*   **Pass/Fail Criteria:**
    1.  The procedure call should fail with an error message indicating a missing `EintragsNr`.
    2.  An entry should be present in `project.dataset.error_log` with:
        *   `error_code = 193`
        *   `error_message` containing "EintragsNr is missing."
        *   `job_kennung` as 'TEST_JOB', and `eintrags_nr` as `NULL` or empty.
    3.  No entries should be created in `project.dataset.job_table`.
    4.  No data processing should occur.

---

### Test Case 3: Job Control - Ignore Already Running Job

*   **Purpose:** Verify that the migrated procedure correctly identifies and ignores a job if an instance with the same `p_jobkennung` and `p_eintragsnr` is already marked as 'RUNNING' in `job_table`. This replicates the "aktive Jobs werden ignoriert" logic.
*   **Setup:**
    1.  Ensure `project.dataset.job_table` and `project.dataset.error_log` are empty.
    2.  Insert a 'RUNNING' entry into `project.dataset.job_table` for the specific job instance.
        ```sql
        INSERT INTO `project.dataset.job_table` (run_id, job_kennung, eintrags_nr, start_time, status, message)
        VALUES ('existing_run_id', 'TEST_JOB', '12345', CURRENT_TIMESTAMP(), 'RUNNING', 'Initial run');
        ```
*   **Action:**
    1.  Execute `project.dataset.r_ausd_ta_c_bfc` with `p_jobkennung = 'TEST_JOB'` and `p_eintragsnr = '12345'`.
        ```sql
        CALL `project.dataset.r_ausd_ta_c_bfc`('TEST_JOB', '12345');
        ```
*   **Pass/Fail Criteria:**
    1.  The procedure should complete successfully (not raise an error).
    2.  The output message from the procedure should indicate that the job was ignored.
    3.  A new entry should be created in `project.dataset.job_table` with:
        *   `job_kennung = 'TEST_JOB'`
        *   `eintrags_nr = '12345'`
        *   `status = 'IGNORED'`
        *   `message` indicating it was ignored.
    4.  The original 'RUNNING' entry should remain unchanged.
    5.  No entries should be created in `project.dataset.error_log`.
    6.  No data processing should occur in `project.dataset.sof_ta_c_bfc` or `project.dataset.sof_ta_c_bfc_akt`.

    ```python
    # pytest assertion example
    def test_job_control_ignore_running_job(bigquery_client):
        # Setup: Clear tables and insert a running job
        bigquery_client.query("TRUNCATE TABLE `project.dataset.error_log`").result()
        bigquery_client.query("TRUNCATE TABLE `project.dataset.job_table`").result()
        bigquery_client.query("""
            INSERT INTO `project.dataset.job_table` (run_id, job_kennung, eintrags_nr, start_time, status, message)
            VALUES ('existing_run_id_1', 'TEST_JOB', '12345', CURRENT_TIMESTAMP(), 'RUNNING', 'Initial run');
        """).result()

        # Action: Call procedure
        result = bigquery_client.query("CALL `project.dataset.r_ausd_ta_c_bfc`('TEST_JOB', '12345')").result()
        # Check output message (if BigQuery allows capturing procedure output easily)
        # For now, rely on table checks.

        # Pass/Fail: Check job_table for IGNORED entry
        ignored_job_query = """
            SELECT run_id, job_kennung, eintrags_nr, status, message
            FROM `project.dataset.job_table`
            WHERE job_kennung = 'TEST_JOB' AND eintrags_nr = '12345' AND status = 'IGNORED'
        """
        ignored_jobs = list(bigquery_client.query(ignored_job_query).result())
        assert len(ignored_jobs) == 1, "Expected one IGNORED job entry"
        assert "already running. Ignoring." in ignored_jobs[0].message

        # Pass/Fail: Check original RUNNING entry is untouched
        running_job_query = """
            SELECT run_id, status
            FROM `project.dataset.job_table`
            WHERE run_id = 'existing_run_id_1'
        """
        running_jobs = list(bigquery_client.query(running_job_query).result())
        assert len(running_jobs) == 1, "Original RUNNING job entry should still exist"
        assert running_jobs[0].status == 'RUNNING'

        # Pass/Fail: error_log should be empty
        error_log_count = bigquery_client.query("SELECT COUNT(1) FROM `project.dataset.error_log`").result().total_rows
        assert error_log_count == 0, "No errors expected when job is ignored"
    ```

---

### Test Case 4: Job Control - Deactivate Old Active Jobs

*   **Purpose:** Verify that a new successful run of the procedure deactivates any *other* existing 'RUNNING' jobs for the same `job_kennung` and `eintrags_nr`. This replicates the "alte aktive Jobs werden einfach dekativiert" logic.
*   **Setup:**
    1.  Ensure `project.dataset.job_table` and `project.dataset.error_log` are empty.
    2.  Insert one or more 'RUNNING' entries into `project.dataset.job_table` for the specific job instance, with different `run_id`s.
        ```sql
        INSERT INTO `project.dataset.job_table` (run_id, job_kennung, eintrags_nr, start_time, status, message)
        VALUES
            ('old_run_id_1', 'TEST_JOB', '12345', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), 'RUNNING', 'Old run 1'),
            ('old_run_id_2', 'TEST_JOB', '12345', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE), 'RUNNING', 'Old run 2'),
            ('unrelated_run_id', 'OTHER_JOB', '67890', CURRENT_TIMESTAMP(), 'RUNNING', 'Unrelated job');
        ```
    3.  Populate source tables (`sof_ta_cntrct_crs`, etc.) with minimal valid data to allow the core logic to run successfully.
    4.  Ensure `project.dataset.sof_ta_c_bfc` and `project.dataset.sof_ta_c_bfc_akt` are empty or in a known state.
*   **Action:**
    1.  Execute `project.dataset.r_ausd_ta_c_bfc` with `p_jobkennung = 'TEST_JOB'` and `p_eintragsnr = '12345'`.
        ```sql
        CALL `project.dataset.r_ausd_ta_c_bfc`('TEST_JOB', '12345');
        ```
*   **Pass/Fail Criteria:**
    1.  The procedure should complete successfully with `status = 'SUCCESS'`.
    2.  A new entry should be created in `project.dataset.job_table` with:
        *   `job_kennung = 'TEST_JOB'`
        *   `eintrags_nr = '12345'`
        *   `status = 'SUCCESS'`
        *   `processed_records` > 0.
    3.  The entries for `old_run_id_1` and `old_run_id_2` in `project.dataset.job_table` should be updated to:
        *   `status = 'DEACTIVATED'`
        *   `end_time` should be set.
        *   `message` should indicate deactivation by the new run.
    4.  The `unrelated_run_id` entry should remain 'RUNNING'.
    5.  No entries should be created in `project.dataset.error_log` (unless deactivation itself fails, which should be logged but not stop the main job).
    6.  Data processing should have occurred, resulting in new/updated records in `project.dataset.sof_ta_c_bfc`.

---

### Test Case 5: Output Parity - End-to-End Data Transformation

*   **Purpose:** Verify that the final state of the target table `project.dataset.sof_ta_c_bfc` in BigQuery is identical to the legacy Oracle `sof$ta_c_bfc` after a full run. This is the ultimate output parity test.
*   **Setup:**
    1.  **Data Replication:** Ensure all BigQuery source tables (`sof_ta_cntrct_crs`, `sof_ta_barrier`, `sof_ta_cntrct_valid`, `sof_ta_period`) contain an exact, byte-for-byte replica of the corresponding Oracle source tables. This is crucial.
    2.  **Initial State:** Ensure `project.dataset.sof_ta_c_bfc` in BigQuery is in the same initial state (empty or pre-populated) as `sof$ta_c_bfc` in Oracle before the legacy job runs.
    3.  Clear `project.dataset.job_table` and `project.dataset.error_log`.
*   **Action:**
    1.  Execute the legacy KornShell script `k_ausd_v_ta_c_bfc.ksh` with a specific `p_JobKennung` and `p_EintragsNr` on the Oracle environment.
    2.  Execute the BigQuery Stored Procedure `project.dataset.r_ausd_ta_c_bfc` with the *same* `p_jobkennung` and `p_eintragsnr` on the BigQuery environment.
        ```sql
        CALL `project.dataset.r_ausd_ta_c_bfc`('PROD_JOB', 'CURRENT_DATE');
        ```
*   **Pass/Fail Criteria:**
    1.  The `project.dataset.job_table` should show a 'SUCCESS' entry for the BigQuery run.
    2.  The number of rows in `project.dataset.sof_ta_c_bfc` should be identical to the number of rows in the legacy Oracle `sof$ta_c_bfc`.
    3.  A deep comparison of all columns and rows in `project.dataset.sof_ta_c_bfc` and legacy Oracle `sof$ta_c_bfc` should reveal no differences. This can be done by:
        *   Exporting both tables to CSV/JSON and comparing.
        *   Using a SQL `EXCEPT` or `MINUS` query if cross-database querying is possible (e.g., via federated queries or data transfer).
        *   A BigQuery-only comparison:
            ```sql
            -- Assuming a snapshot or external table of legacy Oracle data is available in BQ
            -- Or, if comparing against a known good state in BQ after a previous run
            SELECT * FROM `project.dataset.sof_ta_c_bfc`
            EXCEPT DISTINCT
            SELECT * FROM `project.dataset.legacy_oracle_sof_ta_c_bfc_snapshot`;

            SELECT * FROM `project.dataset.legacy_oracle_sof_ta_c_bfc_snapshot`
            EXCEPT DISTINCT
            SELECT * FROM `project.dataset.sof_ta_c_bfc`;
            ```
            Both queries should return 0 rows.
    4.  The `processed_records` count in `project.dataset.job_table` should match the count reported by the legacy job (e.g., from its logs or temporary file).

---

### Test Case 6: Transformation Correctness - `d_ausd_v_ta_c_bfc_core_logic` (Joins, Aggregations, `GREATEST`, `COALESCE`)

*   **Purpose:** Verify the correctness of the initial population of `project.dataset.sof_ta_c_bfc_akt` within `d_ausd_v_ta_c_bfc_core_logic`, specifically the complex joins, `MAX` aggregations, `GREATEST` function, and `COALESCE` for NULL handling.
*   **Setup:**
    1.  Populate BigQuery source tables (`sof_ta_cntrct_crs`, `sof_ta_barrier`, `sof_ta_cntrct_valid`, `sof_ta_period`) with a diverse dataset, including:
        *   Contracts with all related tables (`barrier`, `cntrct_valid`, `period`) present.
        *   Contracts with some related tables missing (to test `LEFT JOIN` and `COALESCE`).
        *   Data where `bfc_age` values are `NULL` or have different dates across joined tables (to test `GREATEST` and `COALESCE`).
        *   Duplicate `cntrct_id` in `sof_ta_cntrct_crs` to test `GROUP BY` and `MAX`.
    2.  Ensure `project.dataset.sof_ta_c_bfc_akt` is empty.
*   **Action:**
    1.  Execute `project.dataset.d_ausd_v_ta_c_bfc_core_logic` directly (or via `r_ausd_ta_c_bfc` if easier for testing).
        ```sql
        DECLARE v_records INT64;
        CALL `project.dataset.d_ausd_v_ta_c_bfc_core_logic`('test_run_id', 'TEST_JOB', '123', v_records);
        ```
    2.  Execute the equivalent SQL from `d_ausd_v_ta_c_bfc.sql` (the `INSERT INTO sof$ta_c_bfc_akt` part) against the Oracle environment with the same data.
*   **Pass/Fail Criteria:**
    1.  The number of rows in `project.dataset.sof_ta_c_bfc_akt` should match the number of rows in the Oracle `sof$ta_c_bfc_akt` after the equivalent step.
    2.  A row-by-row and column-by-column comparison of `project.dataset.sof_ta_c_bfc_akt` and Oracle `sof$ta_c_bfc_akt` should show exact parity. Pay close attention to `bfc_age` (result of `GREATEST/COALESCE`) and `bfc_count` (result of `COUNT(1)`).
    3.  Verify that `NULL` values in source tables are correctly handled by `COALESCE` and `GREATEST` as per Oracle's behavior (e.g., `GREATEST` with `NULL`s might behave differently between databases if not explicitly handled).

---

### Test Case 7: Transformation Correctness - `MERGE` Logic (Update/Insert Conditions)

*   **Purpose:** Verify that the `MERGE` statement in `d_ausd_v_ta_c_bfc_core_logic` correctly applies `UPDATE` and `INSERT` logic based on the specified conditions (`bfc_age < S.bfc_age` OR `bfc_count <> S.bfc_count`).
*   **Setup:**
    1.  Populate `project.dataset.sof_ta_c_bfc_akt` with data representing new or updated contracts.
    2.  Populate `project.dataset.sof_ta_c_bfc` with existing contract data, ensuring scenarios for:
        *   **New Contracts:** `cntrct_id` in `sof_ta_c_bfc_akt` but not in `sof_ta_c_bfc`.
        *   **Updated Contracts (bfc_age):** `cntrct_id` in both, `S.bfc_age` > `D.bfc_age`.
        *   **Updated Contracts (bfc_count):** `cntrct_id` in both, `S.bfc_count` != `D.bfc_count`.
        *   **Unchanged Contracts:** `cntrct_id` in both, `S.bfc_age` <= `D.bfc_age` AND `S.bfc_count` = `D.bfc_count`.
    3.  Ensure `project.dataset.bfc_get_bindefrist` returns predictable values for testing.
*   **Action:**
    1.  Execute the `MERGE` part of `d_ausd_v_ta_c_bfc_core_logic` (or the full procedure if easier).
    2.  Execute the equivalent Oracle `MERGE` statement with the same data.
*   **Pass/Fail Criteria:**
    1.  The final state of `project.dataset.sof_ta_c_bfc` should exactly match the final state of Oracle `sof$ta_c_bfc`.
    2.  Specifically, verify:
        *   New contracts are `INSERTED`.
        *   Contracts meeting the `UPDATE` conditions are correctly `UPDATED` (all specified columns, including `bindefrist` and `bfc_procedure`).
        *   Contracts not meeting `UPDATE` or `INSERT` conditions remain unchanged.

---

### Test Case 8: Transformation Correctness - `bfc_get_bindefrist` Function Integration

*   **Purpose:** Verify that the `project.dataset.bfc_get_bindefrist` function is correctly invoked and its return value is used in the `MERGE` and `UPDATE` statements within `d_ausd_v_ta_c_bfc_core_logic`.
*   **Setup:**
    1.  Populate `sof_ta_c_bfc_akt` and `sof_ta_c_bfc` with data that will trigger both `INSERT` and `UPDATE` paths in the `MERGE` statement, and also the final `UPDATE` statement.
    2.  Ensure `project.dataset.bfc_get_bindefrist` is implemented and returns known values for specific inputs.
*   **Action:**
    1.  Execute `project.dataset.d_ausd_v_ta_c_bfc_core_logic`.
*   **Pass/Fail Criteria:**
    1.  After execution, query `project.dataset.sof_ta_c_bfc` and verify that the `bindefrist` column for affected rows contains the expected values as if `project.dataset.bfc_get_bindefrist` was called with the correct arguments from `sof_ta_c_bfc_akt` (for `MERGE`) or `sof_ta_c_bfc` (for `UPDATE`).
    2.  Compare these `bindefrist` values against the legacy Oracle output.

---

### Test Case 9: Transformation Correctness - `UPDATE` with `LIMIT` and `bfc_procedure`

*   **Purpose:** Verify the `UPDATE` statement that processes `bindefristen` not yet calculated, specifically its `WHERE` clause (`bfc_procedure < '%s'`) and the `LIMIT %d` clause.
*   **Setup:**
    1.  Populate `project.dataset.sof_ta_c_bfc` with a mix of data:
        *   Rows where `bfc_procedure` is older than `CURRENT_DATE()` (or the `v_bfc_procedure` used in the SP).
        *   Rows where `bfc_procedure` is `CURRENT_DATE()`.
        *   Rows where `bindefrist` is `NULL` but `bfc_procedure` is old.
    2.  Ensure there are more rows meeting the `WHERE` condition than the `v_max_update` limit (e.g., 1000000).
*   **Action:**
    1.  Execute `project.dataset.d_ausd_v_ta_c_bfc_core_logic`.
*   **Pass/Fail Criteria:**
    1.  Only rows where `bfc_procedure` was older than the current run's `v_bfc_procedure` should be considered for update.
    2.  Exactly `v_max_update` (or fewer if fewer match the `WHERE` clause) rows should have their `bindefrist` and `bfc_procedure` updated.
    3.  The `bfc_procedure` column for these updated rows should be set to the `v_bfc_procedure` of the current run.
    4.  Compare the updated rows against the legacy Oracle behavior for this specific `UPDATE` statement.

---

### Test Case 10: Data Quality / Row Count / Schema Assertions

*   **Purpose:** Verify the schema, data types, NULLability, and row counts of the control and target tables.
*   **Setup:**
    1.  Run `project.dataset.r_ausd_ta_c_bfc` successfully with a representative dataset.
*   **Action:**
    1.  Query the schema and row counts of `job_table`, `error_log`, `sof_ta_c_bfc_akt`, and `sof_ta_c_bfc`.
*   **Pass/Fail Criteria:**
    1.  **Schema and Data Types:**
        *   `project.dataset.job_table`:
            *   `run_id` (STRING, NOT NULL)
            *   `job_kennung` (STRING, NOT NULL)
            *   `eintrags_nr` (STRING, NOT NULL)
            *   `start_time` (TIMESTAMP, NOT NULL)
            *   `end_time` (TIMESTAMP, NULLABLE)
            *   `status` (STRING, NOT NULL)
            *   `message` (STRING, NULLABLE)
            *   `processed_records` (INT64, NULLABLE)
        *   `project.dataset.error_log`:
            *   `timestamp` (TIMESTAMP, NOT NULL)
            *   `run_id` (STRING, NOT NULL)
            *   `job_kennung` (STRING, NOT NULL)
            *   `eintrags_nr` (STRING, NOT NULL)
            *   `error_code` (INT64, NULLABLE)
            *   `error_message` (STRING, NOT NULL)
        *   `project.dataset.sof_ta_c_bfc` and `project.dataset.sof_ta_c_bfc_akt`: Verify that the schema (column names, data types, NULLability) matches the legacy Oracle tables after migration, especially for `bindefrist`, `bfc_age`, `bfc_count`, `bfc_procedure`, `commitment_reference_date`, `cntrct_validity_id`.
    2.  **Row Counts:**
        *   `project.dataset.job_table`: Should contain at least one 'SUCCESS' entry for the test run.
        *   `project.dataset.error_log`: Should be empty for a successful run.
        *   `project.dataset.sof_ta_c_bfc_akt`: Should be empty after the procedure completes (due to `TRUNCATE TABLE`).
        *   `project.dataset.sof_ta_c_bfc`: The final row count should match the legacy Oracle table for the same input data.
    3.  **NULL Handling:** Query columns that are expected to be NULLable (e.g., `end_time` in `job_table` for a running job, `message` in `job_table` if not set, `error_code` in `error_log` if not explicitly provided) and verify they correctly store `NULL`.

    ```python
    # pytest assertion example for schema
    def test_table_schemas(bigquery_client):
        # Define expected schemas (simplified for brevity)
        expected_job_table_schema = {
            "run_id": ("STRING", "REQUIRED"),
            "job_kennung": ("STRING", "REQUIRED"),
            "eintrags_nr": ("STRING", "REQUIRED"),
            "start_time": ("TIMESTAMP", "REQUIRED"),
            "end_time": ("TIMESTAMP", "NULLABLE"),
            "status": ("STRING", "REQUIRED"),
            "message": ("STRING", "NULLABLE"),
            "processed_records": ("INT64", "NULLABLE"),
        }
        # ... define for error_log, sof_ta_c_bfc_akt, sof_ta_c_bfc

        def get_bq_schema(table_id):
            table = bigquery_client.get_table(table_id)
            schema = {}
            for field in table.schema:
                schema[field.name] = (field.field_type, field.mode)
            return schema

        # Assert job_table schema
        job_table_schema = get_bq_schema("project.dataset.job_table")
        assert job_table_schema == expected_job_table_schema

        # Assert row counts after a successful run (assuming test_output_parity_full_run has been executed)
        job_table_count = bigquery_client.query("SELECT COUNT(1) FROM `project.dataset.job_table` WHERE status = 'SUCCESS'").result().total_rows
        assert job_table_count >= 1, "Expected at least one successful job entry"

        error_log_count = bigquery_client.query("SELECT COUNT(1) FROM `project.dataset.error_log`").result().total_rows
        assert error_log_count == 0, "Expected error_log to be empty for a successful run"

        sof_ta_c_bfc_akt_count = bigquery_client.query("SELECT COUNT(1) FROM `project.dataset.sof_ta_c_bfc_akt`").result().total_rows
        assert sof_ta_c_bfc_akt_count == 0, "Expected sof_ta_c_bfc_akt to be empty after procedure completion"

        # Compare sof_ta_c_bfc count with legacy (requires a way to get legacy count)
        # legacy_sof_ta_c_bfc_count = get_legacy_oracle_count("sof$ta_c_bfc")
        # bq_sof_ta_c_bfc_count = bigquery_client.query("SELECT COUNT(1) FROM `project.dataset.sof_ta_c_bfc`").result().total_rows
        # assert bq_sof_ta_c_bfc_count == legacy_sof_ta_c_bfc_count
    ```

---

### Test Case 11: External System Replacements - Record Count

*   **Purpose:** Verify that the `processed_records` in `project.dataset.job_table` accurately reflects the number of records processed by the core logic, replacing the legacy temporary file mechanism.
*   **Setup:**
    1.  Populate source tables with a known number of records that will be processed by `d_ausd_v_ta_c_bfc_core_logic`.
    2.  Ensure `project.dataset.job_table` is empty.
*   **Action:**
    1.  Execute `project.dataset.r_ausd_ta_c_bfc` with valid parameters.
*   **Pass/Fail Criteria:**
    1.  After the procedure completes successfully, query `project.dataset.job_table` for the `processed_records` value of the latest run.
    2.  This `processed_records` value should match the actual number of rows inserted/updated in `project.dataset.sof_ta_c_bfc` by the `d_ausd_v_ta_c_bfc_core_logic` procedure.
    3.  This value should also match the `v_records` value that the legacy KornShell script would have read from its temporary file.

---

### Test Case 12: Error Handling - Core Logic Failure

*   **Purpose:** Verify that if `d_ausd_v_ta_c_bfc_core_logic` encounters an error (e.g., due to bad data, schema mismatch, or a simulated error), `r_ausd_ta_c_bfc` correctly logs the error, updates the job status to 'FAILED', and raises an appropriate message.
*   **Setup:**
    1.  Ensure `project.dataset.job_table` and `project.dataset.error_log` are empty.
    2.  **Simulate Error:** Temporarily modify `d_ausd_v_ta_c_bfc_core_logic` to intentionally cause an error (e.g., try to insert a string into an INT64 column, or divide by zero if applicable to the logic).
*   **Action:**
    1.  Execute `project.dataset.r_ausd_ta_c_bfc` with valid parameters.
*   **Pass/Fail Criteria:**
    1.  The procedure call should fail and raise an error.
    2.  An entry should be present in `project.dataset.error_log` with:
        *   `error_code = -3` (or the specific error code from the core logic)
        *   `error_message` reflecting the core logic failure.
    3.  An entry should be present in `project.dataset.job_table` with:
        *   `status = 'FAILED'`
        *   `end_time` set.
        *   `message` reflecting the core logic failure.
    4.  No data processing should have completed successfully in `project.dataset.sof_ta_c_bfc`.

---

These tests cover the critical aspects of the migration, from orchestration and job control to the core data transformation and error handling. The emphasis on comparing against legacy behavior and data ensures that the migrated solution is a true behavioral equivalent.