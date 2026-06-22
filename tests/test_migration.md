As a senior data-migration QA engineer, I've analyzed the provided migration design and generated BigQuery code for `k_ausd_v_ta_notice.ksh`. The migration focuses on translating the KornShell orchestration into BigQuery stored procedures, with the core data transformation logic from `d_ausd_v_ta_notice.sql` encapsulated in `sp_d_ausd_v_ta_notice`.

A critical observation is that while the original KornShell script's purpose explicitly mentions "aktive Jobs werden ignoriert" and "alte aktive Jobs werden einfach dekativiert", and the migration design lists "Logic to deactivate old active jobs" and "Logic to ignore currently active jobs" as conceptual steps for `sp_d_ausd_v_ta_notice`, the provided generated `sp_d_ausd_v_ta_notice` *does not implement this logic*. Instead, it performs a `TRUNCATE` followed by an `INSERT`. This represents a significant behavioral difference and a potential functional gap in the migrated code. I will include a test case to highlight this discrepancy.

The following test cases are designed to validate the migrated BigQuery stored procedures against the specified requirements, focusing on the orchestration logic (`sp_control_ta_notice`) and the assumed behavior of the core data transformation (`sp_d_ausd_v_ta_notice`).

---

## Migration Validation Tests for `k_ausd_v_ta_notice.ksh`

**Test Environment Setup:**

Before running these tests, ensure the following BigQuery components are created in your designated `your_project_id.your_dataset_id`:

1.  **Tables:**
    *   `job_error_log`
    *   `job_run_log`
    *   `job_run_result`
    *   `ta_notice`
    *   `cds_ta_notice` (source table for `sp_d_ausd_v_ta_notice`)
    *   `dwtk_meldungen` (source table for `v_process_date` in `sp_control_ta_notice`)

    ```sql
    -- DDL for cds_ta_notice (mock source table)
    CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.cds_ta_notice` (
        cntrct_id INT64 NOT NULL, -- Assuming original source might be numeric
        valid_from DATE,
        valid_to DATE,
        entry_date_of_notice DATE,
        insert_at TIMESTAMP,
        modified_at TIMESTAMP,
        is_production INT64
    );

    -- DDL for dwtk_meldungen (mock source table)
    CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.dwtk_meldungen` (
        job_kennung STRING,
        timecreated TIMESTAMP,
        -- other columns as needed for context, but not directly used by this job
        message STRING
    );

    -- Ensure all other DDLs from the generated code are applied:
    -- job_error_log, job_run_log, job_run_result, ta_notice
    ```

2.  **Stored Procedures:**
    *   `sp_d_ausd_v_ta_notice`
    *   `sp_control_ta_notice`

    ```sql
    -- Ensure sp_d_ausd_v_ta_notice and sp_control_ta_notice are created as per the generated code.
    ```

---

### Test Case 1: Successful Execution with Valid Parameters

*   **Purpose:** Verify that `sp_control_ta_notice` executes successfully with valid parameters, logs job status correctly, calls the data transformation procedure, and records the processed row count. This covers output parity for logging and transformation correctness for the overall flow.
*   **Setup:**
    1.  Clear all logging tables (`job_run_log`, `job_error_log`, `job_run_result`) and the target table (`ta_notice`).
    2.  Populate `cds_ta_notice` with sample data that should be processed.
    3.  Populate `dwtk_meldungen` to ensure a specific `p_process_date` is derived.

    ```python
    # pytest-style setup
    def setup_successful_run_data(bq_client, project_id, dataset_id):
        # Clear tables
        bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_run_log`").result()
        bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_error_log`").result()
        bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_run_result`").result()
        bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.ta_notice`").result()
        bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.cds_ta_notice`").result()
        bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.dwtk_meldungen`").result()

        # Insert mock data into cds_ta_notice
        # Expected to be processed for p_process_date = '2023-01-15'
        bq_client.query(f"""
            INSERT INTO `{project_id}.{dataset_id}.cds_ta_notice` (cntrct_id, valid_from, valid_to, entry_date_of_notice, insert_at, modified_at, is_production) VALUES
            (101, '2022-01-01', '2024-12-31', '2022-01-01', '2023-01-10 10:00:00 UTC', NULL, 1), -- Included (insert_at <= PDATE, modified_at IS NULL, valid_to > PDATE, is_production = 1)
            (102, '2022-02-01', NULL, '2022-02-01', '2023-01-12 11:00:00 UTC', NULL, 1), -- Included (valid_to IS NULL)
            (103, '2022-03-01', '2023-01-10', '2022-03-01', '2023-01-14 12:00:00 UTC', NULL, 1), -- Excluded (valid_to <= PDATE)
            (104, '2022-04-01', '2024-12-31', '2022-04-01', '2023-01-16 13:00:00 UTC', NULL, 1), -- Excluded (insert_at > PDATE)
            (105, '2022-05-01', '2024-12-31', '2022-05-01', '2023-01-05 14:00:00 UTC', '2023-01-10 15:00:00 UTC', 1), -- Excluded (modified_at <= PDATE)
            (106, '2022-06-01', '2024-12-31', '2022-06-01', '2023-01-08 16:00:00 UTC', '2023-01-20 17:00:00 UTC', 1), -- Included (modified_at > PDATE)
            (107, '2022-07-01', '2024-12-31', '2022-07-01', '2023-01-09 18:00:00 UTC', NULL, 0)  -- Excluded (is_production = 0)
        """).result()

        # Insert mock data into dwtk_meldungen to set p_process_date to '2023-01-15'
        bq_client.query(f"""
            INSERT INTO `{project_id}.{dataset_id}.dwtk_meldungen` (job_kennung, timecreated, message) VALUES
            ('OTHER_JOB', '2023-01-01 00:00:00 UTC', 'Some other job'),
            ('BERT_DROP_TEMP_TABLE', '2023-01-15 08:00:00 UTC', 'Temp table dropped'),
            ('BERT_DROP_TEMP_TABLE', '2023-01-10 09:00:00 UTC', 'Another temp table dropped')
        """).result()

        return "JOB_KENNUNG_001", "ENTRY_001", '2023-01-15', 3 # Expected records
    ```
*   **Action:** Call `sp_control_ta_notice` with the defined `p_job_kennung` and `p_eintrags_nr`.

    ```python
    # pytest-style action
    def test_successful_execution(bq_client, project_id, dataset_id):
        job_kennung, eintrags_nr, expected_process_date, expected_records = setup_successful_run_data(bq_client, project_id, dataset_id)

        bq_client.query(f"""
            CALL `{project_id}.{dataset_id}.sp_control_ta_notice`(
                p_job_kennung => '{job_kennung}',
                p_eintrags_nr => '{eintrags_nr}'
            )
        """).result()
    ```
*   **Pass/Fail Criteria:**
    1.  **`job_run_log` entries:** Two entries exist for `job_kennung='JOB_KENNUNG_001'` and `eintrags_nr='ENTRY_001'`, one with `status='STARTED'` and one with `status='COMPLETED'`.
        ```sql
        SELECT COUNT(*) FROM `your_project_id.your_dataset_id.job_run_log`
        WHERE job_kennung = 'JOB_KENNUNG_001' AND eintrags_nr = 'ENTRY_001' AND status IN ('STARTED', 'COMPLETED');
        -- Expected: 2
        ```
    2.  **`job_error_log` empty:** No entries for this run.
        ```sql
        SELECT COUNT(*) FROM `your_project_id.your_dataset_id.job_error_log`
        WHERE job_kennung = 'JOB_KENNUNG_001' AND eintrags_nr = 'ENTRY_001';
        -- Expected: 0
        ```
    3.  **`job_run_result` record count:** One entry exists with `record_count` matching `expected_records`.
        ```sql
        SELECT record_count FROM `your_project_id.your_dataset_id.job_run_result`
        WHERE job_kennung = 'JOB_KENNUNG_001' AND eintrags_nr = 'ENTRY_001';
        -- Expected: 3
        ```
    4.  **`ta_notice` content:** Contains the expected 3 rows, with `cntrct_id` as `STRING`.
        ```sql
        SELECT cntrct_id, valid_from, valid_to, entry_date_of_notice, insert_at, modified_at, is_production
        FROM `your_project_id.your_dataset_id.ta_notice`
        ORDER BY cntrct_id;
        -- Expected:
        -- cntrct_id | valid_from | valid_to   | entry_date_of_notice | insert_at                 | modified_at               | is_production
        -- ----------|------------|------------|----------------------|---------------------------|---------------------------|--------------
        -- '101'     | '2022-01-01'| '2024-12-31'| '2022-01-01'         | '2023-01-10 10:00:00 UTC' | NULL                      | 1
        -- '102'     | '2022-02-01'| NULL       | '2022-02-01'         | '2023-01-12 11:00:00 UTC' | NULL                      | 1
        -- '106'     | '2022-06-01'| '2024-12-31'| '2022-06-01'         | '2023-01-08 16:00:00 UTC' | '2023-01-20 17:00:00 UTC' | 1
        ```
    5.  **`ta_notice.cntrct_id` type:** Assert that `cntrct_id` is of type `STRING`.
        ```sql
        SELECT data_type FROM `your_project_id.your_dataset_id.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'ta_notice' AND column_name = 'cntrct_id';
        -- Expected: 'STRING'
        ```

---

### Test Case 2: Parameter Validation - Missing `p_job_kennung`

*   **Purpose:** Verify that the procedure correctly handles a missing or empty `p_job_kennung` parameter, logs an error, and marks the job as FAILED. This covers transformation correctness for parameter handling and error handling.
*   **Setup:**
    1.  Clear all logging tables.
    2.  Define `p_eintrags_nr = 'ENTRY_002'`.
*   **Action:** Attempt to call `sp_control_ta_notice` with `p_job_kennung = NULL` and a valid `p_eintrags_nr`.

    ```python
    # pytest-style action
    import pytest
    def test_missing_job_kennung(bq_client, project_id, dataset_id):
        bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_run_log`").result()
        bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_error_log`").result()
        bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_run_result`").result()

        eintrags_nr = 'ENTRY_002'
        with pytest.raises(Exception) as excinfo:
            bq_client.query(f"""
                CALL `{project_id}.{dataset_id}.sp_control_ta_notice`(
                    p_job_kennung => NULL,
                    p_eintrags_nr => '{eintrags_nr}'
                )
            """).result()
        assert "Parameter p_job_kennung cannot be NULL or empty." in str(excinfo.value)
    ```
*   **Pass/Fail Criteria:**
    1.  **Error raised:** The call to `sp_control_ta_notice` raises an error containing the message "Parameter p_job_kennung cannot be NULL or empty."
    2.  **`job_run_log` entries:** Two entries exist for `eintrags_nr='ENTRY_002'`, one with `status='STARTED'` and one with `status='FAILED'`.
        ```sql
        SELECT COUNT(*) FROM `your_project_id.your_dataset_id.job_run_log`
        WHERE eintrags_nr = 'ENTRY_002' AND status IN ('STARTED', 'FAILED');
        -- Expected: 2
        ```
    3.  **`job_error_log` entry:** One entry exists for `eintrags_nr='ENTRY_002'`, with `err_nr = -1` and `err_arg` containing the validation error message.
        ```sql
        SELECT err_nr, err_arg FROM `your_project_id.your_dataset_id.job_error_log`
        WHERE eintrags_nr = 'ENTRY_002';
        -- Expected: err_nr = -1, err_arg LIKE '%Parameter p_job_kennung cannot be NULL or empty.%'
        ```
    4.  **`job_run_result` empty:** No entries for this run.
        ```sql
        SELECT COUNT(*) FROM `your_project_id.your_dataset_id.job_run_result`
        WHERE eintrags_nr = 'ENTRY_002';
        -- Expected: 0
        ```

---

### Test Case 3: Parameter Validation - Missing `p_eintrags_nr`

*   **Purpose:** Verify that the procedure correctly handles a missing or empty `p_eintrags_nr` parameter, logs an error, and marks the job as FAILED. This covers transformation correctness for parameter handling and error handling.
*   **Setup:**
    1.  Clear all logging tables.
    2.  Define `p_job_kennung = 'JOB_KENNUNG_003'`.
*   **Action:** Attempt to call `sp_control_ta_notice` with a valid `p_job_kennung` and `p_eintrags_nr = ''` (empty string).

    ```python
    # pytest-style action
    import pytest
    def test_missing_eintrags_nr(bq_client, project_id, dataset_id):
        bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_run_log`").result()
        bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_error_log`").result()
        bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_run_result`").result()

        job_kennung = 'JOB_KENNUNG_003'
        with pytest.raises(Exception) as excinfo:
            bq_client.query(f"""
                CALL `{project_id}.{dataset_id}.sp_control_ta_notice`(
                    p_job_kennung => '{job_kennung}',
                    p_eintrags_nr => ''
                )
            """).result()
        assert "Parameter p_eintrags_nr cannot be NULL or empty." in str(excinfo.value)
    ```
*   **Pass/Fail Criteria:**
    1.  **Error raised:** The call to `sp_control_ta_notice` raises an error containing the message "Parameter p_eintrags_nr cannot be NULL or empty."
    2.  **`job_run_log` entries:** Two entries exist for `job_kennung='JOB_KENNUNG_003'`, one with `status='STARTED'` and one with `status='FAILED'`.
        ```sql
        SELECT COUNT(*) FROM `your_project_id.your_dataset_id.job_run_log`
        WHERE job_kennung = 'JOB_KENNUNG_003' AND status IN ('STARTED', 'FAILED');
        -- Expected: 2
        ```
    3.  **`job_error_log` entry:** One entry exists for `job_kennung='JOB_KENNUNG_003'`, with `err_nr = -1` and `err_arg` containing the validation error message.
        ```sql
        SELECT err_nr, err_arg FROM `your_project_id.your_dataset_id.job_error_log`
        WHERE job_kennung = 'JOB_KENNUNG_003';
        -- Expected: err_nr = -1, err_arg LIKE '%Parameter p_eintrags_nr cannot be NULL or empty.%'
        ```
    4.  **`job_run_result` empty:** No entries for this run.
        ```sql
        SELECT COUNT(*) FROM `your_project_id.your_dataset_id.job_run_result`
        WHERE job_kennung = 'JOB_KENNUNG_003';
        -- Expected: 0
        ```

---

### Test Case 4: Error During Core Data Transformation (`sp_d_ausd_v_ta_notice`)

*   **Purpose:** Verify that if `sp_d_ausd_v_ta_notice` encounters an error, `sp_control_ta_notice` correctly catches it, logs the error, and marks the job as FAILED. This covers error handling and external system replacements (interaction between SPs).
*   **Setup:**
    1.  Clear all logging tables and `ta_notice`.
    2.  Populate `dwtk_meldungen` to derive a valid `p_process_date`.
    3.  **Simulate an error in `sp_d_ausd_v_ta_notice`:** For testing, temporarily modify `sp_d_ausd_v_ta_notice` to `RAISE` an error immediately after `TRUNCATE` or before `INSERT`.
        ```sql
        -- TEMPORARY MODIFICATION FOR TESTING:
        CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.sp_d_ausd_v_ta_notice`(
            IN p_process_date DATE,
            OUT p_records_processed INT64
        )
        BEGIN
            TRUNCATE TABLE `your_project_id.your_dataset_id.ta_notice`;
            RAISE USING MESSAGE = 'Simulated error during data transformation!'; -- <--- ADD THIS LINE
            -- ... rest of the original procedure ...
        END;
        ```
*   **Action:** Call `sp_control_ta_notice` with valid `p_job_kennung` and `p_eintrags_nr`.

    ```python
    # pytest-style action
    import pytest
    def test_error_in_data_transformation(bq_client, project_id, dataset_id):
        # Setup similar to Test Case 1 for dwtk_meldungen
        bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_run_log`").result()
        bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_error_log`").result()
        bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_run_result`").result()
        bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.ta_notice`").result()
        bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.dwtk_meldungen`").result()
        bq_client.query(f"""
            INSERT INTO `{project_id}.{dataset_id}.dwtk_meldungen` (job_kennung, timecreated, message) VALUES
            ('BERT_DROP_TEMP_TABLE', '2023-01-15 08:00:00 UTC', 'Temp table dropped')
        """).result()

        job_kennung = 'JOB_KENNUNG_004'
        eintrags_nr = 'ENTRY_004'
        with pytest.raises(Exception) as excinfo:
            bq_client.query(f"""
                CALL `{project_id}.{dataset_id}.sp_control_ta_notice`(
                    p_job_kennung => '{job_kennung}',
                    p_eintrags_nr => '{eintrags_nr}'
                )
            """).result()
        assert "Simulated error during data transformation!" in str(excinfo.value)
    ```
*   **Pass/Fail Criteria:**
    1.  **Error raised:** The call to `sp_control_ta_notice` raises an error, propagating the "Simulated error during data transformation!" message.
    2.  **`job_run_log` entries:** Two entries exist for `job_kennung='JOB_KENNUNG_004'` and `eintrags_nr='ENTRY_004'`, one with `status='STARTED'` and one with `status='FAILED'`.
        ```sql
        SELECT COUNT(*) FROM `your_project_id.your_dataset_id.job_run_log`
        WHERE job_kennung = 'JOB_KENNUNG_004' AND eintrags_nr = 'ENTRY_004' AND status IN ('STARTED', 'FAILED');
        -- Expected: 2
        ```
    3.  **`job_error_log` entry:** One entry exists for `job_kennung='JOB_KENNUNG_004'` and `eintrags_nr='ENTRY_004'`, with `err_nr = -1` and `err_arg` containing the simulated error message.
        ```sql
        SELECT err_nr, err_arg FROM `your_project_id.your_dataset_id.job_error_log`
        WHERE job_kennung = 'JOB_KENNUNG_004' AND eintrags_nr = 'ENTRY_004';
        -- Expected: err_nr = -1, err_arg LIKE '%Simulated error during data transformation!%'
        ```
    4.  **`job_run_result` empty:** No entries for this run.
        ```sql
        SELECT COUNT(*) FROM `your_project_id.your_dataset_id.job_run_result`
        WHERE job_kennung = 'JOB_KENNUNG_004' AND eintrags_nr = 'ENTRY_004';
        -- Expected: 0
        ```
    5.  **`ta_notice` state:** The `ta_notice` table should be empty, as the `TRUNCATE` would have succeeded before the `RAISE`.
        ```sql
        SELECT COUNT(*) FROM `your_project_id.your_dataset_id.ta_notice`;
        -- Expected: 0
        ```
    *(Remember to revert the temporary modification to `sp_d_ausd_v_ta_notice` after this test.)*

---

### Test Case 5: `v_process_date` Derivation - `dwtk_meldungen` Empty/No Match

*   **Purpose:** Verify that `sp_control_ta_notice` correctly handles cases where `dwtk_meldungen` is empty or does not contain an entry for `BERT_DROP_TEMP_TABLE`, defaulting `v_process_date` to '1900-01-01'. This covers transformation correctness for date handling and external system replacements (reading from `dwtk_meldungen`).
*   **Setup:**
    1.  Clear all logging tables and `ta_notice`.
    2.  Ensure `dwtk_meldungen` is empty or does not contain any rows where `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    3.  Populate `cds_ta_notice` with data, some of which would match `p_process_date = '1900-01-01'` and some for a later date.

    ```python
    # pytest-style setup
    def setup_default_process_date_data(bq_client, project_id, dataset_id):
        # Clear tables
        bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_run_log`").result()
        bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_error_log`").result()
        bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_run_result`").result()
        bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.ta_notice`").result()
        bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.cds_ta_notice`").result()
        bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.dwtk_meldungen`").result()

        # Insert mock data into cds_ta_notice
        # Expected to be processed for p_process_date = '1900-01-01'
        bq_client.query(f"""
            INSERT INTO `{project_id}.{dataset_id}.cds_ta_notice` (cntrct_id, valid_from, valid_to, entry_date_of_notice, insert_at, modified_at, is_production) VALUES
            (201, '1899-12-01', '1900-02-01', '1899-12-01', '1899-12-30 10:00:00 UTC', NULL, 1), -- Included (insert_at <= PDATE, valid_to > PDATE)
            (202, '1900-01-01', NULL, '1900-01-01', '1900-01-01 00:00:00 UTC', NULL, 1), -- Included (insert_at = PDATE, valid_to IS NULL)
            (203, '1900-01-02', '2000-01-01', '1900-01-02', '1900-01-02 00:00:00 UTC', NULL, 1), -- Excluded (insert_at > PDATE)
            (204, '1899-11-01', '1899-12-31', '1899-11-01', '1899-11-30 10:00:00 UTC', NULL, 1)  -- Excluded (valid_to <= PDATE)
        """).result()

        # dwtk_meldungen is left empty or without 'BERT_DROP_TEMP_TABLE' entries
        # This will cause v_process_date to default to '1900-01-01'
        bq_client.query(f"""
            INSERT INTO `{project_id}.{dataset_id}.dwtk_meldungen` (job_kennung, timecreated, message) VALUES
            ('ANOTHER_JOB', '2023-01-01 00:00:00 UTC', 'Some other job')
        """).result()

        return "JOB_KENNUNG_005", "ENTRY_005", '1900-01-01', 2 # Expected records
    ```
*   **Action:** Call `sp_control_ta_notice` with valid `p_job_kennung` and `p_eintrags_nr`.

    ```python
    # pytest-style action
    def test_default_process_date(bq_client, project_id, dataset_id):
        job_kennung, eintrags_nr, expected_process_date, expected_records = setup_default_process_date_data(bq_client, project_id, dataset_id)

        bq_client.query(f"""
            CALL `{project_id}.{dataset_id}.sp_control_ta_notice`(
                p_job_kennung => '{job_kennung}',
                p_eintrags_nr => '{eintrags_nr}'
            )
        """).result()
    ```
*   **Pass/Fail Criteria:**
    1.  **`job_run_log` entries:** Two entries exist for `job_kennung='JOB_KENNUNG_005'` and `eintrags_nr='ENTRY_005'`, one with `status='STARTED'` and one with `status='COMPLETED'`.
        ```sql
        SELECT COUNT(*) FROM `your_project_id.your_dataset_id.job_run_log`
        WHERE job_kennung = 'JOB_KENNUNG_005' AND eintrags_nr = 'ENTRY_005' AND status IN ('STARTED', 'COMPLETED');
        -- Expected: 2
        ```
    2.  **`job_error_log` empty:** No entries for this run.
        ```sql
        SELECT COUNT(*) FROM `your_project_id.your_dataset_id.job_error_log`
        WHERE job_kennung = 'JOB_KENNUNG_005' AND eintrags_nr = 'ENTRY_005';
        -- Expected: 0
        ```
    3.  **`job_run_result` record count:** One entry exists with `record_count` matching `expected_records`.
        ```sql
        SELECT record_count FROM `your_project_id.your_dataset_id.job_run_result`
        WHERE job_kennung = 'JOB_KENNUNG_005' AND eintrags_nr = 'ENTRY_005';
        -- Expected: 2
        ```
    4.  **`ta_notice` content:** Contains the expected 2 rows, which are those matching the filter criteria with `p_process_date = '1900-01-01'`.
        ```sql
        SELECT cntrct_id FROM `your_project_id.your_dataset_id.ta_notice` ORDER BY cntrct_id;
        -- Expected: ['201', '202']
        ```

---

### Test Case 6: Missing Behavioral Equivalence - Job Deactivation/Ignoring Logic

*   **Purpose:** To highlight the discrepancy between the original script's stated purpose ("aktive Jobs werden ignoriert", "alte aktive Jobs werden einfach dekativiert") and the current implementation of `sp_d_ausd_v_ta_notice` which performs a `TRUNCATE` and `INSERT`. This test will demonstrate that the migrated code *does not* exhibit the job control behavior described in the legacy source and design document.
*   **Setup:**
    1.  Clear `ta_notice`.
    2.  Populate `ta_notice` with some "active job" data that, if the original logic were present, might be ignored or deactivated.
    3.  Populate `cds_ta_notice` with new data.
    4.  Populate `dwtk_meldungen` to derive a `p_process_date`.
*   **Action:** Call `sp_control_ta_notice` with valid parameters.
*   **Pass/Fail Criterion:**
    *   **FAIL (Expected):** The `ta_notice` table will be completely overwritten by the new data from `cds_ta_notice` (after truncation), effectively *losing* any "active job" data that was present before the run. This demonstrates that the "ignore active jobs" and "deactivate old active jobs" logic is missing.
    *   **Expected Outcome (if logic were present):** The `ta_notice` table would contain a merge of existing "active" jobs (potentially ignored or deactivated) and new data, not a complete overwrite.

    ```python
    # pytest-style action
    def test_missing_job_control_logic(bq_client, project_id, dataset_id):
        # Clear tables except ta_notice for initial state
        bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_run_log`").result()
        bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_error_log`").result()
        bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_run_result`").result()
        bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.cds_ta_notice`").result()
        bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.dwtk_meldungen`").result()

        # 1. Populate ta_notice with "active job" data
        bq_client.query(f"""
            INSERT INTO `{project_id}.{dataset_id}.ta_notice` (cntrct_id, valid_from, valid_to, entry_date_of_notice, insert_at, modified_at, is_production) VALUES
            ('900', '2023-01-01', '2025-12-31', '2023-01-01', '2023-01-01 00:00:00 UTC', NULL, 1), -- Existing active job
            ('901', '2023-02-01', '2023-03-01', '2023-02-01', '2023-02-01 00:00:00 UTC', NULL, 1)  -- Existing old active job
        """).result()
        initial_ta_notice_count = bq_client.query(f"SELECT COUNT(*) FROM `{project_id}.{dataset_id}.ta_notice`").result().total_rows
        assert initial_ta_notice_count == 2

        # 2. Populate cds_ta_notice with new data (different from existing)
        bq_client.query(f"""
            INSERT INTO `{project_id}.{dataset_id}.cds_ta_notice` (cntrct_id, valid_from, valid_to, entry_date_of_notice, insert_at, modified_at, is_production) VALUES
            (301, '2023-04-01', '2024-12-31', '2023-04-01', '2023-04-10 10:00:00 UTC', NULL, 1),
            (302, '2023-05-01', NULL, '2023-05-01', '2023-04-12 11:00:00 UTC', NULL, 1)
        """).result()
        expected_new_records = 2 # for p_process_date = '2023-04-15'

        # 3. Populate dwtk_meldungen to set p_process_date
        bq_client.query(f"""
            INSERT INTO `{project_id}.{dataset_id}.dwtk_meldungen` (job_kennung, timecreated, message) VALUES
            ('BERT_DROP_TEMP_TABLE', '2023-04-15 08:00:00 UTC', 'Temp table dropped')
        """).result()

        job_kennung = 'JOB_KENNUNG_006'
        eintrags_nr = 'ENTRY_006'

        # Action: Call the control procedure
        bq_client.query(f"""
            CALL `{project_id}.{dataset_id}.sp_control_ta_notice`(
                p_job_kennung => '{job_kennung}',
                p_eintrags_nr => '{eintrags_nr}'
            )
        """).result()

        # Pass/Fail Criterion check
        final_ta_notice_count = bq_client.query(f"SELECT COUNT(*) FROM `{project_id}.{dataset_id}.ta_notice`").result().total_rows
        # This assertion *should fail* if the original logic was truly implemented.
        # It passes because the TRUNCATE/INSERT overwrites everything.
        assert final_ta_notice_count == expected_new_records # This confirms the TRUNCATE/INSERT behavior

        # Further check to confirm old data is gone
        old_data_exists = bq_client.query(f"SELECT COUNT(*) FROM `{project_id}.{dataset_id}.ta_notice` WHERE cntrct_id IN ('900', '901')").result().total_rows
        assert old_data_exists == 0 # This confirms old data was not ignored/deactivated, but deleted.

        # Log the discrepancy
        print(f"DISCREPANCY DETECTED: The migrated sp_d_ausd_v_ta_notice performs TRUNCATE/INSERT, "
              f"overwriting existing data. This contradicts the legacy script's stated purpose "
              f"('aktive Jobs werden ignoriert', 'alte aktive Jobs werden einfach dekativiert') "
              f"and the design document's conceptual steps for job control logic. "
              f"Behavioral equivalence for job control is NOT met.")
    ```

---