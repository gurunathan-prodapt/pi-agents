As a senior data-migration QA engineer, I've analyzed the migration design and the generated BigQuery code for `k_ausd_v_ta_vvl_upgrade.ksh`. The core data transformation logic from `d_ausd_v_ta_vvl_upgrade.sql` has been embedded, and the orchestration, parameter handling, and job status management have been translated into a BigQuery Stored Procedure.

A key observation is that while the original KornShell script passes `p_JobKennung` and `p_EintragsNr` to the SQL script, the provided BigQuery SQL for the core data transformation (the `INSERT...SELECT` statement) does *not* use these parameters to filter the data. It processes all relevant records from `sof_ta_vvl_dwh`. For these tests, I will proceed with the assumption that this is the intended behavioral equivalence, meaning the parameters are primarily for job tracking and orchestration, not for data filtering within the `d_ausd_v_ta_vvl_upgrade.sql` logic itself. If the original `d_ausd_v_ta_vvl_upgrade.sql` *did* filter by these parameters, the transformation correctness tests would need to be adjusted.

The tests below cover output parity, transformation correctness, external system replacements (job status/logging tables), and data quality/row count assertions.

---

## Migration Validation Tests for `k_ausd_v_ta_vvl_upgrade.ksh`

**Target BigQuery Stored Procedure:** `your_project_id.your_dataset_id.k_ausd_v_ta_vvl_upgrade_sp`

**Dependent Tables:**
*   `your_project_id.your_dataset_id.job_status_table`
*   `your_project_id.your_dataset_id.job_log`
*   `your_project_id.your_dataset_id.dwtk_meldungen` (for `v_stichtag_str` determination)
*   `your_project_id.your_dataset_id.sof_ta_vvl_dwh` (source data)
*   `your_project_id.your_dataset_id.dwh_ta_l_bindefr_aendgr_carm` (lookup data)
*   `your_project_id.your_dataset_id.sof_ta_vvl_upgrade` (target data)

---

### Test Case 1: Successful Data Transformation and Job Completion

**Purpose:** Verify that the BigQuery Stored Procedure correctly processes data according to the `d_ausd_v_ta_vvl_upgrade.sql` logic, updates the target table, and records a successful job completion in the `job_status_table` and `job_log`. This covers output parity and transformation correctness for a typical run.

**Setup:**
1.  Ensure `job_status_table`, `job_log`, `dwtk_meldungen`, `sof_ta_vvl_dwh`, `dwh_ta_l_bindefr_aendgr_carm`, and `sof_ta_vvl_upgrade` tables exist and are empty.
2.  Insert sample data into source and lookup tables:
    *   `dwtk_meldungen`: `('BERT_DROP_TEMP_TABLE', '2023-01-15 10:00:00 UTC')`
    *   `dwh_ta_l_bindefr_aendgr_carm`:
        *   `(101, 'Normaler Upgrade')`
        *   `(102, 'DPPS Diensttyp A13 (EG-Upgrade)')`
        *   `(103, 'Vertragsverlaengerung')`
    *   `sof_ta_vvl_dwh`:
        *   `(1, 101, '2023-01-01 00:00:00 UTC')`
        *   `(1, 101, '2023-01-10 00:00:00 UTC')` (later change for vertrags_id 1)
        *   `(2, 102, '2023-01-05 00:00:00 UTC')`
        *   `(3, 103, '2023-01-02 00:00:00 UTC')`
        *   `(3, 101, '2023-01-08 00:00:00 UTC')` (later change for vertrags_id 3)
        *   `(4, 101, '2023-01-03 00:00:00 UTC')` (no matching `aenderung_am` for max)
3.  Define expected output for `sof_ta_vvl_upgrade` based on the transformation logic:
    *   `vertrags_id=1`: `MAX(aenderung_am)` is '2023-01-10', `vvl_aendgrund_id` is 101 -> 'Normaler Upgrade'
    *   `vertrags_id=2`: `MAX(aenderung_am)` is '2023-01-05', `vvl_aendgrund_id` is 102 -> 'Endgeraeteupgrade' (due to CASE)
    *   `vertrags_id=3`: `MAX(aenderung_am)` is '2023-01-08', `vvl_aendgrund_id` is 101 -> 'Normaler Upgrade'
    *   `vertrags_id=4`: No match for `vvl.aenderung_am = vvl2.upgr_datum` as `vvl2.upgr_datum` would be '2023-01-03' but `vvl.aenderung_am` is also '2023-01-03' and `vvl_aendgrund_id` is 101. This record *should* be included. My manual calculation was wrong. Let's re-evaluate.
        *   `vvl2` subquery: `(1, '2023-01-10'), (2, '2023-01-05'), (3, '2023-01-08'), (4, '2023-01-03')`
        *   Join conditions: `ba.vvl_aendgrund_id = vvl.vvl_aendgrund_id` AND `vvl.vertrags_id = vvl2.vertrags_id` AND `vvl.aenderung_am = vvl2.upgr_datum`
        *   For `vertrags_id=1`: `vvl` has `(1, 101, '2023-01-10')`. `vvl2` has `(1, '2023-01-10')`. `ba` has `(101, 'Normaler Upgrade')`. Matches. Output: `(1, 'Normaler Upgrade', '2023-01-10')`.
        *   For `vertrags_id=2`: `vvl` has `(2, 102, '2023-01-05')`. `vvl2` has `(2, '2023-01-05')`. `ba` has `(102, 'DPPS Diensttyp A13 (EG-Upgrade')`. Matches. Output: `(2, 'Endgeraeteupgrade', '2023-01-05')`.
        *   For `vertrags_id=3`: `vvl` has `(3, 101, '2023-01-08')`. `vvl2` has `(3, '2023-01-08')`. `ba` has `(101, 'Normaler Upgrade')`. Matches. Output: `(3, 'Normaler Upgrade', '2023-01-08')`.
        *   For `vertrags_id=4`: `vvl` has `(4, 101, '2023-01-03')`. `vvl2` has `(4, '2023-01-03')`. `ba` has `(101, 'Normaler Upgrade')`. Matches. Output: `(4, 'Normaler Upgrade', '2023-01-03')`.
    *   Expected records processed: 4.

**Action:**
Execute the BigQuery Stored Procedure:
`CALL your_project_id.your_dataset_id.k_ausd_v_ta_vvl_upgrade_sp('TEST_JOB_1', 'ENTRY_1');`

**Pass/Fail Criteria:**
1.  **Output Parity:** The `sof_ta_vvl_upgrade` table contains exactly the 4 expected records:
    ```sql
    SELECT vertrags_id, upgradegrund, FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', upgradedatum) FROM `your_project_id.your_dataset_id.sof_ta_vvl_upgrade` ORDER BY vertrags_id;
    -- Expected:
    -- (1, 'Normaler Upgrade', '2023-01-10 00:00:00')
    -- (2, 'Endgeraeteupgrade', '2023-01-05 00:00:00')
    -- (3, 'Normaler Upgrade', '2023-01-08 00:00:00')
    -- (4, 'Normaler Upgrade', '2023-01-03 00:00:00')
    ```
2.  **Row Count:** `SELECT COUNT(*) FROM your_project_id.your_dataset_id.sof_ta_vvl_upgrade;` returns `4`.
3.  **Job Status:** `job_status_table` has one record for `('TEST_JOB_1', 'ENTRY_1')` with `status = 'COMPLETED'`, `records_processed = 4`, `start_time` and `end_time` populated.
    ```sql
    SELECT job_id, entry_nr, status, records_processed FROM `your_project_id.your_dataset_id.job_status_table` WHERE job_id = 'TEST_JOB_1' AND entry_nr = 'ENTRY_1';
    -- Expected: ('TEST_JOB_1', 'ENTRY_1', 'COMPLETED', 4)
    ```
4.  **Job Log:** `job_log` contains `INFO` messages for start, truncation, stichtag determination, completion, and deactivation of older jobs (if any were active).
    ```sql
    SELECT COUNT(*) FROM `your_project_id.your_dataset_id.job_log` WHERE job_id = 'TEST_JOB_1' AND entry_nr = 'ENTRY_1' AND log_level = 'INFO';
    -- Expected: >= 4 (for start, stichtag, truncate, deactivate, completion)
    ```

---

### Test Case 2: Parameter Validation - Missing `p_JobKennung`

**Purpose:** Verify that the procedure correctly handles missing required parameters, logs an error, and raises an exception, preventing execution of the core logic.

**Setup:**
1.  Ensure `job_status_table` and `job_log` are empty or cleared of previous test data.
2.  No data setup for source tables is needed as validation should occur before data processing.

**Action:**
Attempt to execute the BigQuery Stored Procedure with a `NULL` `p_job_kennung`:
`CALL your_project_id.your_dataset_id.k_ausd_v_ta_vvl_upgrade_sp(NULL, 'ENTRY_2');`

**Pass/Fail Criteria:**
1.  **Error Handling:** The procedure execution fails with an error message indicating missing `Jobkennung`.
    ```sql
    -- Example of expected error message (actual message might vary slightly based on BQ error reporting)
    -- "Parameter validation failed: Jobkennung is missing."
    ```
2.  **Job Log:** `job_log` contains an `ERROR` message for `('NULL', 'ENTRY_2')` related to the missing `Jobkennung`.
    ```sql
    SELECT log_level, message FROM `your_project_id.your_dataset_id.job_log` WHERE entry_nr = 'ENTRY_2';
    -- Expected: ('ERROR', 'Parameter validation failed: Jobkennung is missing.')
    ```
3.  **No Data Changes:** `sof_ta_vvl_upgrade` remains unchanged (no new data inserted, no truncation). `job_status_table` contains no entry for this job run.

---

### Test Case 3: Parameter Validation - Missing `p_EintragsNr`

**Purpose:** Verify that the procedure correctly handles missing `p_EintragsNr`, logs an error, and raises an exception.

**Setup:**
1.  Ensure `job_status_table` and `job_log` are empty or cleared.

**Action:**
Attempt to execute the BigQuery Stored Procedure with a `NULL` `p_eintrags_nr`:
`CALL your_project_id.your_dataset_id.k_ausd_v_ta_vvl_upgrade_sp('TEST_JOB_3', NULL);`

**Pass/Fail Criteria:**
1.  **Error Handling:** The procedure execution fails with an error message indicating missing `EintragsNr`.
    ```sql
    -- Expected error message: "Parameter validation failed: EintragsNr is missing."
    ```
2.  **Job Log:** `job_log` contains an `ERROR` message for `('TEST_JOB_3', 'NULL')` related to the missing `EintragsNr`.
    ```sql
    SELECT log_level, message FROM `your_project_id.your_dataset_id.job_log` WHERE job_id = 'TEST_JOB_3';
    -- Expected: ('ERROR', 'Parameter validation failed: EintragsNr is missing.')
    ```
3.  **No Data Changes:** `sof_ta_vvl_upgrade` remains unchanged. `job_status_table` contains no entry for this job run.

---

### Test Case 4: Active Job Handling - Ignore Already Active Job

**Purpose:** Verify that if a job with the same `job_id` and `entry_nr` is already `ACTIVE`, the procedure logs a warning and exits gracefully without processing data or updating job status. This directly tests the "aktive Jobs werden ignoriert" logic.

**Setup:**
1.  Clear `job_status_table` and `job_log`.
2.  Insert an `ACTIVE` record into `job_status_table` for the specific job to be tested:
    ```sql
    INSERT INTO `your_project_id.your_dataset_id.job_status_table` (job_id, entry_nr, status, start_time, records_processed)
    VALUES ('TEST_JOB_ACTIVE', 'ENTRY_ACTIVE', 'ACTIVE', CURRENT_TIMESTAMP(), 0);
    ```
3.  Populate source tables as in Test Case 1 (to ensure data *would* be processed if not ignored).
4.  Ensure `sof_ta_vvl_upgrade` is empty.

**Action:**
Execute the BigQuery Stored Procedure with the `job_id` and `entry_nr` that are already active:
`CALL your_project_id.your_dataset_id.k_ausd_v_ta_vvl_upgrade_sp('TEST_JOB_ACTIVE', 'ENTRY_ACTIVE');`

**Pass/Fail Criteria:**
1.  **Graceful Exit:** The procedure completes without raising an exception.
2.  **Job Log:** `job_log` contains a `WARN` message indicating the job was already active and ignored.
    ```sql
    SELECT log_level, message FROM `your_project_id.your_dataset_id.job_log` WHERE job_id = 'TEST_JOB_ACTIVE' AND entry_nr = 'ENTRY_ACTIVE' AND log_level = 'WARN';
    -- Expected: ('WARN', 'Job (Jobkennung: TEST_JOB_ACTIVE, EintragsNr: ENTRY_ACTIVE) is already active. Ignoring this run.')
    ```
3.  **No Data Changes:** `sof_ta_vvl_upgrade` remains empty.
4.  **Job Status Unchanged:** The `job_status_table` record for `('TEST_JOB_ACTIVE', 'ENTRY_ACTIVE')` remains `ACTIVE` (not updated to `COMPLETED` or `FAILED`).
    ```sql
    SELECT status FROM `your_project_id.your_dataset_id.job_status_table` WHERE job_id = 'TEST_JOB_ACTIVE' AND entry_nr = 'ENTRY_ACTIVE';
    -- Expected: 'ACTIVE'
    ```

---

### Test Case 5: Deactivation of Older Active Jobs

**Purpose:** Verify that the procedure correctly deactivates other active jobs for the same `job_id` but different `entry_nr` after its own successful completion. This tests the "alte aktive Jobs werden einfach dekativiert" logic.

**Setup:**
1.  Clear `job_status_table` and `job_log`.
2.  Insert multiple `ACTIVE` records into `job_status_table` for the same `job_id` but different `entry_nr`s, plus one for the current run:
    ```sql
    INSERT INTO `your_project_id.your_dataset_id.job_status_table` (job_id, entry_nr, status, start_time, records_processed) VALUES
    ('TEST_JOB_DEACTIVATE', 'OLD_ENTRY_1', 'ACTIVE', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), 0),
    ('TEST_JOB_DEACTIVATE', 'OLD_ENTRY_2', 'ACTIVE', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE), 0),
    ('TEST_JOB_DEACTIVATE', 'CURRENT_ENTRY', 'INACTIVE', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR), 0), -- This one should not be affected
    ('ANOTHER_JOB', 'SOME_ENTRY', 'ACTIVE', CURRENT_TIMESTAMP(), 0); -- This one should not be affected
    ```
3.  Populate source tables as in Test Case 1.

**Action:**
Execute the BigQuery Stored Procedure for `('TEST_JOB_DEACTIVATE', 'NEW_ENTRY_3')`:
`CALL your_project_id.your_dataset_id.k_ausd_v_ta_vvl_upgrade_sp('TEST_JOB_DEACTIVATE', 'NEW_ENTRY_3');`

**Pass/Fail Criteria:**
1.  **Job Status Update:**
    *   The newly run job `('TEST_JOB_DEACTIVATE', 'NEW_ENTRY_3')` is `COMPLETED`.
    *   `('TEST_JOB_DEACTIVATE', 'OLD_ENTRY_1')` and `('TEST_JOB_DEACTIVATE', 'OLD_ENTRY_2')` are updated to `INACTIVE`.
    *   `('TEST_JOB_DEACTIVATE', 'CURRENT_ENTRY')` remains `INACTIVE`.
    *   `('ANOTHER_JOB', 'SOME_ENTRY')` remains `ACTIVE`.
    ```sql
    SELECT job_id, entry_nr, status FROM `your_project_id.your_dataset_id.job_status_table` ORDER BY entry_nr;
    -- Expected:
    -- ('ANOTHER_JOB', 'SOME_ENTRY', 'ACTIVE')
    -- ('TEST_JOB_DEACTIVATE', 'CURRENT_ENTRY', 'INACTIVE')
    -- ('TEST_JOB_DEACTIVATE', 'NEW_ENTRY_3', 'COMPLETED')
    -- ('TEST_JOB_DEACTIVATE', 'OLD_ENTRY_1', 'INACTIVE')
    -- ('TEST_JOB_DEACTIVATE', 'OLD_ENTRY_2', 'INACTIVE')
    ```
2.  **Job Log:** `job_log` contains an `INFO` message confirming deactivation of older jobs.
    ```sql
    SELECT COUNT(*) FROM `your_project_id.your_dataset_id.job_log` WHERE job_id = 'TEST_JOB_DEACTIVATE' AND entry_nr = 'NEW_ENTRY_3' AND message LIKE '%Deactivated older active jobs%';
    -- Expected: 1
    ```
3.  **Data Transformation:** `sof_ta_vvl_upgrade` contains the correct data as per Test Case 1.

---

### Test Case 6: Empty Source Tables

**Purpose:** Verify that the procedure handles empty source tables gracefully, resulting in an empty target table and `records_processed = 0`.

**Setup:**
1.  Clear all tables: `job_status_table`, `job_log`, `dwtk_meldungen`, `sof_ta_vvl_dwh`, `dwh_ta_l_bindefr_aendgr_carm`, `sof_ta_vvl_upgrade`.
2.  Insert a `dwtk_meldungen` record to avoid `v_stichtag_str` being `19000101` (though it doesn't affect the main `INSERT`).
    *   `dwtk_meldungen`: `('BERT_DROP_TEMP_TABLE', '2023-01-15 10:00:00 UTC')`

**Action:**
Execute the BigQuery Stored Procedure:
`CALL your_project_id.your_dataset_id.k_ausd_v_ta_vvl_upgrade_sp('TEST_JOB_EMPTY', 'ENTRY_EMPTY');`

**Pass/Fail Criteria:**
1.  **Output Parity:** `sof_ta_vvl_upgrade` remains empty.
    ```sql
    SELECT COUNT(*) FROM `your_project_id.your_dataset_id.sof_ta_vvl_upgrade`;
    -- Expected: 0
    ```
2.  **Row Count:** `SELECT COUNT(*) FROM your_project_id.your_dataset_id.sof_ta_vvl_upgrade;` returns `0`.
3.  **Job Status:** `job_status_table` has one record for `('TEST_JOB_EMPTY', 'ENTRY_EMPTY')` with `status = 'COMPLETED'` and `records_processed = 0`.
    ```sql
    SELECT job_id, entry_nr, status, records_processed FROM `your_project_id.your_dataset_id.job_status_table` WHERE job_id = 'TEST_JOB_EMPTY';
    -- Expected: ('TEST_JOB_EMPTY', 'ENTRY_EMPTY', 'COMPLETED', 0)
    ```
4.  **Job Log:** `job_log` contains `INFO` messages, including one indicating 0 records processed.

---

### Test Case 7: Transformation Correctness - `CASE` Statement and `MAX(aenderung_am)` Logic

**Purpose:** Verify the specific logic within the `SELECT` statement, including the `CASE` expression for `upgradegrund` and the `MAX(aenderung_am)` subquery for `upgradedatum`.

**Setup:**
1.  Clear `sof_ta_vvl_dwh`, `dwh_ta_l_bindefr_aendgr_carm`, and `sof_ta_vvl_upgrade`.
2.  Insert specific data to test the `CASE` and `MAX` logic:
    *   `dwh_ta_l_bindefr_aendgr_carm`:
        *   `(201, 'Standard Upgrade')`
        *   `(202, 'DPPS Diensttyp A13 (EG-Upgrade)')`
        *   `(203, 'Another Upgrade Type')`
    *   `sof_ta_vvl_dwh`:
        *   `(10, 201, '2022-05-01 00:00:00 UTC')`
        *   `(10, 201, '2022-05-15 00:00:00 UTC')` (Max for 10)
        *   `(11, 202, '2022-06-10 00:00:00 UTC')` (Triggers CASE)
        *   `(12, 203, '2022-07-01 00:00:00 UTC')`
        *   `(12, 201, '2022-07-20 00:00:00 UTC')` (Max for 12, different `vvl_aendgrund_id` at max date)
3.  Expected output for `sof_ta_vvl_upgrade`:
    *   `vertrags_id=10`: `MAX(aenderung_am)` is '2022-05-15', `vvl_aendgrund_id` is 201 -> 'Standard Upgrade'
    *   `vertrags_id=11`: `MAX(aenderung_am)` is '2022-06-10', `vvl_aendgrund_id` is 202 -> 'Endgeraeteupgrade'
    *   `vertrags_id=12`: `MAX(aenderung_am)` is '2022-07-20', `vvl_aendgrund_id` is 201 -> 'Standard Upgrade'
    *   Expected records processed: 3.

**Action:**
Execute the BigQuery Stored Procedure:
`CALL your_project_id.your_dataset_id.k_ausd_v_ta_vvl_upgrade_sp('TEST_JOB_TRANSFORM', 'ENTRY_TRANSFORM');`

**Pass/Fail Criteria:**
1.  **Output Parity:** The `sof_ta_vvl_upgrade` table contains exactly the 3 expected records with correct `upgradegrund` and `upgradedatum`.
    ```sql
    SELECT vertrags_id, upgradegrund, FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', upgradedatum) FROM `your_project_id.your_dataset_id.sof_ta_vvl_upgrade` ORDER BY vertrags_id;
    -- Expected:
    -- (10, 'Standard Upgrade', '2022-05-15 00:00:00')
    -- (11, 'Endgeraeteupgrade', '2022-06-10 00:00:00')
    -- (12, 'Standard Upgrade', '2022-07-20 00:00:00')
    ```
2.  **Row Count:** `SELECT COUNT(*) FROM your_project_id.your_dataset_id.sof_ta_vvl_upgrade;` returns `3`.
3.  **Job Status:** `job_status_table` shows `('TEST_JOB_TRANSFORM', 'ENTRY_TRANSFORM')` as `COMPLETED` with `records_processed = 3`.

---

### Test Case 8: NULL Handling in Source Data

**Purpose:** Verify how the transformation handles `NULL` values in critical columns, specifically `vvl_aendgrund_id` and `aenderung_am`, which are involved in joins and `MAX` aggregation.

**Setup:**
1.  Clear `sof_ta_vvl_dwh`, `dwh_ta_l_bindefr_aendgr_carm`, and `sof_ta_vvl_upgrade`.
2.  Insert data with `NULL` values:
    *   `dwh_ta_l_bindefr_aendgr_carm`:
        *   `(301, 'Valid Upgrade')`
    *   `sof_ta_vvl_dwh`:
        *   `(20, 301, '2023-02-01 00:00:00 UTC')` (Valid record)
        *   `(21, NULL, '2023-02-05 00:00:00 UTC')` (`vvl_aendgrund_id` is NULL, should not join `ba`)
        *   `(22, 301, NULL)` (`aenderung_am` is NULL, should not be picked by `MAX` or join `vvl2`)
        *   `(23, 301, '2023-02-10 00:00:00 UTC')`
        *   `(23, 301, NULL)` (another record for 23 with NULL date)
3.  Expected output for `sof_ta_vvl_upgrade`:
    *   Only `vertrags_id=20` and `vertrags_id=23` should produce output.
    *   `vertrags_id=20`: `(20, 'Valid Upgrade', '2023-02-01 00:00:00')`
    *   `vertrags_id=23`: `MAX(aenderung_am)` is '2023-02-10'. `vvl_aendgrund_id` is 301. `(23, 'Valid Upgrade', '2023-02-10 00:00:00')`
    *   Expected records processed: 2.

**Action:**
Execute the BigQuery Stored Procedure:
`CALL your_project_id.your_dataset_id.k_ausd_v_ta_vvl_upgrade_sp('TEST_JOB_NULL', 'ENTRY_NULL');`

**Pass/Fail Criteria:**
1.  **Output Parity:** The `sof_ta_vvl_upgrade` table contains exactly the 2 expected records. Records with `NULL` in join keys or `MAX` aggregation keys should be excluded as per standard SQL behavior.
    ```sql
    SELECT vertrags_id, upgradegrund, FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', upgradedatum) FROM `your_project_id.your_dataset_id.sof_ta_vvl_upgrade` ORDER BY vertrags_id;
    -- Expected:
    -- (20, 'Valid Upgrade', '2023-02-01 00:00:00')
    -- (23, 'Valid Upgrade', '2023-02-10 00:00:00')
    ```
2.  **Row Count:** `SELECT COUNT(*) FROM your_project_id.your_dataset_id.sof_ta_vvl_upgrade;` returns `2`.
3.  **Job Status:** `job_status_table` shows `('TEST_JOB_NULL', 'ENTRY_NULL')` as `COMPLETED` with `records_processed = 2`.

---

### Test Case 9: Error Handling during SQL Execution

**Purpose:** Verify that if an error occurs during the core SQL execution (e.g., due to schema mismatch, data type issues, or a simulated error), the procedure catches it, logs the error, updates the job status to `FAILED`, and re-raises the exception.

**Setup:**
1.  Clear `job_status_table` and `job_log`.
2.  Insert an initial `ACTIVE` record into `job_status_table` for the job.
    ```sql
    INSERT INTO `your_project_id.your_dataset_id.job_status_table` (job_id, entry_nr, status, start_time, records_processed)
    VALUES ('TEST_JOB_ERROR', 'ENTRY_ERROR', 'ACTIVE', CURRENT_TIMESTAMP(), 0);
    ```
3.  **Simulate an error:** This is tricky to do cleanly in a stored procedure without modifying the SP itself.
    *   **Option A (Best for testing):** Temporarily modify the SP to introduce a deliberate error (e.g., `SELECT 1/0;` or `INSERT INTO non_existent_table ...;`) within the `BEGIN...END` block that contains the core SQL.
    *   **Option B (Less ideal, relies on data):** Insert data that would cause a data type conversion error if the target column was of a different type (e.g., trying to insert a string into an INT64 column, but the current schema seems robust).
    *   For this test, I will assume **Option A** is used by the test runner.

**Action:**
Execute the BigQuery Stored Procedure, which is temporarily modified to cause an error:
`CALL your_project_id.your_dataset_id.k_ausd_v_ta_vvl_upgrade_sp('TEST_JOB_ERROR', 'ENTRY_ERROR');`

**Pass/Fail Criteria:**
1.  **Error Handling:** The procedure execution fails with an exception.
2.  **Job Log:** `job_log` contains an `ERROR` message for `('TEST_JOB_ERROR', 'ENTRY_ERROR')` with details about the SQL execution failure.
    ```sql
    SELECT log_level, message, error_details FROM `your_project_id.your_dataset_id.job_log` WHERE job_id = 'TEST_JOB_ERROR';
    -- Expected: ('ERROR', 'SQL execution failed within k_ausd_v_ta_vvl_upgrade_sp.', 'Details of the simulated error...')
    ```
3.  **Job Status:** `job_status_table` has one record for `('TEST_JOB_ERROR', 'ENTRY_ERROR')` with `status = 'FAILED'`, `end_time` populated, and `error_message` containing details of the failure.
    ```sql
    SELECT job_id, entry_nr, status, error_message FROM `your_project_id.your_dataset_id.job_status_table` WHERE job_id = 'TEST_JOB_ERROR';
    -- Expected: ('TEST_JOB_ERROR', 'ENTRY_ERROR', 'FAILED', 'Details of the simulated error...')
    ```
4.  **No Data Changes:** `sof_ta_vvl_upgrade` remains unchanged (or in a truncated state if the error occurred after `TRUNCATE` but before `INSERT` completion).

---

### Test Case 10: `dwtk_meldungen` Empty or No Matching `job_kennung`

**Purpose:** Verify the behavior when `dwtk_meldungen` is empty or does not contain a record for `BERT_DROP_TEMP_TABLE`, which affects the `v_stichtag_str` variable.

**Setup:**
1.  Clear `job_status_table`, `job_log`, `dwtk_meldungen`, and `sof_ta_vvl_upgrade`.
2.  Populate source tables `sof_ta_vvl_dwh` and `dwh_ta_l_bindefr_aendgr_carm` as in Test Case 1.
3.  Ensure `dwtk_meldungen` is empty or contains no record with `job_kennung = 'BERT_DROP_TEMP_TABLE'`.

**Action:**
Execute the BigQuery Stored Procedure:
`CALL your_project_id.your_dataset_id.k_ausd_v_ta_vvl_upgrade_sp('TEST_JOB_NO_STICHTAG', 'ENTRY_NO_STICHTAG');`

**Pass/Fail Criteria:**
1.  **Data Transformation:** The core data transformation in `sof_ta_vvl_upgrade` should still complete successfully and produce the same output as Test Case 1, because `v_stichtag_str` is not used in the main `INSERT` statement.
    ```sql
    SELECT COUNT(*) FROM `your_project_id.your_dataset_id.sof_ta_vvl_upgrade`;
    -- Expected: 4 (as per Test Case 1)
    ```
2.  **Job Log:** The `job_log` should show `v_stichtag_str` as `'19000101'` (the `COALESCE` default).
    ```sql
    SELECT message FROM `your_project_id.your_dataset_id.job_log` WHERE job_id = 'TEST_JOB_NO_STICHTAG' AND message LIKE 'Stichtag determined%';
    -- Expected: Message similar to 'Stichtag determined: 19000101. Starting data processing.'
    ```
3.  **Job Status:** The job should complete successfully with `status = 'COMPLETED'` and `records_processed = 4`.

---

### General Considerations for Test Execution:

*   **Idempotency:** Each test case should ideally start from a clean state for the affected tables to ensure isolation. This can be achieved by `TRUNCATE TABLE` or `DELETE FROM` statements in the setup phase of each test.
*   **Project/Dataset Placeholders:** Replace `your_project_id.your_dataset_id` with actual BigQuery project and dataset names.
*   **Automated Testing:** These test cases are designed to be integrated into an automated testing framework (e.g., Python with `pytest` and `google-cloud-bigquery` client library).
*   **Performance:** While not explicitly requested, for large-scale migrations, performance testing (comparing execution times and resource consumption) between legacy and migrated jobs would also be crucial.
*   **Data Volume:** The provided sample data is small. For full confidence, tests should also be run with production-like data volumes to identify any performance bottlenecks or memory issues in BigQuery.