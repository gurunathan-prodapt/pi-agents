The following migration validation tests are designed to ensure the BigQuery stored procedure and its orchestration layer are behaviourally equivalent to the legacy KornShell and Oracle SQL job.

---

### Test Case 1: Happy Path - Full Data Load and Output Parity

**Purpose:**
Verify that the migrated job successfully processes a typical dataset, truncates the target table, inserts all records from the source, and correctly logs job details. This test primarily focuses on output parity and basic transformation correctness.

**Setup:**
1.  **Legacy (Oracle):**
    *   `isbert_schema.dwtk_meldungen`:
        ```sql
        INSERT INTO isbert_schema.dwtk_meldungen (job_kennung, timecreated) VALUES ('BERT_DROP_TEMP_TABLE', TO_TIMESTAMP('2023-01-15 10:00:00', 'YYYY-MM-DD HH24:MI:SS'));
        INSERT INTO isbert_schema.dwtk_meldungen (job_kennung, timecreated) VALUES ('OTHER_JOB', TO_TIMESTAMP('2023-01-16 11:00:00', 'YYYY-MM-DD HH24:MI:SS'));
        ```
    *   `sof$ta_bpr_instance`:
        ```sql
        INSERT INTO sof$ta_bpr_instance (cntrct_id, bpr_id) VALUES (1001, 2001);
        INSERT INTO sof$ta_bpr_instance (cntrct_id, bpr_id) VALUES (1002, 2002);
        INSERT INTO sof$ta_bpr_instance (cntrct_id, bpr_id) VALUES (1003, 2003);
        ```
    *   `sof$ta_bpr_optionen`: Empty or contains pre-existing data (will be truncated).
    *   Environment variables/parameters for `k_ausd_bp_ta_bpr_optionen.ksh`:
        `p_JobKennung="TEST_JOB_01"`, `p_EintragsNr="001"`, `p_Stichtag="20012023"`, `p_wiederanlaufWert="0"`

2.  **Migrated (BigQuery):**
    *   `my-gcp-project.isbert_schema_dataset.dwtk_meldungen`:
        ```sql
        TRUNCATE TABLE `my-gcp-project.isbert_schema_dataset.dwtk_meldungen`;
        INSERT INTO `my-gcp-project.isbert_schema_dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES ('BERT_DROP_TEMP_TABLE', '2023-01-15 10:00:00 UTC');
        INSERT INTO `my-gcp-project.isbert_schema_dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES ('OTHER_JOB', '2023-01-16 11:00:00 UTC');
        ```
    *   `my-gcp-project.my_dataset.sof_ta_bpr_instance`:
        ```sql
        TRUNCATE TABLE `my-gcp-project.my_dataset.sof_ta_bpr_instance`;
        INSERT INTO `my-gcp-project.my_dataset.sof_ta_bpr_instance` (cntrct_id, bpr_id) VALUES (1001, 2001);
        INSERT INTO `my-gcp-project.my_dataset.sof_ta_bpr_instance` (cntrct_id, bpr_id) VALUES (1002, 2002);
        INSERT INTO `my-gcp-project.my_dataset.sof_ta_bpr_instance` (cntrct_id, bpr_id) VALUES (1003, 2003);
        ```
    *   `my-gcp-project.my_dataset.sof_ta_bpr_optionen`: Empty or contains pre-existing data.
    *   `my-gcp-project.my_dataset.error_log`: Empty.
    *   `my-gcp-project.my_dataset.job_log`: Empty.
    *   Parameters for `sp_d_ausd_bp_ta_bpr_optionen` (via Python script):
        `job_kennung="TEST_JOB_01"`, `eintrags_nr="001"`, `stichtag="20012023"`, `wiederanlauf_wert="0"`

**Action:**
1.  **Legacy:**
    ```bash
    # Assuming environment is set up and scripts are in PATH
    ./k_ausd_bp_ta_bpr_optionen.ksh -j "TEST_JOB_01" -f "001" -s "20012023" -l "0"
    ```
2.  **Migrated:**
    ```python
    # Assuming Python script is in current directory
    python invoke_sp_d_ausd_bp_ta_bpr_optionen.py \
        --project_id "my-gcp-project" \
        --dataset_id "my_dataset" \
        --isbert_schema_dataset_id "isbert_schema_dataset" \
        --job_kennung "TEST_JOB_01" \
        --eintrags_nr "001" \
        --stichtag "20012023" \
        --wiederanlauf_wert "0"
    ```

**Pass/Fail Criterion:**
*   **Output Parity (Data):** The data in Oracle `sof$ta_bpr_optionen` must be identical to `my-gcp-project.my_dataset.sof_ta_bpr_optionen`.
    *   **BigQuery Assertion:**
        ```sql
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM `my-gcp-project.my_dataset.sof_ta_bpr_optionen`) = 3
                AND (SELECT COUNT(DISTINCT cntrct_id) FROM `my-gcp-project.my_dataset.sof_ta_bpr_optionen`) = 3
                AND (SELECT COUNT(DISTINCT bpr_id) FROM `my-gcp-project.my_dataset.sof_ta_bpr_optionen`) = 3
                AND (SELECT COUNT(*) FROM `my-gcp-project.my_dataset.sof_ta_bpr_optionen` WHERE cntrct_id = 1001 AND bpr_id = 2001) = 1
                AND (SELECT COUNT(*) FROM `my-gcp-project.my_dataset.sof_ta_bpr_optionen` WHERE cntrct_id = 1002 AND bpr_id = 2002) = 1
                AND (SELECT COUNT(*) FROM `my-gcp-project.my_dataset.sof_ta_bpr_optionen` WHERE cntrct_id = 1003 AND bpr_id = 2003) = 1
                THEN 'PASS'
                ELSE 'FAIL'
            END AS test_result;
        ```
*   **Output Parity (Logging):** The legacy job should report successful completion and the number of records. The migrated job must have a corresponding entry in `my-gcp-project.my_dataset.job_log` and no entries in `my-gcp-project.my_dataset.error_log`.
    *   **BigQuery Assertion:**
        ```sql
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM `my-gcp-project.my_dataset.error_log`) = 0
                AND (SELECT COUNT(*) FROM `my-gcp-project.my_dataset.job_log`
                     WHERE job_kennung = 'TEST_JOB_01'
                       AND eintrags_nr = '001'
                       AND stichtag = '20012023'
                       AND wiederanlauf_wert = '0'
                       AND records = 3
                       AND tab_name = 'sof_ta_bpr_optionen') = 1
                THEN 'PASS'
                ELSE 'FAIL'
            END AS test_result;
        ```
*   **Transformation Correctness:** `cntrct_id` and `bpr_id` values are correctly transferred without modification.
*   **External System Replacements:** Oracle table reads are correctly replaced by BigQuery table reads. SQL*Plus execution is replaced by BigQuery stored procedure execution.
*   **Data Quality/Row Count/Schema:** The target table `sof_ta_bpr_optionen` has 3 rows, matching the source. The schema (columns `cntrct_id`, `bpr_id` as `INT64`) is as expected.

---

### Test Case 2: Empty Source Table

**Purpose:**
Verify the job's behavior when the source table (`sof_ta_bpr_instance`) is empty. The target table should also be empty, and the job log should reflect 0 records processed.

**Setup:**
1.  **Legacy (Oracle):**
    *   `isbert_schema.dwtk_meldungen`: Contains the same entry as Test Case 1.
    *   `sof$ta_bpr_instance`: Empty.
    *   `sof$ta_bpr_optionen`: Empty or contains pre-existing data.
    *   Parameters: `p_JobKennung="TEST_JOB_02"`, `p_EintragsNr="002"`, `p_Stichtag="21012023"`, `p_wiederanlaufWert="0"`

2.  **Migrated (BigQuery):**
    *   `my-gcp-project.isbert_schema_dataset.dwtk_meldungen`: Contains the same entry as Test Case 1.
    *   `my-gcp-project.my_dataset.sof_ta_bpr_instance`: Empty.
        ```sql
        TRUNCATE TABLE `my-gcp-project.my_dataset.sof_ta_bpr_instance`;
        ```
    *   `my-gcp-project.my_dataset.sof_ta_bpr_optionen`: Empty or contains pre-existing data.
    *   `my-gcp-project.my_dataset.error_log`: Empty.
    *   `my-gcp-project.my_dataset.job_log`: Empty.
    *   Parameters: `job_kennung="TEST_JOB_02"`, `eintrags_nr="002"`, `stichtag="21012023"`, `wiederanlauf_wert="0"`

**Action:**
1.  **Legacy:**
    ```bash
    ./k_ausd_bp_ta_bpr_optionen.ksh -j "TEST_JOB_02" -f "002" -s "21012023" -l "0"
    ```
2.  **Migrated:**
    ```python
    python invoke_sp_d_ausd_bp_ta_bpr_optionen.py \
        --project_id "my-gcp-project" \
        --dataset_id "my_dataset" \
        --isbert_schema_dataset_id "isbert_schema_dataset" \
        --job_kennung "TEST_JOB_02" \
        --eintrags_nr "002" \
        --stichtag "21012023" \
        --wiederanlauf_wert "0"
    ```

**Pass/Fail Criterion:**
*   **Output Parity (Data):** Both Oracle `sof$ta_bpr_optionen` and BigQuery `my-gcp-project.my_dataset.sof_ta_bpr_optionen` must be empty.
    *   **BigQuery Assertion:**
        ```sql
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM `my-gcp-project.my_dataset.sof_ta_bpr_optionen`) = 0
                THEN 'PASS'
                ELSE 'FAIL'
            END AS test_result;
        ```
*   **Output Parity (Logging):** The job log entry must show 0 records processed.
    *   **BigQuery Assertion:**
        ```sql
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM `my-gcp-project.my_dataset.error_log`) = 0
                AND (SELECT COUNT(*) FROM `my-gcp-project.my_dataset.job_log`
                     WHERE job_kennung = 'TEST_JOB_02'
                       AND records = 0) = 1
                THEN 'PASS'
                ELSE 'FAIL'
            END AS test_result;
        ```
*   **Data Quality/Row Count:** The target table has 0 rows.

---

### Test Case 3: Target Table Truncation

**Purpose:**
Verify that the `TRUNCATE TABLE` operation correctly clears any pre-existing data in the target table before inserting new records.

**Setup:**
1.  **Legacy (Oracle):**
    *   `isbert_schema.dwtk_meldungen`: Contains the same entry as Test Case 1.
    *   `sof$ta_bpr_instance`:
        ```sql
        INSERT INTO sof$ta_bpr_instance (cntrct_id, bpr_id) VALUES (3001, 4001);
        INSERT INTO sof$ta_bpr_instance (cntrct_id, bpr_id) VALUES (3002, 4002);
        ```
    *   `sof$ta_bpr_optionen`: Contains pre-existing data that should be removed.
        ```sql
        INSERT INTO sof$ta_bpr_optionen (cntrct_id, bpr_id) VALUES (9999, 8888);
        INSERT INTO sof$ta_bpr_optionen (cntrct_id, bpr_id) VALUES (7777, 6666);
        ```
    *   Parameters: `p_JobKennung="TEST_JOB_03"`, `p_EintragsNr="003"`, `p_Stichtag="22012023"`, `p_wiederanlaufWert="0"`

2.  **Migrated (BigQuery):**
    *   `my-gcp-project.isbert_schema_dataset.dwtk_meldungen`: Contains the same entry as Test Case 1.
    *   `my-gcp-project.my_dataset.sof_ta_bpr_instance`:
        ```sql
        TRUNCATE TABLE `my-gcp-project.my_dataset.sof_ta_bpr_instance`;
        INSERT INTO `my-gcp-project.my_dataset.sof_ta_bpr_instance` (cntrct_id, bpr_id) VALUES (3001, 4001);
        INSERT INTO `my-gcp-project.my_dataset.sof_ta_bpr_instance` (cntrct_id, bpr_id) VALUES (3002, 4002);
        ```
    *   `my-gcp-project.my_dataset.sof_ta_bpr_optionen`:
        ```sql
        TRUNCATE TABLE `my-gcp-project.my_dataset.sof_ta_bpr_optionen`; -- Ensure it's clean before pre-populating
        INSERT INTO `my-gcp-project.my_dataset.sof_ta_bpr_optionen` (cntrct_id, bpr_id) VALUES (9999, 8888);
        INSERT INTO `my-gcp-project.my_dataset.sof_ta_bpr_optionen` (cntrct_id, bpr_id) VALUES (7777, 6666);
        ```
    *   `my-gcp-project.my_dataset.error_log`: Empty.
    *   `my-gcp-project.my_dataset.job_log`: Empty.
    *   Parameters: `job_kennung="TEST_JOB_03"`, `eintrags_nr="003"`, `stichtag="22012023"`, `wiederanlauf_wert="0"`

**Action:**
1.  **Legacy:**
    ```bash
    ./k_ausd_bp_ta_bpr_optionen.ksh -j "TEST_JOB_03" -f "003" -s "22012023" -l "0"
    ```
2.  **Migrated:**
    ```python
    python invoke_sp_d_ausd_bp_ta_bpr_optionen.py \
        --project_id "my-gcp-project" \
        --dataset_id "my_dataset" \
        --isbert_schema_dataset_id "isbert_schema_dataset" \
        --job_kennung "TEST_JOB_03" \
        --eintrags_nr "003" \
        --stichtag "22012023" \
        --wiederanlauf_wert "0"
    ```

**Pass/Fail Criterion:**
*   **Output Parity (Data):** Both Oracle `sof$ta_bpr_optionen` and BigQuery `my-gcp-project.my_dataset.sof_ta_bpr_optionen` must contain only the new data (2 rows: (3001, 4001), (3002, 4002)) and none of the pre-existing data.
    *   **BigQuery Assertion:**
        ```sql
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM `my-gcp-project.my_dataset.sof_ta_bpr_optionen`) = 2
                AND (SELECT COUNT(*) FROM `my-gcp-project.my_dataset.sof_ta_bpr_optionen` WHERE cntrct_id = 3001 AND bpr_id = 4001) = 1
                AND (SELECT COUNT(*) FROM `my-gcp-project.my_dataset.sof_ta_bpr_optionen` WHERE cntrct_id = 3002 AND bpr_id = 4002) = 1
                AND (SELECT COUNT(*) FROM `my-gcp-project.my_dataset.sof_ta_bpr_optionen` WHERE cntrct_id IN (9999, 7777)) = 0 -- Verify old data is gone
                THEN 'PASS'
                ELSE 'FAIL'
            END AS test_result;
        ```
*   **External System Replacements:** The Oracle `DWPA_UTIL_SKRIPT.runstatement` for `TRUNCATE` is correctly replaced by BigQuery's `TRUNCATE TABLE`.

---

### Test Case 4: Missing Required Parameter (p_JobKennung)

**Purpose:**
Verify that the migrated job correctly identifies and handles missing required parameters, logs an error, and aborts without modifying the target table.

**Setup:**
1.  **Legacy (Oracle):**
    *   `isbert_schema.dwtk_meldungen`: Contains the same entry as Test Case 1.
    *   `sof$ta_bpr_instance`: Contains data (e.g., from Test Case 1 setup).
    *   `sof$ta_bpr_optionen`: Contains known data (e.g., from Test Case 1 setup) that should remain unchanged.
    *   Parameters: `p_JobKennung` is *missing*, `p_EintragsNr="004"`, `p_Stichtag="23012023"`, `p_wiederanlaufWert="0"`

2.  **Migrated (BigQuery):**
    *   `my-gcp-project.isbert_schema_dataset.dwtk_meldungen`: Contains the same entry as Test Case 1.
    *   `my-gcp-project.my_dataset.sof_ta_bpr_instance`: Contains data (e.g., from Test Case 1 setup).
    *   `my-gcp-project.my_dataset.sof_ta_bpr_optionen`: Contains known data (e.g., from Test Case 1 setup) that should remain unchanged.
        ```sql
        -- Assuming it has 3 rows from a previous successful run
        INSERT INTO `my-gcp-project.my_dataset.sof_ta_bpr_optionen` (cntrct_id, bpr_id) VALUES (1001, 2001), (1002, 2002), (1003, 2003);
        ```
    *   `my-gcp-project.my_dataset.error_log`: Empty.
    *   `my-gcp-project.my_dataset.job_log`: Empty.
    *   Parameters: `job_kennung` is *missing*, `eintrags_nr="004"`, `stichtag="23012023"`, `wiederanlauf_wert="0"`

**Action:**
1.  **Legacy:**
    ```bash
    # Note: -j is omitted
    ./k_ausd_bp_ta_bpr_optionen.ksh -f "004" -s "23012023" -l "0"
    ```
2.  **Migrated:**
    ```python
    # Note: --job_kennung is omitted
    # This command is expected to fail and raise an exception
    try:
        python invoke_sp_d_ausd_bp_ta_bpr_optionen.py \
            --project_id "my-gcp-project" \
            --dataset_id "my_dataset" \
            --isbert_schema_dataset_id "isbert_schema_dataset" \
            --eintrags_nr "004" \
            --stichtag "23012023" \
            --wiederanlauf_wert "0"
    except Exception as e:
        print(f"Migrated job failed as expected: {e}")
    ```

**Pass/Fail Criterion:**
*   **Output Parity (Error Handling):** Both legacy and migrated jobs must fail. The legacy job should output an error message (e.g., "FEHLER: 0 E 193 Jobkennung"). The migrated job must log an error in `my-gcp-project.my_dataset.error_log` with the specific error code and message.
    *   **BigQuery Assertion:**
        ```sql
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM `my-gcp-project.my_dataset.error_log`
                     WHERE job_name = 'sp_d_ausd_bp_ta_bpr_optionen'
                       AND error_nr = 1001
                       AND error_arg = 'p_JobKennung cannot be NULL or empty') = 1
                AND (SELECT COUNT(*) FROM `my-gcp-project.my_dataset.job_log`) = 0 -- No successful job log
                THEN 'PASS'
                ELSE 'FAIL'
            END AS test_result;
        ```
*   **Data Quality/Row Count:** The target table `my-gcp-project.my_dataset.sof_ta_bpr_optionen` must remain unchanged (e.g., still contain the 3 pre-existing rows).
    *   **BigQuery Assertion:**
        ```sql
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM `my-gcp-project.my_dataset.sof_ta_bpr_optionen`) = 3
                AND (SELECT COUNT(*) FROM `my-gcp-project.my_dataset.sof_ta_bpr_optionen` WHERE cntrct_id = 1001 AND bpr_id = 2001) = 1
                THEN 'PASS'
                ELSE 'FAIL'
            END AS test_result;
        ```
*   **Transformation Correctness:** No data transformation should occur.

---

### Test Case 5: Invalid Date Format (p_Stichtag)

**Purpose:**
Verify that the migrated job correctly validates the `p_Stichtag` format, logs an error, and aborts without modifying the target table.

**Setup:**
1.  **Legacy (Oracle):**
    *   `isbert_schema.dwtk_meldungen`: Contains the same entry as Test Case 1.
    *   `sof$ta_bpr_instance`: Contains data (e.g., from Test Case 1 setup).
    *   `sof$ta_bpr_optionen`: Contains known data (e.g., from Test Case 1 setup) that should remain unchanged.
    *   Parameters: `p_JobKennung="TEST_JOB_05"`, `p_EintragsNr="005"`, `p_Stichtag="2023-01-24"` (invalid format), `p_wiederanlaufWert="0"`

2.  **Migrated (BigQuery):**
    *   `my-gcp-project.isbert_schema_dataset.dwtk_meldungen`: Contains the same entry as Test Case 1.
    *   `my-gcp-project.my_dataset.sof_ta_bpr_instance`: Contains data (e.g., from Test Case 1 setup).
    *   `my-gcp-project.my_dataset.sof_ta_bpr_optionen`: Contains known data (e.g., from Test Case 1 setup) that should remain unchanged.
        ```sql
        -- Assuming it has 3 rows from a previous successful run
        INSERT INTO `my-gcp-project.my_dataset.sof_ta_bpr_optionen` (cntrct_id, bpr_id) VALUES (1001, 2001), (1002, 2002), (1003, 2003);
        ```
    *   `my-gcp-project.my_dataset.error_log`: Empty.
    *   `my-gcp-project.my_dataset.job_log`: Empty.
    *   Parameters: `job_kennung="TEST_JOB_05"`, `eintrags_nr="005"`, `stichtag="2023-01-24"` (invalid format), `wiederanlauf_wert="0"`

**Action:**
1.  **Legacy:**
    ```bash
    # This command is expected to fail due to date check
    ./k_ausd_bp_ta_bpr_optionen.ksh -j "TEST_JOB_05" -f "005" -s "2023-01-24" -l "0"
    ```
2.  **Migrated:**
    ```python
    # This command is expected to fail and raise an exception
    try:
        python invoke_sp_d_ausd_bp_ta_bpr_optionen.py \
            --project_id "my-gcp-project" \
            --dataset_id "my_dataset" \
            --isbert_schema_dataset_id "isbert_schema_dataset" \
            --job_kennung "TEST_JOB_05" \
            --eintrags_nr "005" \
            --stichtag "2023-01-24" \
            --wiederanlauf_wert "0"
    except Exception as e:
        print(f"Migrated job failed as expected: {e}")
    ```

**Pass/Fail Criterion:**
*   **Output Parity (Error Handling):** Both legacy and migrated jobs must fail. The legacy job should output an error message (e.g., "FEHLER: 0 E 194 Datum"). The migrated job must log an error in `my-gcp-project.my_dataset.error_log` with the specific error code and message.
    *   **BigQuery Assertion:**
        ```sql
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM `my-gcp-project.my_dataset.error_log`
                     WHERE job_name = 'sp_d_ausd_bp_ta_bpr_optionen'
                       AND error_nr = 1004
                       AND error_arg = 'Invalid date format for p_Stichtag. Expected DDMMYYYY.') = 1
                AND (SELECT COUNT(*) FROM `my-gcp-project.my_dataset.job_log`) = 0 -- No successful job log
                THEN 'PASS'
                ELSE 'FAIL'
            END AS test_result;
        ```
*   **Data Quality/Row Count:** The target table `my-gcp-project.my_dataset.sof_ta_bpr_optionen` must remain unchanged (e.g., still contain the 3 pre-existing rows).
    *   **BigQuery Assertion:**
        ```sql
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM `my-gcp-project.my_dataset.sof_ta_bpr_optionen`) = 3
                THEN 'PASS'
                ELSE 'FAIL'
            END AS test_result;
        ```
*   **Transformation Correctness:** No data transformation should occur.

---

### Test Case 6: `dwtk_meldungen` Date Derivation (Edge Case - No Matching Entry)

**Purpose:**
Verify the `v_datum` derivation logic when no matching `job_kennung = 'BERT_DROP_TEMP_TABLE'` entry exists in `dwtk_meldungen`. It should default to '19000101'.

**Setup:**
1.  **Legacy (Oracle):**
    *   `isbert_schema.dwtk_meldungen`:
        ```sql
        TRUNCATE TABLE isbert_schema.dwtk_meldungen;
        INSERT INTO isbert_schema.dwtk_meldungen (job_kennung, timecreated) VALUES ('OTHER_JOB_A', TO_TIMESTAMP('2023-01-10 09:00:00', 'YYYY-MM-DD HH24:MI:SS'));
        INSERT INTO isbert_schema.dwtk_meldungen (job_kennung, timecreated) VALUES ('OTHER_JOB_B', TO_TIMESTAMP('2023-01-11 10:00:00', 'YYYY-MM-DD HH24:MI:SS'));
        ```
    *   `sof$ta_bpr_instance`: Contains data (e.g., from Test Case 1 setup).
    *   `sof$ta_bpr_optionen`: Empty or contains pre-existing data.
    *   Parameters: `p_JobKennung="TEST_JOB_06"`, `p_EintragsNr="006"`, `p_Stichtag="25012023"`, `p_wiederanlaufWert="0"`

2.  **Migrated (BigQuery):**
    *   `my-gcp-project.isbert_schema_dataset.dwtk_meldungen`:
        ```sql
        TRUNCATE TABLE `my-gcp-project.isbert_schema_dataset.dwtk_meldungen`;
        INSERT INTO `my-gcp-project.isbert_schema_dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES ('OTHER_JOB_A', '2023-01-10 09:00:00 UTC');
        INSERT INTO `my-gcp-project.isbert_schema_dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES ('OTHER_JOB_B', '2023-01-11 10:00:00 UTC');
        ```
    *   `my-gcp-project.my_dataset.sof_ta_bpr_instance`: Contains data (e.g., from Test Case 1 setup).
    *   `my-gcp-project.my_dataset.sof_ta_bpr_optionen`: Empty or contains pre-existing data.
    *   `my-gcp-project.my_dataset.error_log`: Empty.
    *   `my-gcp-project.my_dataset.job_log`: Empty.
    *   Parameters: `job_kennung="TEST_JOB_06"`, `eintrags_nr="006"`, `stichtag="25012023"`, `wiederanlauf_wert="0"`

**Action:**
1.  **Legacy:**
    ```bash
    ./k_ausd_bp_ta_bpr_optionen.ksh -j "TEST_JOB_06" -f "006" -s "25012023" -l "0"
    ```
2.  **Migrated:**
    ```python
    python invoke_sp_d_ausd_bp_ta_bpr_optionen.py \
        --project_id "my-gcp-project" \
        --dataset_id "my_dataset" \
        --isbert_schema_dataset_id "isbert_schema_dataset" \
        --job_kennung "TEST_JOB_06" \
        --eintrags_nr "006" \
        --stichtag "25012023" \
        --wiederanlauf_wert "0"
    ```

**Pass/Fail Criterion:**
*   **Output Parity (Data):** The data in Oracle `sof$ta_bpr_optionen` must be identical to `my-gcp-project.my_dataset.sof_ta_bpr_optionen` (i.e., the data from `sof_ta_bpr_instance` should be loaded successfully, as `v_datum` is not used in the `INSERT` statement).
    *   **BigQuery Assertion:**
        ```sql
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM `my-gcp-project.my_dataset.sof_ta_bpr_optionen`) = 3 -- Assuming 3 rows in sof_ta_bpr_instance
                THEN 'PASS'
                ELSE 'FAIL'
            END AS test_result;
        ```
*   **Transformation Correctness:** The `v_datum` variable inside the BigQuery stored procedure should have been correctly set to '19000101'. (This is an internal check, not directly observable from `job_log` as `v_datum` is not logged. If `v_datum` were critical for the `INSERT` logic, this would be a failure. Since it's not, we verify the job still completes successfully).
*   **External System Replacements:** The BigQuery `SELECT IFNULL(MAX(FORMAT_DATE('%Y%m%d', DATE(timecreated))), '19000101')` logic correctly handles the absence of matching records.

---

### Test Case 7: Orchestration Layer - Default `stichtag`

**Purpose:**
Verify that the Python orchestration script correctly defaults the `stichtag` parameter to yesterday's date in `DDMMYYYY` format when it is not explicitly provided.

**Setup:**
1.  **Legacy (Oracle):** Not directly applicable as `stichtag` is always explicitly passed to the ksh script. This test focuses on the Python orchestration layer's behavior.
    *   `isbert_schema.dwtk_meldungen`: Contains the same entry as Test Case 1.
    *   `sof$ta_bpr_instance`: Contains data (e.g., from Test Case 1 setup).
    *   `sof$ta_bpr_optionen`: Empty or contains pre-existing data.

2.  **Migrated (BigQuery):**
    *   `my-gcp-project.isbert_schema_dataset.dwtk_meldungen`: Contains the same entry as Test Case 1.
    *   `my-gcp-project.my_dataset.sof_ta_bpr_instance`: Contains data (e.g., from Test Case 1 setup).
    *   `my-gcp-project.my_dataset.sof_ta_bpr_optionen`: Empty or contains pre-existing data.
    *   `my-gcp-project.my_dataset.error_log`: Empty.
    *   `my-gcp-project.my_dataset.job_log`: Empty.
    *   Parameters: `job_kennung="TEST_JOB_07"`, `eintrags_nr="007"`, `stichtag` is *omitted*, `wiederanlauf_wert=""`

**Action:**
1.  **Legacy:** Not applicable for this specific test of Python orchestration default behavior.
2.  **Migrated:**
    ```python
    # Note: --stichtag is omitted
    python invoke_sp_d_ausd_bp_ta_bpr_optionen.py \
        --project_id "my-gcp-project" \
        --dataset_id "my_dataset" \
        --isbert_schema_dataset_id "isbert_schema_dataset" \
        --job_kennung "TEST_JOB_07" \
        --eintrags_nr "007" \
        --wiederanlauf_wert ""
    ```

**Pass/Fail Criterion:**
*   **External System Replacements:** The `job_log` entry for `stichtag` must match yesterday's date in `DDMMYYYY` format. The `wiederanlauf_wert` should be logged as an empty string `""` as passed by the Python script's default.
    *   **BigQuery Assertion:**
        ```sql
        SELECT
            CASE
                WHEN (SELECT COUNT(*) FROM `my-gcp-project.my_dataset.error_log`) = 0
                AND (SELECT COUNT(*) FROM `my-gcp-project.my_dataset.job_log`
                     WHERE job_kennung = 'TEST_JOB_07'
                       AND eintrags_nr = '007'
                       AND stichtag = FORMAT_DATE('%d%m%Y', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
                       AND wiederanlauf_wert = '' -- Python default for optional arg
                       AND records = 3) = 1 -- Assuming 3 rows in sof_ta_bpr_instance
                THEN 'PASS'
                ELSE 'FAIL'
            END AS test_result;
        ```
*   **Data Quality/Row Count:** The target table `my-gcp-project.my_dataset.sof_ta_bpr_optionen` should contain the expected data from `sof_ta_bpr_instance`.

---