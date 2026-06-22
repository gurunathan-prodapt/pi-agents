As a senior data-migration QA engineer, I've analyzed the provided migration design and generated code for `k_aurd_rechstan.ksh` to `r_aurd_rechstan_sp` in BigQuery. The following test cases are designed to ensure behavioral equivalence, data integrity, and correct integration with the new GCP architecture.

---

### **Pre-requisite Setup for All Tests**

Before running any tests, ensure the following:

1.  **BigQuery Project and Dataset:** `my_project.my_dataset` exists.
2.  **DDL Deployment:** The DDLs for `job_table`, `error_log`, `RKopfStan`, and `source_rechstan_data` (mock source) are deployed to BigQuery.
3.  **Stored Procedure Deployment:** The `r_aurd_rechstan_sp.sql` stored procedure is deployed.
4.  **Mock Source Data:** The `source_rechstan_data` table is populated with diverse test data.

```sql
-- DDL for mock source_rechstan_data (if not already created)
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.source_rechstan_data`
(
    source_id STRING NOT NULL,
    source_date DATE NOT NULL,
    source_attr_1 STRING,
    source_attr_2 INT64
)
PARTITION BY source_date;

-- Sample data for source_rechstan_data
-- This data will be used across various tests.
TRUNCATE TABLE `my_project.my_dataset.source_rechstan_data`;
INSERT INTO `my_project.my_dataset.source_rechstan_data` (source_id, source_date, source_attr_1, source_attr_2) VALUES
('SRC001', '2023-10-26', 'SourceA', 100),
('SRC002', '2023-10-26', 'SourceB', 200),
('SRC003', '2023-10-27', 'SourceC', 300),
('SRC004', '2023-10-27', 'SourceD', 400),
('SRC005', '2023-10-28', 'SourceE', 500),
('SRC006', '2023-10-28', 'SourceF', 600),
('SRC007', '2023-10-29', 'SourceG', 700),
('SRC008', '2023-10-29', 'SourceH', 800),
('SRC009', '2023-10-29', 'SourceI', 900);
```

---

### **Test Case 1: Mandatory Parameter Validation - Missing `p_job_kennung`**

*   **Purpose:** Verify that the stored procedure correctly identifies and raises an error when a mandatory parameter (`p_job_kennung`) is missing or empty, mimicking the legacy script's `pruefeParameterGesetzt` behavior.
*   **Setup:**
    *   Ensure `job_table` and `error_log` are empty.
*   **Action:**
    *   Call the `r_aurd_rechstan` stored procedure with `p_job_kennung` as `NULL` or an empty string.
*   **Expected Result / Pass/Fail Criterion:**
    *   The stored procedure execution fails.
    *   An entry is recorded in `my_project.my_dataset.error_log` with `error_message` containing "JobKennung (p_job_kennung) is missing."
    *   An entry is recorded in `my_project.my_dataset.job_table` with `status` 'FAILED' and `error_message` containing "JobKennung (p_job_kennung) is missing."

```sql
-- Action: Call SP with missing p_job_kennung
-- This will cause the SP to fail and log the error.
CALL `my_project.my_dataset.r_aurd_rechstan`(
    NULL, -- p_job_kennung is NULL
    '1001',
    '26102023',
    0
);

-- Pass/Fail Criterion (SQL Assertions):
-- Check error_log
SELECT
    COUNT(1)
FROM
    `my_project.my_dataset.error_log`
WHERE
    error_message LIKE '%JobKennung (p_job_kennung) is missing%'
    AND procedure_name = 'r_aurd_rechstan';
-- Expected: 1

-- Check job_table status
SELECT
    COUNT(1)
FROM
    `my_project.my_dataset.job_table`
WHERE
    status = 'FAILED'
    AND error_message LIKE '%JobKennung (p_job_kennung) is missing%';
-- Expected: 1
```

```python
# Pytest example for Test Case 1
def test_missing_job_kennung(bigquery_client, cleanup_tables):
    result = call_r_aurd_rechstan_sp(bigquery_client, None, '1001', '26102023', 0)
    assert result["status"] == "FAILED"
    assert "JobKennung (p_job_kennung) is missing" in result["error"]

    # Verify error_log entry
    error_log_query = """
        SELECT COUNT(1) FROM `my_project.my_dataset.error_log`
        WHERE error_message LIKE '%JobKennung (p_job_kennung) is missing%'
        AND procedure_name = 'r_aurd_rechstan'
    """
    error_log_count = bigquery_client.query(error_log_query).result().total_rows
    assert error_log_count == 1

    # Verify job_table entry
    job_table_query = """
        SELECT COUNT(1) FROM `my_project.my_dataset.job_table`
        WHERE status = 'FAILED' AND error_message LIKE '%JobKennung (p_job_kennung) is missing%'
    """
    job_table_count = bigquery_client.query(job_table_query).result().total_rows
    assert job_table_count == 1
```

---

### **Test Case 2: Mandatory Parameter Validation - Missing `p_stichtag`**

*   **Purpose:** Verify that the stored procedure correctly identifies and raises an error when `p_stichtag` is missing or empty, mimicking the legacy script's `pruefeParameterGesetzt` behavior.
*   **Setup:**
    *   Ensure `job_table` and `error_log` are empty.
*   **Action:**
    *   Call the `r_aurd_rechstan` stored procedure with `p_stichtag` as `NULL` or an empty string.
*   **Expected Result / Pass/Fail Criterion:**
    *   The stored procedure execution fails.
    *   An entry is recorded in `my_project.my_dataset.error_log` with `error_message` containing "Stichtag (p_stichtag) is missing."
    *   An entry is recorded in `my_project.my_dataset.job_table` with `status` 'FAILED' and `error_message` containing "Stichtag (p_stichtag) is missing."

```sql
-- Action: Call SP with missing p_stichtag
CALL `my_project.my_dataset.r_aurd_rechstan`(
    'JOB_ID_TEST',
    '1001',
    '', -- p_stichtag is empty string
    0
);

-- Pass/Fail Criterion (SQL Assertions):
SELECT
    COUNT(1)
FROM
    `my_project.my_dataset.error_log`
WHERE
    error_message LIKE '%Stichtag (p_stichtag) is missing%'
    AND procedure_name = 'r_aurd_rechstan';
-- Expected: 1

SELECT
    COUNT(1)
FROM
    `my_project.my_dataset.job_table`
WHERE
    status = 'FAILED'
    AND error_message LIKE '%Stichtag (p_stichtag) is missing%';
-- Expected: 1
```

---

### **Test Case 3: Date Format Validation - Invalid `p_stichtag`**

*   **Purpose:** Verify that the stored procedure correctly validates the `p_stichtag` format (DDMMYYYY) and raises an error for invalid formats, replicating `DWDate_Datum_Check` functionality.
*   **Setup:**
    *   Ensure `job_table` and `error_log` are empty.
*   **Action:**
    *   Call the `r_aurd_rechstan` stored procedure with `p_stichtag` in an incorrect format (e.g., '2023-10-26', '26/10/2023', 'ABC').
*   **Expected Result / Pass/Fail Criterion:**
    *   The stored procedure execution fails.
    *   An entry is recorded in `my_project.my_dataset.error_log` with `error_message` containing "Invalid date format for Stichtag (p_stichtag):" and the provided invalid date.
    *   An entry is recorded in `my_project.my_dataset.job_table` with `status` 'FAILED' and a similar error message.

```sql
-- Action: Call SP with invalid p_stichtag format
CALL `my_project.my_dataset.r_aurd_rechstan`(
    'JOB_ID_TEST',
    '1001',
    '2023-10-26', -- Invalid format (YYYY-MM-DD instead of DDMMYYYY)
    0
);

-- Pass/Fail Criterion (SQL Assertions):
SELECT
    COUNT(1)
FROM
    `my_project.my_dataset.error_log`
WHERE
    error_message LIKE '%Invalid date format for Stichtag (p_stichtag): 2023-10-26%'
    AND procedure_name = 'r_aurd_rechstan';
-- Expected: 1

SELECT
    COUNT(1)
FROM
    `my_project.my_dataset.job_table`
WHERE
    status = 'FAILED'
    AND error_message LIKE '%Invalid date format for Stichtag (p_stichtag): 2023-10-26%';
-- Expected: 1
```

---

### **Test Case 4: Default `p_wiederanlauf_wert` Handling**

*   **Purpose:** Verify that `p_wiederanlauf_wert` defaults to `0` when not provided, matching the legacy script's `if [[ -z "$p_wiederanlaufWert" ]]` logic.
*   **Setup:**
    *   Ensure `job_table` is empty.
*   **Action:**
    *   Call the `r_aurd_rechstan` stored procedure omitting the `p_wiederanlauf_wert` parameter.
*   **Expected Result / Pass/Fail Criterion:**
    *   The stored procedure executes successfully (assuming valid other parameters and source data).
    *   An entry is recorded in `my_project.my_dataset.job_table` where `restart_value` is `0`.

```sql
-- Action: Call SP without p_wiederanlauf_wert
CALL `my_project.my_dataset.r_aurd_rechstan`(
    'JOB_ID_TEST_DEFAULT_RESTART',
    '1002',
    '26102023' -- p_wiederanlauf_wert omitted
);

-- Pass/Fail Criterion (SQL Assertions):
SELECT
    COUNT(1)
FROM
    `my_project.my_dataset.job_table`
WHERE
    job_id = (SELECT job_id FROM `my_project.my_dataset.job_table` WHERE entry_number = '1002' LIMIT 1)
    AND restart_value = 0
    AND status = 'SUCCESS';
-- Expected: 1
```

---

### **Test Case 5: Successful Execution - Initial Data Load (Output Parity & Transformation Correctness)**

*   **Purpose:** Verify that the stored procedure successfully processes new data from `source_rechstan_data` into `RKopfStan` for a given `p_stichtag`, and correctly logs the job status and record count. This covers output parity for `RKopfStan` and `job_table`.
*   **Setup:**
    *   Ensure `RKopfStan`, `job_table`, and `error_log` are empty.
    *   `source_rechstan_data` contains data for '2023-10-26' (2 records).
*   **Action:**
    *   Call the `r_aurd_rechstan` stored procedure with `p_stichtag = '26102023'`.
*   **Expected Result / Pass/Fail Criterion:**
    *   The stored procedure executes successfully.
    *   `my_project.my_dataset.RKopfStan` contains 2 new records with `stichtag_date = '2023-10-26'`, matching `source_id`, `source_attr_1`, `source_attr_2`.
    *   `my_project.my_dataset.job_table` contains one entry with `status = 'SUCCESS'`, `processed_records = 2`, and `reference_date = '2023-10-26'`.
    *   `error_log` remains empty.

```sql
-- Action: Call SP for initial load
CALL `my_project.my_dataset.r_aurd_rechstan`(
    'JOB_ID_INITIAL_LOAD',
    '1003',
    '26102023',
    0
);

-- Pass/Fail Criterion (SQL Assertions):
-- Check RKopfStan content and count
SELECT
    COUNT(1)
FROM
    `my_project.my_dataset.RKopfStan`
WHERE
    stichtag_date = '2023-10-26';
-- Expected: 2

SELECT
    COUNT(1)
FROM
    `my_project.my_dataset.RKopfStan` AS T
JOIN
    `my_project.my_dataset.source_rechstan_data` AS S
ON
    T.rkopf_id = S.source_id
    AND T.stichtag_date = S.source_date
WHERE
    T.stichtag_date = '2023-10-26'
    AND T.attribute_1 = S.source_attr_1
    AND T.attribute_2 = S.source_attr_2;
-- Expected: 2 (ensures data parity)

-- Check job_table entry
SELECT
    COUNT(1)
FROM
    `my_project.my_dataset.job_table`
WHERE
    entry_number = '1003'
    AND reference_date = '2023-10-26'
    AND status = 'SUCCESS'
    AND processed_records = 2;
-- Expected: 1

-- Check error_log (should be empty)
SELECT COUNT(1) FROM `my_project.my_dataset.error_log`;
-- Expected: 0
```

---

### **Test Case 6: Successful Execution - Data Update (Transformation Correctness)**

*   **Purpose:** Verify that the stored procedure correctly updates existing records in `RKopfStan` when matching `source_rechstan_data` is processed again, demonstrating the `MERGE` statement's `WHEN MATCHED THEN UPDATE` clause.
*   **Setup:**
    *   Run Test Case 5 first to populate `RKopfStan` with data for '2023-10-26'.
    *   Update `source_rechstan_data` for '2023-10-26' to simulate changes.
*   **Action:**
    *   Update `source_rechstan_data` for `source_id = 'SRC001'` (e.g., `source_attr_1 = 'UpdatedA'`, `source_attr_2 = 150`).
    *   Call the `r_aurd_rechstan` stored procedure again with `p_stichtag = '26102023'`.
*   **Expected Result / Pass/Fail Criterion:**
    *   The stored procedure executes successfully.
    *   `my_project.my_dataset.RKopfStan` for `rkopf_id = 'SRC001'` has `attribute_1 = 'UpdatedA'`, `attribute_2 = 150`, and an updated `creation_timestamp`.
    *   The total count of records for `stichtag_date = '2023-10-26'` in `RKopfStan` remains 2 (no new inserts, only updates).
    *   `my_project.my_dataset.job_table` contains a new entry (or updated if job_id was the same) with `status = 'SUCCESS'` and `processed_records = 2` (reflecting 2 records were processed, even if one was an update).

```sql
-- Setup: First, run Test Case 5 to populate initial data.
-- Then, update source data:
UPDATE `my_project.my_dataset.source_rechstan_data`
SET source_attr_1 = 'UpdatedA', source_attr_2 = 150
WHERE source_id = 'SRC001' AND source_date = '2023-10-26';

-- Action: Call SP again for the same date
CALL `my_project.my_dataset.r_aurd_rechstan`(
    'JOB_ID_UPDATE_LOAD',
    '1004',
    '26102023',
    0
);

-- Pass/Fail Criterion (SQL Assertions):
-- Check updated record in RKopfStan
SELECT
    attribute_1, attribute_2
FROM
    `my_project.my_dataset.RKopfStan`
WHERE
    rkopf_id = 'SRC001' AND stichtag_date = '2023-10-26';
-- Expected: ('UpdatedA', 150)

-- Check total count for the date (should remain 2)
SELECT
    COUNT(1)
FROM
    `my_project.my_dataset.RKopfStan`
WHERE
    stichtag_date = '2023-10-26';
-- Expected: 2

-- Check job_table entry for the update run
SELECT
    COUNT(1)
FROM
    `my_project.my_dataset.job_table`
WHERE
    entry_number = '1004'
    AND reference_date = '2023-10-26'
    AND status = 'SUCCESS'
    AND processed_records = 2; -- 2 records were processed (1 update, 1 no-change merge)
-- Expected: 1
```

---

### **Test Case 7: Successful Execution - No Matching Source Data**

*   **Purpose:** Verify that the stored procedure handles cases where no matching data is found in the source for the given `p_stichtag`, resulting in zero records processed and a successful job status.
*   **Setup:**
    *   Ensure `RKopfStan` does not contain data for '2023-10-30'.
    *   `source_rechstan_data` does not contain data for '2023-10-30'.
*   **Action:**
    *   Call the `r_aurd_rechstan` stored procedure with `p_stichtag = '30102023'`.
*   **Expected Result / Pass/Fail Criterion:**
    *   The stored procedure executes successfully.
    *   `my_project.my_dataset.RKopfStan` remains unchanged for `stichtag_date = '2023-10-30'` (0 records).
    *   `my_project.my_dataset.job_table` contains one entry with `status = 'SUCCESS'`, `processed_records = 0`, and `reference_date = '2023-10-30'`.
    *   `error_log` remains empty.

```sql
-- Action: Call SP for a date with no source data
CALL `my_project.my_dataset.r_aurd_rechstan`(
    'JOB_ID_NO_DATA',
    '1005',
    '30102023', -- No data for this date in source_rechstan_data
    0
);

-- Pass/Fail Criterion (SQL Assertions):
-- Check RKopfStan content (should be 0 records for this date)
SELECT
    COUNT(1)
FROM
    `my_project.my_dataset.RKopfStan`
WHERE
    stichtag_date = '2023-10-30';
-- Expected: 0

-- Check job_table entry
SELECT
    COUNT(1)
FROM
    `my_project.my_dataset.job_table`
WHERE
    entry_number = '1005'
    AND reference_date = '2023-10-30'
    AND status = 'SUCCESS'
    AND processed_records = 0;
-- Expected: 1
```

---

### **Test Case 8: Date Derivation Correctness (`v_heute_date`, `v_gestern_date`)**

*   **Purpose:** Verify that the internal date variables `v_heute_date` and `v_gestern_date` are correctly calculated using BigQuery's native functions, replacing the `gestern.ksh` script.
*   **Setup:**
    *   This test is primarily conceptual as these variables are internal to the SP and not directly exposed in the output tables. However, their correct calculation is crucial. We can infer their correctness by checking the `job_table`'s `start_time` and `reference_date` in relation to `CURRENT_DATE()`.
*   **Action:**
    *   Call the `r_aurd_rechstan` stored procedure with valid parameters.
*   **Expected Result / Pass/Fail Criterion:**
    *   The job completes successfully.
    *   The `job_table` entry's `start_time` should be close to `CURRENT_TIMESTAMP()`.
    *   The `reference_date` in `job_table` should match the parsed `p_stichtag`.
    *   (Implicit) If the core `d_aurd_rechstan.sql` logic *used* `v_heute_date` or `v_gestern_date` for filtering or transformation, we would assert on the data in `RKopfStan` reflecting these dates. Since the placeholder `MERGE` only uses `v_reference_date`, we can only verify their declaration and assignment.

```sql
-- Action: Call SP with valid parameters
CALL `my_project.my_dataset.r_aurd_rechstan`(
    'JOB_ID_DATE_CHECK',
    '1006',
    '27102023',
    0
);

-- Pass/Fail Criterion (SQL Assertions):
-- Check job_table entry for the run
SELECT
    start_time, reference_date
FROM
    `my_project.my_dataset.job_table`
WHERE
    entry_number = '1006'
    AND status = 'SUCCESS';
-- Expected: start_time should be recent (within seconds of query execution).
--           reference_date should be DATE('2023-10-27').
-- This indirectly confirms the SP's internal date handling is consistent with BQ's CURRENT_DATE().
```

---

### **Test Case 9: External System Replacements - Job Management System (`job_table` and `error_log`)**

*   **Purpose:** Verify that the `job_table` and `error_log` tables are correctly utilized for job status tracking and error reporting, replacing the legacy script's commented-out `FOSJobErzeugeEintrag` and `f_alis_msgerr.ksh` functionality.
*   **Setup:**
    *   This test is covered by the previous test cases (1, 2, 3, 5, 6, 7) which assert on the content of `job_table` and `error_log` under various scenarios (success, failure, different record counts).
*   **Action:**
    *   Execute a successful run (e.g., Test Case 5).
    *   Execute a failed run (e.g., Test Case 1).
*   **Expected Result / Pass/Fail Criterion:**
    *   **Successful Run:** `job_table` contains a 'RUNNING' entry at the start, updated to 'SUCCESS' with `end_time` and `processed_records` at the end. `error_log` remains empty.
    *   **Failed Run:** `job_table` contains a 'RUNNING' entry at the start, updated to 'FAILED' with `end_time` and `error_message` at the end. `error_log` contains a detailed entry for the error.
    *   All fields in `job_table` and `error_log` are populated as per their DDLs and the SP logic.

```sql
-- Pass/Fail Criterion (SQL Assertions - summary of previous checks):
-- Verify a successful job entry
SELECT
    COUNT(1)
FROM
    `my_project.my_dataset.job_table`
WHERE
    status = 'SUCCESS'
    AND start_time IS NOT NULL
    AND end_time IS NOT NULL
    AND processed_records IS NOT NULL
    AND reference_date IS NOT NULL;
-- Expected: > 0 (after running successful tests)

-- Verify a failed job entry
SELECT
    COUNT(1)
FROM
    `my_project.my_dataset.job_table`
WHERE
    status = 'FAILED'
    AND start_time IS NOT NULL
    AND end_time IS NOT NULL
    AND error_message IS NOT NULL;
-- Expected: > 0 (after running failed tests)

-- Verify error_log entry for a failed job
SELECT
    COUNT(1)
FROM
    `my_project.my_dataset.error_log`
WHERE
    log_time IS NOT NULL
    AND job_id IS NOT NULL
    AND error_message IS NOT NULL
    AND procedure_name = 'r_aurd_rechstan';
-- Expected: > 0 (after running failed tests)
```

---

### **Test Case 10: Orchestration Layer Integration (Cloud Composer DAG)**

*   **Purpose:** Verify that the Cloud Composer DAG can successfully invoke the BigQuery Stored Procedure and pass parameters correctly, ensuring the end-to-end orchestration works as designed.
*   **Setup:**
    *   Cloud Composer environment is set up.
    *   The `orchestration_dag.py` is deployed to the Composer environment.
    *   BigQuery connection is configured in Airflow.
    *   `RKopfStan`, `job_table`, `error_log` are empty.
*   **Action:**
    *   Manually trigger the `r_aurd_rechstan_orchestration` DAG in Cloud Composer for a specific `execution_date` (e.g., `2023-10-29`).
*   **Expected Result / Pass/Fail Criterion:**
    *   The Airflow DAG run completes successfully.
    *   The `execute_r_aurd_rechstan_sp` task within the DAG completes successfully.
    *   `my_project.my_dataset.RKopfStan` contains the expected records for `stichtag_date = '2023-10-29'` (3 records from `source_rechstan_data`).
    *   `my_project.my_dataset.job_table` contains an entry with `status = 'SUCCESS'`, `processed_records = 3`, and `reference_date = '2023-10-29'`.
    *   The `p_stichtag` parameter passed to the SP (e.g., '29102023') is correctly formatted by the DAG's `execution_date.strftime('%d%m%Y')`.

```sql
-- Action: Trigger the Airflow DAG for execution_date = 2023-10-29.
-- (This is an external action, not SQL)

-- Pass/Fail Criterion (SQL Assertions after DAG run):
-- Check RKopfStan content and count for the DAG's execution date
SELECT
    COUNT(1)
FROM
    `my_project.my_dataset.RKopfStan`
WHERE
    stichtag_date = '2023-10-29';
-- Expected: 3 (based on source_rechstan_data for this date)

-- Check job_table entry for the DAG run
SELECT
    COUNT(1)
FROM
    `my_project.my_dataset.job_table`
WHERE
    job_id = 'DAILY_RECHSTAN_JOB' -- As defined in DAG parameters
    AND entry_number = '1001' -- As defined in DAG parameters
    AND reference_date = '2023-10-29'
    AND status = 'SUCCESS'
    AND processed_records = 3;
-- Expected: 1
```