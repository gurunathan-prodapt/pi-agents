As a senior data-migration QA engineer, I've analyzed the provided migration design and the legacy KornShell script `k_ausd_v_ta_cntrct_crs2.ksh`. The migration involves translating orchestration logic, parameter handling, job status management, and core data processing (from `d_ausd_v_ta_cntrct_crs2.sql`) into BigQuery Stored Procedures and tables.

The tests below are designed to validate the behavioral equivalence of the migrated BigQuery components against the legacy system's documented behavior and the migration design's specifications.

---

## Migration Validation Tests for `k_ausd_v_ta_cntrct_crs2.ksh`

### Test Setup Prerequisites

Before running any tests, ensure the following BigQuery tables and procedures are deployed:

*   `project.dataset.job_table` (DDL provided)
*   `project.dataset.error_log` (DDL provided)
*   `project.dataset.sof_ta_cntrct_crs2` (DDL provided)
*   `project.dataset.p_ausd_v_ta_cntrct_crs2_data_logic` (Stored Procedure provided)
*   `project.dataset.k_ausd_v_ta_cntrct_crs2` (Stored Procedure provided)

Additionally, mock or populate the following source tables with representative data:

*   `project.dataset.dwtk_meldungen`: Contains `job_kennung` and `timecreated` for `v_stichtag_yyyymmdd` calculation.
*   `project.dataset.sof_ta_cntrct_crs`: The primary source table for the data transformation.

For comparison, it's ideal to have a "golden copy" of the expected output from the legacy system for `sof_ta_cntrct_crs2` under various input conditions. This can be stored in a BigQuery table like `project.dataset.golden_sof_ta_cntrct_crs2`.

---

### Test Case 1: Successful Execution with Valid Parameters

**Purpose:** Verify that the BigQuery procedure executes successfully with valid parameters, processes data correctly, updates the job table, and logs success. This covers output parity for a standard successful run.

**Setup:**
1.  Ensure `project.dataset.job_table`, `project.dataset.error_log`, and `project.dataset.sof_ta_cntrct_crs2` are empty.
2.  Populate `project.dataset.dwtk_meldungen` with sample data, including an entry for `job_kennung = 'BERT_DROP_TEMP_TABLE'` with a `timecreated` value.
3.  Populate `project.dataset.sof_ta_cntrct_crs` with a diverse set of contract data, including:
    *   Contracts with `cntrct_ty <> 10` and a matching `cntrct_parent` where the parent has `cntrct_ty = 10`.
    *   Contracts with `cntrct_ty <> 10` and a `cntrct_parent` that does *not* have `cntrct_ty = 10`.
    *   Contracts with `cntrct_ty <> 10` and `NULL` `cntrct_parent`.
    *   Contracts with `cntrct_ty = 10` (these should be filtered out from the main selection).
4.  Obtain the expected output for `sof_ta_cntrct_crs2` from a legacy run with the same input data and store it in `project.dataset.golden_sof_ta_cntrct_crs2`.

**Action:**
Execute the main BigQuery Stored Procedure with valid parameters:

```sql
CALL project.dataset.k_ausd_v_ta_cntrct_crs2(
    p_job_kennung => 'TEST_JOB_1',
    p_eintrags_nr => 'ENTRY_001'
);
```

**Pass/Fail Criterion:**
1.  The `CALL` statement completes successfully without raising an error.
2.  **Job Table Assertion:**
    *   Exactly one entry exists in `project.dataset.job_table` for `job_id = 'TEST_JOB_1'` and `entry_number = 'ENTRY_001'`.
    *   This entry has `status = 'SUCCESS'`, `table_name = 'ta_cntrct_crs2'`.
    *   `start_timestamp` is populated, `end_timestamp` is populated and after `start_timestamp`.
    *   `record_count` matches the number of rows in `project.dataset.sof_ta_cntrct_crs2`.
    *   `error_message` is `NULL`.
3.  **Error Log Assertion:**
    *   At least one `INFO` entry exists in `project.dataset.error_log` for `job_id = 'TEST_JOB_1'` and `entry_number = 'ENTRY_001'` indicating successful completion and processed records.
    *   No `ERROR` entries exist for this job run.
4.  **Output Parity (Data Transformation Assertion):**
    *   The data in `project.dataset.sof_ta_cntrct_crs2` is identical to the data in `project.dataset.golden_sof_ta_cntrct_crs2`.

```python
# Example pytest assertion for data parity
def test_successful_execution_data_parity(bigquery_client):
    # ... (setup code to populate source tables and call SP) ...

    # Assert job_table status
    query_job_status = """
        SELECT status, record_count, error_message
        FROM project.dataset.job_table
        WHERE job_id = 'TEST_JOB_1' AND entry_number = 'ENTRY_001'
        ORDER BY start_timestamp DESC LIMIT 1
    """
    job_status_result = bigquery_client.query(query_job_status).result()
    assert job_status_result.total_rows == 1
    row = list(job_status_result)[0]
    assert row['status'] == 'SUCCESS'
    assert row['error_message'] is None

    # Assert error_log for no errors
    query_error_log = """
        SELECT COUNT(*) FROM project.dataset.error_log
        WHERE job_id = 'TEST_JOB_1' AND entry_number = 'ENTRY_001' AND severity = 'ERROR'
    """
    error_count = bigquery_client.query(query_error_log).result().scalar_value()
    assert error_count == 0

    # Assert data parity
    query_data_parity = """
        SELECT
            (SELECT COUNT(*) FROM project.dataset.sof_ta_cntrct_crs2) = (SELECT COUNT(*) FROM project.dataset.golden_sof_ta_cntrct_crs2) AS count_match,
            (SELECT COUNT(*) FROM (
                SELECT * FROM project.dataset.sof_ta_cntrct_crs2
                EXCEPT DISTINCT
                SELECT * FROM project.dataset.golden_sof_ta_cntrct_crs2
            )) = 0 AS diff_match
    """
    data_parity_result = bigquery_client.query(query_data_parity).result()
    row = list(data_parity_result)[0]
    assert row['count_match'] is True, "Row counts do not match."
    assert row['diff_match'] is True, "Data content does not match golden copy."
```

---

### Test Case 2: Missing `p_job_kennung` Parameter

**Purpose:** Validate that the procedure correctly handles a missing `p_job_kennung` parameter, raises an error, and logs it. This tests parameter validation and error handling.

**Setup:**
1.  Ensure `project.dataset.job_table` and `project.dataset.error_log` are empty.

**Action:**
Attempt to execute the procedure with `p_job_kennung` as `NULL` or an empty string.

```sql
-- Attempt 1: NULL
CALL project.dataset.k_ausd_v_ta_cntrct_crs2(
    p_job_kennung => NULL,
    p_eintrags_nr => 'ENTRY_002'
);

-- Attempt 2: Empty string
CALL project.dataset.k_ausd_v_ta_cntrct_crs2(
    p_job_kennung => '',
    p_eintrags_nr => 'ENTRY_002'
);
```

**Pass/Fail Criterion:**
1.  The `CALL` statement raises an error with a message similar to "FEHLER: Jobkennung (j) ist ein notwendiges Argument und fehlt."
2.  **Job Table Assertion:** No entries are created in `project.dataset.job_table`.
3.  **Error Log Assertion:**
    *   Exactly one `ERROR` entry exists in `project.dataset.error_log` for the attempted `job_id` (which might be 'UNKNOWN' or `NULL`) and `entry_number = 'ENTRY_002'`.
    *   The `message` in the error log contains "Jobkennung (j) ist ein notwendiges Argument und fehlt."

```python
# Example pytest assertion for missing parameter
import pytest

def test_missing_job_kennung_parameter(bigquery_client):
    # ... (clear tables) ...

    with pytest.raises(Exception) as excinfo:
        bigquery_client.query("""
            CALL project.dataset.k_ausd_v_ta_cntrct_crs2(
                p_job_kennung => NULL,
                p_eintrags_nr => 'ENTRY_002'
            )
        """).result()
    assert "Jobkennung (j) ist ein notwendiges Argument und fehlt." in str(excinfo.value)

    # Assert job_table is empty
    query_job_table_count = "SELECT COUNT(*) FROM project.dataset.job_table"
    assert bigquery_client.query(query_job_table_count).result().scalar_value() == 0

    # Assert error_log entry
    query_error_log = """
        SELECT message, severity FROM project.dataset.error_log
        WHERE entry_number = 'ENTRY_002' AND severity = 'ERROR'
    """
    error_log_result = bigquery_client.query(query_error_log).result()
    assert error_log_result.total_rows == 1
    row = list(error_log_result)[0]
    assert "Jobkennung (j) ist ein notwendiges Argument und fehlt." in row['message']
```

---

### Test Case 3: Missing `p_eintrags_nr` Parameter

**Purpose:** Validate that the procedure correctly handles a missing `p_eintrags_nr` parameter, raises an error, and logs it.

**Setup:**
1.  Ensure `project.dataset.job_table` and `project.dataset.error_log` are empty.

**Action:**
Attempt to execute the procedure with `p_eintrags_nr` as `NULL` or an empty string.

```sql
-- Attempt 1: NULL
CALL project.dataset.k_ausd_v_ta_cntrct_crs2(
    p_job_kennung => 'TEST_JOB_3',
    p_eintrags_nr => NULL
);

-- Attempt 2: Empty string
CALL project.dataset.k_ausd_v_ta_cntrct_crs2(
    p_job_kennung => 'TEST_JOB_3',
    p_eintrags_nr => ''
);
```

**Pass/Fail Criterion:**
1.  The `CALL` statement raises an error with a message similar to "FEHLER: EintragsNr (f) ist ein notwendiges Argument und fehlt."
2.  **Job Table Assertion:** No entries are created in `project.dataset.job_table`.
3.  **Error Log Assertion:**
    *   Exactly one `ERROR` entry exists in `project.dataset.error_log` for `job_id = 'TEST_JOB_3'` and the attempted `entry_number` (which might be 'UNKNOWN' or `NULL`).
    *   The `message` in the error log contains "EintragsNr (f) ist ein notwendiges Argument und fehlt."

---

### Test Case 4: Job Already Running (Ignore Active Jobs)

**Purpose:** Verify that if a job with the same `job_id` and `entry_number` is already marked as `RUNNING`, the new invocation is ignored, and an informational message is logged, without performing data processing. This tests the "ignore active jobs" logic.

**Setup:**
1.  Insert an entry into `project.dataset.job_table` with `job_id = 'TEST_JOB_4'`, `entry_number = 'ENTRY_004'`, `status = 'RUNNING'`, and a recent `start_timestamp`.
2.  Ensure `project.dataset.sof_ta_cntrct_crs2` is empty or contains known data from a previous run (it should not be modified by this test).
3.  Ensure `project.dataset.error_log` is empty.

**Action:**
Execute the main BigQuery Stored Procedure with the same `job_id` and `entry_number` as the already running job.

```sql
CALL project.dataset.k_ausd_v_ta_cntrct_crs2(
    p_job_kennung => 'TEST_JOB_4',
    p_eintrags_nr => 'ENTRY_004'
);
```

**Pass/Fail Criterion:**
1.  The `CALL` statement completes successfully (does not raise an error).
2.  **Job Table Assertion:**
    *   No new entries are added to `project.dataset.job_table`.
    *   The existing `RUNNING` entry remains unchanged.
3.  **Error Log Assertion:**
    *   Exactly one `INFO` entry exists in `project.dataset.error_log` for `job_id = 'TEST_JOB_4'` and `entry_number = 'ENTRY_004'`.
    *   The `message` contains "is already running. Ignoring."
4.  **Data Transformation Assertion:** `project.dataset.sof_ta_cntrct_crs2` remains unchanged (no new data inserted, no truncation).

---

### Test Case 5: Deactivate Old Active Jobs

**Purpose:** Verify that if a previous job run for the same `job_id` and `entry_number` was left in `RUNNING` status (e.g., due to a system crash), the new invocation deactivates the old entry before starting a new one.

**Setup:**
1.  Insert an entry into `project.dataset.job_table` with `job_id = 'TEST_JOB_5'`, `entry_number = 'ENTRY_005'`, `status = 'RUNNING'`, and an older `start_timestamp`.
2.  Populate source tables (`dwtk_meldungen`, `sof_ta_cntrct_crs`) as in Test Case 1.
3.  Ensure `project.dataset.sof_ta_cntrct_crs2` is empty.
4.  Ensure `project.dataset.error_log` is empty.

**Action:**
Execute the main BigQuery Stored Procedure with the same `job_id` and `entry_number`.

```sql
CALL project.dataset.k_ausd_v_ta_cntrct_crs2(
    p_job_kennung => 'TEST_JOB_5',
    p_eintrags_nr => 'ENTRY_005'
);
```

**Pass/Fail Criterion:**
1.  The `CALL` statement completes successfully.
2.  **Job Table Assertion:**
    *   The original `RUNNING` entry for `TEST_JOB_5`/`ENTRY_005` is updated to `status = 'DEACTIVATED'` and `end_timestamp` is populated.
    *   A new entry is created for `TEST_JOB_5`/`ENTRY_005` with `status = 'SUCCESS'`, a new `start_timestamp`, and `record_count` populated.
    *   The `start_timestamp` of the new entry is later than the `end_timestamp` of the deactivated entry.
3.  **Error Log Assertion:**
    *   At least one `INFO` entry exists for the successful completion of the new run.
    *   No `ERROR` entries.
4.  **Data Transformation Assertion:** `project.dataset.sof_ta_cntrct_crs2` contains the correctly processed data (as per Test Case 1).

---

### Test Case 6: Error During Data Processing (`p_ausd_v_ta_cntrct_crs2_data_logic`)

**Purpose:** Verify that if an error occurs during the core data processing (e.g., due to invalid data, schema mismatch, or a simulated error), the main procedure catches it, updates the job table to `FAILED`, logs the error, and re-raises it.

**Setup:**
1.  Ensure `project.dataset.job_table` and `project.dataset.error_log` are empty.
2.  **Simulate an error:** This can be done by:
    *   Temporarily modifying `p_ausd_v_ta_cntrct_crs2_data_logic` to `RAISE` an error unconditionally.
    *   Introducing data into `project.dataset.sof_ta_cntrct_crs` that would cause a data type conversion error during the `INSERT` (e.g., a non-numeric string into an `INT64` column, if such a conversion is implicitly attempted).
    *   Dropping a required source table (`dwtk_meldungen` or `sof_ta_cntrct_crs`) before the test.
3.  Populate `project.dataset.dwtk_meldungen` and `project.dataset.sof_ta_cntrct_crs` with data that would normally succeed, but for the simulated error.

**Action:**
Execute the main BigQuery Stored Procedure with valid parameters, triggering the simulated error.

```sql
CALL project.dataset.k_ausd_v_ta_cntrct_crs2(
    p_job_kennung => 'TEST_JOB_6',
    p_eintrags_nr => 'ENTRY_006'
);
```

**Pass/Fail Criterion:**
1.  The `CALL` statement raises an error.
2.  **Job Table Assertion:**
    *   Exactly one entry exists in `project.dataset.job_table` for `job_id = 'TEST_JOB_6'` and `entry_number = 'ENTRY_006'`.
    *   This entry has `status = 'FAILED'`, `table_name = 'ta_cntrct_crs2'`.
    *   `start_timestamp` and `end_timestamp` are populated.
    *   `record_count` is `NULL` or `0` (depending on when the error occurred).
    *   `error_message` is populated with details of the error.
3.  **Error Log Assertion:**
    *   At least one `ERROR` entry exists in `project.dataset.error_log` for `job_id = 'TEST_JOB_6'` and `entry_number = 'ENTRY_006'`.
    *   The `message` in the error log contains details of the simulated error.
4.  **Data Transformation Assertion:** `project.dataset.sof_ta_cntrct_crs2` should be empty or in an inconsistent state (depending on the error point), reflecting the failed operation.

---

### Test Case 7: `v_stichtag_yyyymmdd` Derivation

**Purpose:** Verify the correct calculation of `v_stichtag_yyyymmdd` from `project.dataset.dwtk_meldungen`, including `NULL` handling.

**Setup:**
1.  Clear `project.dataset.dwtk_meldungen`.
2.  **Scenario A (Populated):** Insert multiple entries into `project.dataset.dwtk_meldungen` for `job_kennung = 'BERT_DROP_TEMP_TABLE'` with varying `timecreated` values.
3.  **Scenario B (Empty):** Ensure `project.dataset.dwtk_meldungen` has no entries for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.

**Action:**
This logic is internal to `p_ausd_v_ta_cntrct_crs2_data_logic`. To test it directly, one might need to create a temporary test procedure that calls `p_ausd_v_ta_cntrct_crs2_data_logic` and then queries the `v_stichtag_yyyymmdd` variable if it were an `OUT` parameter, or inspect the `error_log` if an error occurs due to an unexpected date. A more practical approach is to ensure the overall data output is correct, implying this calculation was correct.

However, for direct unit testing of this specific logic:
*   Create a temporary procedure `test_stichtag_logic` that declares `v_stichtag_yyyymmdd`, executes the `SELECT IFNULL(...) INTO v_stichtag_yyyymmdd` statement, and then `SELECT v_stichtag_yyyymmdd` to return it.

**Pass/Fail Criterion:**
1.  **Scenario A (Populated):** The returned `v_stichtag_yyyymmdd` matches `FORMAT_DATE('%Y%m%d', MAX(timecreated))` from the `dwtk_meldungen` table for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
2.  **Scenario B (Empty):** The returned `v_stichtag_yyyymmdd` is `'19000101'`.

```sql
-- Example for Scenario A (assuming test_stichtag_logic exists)
-- Setup: Insert into dwtk_meldungen
INSERT INTO project.dataset.dwtk_meldungen (job_kennung, timecreated) VALUES
('BERT_DROP_TEMP_TABLE', '2023-01-15 10:00:00 UTC'),
('BERT_DROP_TEMP_TABLE', '2023-01-20 12:30:00 UTC'),
('OTHER_JOB', '2023-01-10 08:00:00 UTC');

-- Action: Call test procedure
-- (Assuming test_stichtag_logic returns the calculated v_stichtag_yyyymmdd)
-- CALL project.dataset.test_stichtag_logic(OUT calculated_stichtag);
-- SELECT calculated_stichtag;

-- Expected: '20230120'

-- Example for Scenario B (assuming test_stichtag_logic exists)
-- Setup: Ensure no 'BERT_DROP_TEMP_TABLE' entries
DELETE FROM project.dataset.dwtk_meldungen WHERE job_kennung = 'BERT_DROP_TEMP_TABLE';

-- Action: Call test procedure
-- CALL project.dataset.test_stichtag_logic(OUT calculated_stichtag);
-- SELECT calculated_stichtag;

-- Expected: '19000101'
```

---

### Test Case 8: Data Transformation Logic - Join and Filter Correctness

**Purpose:** Verify the core `INSERT` statement in `p_ausd_v_ta_cntrct_crs2_data_logic` correctly applies the `LEFT JOIN` with the `cr.cntrct_ty = 10` condition and the `c.cntrct_ty <> 10` filter. This is crucial for transformation correctness.

**Setup:**
1.  Ensure `project.dataset.sof_ta_cntrct_crs2` is empty.
2.  Populate `project.dataset.sof_ta_cntrct_crs` with specific test data covering various join scenarios:
    *   Contract `c` with `cntrct_ty <> 10` and `cntrct_parent` matching a `cr` where `cr.cntrct_ty = 10`. (Expected: `RV_NUM` populated)
    *   Contract `c` with `cntrct_ty <> 10` and `cntrct_parent` matching a `cr` where `cr.cntrct_ty <> 10`. (Expected: `RV_NUM` is `NULL` due to join condition `cr.cntrct_ty = 10`)
    *   Contract `c` with `cntrct_ty <> 10` and `cntrct_parent` is `NULL`. (Expected: `RV_NUM` is `NULL`)
    *   Contract `c` with `cntrct_ty = 10`. (Expected: Row *not* inserted into `sof_ta_cntrct_crs2` due to `WHERE c.cntrct_ty <> 10`)
    *   Contract `c` with `cntrct_ty <> 10` and `cntrct_parent` that has no match in `cr.cntrct_id`. (Expected: `RV_NUM` is `NULL`)
3.  Obtain the expected output for `sof_ta_cntrct_crs2` from a legacy run or manual calculation for this specific test data.

**Action:**
Execute the main BigQuery Stored Procedure (or directly `p_ausd_v_ta_cntrct_crs2_data_logic` if possible for isolation).

```sql
CALL project.dataset.k_ausd_v_ta_cntrct_crs2(
    p_job_kennung => 'TEST_JOB_8',
    p_eintrags_nr => 'ENTRY_008'
);
```

**Pass/Fail Criterion:**
1.  The `CALL` statement completes successfully.
2.  The data in `project.dataset.sof_ta_cntrct_crs2` exactly matches the manually calculated or legacy-generated expected output for the given test data, specifically verifying:
    *   Rows with `c.cntrct_ty = 10` are excluded.
    *   `RV_NUM` is correctly populated only when `c.cntrct_parent` matches a `cr.cntrct_id` AND `cr.cntrct_ty = 10`.
    *   `RV_NUM` is `NULL` in all other cases where `c.cntrct_ty <> 10`.

```sql
-- Example SQL assertion for a specific row
-- Assuming a contract C1 with parent P1 (type 10) and C2 with parent P2 (type 20)
SELECT
    (SELECT RV_NUM FROM project.dataset.sof_ta_cntrct_crs2 WHERE cntrct_id = 'C1') = 'P1_CONTRACT_NUMBER' AS C1_RV_MATCH,
    (SELECT RV_NUM FROM project.dataset.sof_ta_cntrct_crs2 WHERE cntrct_id = 'C2') IS NULL AS C2_RV_NULL,
    (SELECT COUNT(*) FROM project.dataset.sof_ta_cntrct_crs2 WHERE cntrct_id = 'P1') = 0 AS P1_EXCLUDED
```

---

### Test Case 9: Empty Source Table (`sof_ta_cntrct_crs`)

**Purpose:** Verify the procedure handles an empty source table gracefully, resulting in an empty target table and a record count of 0.

**Setup:**
1.  Ensure `project.dataset.job_table` and `project.dataset.error_log` are empty.
2.  Ensure `project.dataset.sof_ta_cntrct_crs2` is empty.
3.  Populate `project.dataset.dwtk_meldungen` (as it's a separate dependency).
4.  Ensure `project.dataset.sof_ta_cntrct_crs` is completely empty.

**Action:**
Execute the main BigQuery Stored Procedure.

```sql
CALL project.dataset.k_ausd_v_ta_cntrct_crs2(
    p_job_kennung => 'TEST_JOB_9',
    p_eintrags_nr => 'ENTRY_009'
);
```

**Pass/Fail Criterion:**
1.  The `CALL` statement completes successfully.
2.  **Job Table Assertion:**
    *   Entry for `TEST_JOB_9`/`ENTRY_009` has `status = 'SUCCESS'`.
    *   `record_count = 0`.
3.  **Error Log Assertion:** No `ERROR` entries.
4.  **Data Transformation Assertion:** `project.dataset.sof_ta_cntrct_crs2` is empty.

---

### Test Case 10: Data Quality - Schema and Type Assertions

**Purpose:** Verify that the schema and data types of the target table `sof_ta_cntrct_crs2` are as expected and that data is inserted without type coercion issues.

**Setup:**
1.  Perform a successful run of the procedure (e.g., Test Case 1).
2.  Have the expected schema for `sof_ta_cntrct_crs2` documented.

**Action:**
Query the schema of `project.dataset.sof_ta_cntrct_crs2` and inspect its contents.

```sql
-- Query schema
SELECT
    column_name,
    data_type
FROM
    project.dataset.INFORMATION_SCHEMA.COLUMNS
WHERE
    table_name = 'sof_ta_cntrct_crs2'
ORDER BY
    ordinal_position;

-- Query sample data to check for unexpected NULLs or truncated values
SELECT * FROM project.dataset.sof_ta_cntrct_crs2 LIMIT 10;
```

**Pass/Fail Criterion:**
1.  The schema of `project.dataset.sof_ta_cntrct_crs2` matches the DDL provided in the migration design document (e.g., `cntrct_id STRING`, `obj_version INT64`, `valid_from DATE`, etc.).
2.  No data type errors or unexpected `NULL` values are observed in the sample data, indicating correct type handling during insertion.
3.  String lengths are not truncated if the source data was longer than a default BigQuery string type might imply (though BigQuery `STRING` is generally flexible).

---

### Test Case 11: `TRUNCATE TABLE` Behavior

**Purpose:** Verify that `project.dataset.sof_ta_cntrct_crs2` is truncated at the beginning of each successful data processing run.

**Setup:**
1.  Populate `project.dataset.sof_ta_cntrct_crs2` with some dummy data.
2.  Populate source tables (`dwtk_meldungen`, `sof_ta_cntrct_crs`) with data that will result in a non-empty output.

**Action:**
Execute the main BigQuery Stored Procedure.

```sql
CALL project.dataset.k_ausd_v_ta_cntrct_crs2(
    p_job_kennung => 'TEST_JOB_11',
    p_eintrags_nr => 'ENTRY_011'
);
```

**Pass/Fail Criterion:**
1.  The `CALL` statement completes successfully.
2.  The final content of `project.dataset.sof_ta_cntrct_crs2` contains *only* the data from the current run, and none of the dummy data inserted during setup. This implicitly confirms the `TRUNCATE` operation occurred.

---

These test cases cover the critical aspects of the migration, from orchestration and error handling to data transformation and output parity. Remember to reset the state of the tables (especially `job_table`, `error_log`, and `sof_ta_cntrct_crs2`) before each test run to ensure isolation and reproducibility.