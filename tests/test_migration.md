As a senior data-migration QA engineer, I have analyzed the migration design document and the generated BigQuery code for `k_ausd_v_ta_cntrct_valid.ksh`. The following test cases are designed to validate the migrated solution against the specified requirements, covering output parity, transformation correctness, external system replacements, and data quality assertions. I have also highlighted potential behavioral discrepancies where the BigQuery implementation deviates from the legacy script's stated functionality.

---

## Migration Validation Tests: `k_ausd_v_ta_cntrct_valid.ksh`

### 1. Test Case: Successful Execution - Output Parity & Transformation Correctness

*   **Purpose:** Verify that the BigQuery stored procedure successfully executes, processes data, and produces the same output in `ta_cntrct_valid` as the legacy script, given identical input data and `v_datum_str` context. This covers core transformation logic, including `v_datum_str` derivation, filtering, and column mapping.
*   **Setup:**
    1.  **Legacy System:** Populate the legacy Oracle `dwtk_meldungen` table with sample data, including a `timecreated` value that will result in a specific `v_datum_str` (e.g., `MAX(timecreated)` for `job_kennung = 'BERT_DROP_TEMP_TABLE'` is `2023-01-15 10:00:00`).
    2.  **Legacy System:** Populate the legacy Oracle `cds_ta_cntrct_validity` table with a diverse set of test data, covering:
        *   Rows where `insert_at <= v_datum_str` and `modified_at IS NULL`.
        *   Rows where `insert_at <= v_datum_str` and `modified_at > v_datum_str`.
        *   Rows where `insert_at <= v_datum_str` and `modified_at <= v_datum_str` (should be filtered out).
        *   Rows where `insert_at > v_datum_str` (should be filtered out).
        *   Rows with various data types for `cntrct_validity_id`, `first_period_id`, etc., to test casting.
    3.  **BigQuery System:** Ensure `project.dataset.ta_cntrct_valid`, `job_table`, `error_log`, `job_result_log` are empty.
    4.  **BigQuery System:** Populate `project.dataset.dwtk_meldungen` and `project.dataset.cds_ta_cntrct_validity` with the *exact same data* as their legacy Oracle counterparts.
    5.  Define `p_JobKennung` (e.g., 'TEST_JOB_1') and `p_EintragsNr` (e.g., '20230115') for the test.
*   **Action:**
    1.  Execute the legacy `k_ausd_v_ta_cntrct_valid.ksh` script with `p_JobKennung='TEST_JOB_1'` and `p_EintragsNr='20230115'` against the legacy Oracle database. Capture the final state of `ta_cntrct_valid` and the `v_records` count.
    2.  Execute the BigQuery stored procedure `project.dataset.bert_k_ausd_v_ta_cntrct_valid` with `p_job_kennung='TEST_JOB_1'` and `p_eintrags_nr='20230115'`.
*   **Pass/Fail Criterion:**
    1.  The number of rows in `project.dataset.ta_cntrct_valid` must be identical to the number of rows in the legacy `ta_cntrct_valid` table.
    2.  The content of `project.dataset.ta_cntrct_valid` must be identical to the content of the legacy `ta_cntrct_valid` table (order-independent comparison of all columns).
    3.  The `records_processed` value in `project.dataset.job_result_log` for `job_id='TEST_JOB_1'` and `entry_number='20230115'` must match the `v_records` count from the legacy script.
    4.  The `job_table` entry for `job_id='TEST_JOB_1'` and `entry_number='20230115'` must show `status = 'SUCCESS'`.
    5.  No entries should be present in `error_log` for this job run.

```sql
-- Setup: Populate BigQuery dwtk_meldungen for v_datum_str = '20230115'
TRUNCATE TABLE `project.dataset.dwtk_meldungen`;
INSERT INTO `project.dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES
('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-01-15 10:00:00')),
('OTHER_JOB', TIMESTAMP('2023-01-14 09:00:00'));

-- Setup: Populate BigQuery cds_ta_cntrct_validity
TRUNCATE TABLE `project.dataset.cds_ta_cntrct_validity`;
INSERT INTO `project.dataset.cds_ta_cntrct_validity` (cntrct_validity_id, first_period_id, following_period_id, first_notice_period_id, follow_notice_period_id, insert_at, modified_at) VALUES
(1, 'P1', 'P2', 'N1', 'N2', DATE('2023-01-10'), NULL), -- Included
(2, 'P3', 'P4', 'N3', 'N4', DATE('2023-01-12'), DATE('2023-01-16')), -- Included (modified_at > v_datum_str)
(3, 'P5', 'P6', 'N5', 'N6', DATE('2023-01-14'), DATE('2023-01-14')), -- Excluded (modified_at <= v_datum_str)
(4, 'P7', 'P8', 'N7', 'N8', DATE('2023-01-16'), NULL), -- Excluded (insert_at > v_datum_str)
(5, 'P9', 'P10', 'N9', 'N10', DATE('2023-01-15'), NULL); -- Included (insert_at <= v_datum_str)

-- Action: Call the BigQuery Stored Procedure
CALL `project.dataset.bert_k_ausd_v_ta_cntrct_valid`('TEST_JOB_1', '20230115');

-- Pass/Fail Criterion (BigQuery SQL assertions)
-- Verify ta_cntrct_valid content (compare with legacy output)
SELECT
    cntrct_validity_id,
    first_period_id,
    following_period_id,
    first_notice_period_id,
    follow_notice_period_id,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', bfc_age) AS bfc_age_formatted
FROM `project.dataset.ta_cntrct_valid`
ORDER BY cntrct_validity_id;
-- Expected rows: ('1', 'P1', 'P2', 'N1', 'N2', '2023-01-10 00:00:00'), ('2', 'P3', 'P4', 'N3', 'N4', '2023-01-12 00:00:00'), ('5', 'P9', 'P10', 'N9', 'N10', '2023-01-15 00:00:00')

-- Verify records_processed count
SELECT records_processed FROM `project.dataset.job_result_log` WHERE job_id = 'TEST_JOB_1' AND entry_number = '20230115';
-- Expected: 3

-- Verify job_table status
SELECT status FROM `project.dataset.job_table` WHERE job_id = 'TEST_JOB_1' AND entry_number = '20230115';
-- Expected: 'SUCCESS'

-- Verify no errors
SELECT COUNT(*) FROM `project.dataset.error_log` WHERE job_id = 'TEST_JOB_1' AND entry_number = '20230115';
-- Expected: 0
```

### 2. Test Case: Parameter Validation - Missing `p_job_kennung`

*   **Purpose:** Verify that the stored procedure correctly handles missing required parameters and logs an error, mirroring the legacy script's early exit behavior.
*   **Setup:**
    1.  Ensure `job_table`, `error_log`, `job_result_log` are empty.
*   **Action:**
    1.  Attempt to execute the BigQuery stored procedure `project.dataset.bert_k_ausd_v_ta_cntrct_valid` with `p_job_kennung` as `NULL` and a valid `p_eintrags_nr` (e.g., '20230115').
*   **Pass/Fail Criterion:**
    1.  The stored procedure execution must fail with a `RAISE SCRIPT_EXCEPTION` containing the message "Required parameters p_job_kennung or p_eintrags_nr are missing."
    2.  An entry must be present in `project.dataset.error_log` with `job_name = 'k_ausd_v_ta_cntrct_valid'`, `error_message` indicating missing parameters, and `error_severity = 'ERROR'`.
    3.  The `job_table` entry for the run (where `job_id` is `NULL` and `entry_number` is '20230115') must show `status = 'FAILED'`.
    4.  No data should be inserted into `ta_cntrct_valid`.

```sql
-- Action: Call the BigQuery Stored Procedure with missing p_job_kennung
-- This call is expected to fail.
CALL `project.dataset.bert_k_ausd_v_ta_cntrct_valid`(NULL, '20230115');

-- Pass/Fail Criterion (BigQuery SQL assertions)
-- Verify error_log entry
SELECT error_message FROM `project.dataset.error_log` WHERE job_name = 'k_ausd_v_ta_cntrct_valid' AND entry_number = '20230115';
-- Expected: 'Required parameters p_job_kennung or p_eintrags_nr are missing.'

-- Verify job_table status
SELECT status, message FROM `project.dataset.job_table` WHERE job_id IS NULL AND entry_number = '20230115';
-- Expected: status = 'FAILED', message contains 'Required parameters...'

-- Verify ta_cntrct_valid is empty
SELECT COUNT(*) FROM `project.dataset.ta_cntrct_valid`;
-- Expected: 0
```

### 3. Test Case: Parameter Validation - Missing `p_eintrags_nr`

*   **Purpose:** Verify that the stored procedure correctly handles missing required parameters and logs an error.
*   **Setup:**
    1.  Ensure `job_table`, `error_log`, `job_result_log` are empty.
*   **Action:**
    1.  Attempt to execute the BigQuery stored procedure `project.dataset.bert_k_ausd_v_ta_cntrct_valid` with `p_eintrags_nr` as `NULL` and a valid `p_job_kennung` (e.g., 'TEST_JOB_2').
*   **Pass/Fail Criterion:**
    1.  The stored procedure execution must fail with a `RAISE SCRIPT_EXCEPTION` containing the message "Required parameters p_job_kennung or p_eintrags_nr are missing."
    2.  An entry must be present in `project.dataset.error_log` with `job_name = 'k_ausd_v_ta_cntrct_valid'`, `error_message` indicating missing parameters, and `error_severity = 'ERROR'`.
    3.  The `job_table` entry for the run (where `job_id` is 'TEST_JOB_2' and `entry_number` is `NULL`) must show `status = 'FAILED'`.
    4.  No data should be inserted into `ta_cntrct_valid`.

```sql
-- Action: Call the BigQuery Stored Procedure with missing p_eintrags_nr
CALL `project.dataset.bert_k_ausd_v_ta_cntrct_valid`('TEST_JOB_2', NULL);

-- Pass/Fail Criterion (BigQuery SQL assertions)
-- Verify error_log entry
SELECT error_message FROM `project.dataset.error_log` WHERE job_name = 'k_ausd_v_ta_cntrct_valid' AND job_id = 'TEST_JOB_2';
-- Expected: 'Required parameters p_job_kennung or p_eintrags_nr are missing.'

-- Verify job_table status
SELECT status, message FROM `project.dataset.job_table` WHERE job_id = 'TEST_JOB_2' AND entry_number IS NULL;
-- Expected: status = 'FAILED', message contains 'Required parameters...'

-- Verify ta_cntrct_valid is empty
SELECT COUNT(*) FROM `project.dataset.ta_cntrct_valid`;
-- Expected: 0
```

### 4. Test Case: Transformation Correctness - `v_datum_str` Edge Cases

*   **Purpose:** Verify the correct derivation of `v_datum_str` from `dwtk_meldungen`, especially when `timecreated` is `NULL` or when there are no matching entries, resulting in the default '19000101'.
*   **Setup:**
    1.  Clear `project.dataset.dwtk_meldungen`.
    2.  Populate `project.dataset.cds_ta_cntrct_validity` with data that would be included if `v_datum_str` is '19000101' (e.g., `insert_at` in 1899 or 1900-01-01).
    3.  Ensure `ta_cntrct_valid`, `job_table`, `error_log`, `job_result_log` are empty.
*   **Action:**
    1.  Execute the BigQuery stored procedure `project.dataset.bert_k_ausd_v_ta_cntrct_valid` with valid parameters (e.g., 'TEST_JOB_3', '20230116').
*   **Pass/Fail Criterion:**
    1.  The `job_table` entry must show `status = 'SUCCESS'`.
    2.  The `records_processed` in `job_result_log` should reflect the count of rows from `cds_ta_cntrct_validity` that satisfy the filter conditions with `v_datum_str = '19000101'`.
    3.  No errors in `error_log`.

```sql
-- Setup: Clear dwtk_meldungen and cds_ta_cntrct_validity
TRUNCATE TABLE `project.dataset.dwtk_meldungen`;
TRUNCATE TABLE `project.dataset.cds_ta_cntrct_validity`;

-- Setup: Populate cds_ta_cntrct_validity for '19000101' v_datum_str
INSERT INTO `project.dataset.cds_ta_cntrct_validity` (cntrct_validity_id, insert_at, modified_at) VALUES
(10, DATE('1899-12-31'), NULL), -- Should be included (insert_at <= 19000101)
(11, DATE('1900-01-01'), NULL), -- Should be included
(12, DATE('1900-01-02'), NULL); -- Should be excluded (insert_at > 19000101)

-- Action: Call the BigQuery Stored Procedure
CALL `project.dataset.bert_k_ausd_v_ta_cntrct_valid`('TEST_JOB_3', '20230116');

-- Pass/Fail Criterion (BigQuery SQL assertions)
-- Verify ta_cntrct_valid content
SELECT COUNT(*) FROM `project.dataset.ta_cntrct_valid`;
-- Expected: 2 (rows 10 and 11)

-- Verify records_processed count
SELECT records_processed FROM `project.dataset.job_result_log` WHERE job_id = 'TEST_JOB_3' AND entry_number = '20230116';
-- Expected: 2

-- Verify job_table status
SELECT status FROM `project.dataset.job_table` WHERE job_id = 'TEST_JOB_3' AND entry_number = '20230116';
-- Expected: 'SUCCESS'
```

### 5. Test Case: Transformation Correctness - `TRUNCATE` behavior

*   **Purpose:** Verify that the `ta_cntrct_valid` table is truncated before new data is inserted, ensuring a clean state for each run.
*   **Setup:**
    1.  Populate `project.dataset.dwtk_meldungen` to ensure a valid `v_datum_str` (e.g., `20230201`).
    2.  Populate `project.dataset.cds_ta_cntrct_validity` with some data that should be inserted.
    3.  Pre-populate `project.dataset.ta_cntrct_valid` with existing data that should be removed by the `TRUNCATE` operation.
    4.  Ensure `job_table`, `error_log`, `job_result_log` are empty.
*   **Action:**
    1.  Execute the BigQuery stored procedure `project.dataset.bert_k_ausd_v_ta_cntrct_valid` with valid parameters (e.g., 'TEST_JOB_4', '20230201').
*   **Pass/Fail Criterion:**
    1.  The final count of rows in `project.dataset.ta_cntrct_valid` must only reflect the newly inserted rows, and none of the pre-existing rows.
    2.  The `records_processed` in `job_result_log` should match the count of newly inserted rows.

```sql
-- Setup: Populate dwtk_meldungen for v_datum_str = '20230201'
TRUNCATE TABLE `project.dataset.dwtk_meldungen`;
INSERT INTO `project.dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES
('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-02-01 10:00:00'));

-- Setup: Populate cds_ta_cntrct_validity
TRUNCATE TABLE `project.dataset.cds_ta_cntrct_validity`;
INSERT INTO `project.dataset.cds_ta_cntrct_validity` (cntrct_validity_id, insert_at, modified_at) VALUES
(20, DATE('2023-01-25'), NULL), -- Should be included
(21, DATE('2023-02-01'), DATE('2023-02-02')); -- Should be included

-- Setup: Pre-populate ta_cntrct_valid with old data
TRUNCATE TABLE `project.dataset.ta_cntrct_valid`;
INSERT INTO `project.dataset.ta_cntrct_valid` (cntrct_validity_id, first_period_id, bfc_age) VALUES
('OLD_1', 'P_OLD_1', TIMESTAMP('2022-01-01')),
('OLD_2', 'P_OLD_2', TIMESTAMP('2022-02-01'));

-- Action: Call the BigQuery Stored Procedure
CALL `project.dataset.bert_k_ausd_v_ta_cntrct_valid`('TEST_JOB_4', '20230201');

-- Pass/Fail Criterion (BigQuery SQL assertions)
-- Verify ta_cntrct_valid content
SELECT cntrct_validity_id FROM `project.dataset.ta_cntrct_valid` ORDER BY cntrct_validity_id;
-- Expected: '20', '21' (OLD_1, OLD_2 should be gone)

-- Verify records_processed count
SELECT records_processed FROM `project.dataset.job_result_log` WHERE job_id = 'TEST_JOB_4' AND entry_number = '20230201';
-- Expected: 2
```

### 6. Test Case: Data Quality - Schema and Data Types of `ta_cntrct_valid`

*   **Purpose:** Verify that the schema and data types of the target table `ta_cntrct_valid` match the expected design and handle type conversions correctly, especially `DATE` to `TIMESTAMP`.
*   **Setup:**
    1.  Ensure `project.dataset.ta_cntrct_valid` is created as per `bigquery/ddl/ta_cntrct_valid.sql`.
    2.  Populate `project.dataset.dwtk_meldungen` and `project.dataset.cds_ta_cntrct_validity` with data, including values that might test type casting (e.g., numeric IDs in source becoming STRING in target).
*   **Action:**
    1.  Execute the BigQuery stored procedure `project.dataset.bert_k_ausd_v_ta_cntrct_valid` with valid parameters.
*   **Pass/Fail Criterion:**
    1.  The schema of `project.dataset.ta_cntrct_valid` must match the DDL provided (e.g., `cntrct_validity_id` is STRING, `bfc_age` is TIMESTAMP).
    2.  All inserted data must conform to the target data types without errors or unexpected truncation/conversion issues. Specifically, `cv.insert_at` (DATE) should be correctly converted to `bfc_age` (TIMESTAMP) with the time component set to `00:00:00`.

```python
# Python (pytest) example for schema assertion
import pytest
from google.cloud import bigquery

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client()

def test_ta_cntrct_valid_schema_and_types(bq_client):
    table_id = "project.dataset.ta_cntrct_valid"
    table = bq_client.get_table(table_id)

    expected_schema = {
        "cntrct_validity_id": "STRING",
        "first_period_id": "STRING",
        "following_period_id": "STRING",
        "first_notice_period_id": "STRING",
        "follow_notice_period_id": "STRING",
        "bfc_age": "TIMESTAMP",
        "created_at": "TIMESTAMP",
    }

    actual_schema = {field.name: field.field_type for field in table.schema}

    assert actual_schema == expected_schema, \
        f"Schema mismatch for {table_id}. Expected: {expected_schema}, Got: {actual_schema}"

    # After running the SP (e.g., from Test Case 1), query a sample row and check the type/format of bfc_age.
    query = """
    SELECT bfc_age FROM `project.dataset.ta_cntrct_valid` WHERE cntrct_validity_id = '1' LIMIT 1
    """
    rows = bq_client.query(query).result()
    for row in rows:
        assert isinstance(row.bfc_age, bigquery.dbapi.timestamp.Timestamp), \
            "bfc_age is not a BigQuery Timestamp type"
        assert str(row.bfc_age).endswith(" 00:00:00+00:00"), \
            "bfc_age timestamp does not have expected time component (00:00:00)"
```

### 7. Test Case: Error Handling - Core SQL Logic Failure

*   **Purpose:** Verify that if the core data processing logic within the stored procedure fails (e.g., due to a data type mismatch, constraint violation, or explicit error), the error is caught, logged, and the job status is updated to `FAILED`.
*   **Setup:**
    1.  Populate `project.dataset.dwtk_meldungen` to ensure a valid `v_datum_str`.
    2.  **Simulate Error:** For testing purposes, either temporarily modify the stored procedure to `RAISE SCRIPT_EXCEPTION` within the `INSERT` block, or introduce data in `cds_ta_cntrct_validity` that would cause a type error if `ta_cntrct_valid` had a stricter schema (e.g., attempting to insert a non-numeric string into an `INT64` column).
    3.  Ensure `job_table`, `error_log`, `job_result_log` are empty.
*   **Action:**
    1.  Execute the BigQuery stored procedure `project.dataset.bert_k_ausd_v_ta_cntrct_valid` (or its error-simulating test version) with valid parameters (e.g., 'TEST_JOB_5', '20230301').
*   **Pass/Fail Criterion:**
    1.  The stored procedure execution must fail.
    2.  An entry must be present in `project.dataset.error_log` with `job_name = 'k_ausd_v_ta_cntrct_valid'`, `error_message` reflecting the internal SQL error, and `error_severity = 'ERROR'`.
    3.  The `job_table` entry for the run must show `status = 'FAILED'` and `message` containing the error details.
    4.  `ta_cntrct_valid` should be empty, indicating a transaction rollback.

```sql
-- Setup: Populate dwtk_meldungen
TRUNCATE TABLE `project.dataset.dwtk_meldungen`;
INSERT INTO `project.dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES
('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-03-01 10:00:00'));

-- Setup: Populate cds_ta_cntrct_validity (data that would normally succeed)
TRUNCATE TABLE `project.dataset.cds_ta_cntrct_validity`;
INSERT INTO `project.dataset.cds_ta_cntrct_validity` (cntrct_validity_id, insert_at) VALUES
(30, DATE('2023-02-28'));

-- Action: Call the BigQuery Stored Procedure (assuming a test version or injected error)
-- For example, if `cntrct_validity_id` in `ta_cntrct_valid` was INT64 and `cds_ta_cntrct_validity` had 'ABC'
-- Or, for direct testing, temporarily modify the SP:
/*
CREATE OR REPLACE PROCEDURE `project.dataset.bert_k_ausd_v_ta_cntrct_valid_TEST_ERROR`(
    p_job_kennung STRING, p_eintrags_nr STRING
)
BEGIN
    -- ... (initial setup like original SP) ...
    RAISE SCRIPT_EXCEPTION('Simulated core SQL logic failure for testing.');
    -- ... (rest of the SP) ...
END;
CALL `project.dataset.bert_k_ausd_v_ta_cntrct_valid_TEST_ERROR`('TEST_JOB_5', '20230301');
*/
CALL `project.dataset.bert_k_ausd_v_ta_cntrct_valid`('TEST_JOB_5', '20230301'); -- Assuming an error will occur

-- Pass/Fail Criterion (BigQuery SQL assertions)
-- Verify error_log entry
SELECT error_message FROM `project.dataset.error_log` WHERE job_id = 'TEST_JOB_5' AND entry_number = '20230301';
-- Expected: 'Stored Procedure failed: Simulated core SQL logic failure for testing.' (or similar BigQuery error message)

-- Verify job_table status
SELECT status, message FROM `project.dataset.job_table` WHERE job_id = 'TEST_JOB_5' AND entry_number = '20230301';
-- Expected: status = 'FAILED', message contains 'Failed: Simulated core SQL logic failure...'

-- Verify ta_cntrct_valid is empty (due to transaction rollback)
SELECT COUNT(*) FROM `project.dataset.ta_cntrct_valid`;
-- Expected: 0
```

### 8. Test Case: External System Replacement - `h_alis_sqlplus.ksh` and Oracle Reads

*   **Purpose:** Verify that the BigQuery stored procedure correctly replaces the Oracle SQL*Plus execution and direct Oracle table reads with BigQuery table operations. Specifically, test the `v_datum_str` derivation which previously involved an Oracle `dwtk_meldungen` table.
*   **Setup:**
    1.  Ensure `project.dataset.dwtk_meldungen` is populated with various `timecreated` values, including `NULL`s, and multiple entries for `BERT_DROP_TEMP_TABLE`.
    2.  Ensure `project.dataset.cds_ta_cntrct_validity` is populated with data to test the filter.
    3.  Ensure `ta_cntrct_valid` is empty.
*   **Action:**
    1.  Execute the BigQuery stored procedure `project.dataset.bert_k_ausd_v_ta_cntrct_valid` with valid parameters (e.g., 'TEST_JOB_6', '20230404').
*   **Pass/Fail Criterion:**
    1.  The `v_datum_str` used internally by the BigQuery SP (which can be inferred from the `ta_cntrct_valid` filter results) must be equivalent to what the legacy Oracle script would have calculated using `SELECT NVL(TO_CHAR(MAX(m.timecreated), 'YYYYMMDD'), '19000101') FROM dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    2.  The final data in `ta_cntrct_valid` must reflect the correct application of this `v_datum_str` in the filtering logic.
    3.  The job completes successfully.

```sql
-- Setup: Populate dwtk_meldungen to test MAX and NVL/COALESCE logic
TRUNCATE TABLE `project.dataset.dwtk_meldungen`;
INSERT INTO `project.dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES
('OTHER_JOB', TIMESTAMP('2023-04-01 08:00:00')),
('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-04-02 10:00:00')),
('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-04-01 12:00:00')),
('ANOTHER_JOB', TIMESTAMP('2023-04-03 14:00:00'));
-- Expected v_datum_str: MAX for 'BERT_DROP_TEMP_TABLE' is '2023-04-02 10:00:00', so '20230402'.

-- Setup: Populate cds_ta_cntrct_validity to verify filter
TRUNCATE TABLE `project.dataset.cds_ta_cntrct_validity`;
INSERT INTO `project.dataset.cds_ta_cntrct_validity` (cntrct_validity_id, insert_at, modified_at) VALUES
(40, DATE('2023-04-01'), NULL), -- Should be included (insert_at <= 20230402)
(41, DATE('2023-04-02'), NULL), -- Should be included (insert_at <= 20230402)
(42, DATE('2023-04-03'), NULL); -- Should be excluded (insert_at > 20230402)

-- Action: Call the BigQuery Stored Procedure
CALL `project.dataset.bert_k_ausd_v_ta_cntrct_valid`('TEST_JOB_6', '20230404');

-- Pass/Fail Criterion (BigQuery SQL assertions)
-- Verify ta_cntrct_valid content
SELECT cntrct_validity_id FROM `project.dataset.ta_cntrct_valid` ORDER BY cntrct_validity_id;
-- Expected: '40', '41'

-- Verify records_processed count
SELECT records_processed FROM `project.dataset.job_result_log` WHERE job_id = 'TEST_JOB_6' AND entry_number = '20230404';
-- Expected: 2

-- Verify job status
SELECT status FROM `project.dataset.job_table` WHERE job_id = 'TEST_JOB_6';
-- Expected: 'SUCCESS'
```

### 9. Test Case: Concurrency Handling - "Ignoring Active Jobs" (Behavioral Discrepancy)

*   **Purpose:** Identify and document the behavioral difference regarding "ignoring active jobs" between the legacy script and the migrated BigQuery stored procedure, as the BigQuery code does not explicitly implement this check.
*   **Setup:**
    1.  Manually insert an entry into `project.dataset.job_table` for a specific `job_id` and `entry_number` with `status = 'RUNNING'` (simulating an already active job).
    2.  Populate `project.dataset.dwtk_meldungen` and `project.dataset.cds_ta_cntrct_validity` with data that would lead to a successful run.
    3.  Ensure `ta_cntrct_valid` is empty.
*   **Action:**
    1.  Execute the BigQuery stored procedure `project.dataset.bert_k_ausd_v_ta_cntrct_valid` with the *same* `p_job_kennung` and `p_eintrags_nr` as the manually inserted 'RUNNING' job.
*   **Pass/Fail Criterion:**
    1.  **FAIL (Behavioral Discrepancy):** The BigQuery stored procedure will proceed to execute, truncate `ta_cntrct_valid`, and insert data. It will also insert a new 'RUNNING' entry into `job_table` (or update the existing one to 'RUNNING' and then 'SUCCESS'), resulting in a successful completion of the second job. This is *different* from the legacy script's stated behavior of "ignoring active jobs."
    2.  **Expected BigQuery Behavior:** The `job_table` will contain at least one entry for `job_id='CONCURRENCY_TEST'` and `entry_number='20230501'` with `status = 'SUCCESS'` (overwriting or adding to the initial 'RUNNING' entry). The `ta_cntrct_valid` table will be populated with data from the second run.
    3.  **Recommendation:** This test highlights a gap. The BigQuery SP needs to implement a check for existing 'RUNNING' jobs for the same `job_id` and `entry_number` at the beginning of its execution and exit gracefully if one is found, to match the legacy behavior.

```sql
-- Setup: Manually insert an active job entry
TRUNCATE TABLE `project.dataset.job_table`;
INSERT INTO `project.dataset.job_table` (job_name, job_id, entry_number, start_timestamp, status)
VALUES ('k_ausd_v_ta_cntrct_valid', 'CONCURRENCY_TEST', '20230501', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), 'RUNNING');

-- Setup: Populate dwtk_meldungen and cds_ta_cntrct_validity for successful run
TRUNCATE TABLE `project.dataset.dwtk_meldungen`;
INSERT INTO `project.dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES
('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-05-01 10:00:00'));
TRUNCATE TABLE `project.dataset.cds_ta_cntrct_validity`;
INSERT INTO `project.dataset.cds_ta_cntrct_validity` (cntrct_validity_id, insert_at) VALUES
(50, DATE('2023-04-30'));
TRUNCATE TABLE `project.dataset.ta_cntrct_valid`;

-- Action: Execute the BigQuery SP with the same job_id/entry_number
CALL `project.dataset.bert_k_ausd_v_ta_cntrct_valid`('CONCURRENCY_TEST', '20230501');

-- Pass/Fail Criterion (BigQuery SQL assertions)
-- Verify job_table entries
SELECT status, start_timestamp, end_timestamp
FROM `project.dataset.job_table`
WHERE job_id = 'CONCURRENCY_TEST' AND entry_number = '20230501'
ORDER BY start_timestamp;
-- Expected (current BQ code behavior):
-- At least one entry with status 'SUCCESS' and a recent end_timestamp.
-- If the initial INSERT creates a new row, there might be two entries:
-- ('RUNNING', <old_timestamp>, NULL)
-- ('SUCCESS', <new_timestamp>, <new_end_timestamp>)
-- If the initial INSERT is effectively an UPSERT or the UPDATE targets the first row:
-- ('SUCCESS', <old_timestamp_or_new_timestamp>, <new_end_timestamp>)

-- Verify ta_cntrct_valid content
SELECT COUNT(*) FROM `project.dataset.ta_cntrct_valid`;
-- Expected: 1 (row 50) - indicating the job ran and truncated/repopulated.
```

### 10. Test Case: "Deactivating old active jobs" (Behavioral Discrepancy / Missing Logic)

*   **Purpose:** Identify and document the behavioral difference regarding "deactivating old active jobs" between the legacy script and the migrated BigQuery stored procedure, as this logic is not present in the provided BigQuery code.
*   **Setup:**
    1.  Manually insert an entry into `project.dataset.job_table` for a *different* `job_id` but the same `job_name` with `status = 'ACTIVE'` (or 'RUNNING' if 'ACTIVE' is not a defined status in `job_table`). This simulates an "old active job" that the legacy script would deactivate.
    2.  Populate `project.dataset.dwtk_meldungen` and `project.dataset.cds_ta_cntrct_validity` for a successful run of the *new* job.
    3.  Ensure `ta_cntrct_valid` is empty.
*   **Action:**
    1.  Execute the BigQuery stored procedure `project.dataset.bert_k_ausd_v_ta_cntrct_valid` with valid, *new* parameters (e.g., 'NEW_JOB_RUN', '20230502').
*   **Pass/Fail Criterion:**
    1.  **FAIL (Behavioral Discrepancy):** The BigQuery stored procedure will complete successfully, but the manually inserted "old active job" in `job_table` (`job_id='OLD_ACTIVE_JOB'`) will remain in its original 'ACTIVE'/'RUNNING' state. The legacy script explicitly states "alte aktive Jobs werden einfach dekativiert". This logic is missing in the BigQuery SP.
    2.  **Expected BigQuery Behavior:** The `job_table` entry for `job_id='OLD_ACTIVE_JOB'` will retain its `status = 'ACTIVE'` (or 'RUNNING'). The new job (`job_id='NEW_JOB_RUN'`) will complete successfully.
    3.  **Recommendation:** This test highlights a gap. The BigQuery SP needs to implement logic to identify and deactivate older, active jobs related to this process, if that was a critical function of the legacy script. This would typically involve an `UPDATE` statement on `job_table` for specific `job_name` and `status` values.

```sql
-- Setup: Manually insert an "old active job" entry
TRUNCATE TABLE `project.dataset.job_table`;
INSERT INTO `project.dataset.job_table` (job_name, job_id, entry_number, start_timestamp, status)
VALUES ('k_ausd_v_ta_cntrct_valid', 'OLD_ACTIVE_JOB', '20230501', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR), 'ACTIVE');
-- Assuming 'ACTIVE' is a status that needs deactivation. If not, use 'RUNNING'.

-- Setup: Populate dwtk_meldungen and cds_ta_cntrct_validity for successful run of the new job
TRUNCATE TABLE `project.dataset.dwtk_meldungen`;
INSERT INTO `project.dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES
('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-05-02 10:00:00'));
TRUNCATE TABLE `project.dataset.cds_ta_cntrct_validity`;
INSERT INTO `project.dataset.cds_ta_cntrct_validity` (cntrct_validity_id, insert_at) VALUES
(60, DATE('2023-05-01'));
TRUNCATE TABLE `project.dataset.ta_cntrct_valid`;

-- Action: Execute the BigQuery SP for a new job run
CALL `project.dataset.bert_k_ausd_v_ta_cntrct_valid`('NEW_JOB_RUN', '20230502');

-- Pass/Fail Criterion (BigQuery SQL assertions)
-- Verify status of the "old active job"
SELECT status
FROM `project.dataset.job_table`
WHERE job_id = 'OLD_ACTIVE_JOB' AND entry_number = '20230501';
-- Expected (current BQ code behavior): 'ACTIVE' (or 'RUNNING') - it should NOT have changed.
-- Expected (legacy behavior): 'INACTIVE' or 'DEACTIVATED'.

-- Verify new job completed successfully
SELECT status FROM `project.dataset.job_table` WHERE job_id = 'NEW_JOB_RUN' AND entry_number = '20230502';
-- Expected: 'SUCCESS'
```