As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the migration of `k_ausd_v_ta_bp_ref.ksh` to BigQuery. These tests aim to ensure behavioral equivalence across output parity, transformation correctness, external system replacements, and data quality.

The tests are structured with a clear purpose, setup, action, and concrete pass/fail criteria, including runnable BigQuery SQL assertions where applicable.

---

## Pre-requisite DDLs for Test Environment

Before running the tests, ensure the following BigQuery tables and procedures are created in `your_gcp_project.your_bq_dataset`. These include the migrated components and mock tables for source data.

```sql
-- DDL for BigQuery table: your_gcp_project.your_bq_dataset.job_table
CREATE TABLE IF NOT EXISTS your_gcp_project.your_bq_dataset.job_table (
    job_kennung STRING NOT NULL OPTIONS(description="Unique identifier for the job"),
    eintrags_nr STRING NOT NULL OPTIONS(description="Entry number for the job instance"),
    active_flag BOOL NOT NULL OPTIONS(description="Indicates if the job is currently active"),
    start_time DATETIME OPTIONS(description="Timestamp when the job started"),
    end_time DATETIME OPTIONS(description="Timestamp when the job ended"),
    status STRING OPTIONS(description="Current status of the job (e.g., 'RUNNING', 'COMPLETED', 'FAILED')"),
    message STRING OPTIONS(description="Additional status message or error details")
);

-- DDL for BigQuery table: your_gcp_project.your_bq_dataset.error_log
CREATE TABLE IF NOT EXISTS your_gcp_project.your_bq_dataset.error_log (
    error_ts DATETIME NOT NULL OPTIONS(description="Timestamp of the error"),
    error_nr INT64 OPTIONS(description="Error number or code"),
    error_arg STRING OPTIONS(description="Error message or argument"),
    procedure_name STRING NOT NULL OPTIONS(description="Name of the procedure where the error occurred")
);

-- DDL for BigQuery table: your_gcp_project.your_bq_dataset.job_result
CREATE TABLE IF NOT EXISTS your_gcp_project.your_bq_dataset.job_result (
    job_kennung STRING NOT NULL OPTIONS(description="Unique identifier for the job"),
    eintrags_nr STRING NOT NULL OPTIONS(description="Entry number for the job instance"),
    record_count INT64 OPTIONS(description="Number of records processed or inserted"),
    created_ts DATETIME NOT NULL OPTIONS(description="Timestamp when the result was recorded")
);

-- DDL for mock source table: your_gcp_project.your_bq_dataset.dwtk_meldungen
CREATE TABLE IF NOT EXISTS your_gcp_project.your_bq_dataset.dwtk_meldungen (
    job_kennung STRING,
    timecreated TIMESTAMP
);

-- DDL for mock source table: your_gcp_project.your_bq_dataset.cds_ta_bp_ref
CREATE TABLE IF NOT EXISTS your_gcp_project.your_bq_dataset.cds_ta_bp_ref (
    cntrct_cp2_id STRING,
    bp_id STRING,
    insert_at TIMESTAMP,
    modified_at TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_production INT64,
    bp_ref_ty INT64
);

-- DDL for target table: your_gcp_project.your_bq_dataset.sof_ta_bp_ref
CREATE TABLE IF NOT EXISTS your_gcp_project.your_bq_dataset.sof_ta_bp_ref (
    cntrct_cp2_id STRING,
    bp_id STRING
);

-- Helper procedure to clear all test-related tables
CREATE OR REPLACE PROCEDURE your_gcp_project.your_bq_dataset.clear_test_tables()
BEGIN
    TRUNCATE TABLE your_gcp_project.your_bq_dataset.job_table;
    TRUNCATE TABLE your_gcp_project.your_bq_dataset.error_log;
    TRUNCATE TABLE your_gcp_project.your_bq_dataset.job_result;
    TRUNCATE TABLE your_gcp_project.your_bq_dataset.dwtk_meldungen;
    TRUNCATE TABLE your_gcp_project.your_bq_dataset.cds_ta_bp_ref;
    TRUNCATE TABLE your_gcp_project.your_bq_dataset.sof_ta_bp_ref;
END;
```

---

## Test Cases

### Test Case 1: Successful Execution - Output Parity & Row Count

**Purpose:** Verify that a standard successful execution of the control procedure correctly orchestrates the business logic, updates job status, and records the correct row count, matching the expected output of the legacy job.

**Setup:**
1.  Clear all test tables.
2.  Populate `dwtk_meldungen` to establish a `v_datum`.
3.  Populate `cds_ta_bp_ref` with data that will satisfy the `d_ausd_v_ta_bp_ref` filter conditions.
4.  Insert a dummy job entry into `job_table` to test initial status update.

```sql
CALL your_gcp_project.your_bq_dataset.clear_test_tables();

INSERT INTO your_gcp_project.your_bq_dataset.dwtk_meldungen (job_kennung, timecreated)
VALUES ('BERT_DROP_TEMP_TABLE', '2023-01-15 10:00:00 UTC');

INSERT INTO your_gcp_project.your_bq_dataset.cds_ta_bp_ref (cntrct_cp2_id, bp_id, insert_at, modified_at, valid_from, valid_to, is_production, bp_ref_ty)
VALUES
    ('C1', 'B1', '2023-01-10 00:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 4), -- Should be processed
    ('C2', 'B2', '2023-01-14 00:00:00 UTC', '2023-01-16 00:00:00 UTC', '2023-01-01 00:00:00 UTC', NULL, 1, 4), -- Should be processed (modified_at > v_datum)
    ('C3', 'B3', '2023-01-16 00:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 4), -- Should NOT be processed (insert_at > v_datum)
    ('C4', 'B4', '2023-01-10 00:00:00 UTC', '2023-01-14 00:00:00 UTC', '2023-01-01 00:00:00 UTC', NULL, 1, 4), -- Should NOT be processed (modified_at <= v_datum)
    ('C5', 'B5', '2023-01-10 00:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 0, 4), -- Should NOT be processed (is_production = 0)
    ('C6', 'B6', '2023-01-10 00:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 5); -- Should NOT be processed (bp_ref_ty != 4)

INSERT INTO your_gcp_project.your_bq_dataset.job_table (job_kennung, eintrags_nr, active_flag, start_time, status, message)
VALUES ('JOB_A', 'ENTRY_OLD', TRUE, '2023-01-01 00:00:00 UTC', 'RUNNING', 'Old job running');
```

**Action:**
Execute the main control procedure.

```sql
CALL your_gcp_project.your_bq_dataset.r_ausd_vertrag_control('JOB_A', 'ENTRY_NEW');
```

**Pass/Fail Criterion:**
1.  **Job Status:** The `job_table` should show `JOB_A/ENTRY_NEW` as `COMPLETED` and `JOB_A/ENTRY_OLD` as `DEACTIVATED`.
2.  **Record Count:** `job_result` should contain one entry for `JOB_A/ENTRY_NEW` with `record_count = 2`.
3.  **Target Data:** `sof_ta_bp_ref` should contain exactly 2 rows with specific `cntrct_cp2_id` and `bp_id` values.
4.  **Error Log:** `error_log` should be empty.

```sql
-- Assertion 1: Job Status
SELECT
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.job_table WHERE job_kennung = 'JOB_A' AND eintrags_nr = 'ENTRY_NEW' AND active_flag = FALSE AND status = 'COMPLETED' AND message = 'Job completed successfully') = 1 AS new_job_completed,
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.job_table WHERE job_kennung = 'JOB_A' AND eintrags_nr = 'ENTRY_OLD' AND active_flag = FALSE AND status = 'DEACTIVATED' AND message = 'Deactivated by new job instance') = 1 AS old_job_deactivated;

-- Assertion 2: Record Count
SELECT
    (SELECT record_count FROM your_gcp_project.your_bq_dataset.job_result WHERE job_kennung = 'JOB_A' AND eintrags_nr = 'ENTRY_NEW') = 2 AS record_count_correct;

-- Assertion 3: Target Data
SELECT
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.sof_ta_bp_ref) = 2 AS target_row_count_correct,
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.sof_ta_bp_ref WHERE cntrct_cp2_id = 'C1' AND bp_id = 'B1') = 1 AS row_c1_b1_exists,
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.sof_ta_bp_ref WHERE cntrct_cp2_id = 'C2' AND bp_id = 'B2') = 1 AS row_c2_b2_exists;

-- Assertion 4: Error Log
SELECT (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.error_log) = 0 AS error_log_empty;
```

---

### Test Case 2: Parameter Validation - Missing JobKennung

**Purpose:** Verify that the control procedure correctly handles missing `p_JobKennung` parameters, logs the error, and aborts execution. This tests the replacement of `h_alis_parameter.ksh` and `f_alis_msgerr.ksh`.

**Setup:**
1.  Clear all test tables.

```sql
CALL your_gcp_project.your_bq_dataset.clear_test_tables();
```

**Action:**
Execute the main control procedure with a `NULL` `p_JobKennung`.

```sql
-- This call is expected to fail and raise an error.
-- In a pytest context, you'd assert that the call raises an exception.
-- In BigQuery, you can observe the error message.
CALL your_gcp_project.your_bq_dataset.r_ausd_vertrag_control(NULL, 'ENTRY_1');
```

**Pass/Fail Criterion:**
1.  **Error Handling:** The procedure call should fail with an error message indicating a missing `p_JobKennung`.
2.  **Error Log:** `error_log` should contain one entry with `error_nr = 1001`, `error_arg` related to `p_JobKennung`, and `procedure_name = 'r_ausd_vertrag_control'`.
3.  **No Side Effects:** `job_table`, `job_result`, and `sof_ta_bp_ref` should remain empty.

```sql
-- Assertion 1 & 2: Error Log
SELECT
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.error_log) = 1 AS error_logged,
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.error_log WHERE error_nr = 1001 AND error_arg LIKE '%p_JobKennung%' AND procedure_name = 'r_ausd_vertrag_control') = 1 AS correct_error_details;

-- Assertion 3: No Side Effects
SELECT
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.job_table) = 0 AS job_table_empty,
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.job_result) = 0 AS job_result_empty,
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.sof_ta_bp_ref) = 0 AS sof_ta_bp_ref_empty;
```

---

### Test Case 3: Parameter Validation - Missing EintragsNr

**Purpose:** Verify that the control procedure correctly handles missing `p_EintragsNr` parameters, logs the error, and aborts execution.

**Setup:**
1.  Clear all test tables.

```sql
CALL your_gcp_project.your_bq_dataset.clear_test_tables();
```

**Action:**
Execute the main control procedure with a `NULL` `p_EintragsNr`.

```sql
-- This call is expected to fail and raise an error.
CALL your_gcp_project.your_bq_dataset.r_ausd_vertrag_control('JOB_B', NULL);
```

**Pass/Fail Criterion:**
1.  **Error Handling:** The procedure call should fail with an error message indicating a missing `p_EintragsNr`.
2.  **Error Log:** `error_log` should contain one entry with `error_nr = 1002`, `error_arg` related to `p_EintragsNr`, and `procedure_name = 'r_ausd_vertrag_control'`.
3.  **No Side Effects:** `job_table`, `job_result`, and `sof_ta_bp_ref` should remain empty.

```sql
-- Assertion 1 & 2: Error Log
SELECT
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.error_log) = 1 AS error_logged,
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.error_log WHERE error_nr = 1002 AND error_arg LIKE '%p_EintragsNr%' AND procedure_name = 'r_ausd_vertrag_control') = 1 AS correct_error_details;

-- Assertion 3: No Side Effects
SELECT
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.job_table) = 0 AS job_table_empty,
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.job_result) = 0 AS job_result_empty,
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.sof_ta_bp_ref) = 0 AS sof_ta_bp_ref_empty;
```

---

### Test Case 4: Job Deactivation Logic

**Purpose:** Verify that the `job_table` correctly deactivates older active jobs for the same `JobKennung` when a new job instance starts.

**Setup:**
1.  Clear all test tables.
2.  Insert multiple active job entries for the same `JobKennung` but different `EintragsNr`.

```sql
CALL your_gcp_project.your_bq_dataset.clear_test_tables();

INSERT INTO your_gcp_project.your_bq_dataset.job_table (job_kennung, eintrags_nr, active_flag, start_time, status, message)
VALUES
    ('JOB_C', 'ENTRY_OLD_1', TRUE, '2023-01-01 00:00:00 UTC', 'RUNNING', 'Old job 1'),
    ('JOB_C', 'ENTRY_OLD_2', TRUE, '2023-01-02 00:00:00 UTC', 'RUNNING', 'Old job 2'),
    ('JOB_D', 'ENTRY_OTHER', TRUE, '2023-01-03 00:00:00 UTC', 'RUNNING', 'Other job');

INSERT INTO your_gcp_project.your_bq_dataset.dwtk_meldungen (job_kennung, timecreated)
VALUES ('BERT_DROP_TEMP_TABLE', '2023-01-15 10:00:00 UTC'); -- To allow d_ausd_v_ta_bp_ref to run
```

**Action:**
Execute the main control procedure for `JOB_C` with a new `EintragsNr`.

```sql
CALL your_gcp_project.your_bq_dataset.r_ausd_vertrag_control('JOB_C', 'ENTRY_NEW');
```

**Pass/Fail Criterion:**
1.  **Deactivation:** `JOB_C/ENTRY_OLD_1` and `JOB_C/ENTRY_OLD_2` should be `active_flag = FALSE`, `status = 'DEACTIVATED'`, and `end_time` populated.
2.  **New Job Status:** `JOB_C/ENTRY_NEW` should be `active_flag = FALSE`, `status = 'COMPLETED'`.
3.  **Other Jobs Unaffected:** `JOB_D/ENTRY_OTHER` should remain `active_flag = TRUE`, `status = 'RUNNING'`.

```sql
-- Assertion 1: Deactivation
SELECT
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.job_table WHERE job_kennung = 'JOB_C' AND eintrags_nr IN ('ENTRY_OLD_1', 'ENTRY_OLD_2') AND active_flag = FALSE AND status = 'DEACTIVATED' AND message = 'Deactivated by new job instance' AND end_time IS NOT NULL) = 2 AS old_jobs_deactivated;

-- Assertion 2: New Job Status
SELECT
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.job_table WHERE job_kennung = 'JOB_C' AND eintrags_nr = 'ENTRY_NEW' AND active_flag = FALSE AND status = 'COMPLETED') = 1 AS new_job_completed;

-- Assertion 3: Other Jobs Unaffected
SELECT
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.job_table WHERE job_kennung = 'JOB_D' AND eintrags_nr = 'ENTRY_OTHER' AND active_flag = TRUE AND status = 'RUNNING') = 1 AS other_job_unaffected;
```

---

### Test Case 5: `d_ausd_v_ta_bp_ref` - `v_datum` Calculation (No `dwtk_meldungen` entry)

**Purpose:** Verify that `d_ausd_v_ta_bp_ref` correctly defaults `v_datum` to '1900-01-01' when no matching entry is found in `dwtk_meldungen`.

**Setup:**
1.  Clear all test tables.
2.  Populate `cds_ta_bp_ref` with data, some of which would only be processed if `v_datum` is '1900-01-01'.
3.  Ensure `dwtk_meldungen` is empty or has no matching `job_kennung`.

```sql
CALL your_gcp_project.your_bq_dataset.clear_test_tables();

INSERT INTO your_gcp_project.your_bq_dataset.cds_ta_bp_ref (cntrct_cp2_id, bp_id, insert_at, modified_at, valid_from, valid_to, is_production, bp_ref_ty)
VALUES
    ('C_EARLY', 'B_EARLY', '1899-12-31 00:00:00 UTC', NULL, '1899-12-30 00:00:00 UTC', NULL, 1, 4), -- Should be processed if v_datum is 1900-01-01
    ('C_LATE', 'B_LATE', '2023-01-01 00:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 4); -- Should NOT be processed if v_datum is 1900-01-01
```

**Action:**
Execute the main control procedure.

```sql
CALL your_gcp_project.your_bq_dataset.r_ausd_vertrag_control('JOB_E', 'ENTRY_1');
```

**Pass/Fail Criterion:**
1.  **Target Data:** `sof_ta_bp_ref` should contain exactly 1 row, corresponding to `C_EARLY/B_EARLY`.
2.  **Record Count:** `job_result` should show `record_count = 1`.

```sql
-- Assertion 1: Target Data
SELECT
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.sof_ta_bp_ref) = 1 AS target_row_count_correct,
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.sof_ta_bp_ref WHERE cntrct_cp2_id = 'C_EARLY' AND bp_id = 'B_EARLY') = 1 AS early_row_exists;

-- Assertion 2: Record Count
SELECT
    (SELECT record_count FROM your_gcp_project.your_bq_dataset.job_result WHERE job_kennung = 'JOB_E' AND eintrags_nr = 'ENTRY_1') = 1 AS record_count_correct;
```

---

### Test Case 6: `d_ausd_v_ta_bp_ref` - Transformation Logic (Edge Cases)

**Purpose:** Verify the correctness of the `INSERT` statement's `WHERE` clauses in `d_ausd_v_ta_bp_ref`, specifically around date comparisons and NULL handling for `modified_at` and `valid_to`.

**Setup:**
1.  Clear all test tables.
2.  Populate `dwtk_meldungen` to set `v_datum` to '2023-01-15'.
3.  Populate `cds_ta_bp_ref` with various date scenarios and NULLs.

```sql
CALL your_gcp_project.your_bq_dataset.clear_test_tables();

INSERT INTO your_gcp_project.your_bq_dataset.dwtk_meldungen (job_kennung, timecreated)
VALUES ('BERT_DROP_TEMP_TABLE', '2023-01-15 10:00:00 UTC'); -- v_datum will be 2023-01-15

INSERT INTO your_gcp_project.your_bq_dataset.cds_ta_bp_ref (cntrct_cp2_id, bp_id, insert_at, modified_at, valid_from, valid_to, is_production, bp_ref_ty)
VALUES
    -- Valid cases (should be inserted)
    ('V1', 'B1', '2023-01-15 00:00:00 UTC', NULL, '2023-01-15 00:00:00 UTC', NULL, 1, 4), -- All dates <= v_datum, NULLs handled
    ('V2', 'B2', '2023-01-10 00:00:00 UTC', '2023-01-16 00:00:00 UTC', '2023-01-01 00:00:00 UTC', '2023-01-16 00:00:00 UTC', 1, 4), -- modified_at > v_datum, valid_to > v_datum

    -- Invalid cases (should NOT be inserted)
    ('I1', 'B1', '2023-01-16 00:00:00 UTC', NULL, '2023-01-15 00:00:00 UTC', NULL, 1, 4), -- insert_at > v_datum
    ('I2', 'B2', '2023-01-10 00:00:00 UTC', '2023-01-14 00:00:00 UTC', '2023-01-01 00:00:00 UTC', NULL, 1, 4), -- modified_at <= v_datum
    ('I3', 'B3', '2023-01-10 00:00:00 UTC', NULL, '2023-01-16 00:00:00 UTC', NULL, 1, 4), -- valid_from > v_datum
    ('I4', 'B4', '2023-01-10 00:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', '2023-01-14 00:00:00 UTC', 1, 4), -- valid_to <= v_datum
    ('I5', 'B5', '2023-01-10 00:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 0, 4), -- is_production = 0
    ('I6', 'B6', '2023-01-10 00:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 5); -- bp_ref_ty != 4
```

**Action:**
Execute the main control procedure.

```sql
CALL your_gcp_project.your_bq_dataset.r_ausd_vertrag_control('JOB_F', 'ENTRY_1');
```

**Pass/Fail Criterion:**
1.  **Target Data:** `sof_ta_bp_ref` should contain exactly 2 rows, corresponding to `V1/B1` and `V2/B2`.
2.  **Record Count:** `job_result` should show `record_count = 2`.

```sql
-- Assertion 1: Target Data
SELECT
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.sof_ta_bp_ref) = 2 AS target_row_count_correct,
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.sof_ta_bp_ref WHERE cntrct_cp2_id = 'V1' AND bp_id = 'B1') = 1 AS v1_row_exists,
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.sof_ta_bp_ref WHERE cntrct_cp2_id = 'V2' AND bp_id = 'B2') = 1 AS v2_row_exists;

-- Assertion 2: Record Count
SELECT
    (SELECT record_count FROM your_gcp_project.your_bq_dataset.job_result WHERE job_kennung = 'JOB_F' AND eintrags_nr = 'ENTRY_1') = 2 AS record_count_correct;
```

---

### Test Case 7: Error During Core Business Logic Execution

**Purpose:** Verify that if `d_ausd_v_ta_bp_ref` encounters an error (e.g., due to invalid data or schema issues), `r_ausd_vertrag_control` catches it, logs it, and updates the job status to `FAILED`. This tests the robustness of the error handling and external system replacement for `f_alis_msgerr.ksh`.

**Setup:**
1.  Clear all test tables.
2.  Insert a job entry into `job_table` to be marked as `RUNNING`.
3.  **Simulate an error in `d_ausd_v_ta_bp_ref`:** This requires modifying `d_ausd_v_ta_bp_ref` temporarily to force an error, or creating a mock version that always fails. For this test, we'll assume a temporary modification or a mock procedure `d_ausd_v_ta_bp_ref_FAIL` that raises an error.

```sql
CALL your_gcp_project.your_bq_dataset.clear_test_tables();

INSERT INTO your_gcp_project.your_bq_dataset.job_table (job_kennung, eintrags_nr, active_flag, start_time, status, message)
VALUES ('JOB_G', 'ENTRY_1', TRUE, CURRENT_DATETIME(), 'RUNNING', 'Job started');

-- Temporarily create a failing version of d_ausd_v_ta_bp_ref for this test
CREATE OR REPLACE PROCEDURE your_gcp_project.your_bq_dataset.d_ausd_v_ta_bp_ref(
    OUT p_record_count INT64
)
BEGIN
    RAISE BQ.ABORT_TRANSACTION('Simulated error during d_ausd_v_ta_bp_ref execution.');
END;
```

**Action:**
Execute the main control procedure.

```sql
-- This call is expected to fail and raise an error.
CALL your_gcp_project.your_bq_dataset.r_ausd_vertrag_control('JOB_G', 'ENTRY_1');
```

**Pass/Fail Criterion:**
1.  **Error Handling:** The procedure call should fail.
2.  **Error Log:** `error_log` should contain one entry with `error_nr = 1003`, `error_arg` indicating the error from `d_ausd_v_ta_bp_ref`, and `procedure_name = 'r_ausd_vertrag_control'`.
3.  **Job Status:** The `job_table` entry for `JOB_G/ENTRY_1` should be `active_flag = FALSE`, `status = 'FAILED'`, and `message` containing the error details.
4.  **No Data Changes:** `job_result` and `sof_ta_bp_ref` should remain empty (or unchanged if they had initial data).

```sql
-- Assertion 1 & 2: Error Log
SELECT
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.error_log) = 1 AS error_logged,
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.error_log WHERE error_nr = 1003 AND error_arg LIKE '%Simulated error%' AND procedure_name = 'r_ausd_vertrag_control') = 1 AS correct_error_details;

-- Assertion 3: Job Status
SELECT
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.job_table WHERE job_kennung = 'JOB_G' AND eintrags_nr = 'ENTRY_1' AND active_flag = FALSE AND status = 'FAILED' AND message LIKE '%Error executing d_ausd_v_ta_bp_ref%') = 1 AS job_failed_correctly;

-- Assertion 4: No Data Changes
SELECT
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.job_result) = 0 AS job_result_empty,
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.sof_ta_bp_ref) = 0 AS sof_ta_bp_ref_empty;

-- IMPORTANT: Revert d_ausd_v_ta_bp_ref to its correct implementation after this test.
-- CALL your_gcp_project.your_bq_dataset.revert_d_ausd_v_ta_bp_ref_to_original(); -- (Assuming such a procedure exists or manual re-deployment)
```

---

### Test Case 8: External System Replacement - Temporary File to `job_result`

**Purpose:** Verify that the record count, which was previously written to a temporary file in the legacy system, is now correctly captured as an `OUT` parameter from `d_ausd_v_ta_bp_ref` and persisted into the `job_result` BigQuery table.

**Setup:**
1.  Clear all test tables.
2.  Populate `dwtk_meldungen` and `cds_ta_bp_ref` such that `d_ausd_v_ta_bp_ref` will process a known number of records (e.g., 3).

```sql
CALL your_gcp_project.your_bq_dataset.clear_test_tables();

INSERT INTO your_gcp_project.your_bq_dataset.dwtk_meldungen (job_kennung, timecreated)
VALUES ('BERT_DROP_TEMP_TABLE', '2023-01-15 10:00:00 UTC');

INSERT INTO your_gcp_project.your_bq_dataset.cds_ta_bp_ref (cntrct_cp2_id, bp_id, insert_at, modified_at, valid_from, valid_to, is_production, bp_ref_ty)
VALUES
    ('X1', 'Y1', '2023-01-10 00:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 4),
    ('X2', 'Y2', '2023-01-12 00:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 4),
    ('X3', 'Y3', '2023-01-14 00:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 4);
```

**Action:**
Execute the main control procedure.

```sql
CALL your_gcp_project.your_bq_dataset.r_ausd_vertrag_control('JOB_H', 'ENTRY_1');
```

**Pass/Fail Criterion:**
1.  **Record Count in `job_result`:** The `job_result` table should contain one entry for `JOB_H/ENTRY_1` with `record_count = 3`.
2.  **Target Table Count:** `sof_ta_bp_ref` should also contain 3 rows.

```sql
-- Assertion 1: Record Count in job_result
SELECT
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.job_result WHERE job_kennung = 'JOB_H' AND eintrags_nr = 'ENTRY_1' AND record_count = 3) = 1 AS job_result_record_count_correct;

-- Assertion 2: Target Table Count
SELECT
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.sof_ta_bp_ref) = 3 AS sof_ta_bp_ref_count_correct;
```

---

### Test Case 9: Data Quality - NULL Handling in `sof_ta_bp_ref`

**Purpose:** Verify that the `INSERT` into `sof_ta_bp_ref` correctly handles `NULL` values for `cntrct_cp2_id` and `bp_id` if the source allows them, or if the target schema enforces `NOT NULL`, that an error is raised. (The provided DDL for `sof_ta_bp_ref` does not specify `NOT NULL`, so we expect `NULL`s to be inserted if present in source).

**Setup:**
1.  Clear all test tables.
2.  Populate `dwtk_meldungen` to set `v_datum`.
3.  Populate `cds_ta_bp_ref` with an entry where `cntrct_cp2_id` or `bp_id` is `NULL`.

```sql
CALL your_gcp_project.your_bq_dataset.clear_test_tables();

INSERT INTO your_gcp_project.your_bq_dataset.dwtk_meldungen (job_kennung, timecreated)
VALUES ('BERT_DROP_TEMP_TABLE', '2023-01-15 10:00:00 UTC');

INSERT INTO your_gcp_project.your_bq_dataset.cds_ta_bp_ref (cntrct_cp2_id, bp_id, insert_at, modified_at, valid_from, valid_to, is_production, bp_ref_ty)
VALUES
    ('N1', NULL, '2023-01-10 00:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 4),
    (NULL, 'N2', '2023-01-10 00:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 4);
```

**Action:**
Execute the main control procedure.

```sql
CALL your_gcp_project.your_bq_dataset.r_ausd_vertrag_control('JOB_I', 'ENTRY_1');
```

**Pass/Fail Criterion:**
1.  **Target Data:** `sof_ta_bp_ref` should contain 2 rows.
2.  **NULL Values:** One row should have `bp_id IS NULL` and the other `cntrct_cp2_id IS NULL`.

```sql
-- Assertion 1: Target Data Count
SELECT (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.sof_ta_bp_ref) = 2 AS target_row_count_correct;

-- Assertion 2: NULL Values
SELECT
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.sof_ta_bp_ref WHERE cntrct_cp2_id = 'N1' AND bp_id IS NULL) = 1 AS null_bp_id_row_exists,
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.sof_ta_bp_ref WHERE cntrct_cp2_id IS NULL AND bp_id = 'N2') = 1 AS null_cntrct_cp2_id_row_exists;
```

---

### Test Case 10: Idempotency - Running the job multiple times

**Purpose:** Verify that running the job multiple times with the same parameters (or new parameters after a previous run) behaves predictably, specifically regarding `TRUNCATE` and job status updates.

**Setup:**
1.  Clear all test tables.
2.  Populate `dwtk_meldungen` and `cds_ta_bp_ref` to ensure 2 records are processed.

```sql
CALL your_gcp_project.your_bq_dataset.clear_test_tables();

INSERT INTO your_gcp_project.your_bq_dataset.dwtk_meldungen (job_kennung, timecreated)
VALUES ('BERT_DROP_TEMP_TABLE', '2023-01-15 10:00:00 UTC');

INSERT INTO your_gcp_project.your_bq_dataset.cds_ta_bp_ref (cntrct_cp2_id, bp_id, insert_at, modified_at, valid_from, valid_to, is_production, bp_ref_ty)
VALUES
    ('ID1', 'BP1', '2023-01-10 00:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 4),
    ('ID2', 'BP2', '2023-01-12 00:00:00 UTC', NULL, '2023-01-01 00:00:00 UTC', NULL, 1, 4);
```

**Action:**
Execute the main control procedure twice with different `EintragsNr` values.

```sql
CALL your_gcp_project.your_bq_dataset.r_ausd_vertrag_control('JOB_J', 'ENTRY_RUN1');
CALL your_gcp_project.your_bq_dataset.r_ausd_vertrag_control('JOB_J', 'ENTRY_RUN2');
```

**Pass/Fail Criterion:**
1.  **Target Data:** `sof_ta_bp_ref` should contain exactly 2 rows after the second run (due to `TRUNCATE`).
2.  **Job Status:** `job_table` should show `JOB_J/ENTRY_RUN1` as `DEACTIVATED` and `JOB_J/ENTRY_RUN2` as `COMPLETED`.
3.  **Record Counts:** `job_result` should have two entries, each with `record_count = 2`.

```sql
-- Assertion 1: Target Data (Idempotency via TRUNCATE)
SELECT
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.sof_ta_bp_ref) = 2 AS final_target_row_count_correct,
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.sof_ta_bp_ref WHERE cntrct_cp2_id = 'ID1' AND bp_id = 'BP1') = 1 AS id1_exists,
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.sof_ta_bp_ref WHERE cntrct_cp2_id = 'ID2' AND bp_id = 'BP2') = 1 AS id2_exists;

-- Assertion 2: Job Status
SELECT
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.job_table WHERE job_kennung = 'JOB_J' AND eintrags_nr = 'ENTRY_RUN1' AND active_flag = FALSE AND status = 'DEACTIVATED') = 1 AS run1_deactivated,
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.job_table WHERE job_kennung = 'JOB_J' AND eintrags_nr = 'ENTRY_RUN2' AND active_flag = FALSE AND status = 'COMPLETED') = 1 AS run2_completed;

-- Assertion 3: Record Counts
SELECT
    (SELECT COUNT(*) FROM your_gcp_project.your_bq_dataset.job_result WHERE job_kennung = 'JOB_J' AND record_count = 2) = 2 AS two_record_results_exist;
```