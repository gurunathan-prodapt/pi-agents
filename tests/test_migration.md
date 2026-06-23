As a senior data-migration QA engineer, I've analyzed the provided KornShell script `k_ausd_v_ta_period.ksh` and its BigQuery migration design and generated code. The migration focuses on re-implementing the orchestration logic, parameter handling, error management, and job status tracking within BigQuery. The core data transformation logic from `d_ausd_v_ta_period.sql` is a placeholder in the generated code, which is a significant unresolved item.

The following test cases are designed to validate the behavioral equivalence of the migrated BigQuery Stored Procedure `project.dataset.r_ausd_vertrag_control` against the legacy KornShell script, covering the specified areas.

---

## General Test Setup & Assumptions

Before running any tests, ensure the following:

1.  **BigQuery Environment**: A dedicated BigQuery project and dataset (`project.dataset`) are set up for testing.
2.  **Table Creation**: The `job_table` and `error_log` tables are created in the test dataset using the provided DDLs:
    *   `sql/ddl/job_table.sql`
    *   `sql/ddl/error_log.sql`
3.  **Test Data Isolation**: For each test case, the `job_table` and `error_log` tables should be cleared to ensure test isolation. This can be done with `TRUNCATE TABLE` statements.
4.  **`d_ausd_v_ta_period.sql` Simulation**: Since the actual content of `d_ausd_v_ta_period.sql` is unknown and represented by a placeholder (`SET v_records = 0;`) in the generated code, we will simulate its behavior for testing purposes.
    *   For tests requiring a specific `v_records` count, we will assume the placeholder `SET v_records = 0;` is temporarily replaced with `SET v_records = <expected_count>;` or that a mock `d_ausd_v_ta_period` procedure is called that returns a specific count.
    *   For tests requiring an error during the core SQL logic, we will assume the placeholder block is modified to `RAISE;` or `ERROR('Simulated error');` to trigger the `EXCEPTION WHEN ERROR` block.
5.  **Legacy Script Execution**: To establish output parity, the legacy `k_ausd_v_ta_period.ksh` script would need to be executed in a controlled environment, capturing its console output, exit code, and any database/filesystem changes. This is typically done via shell scripting or a test harness.

---

## Test Cases

### Test Case 1: Successful Execution with Valid Parameters

**Purpose**: Verify that the migrated procedure executes successfully with valid input parameters, correctly updates the `job_table`, and produces the expected output. This covers output parity and basic transformation correctness for job management.

**Setup**:
*   `job_table` and `error_log` are empty.
*   Assume the `d_ausd_v_ta_period.sql` logic (placeholder) would result in `v_records = 50`. (For the provided generated code, `v_records` will be 0, so we'll test for 0 initially and note the need for actual `d_ausd_v_ta_period.sql` integration).

**Action**:
Call the BigQuery Stored Procedure with valid `p_JobKennung` and `p_EintragsNr`.

```sql
-- Clear tables for test isolation
TRUNCATE TABLE `project.dataset.job_table`;
TRUNCATE TABLE `project.dataset.error_log`;

-- Call the procedure
CALL `project.dataset.r_ausd_vertrag_control`('JOB_A', 'ENTRY_123');
```

**Expected Outcome / Pass/Fail Criterion**:
1.  **Output Parity**: The procedure should return a result set indicating completion and processed records.
    *   **Legacy (simulated)**: Console output similar to:
        ```
        ---------- ENDE Datenverarbeitung ----------
        # (followed by potential output from starteSQLSkript)
        # (exit code 0)
        ```
    *   **Migrated**: The `SELECT` statement at the end of the procedure should return one row:
        ```
        job_kennung | eintrags_nr | tab_name   | records_processed | status
        ------------|-------------|------------|-------------------|---------
        JOB_A       | ENTRY_123   | ta_period  | 0                 | COMPLETED
        ```
        *(Note: `records_processed` is 0 due to the placeholder. Once `d_ausd_v_ta_period.sql` is integrated, this should reflect the actual count.)*
2.  **`job_table` Assertion**: One entry should be present in `job_table` for `JOB_A` and `ENTRY_123`.
    *   `job_kennung` = 'JOB_A'
    *   `eintrags_nr` = 'ENTRY_123'
    *   `tab_name` = 'ta_period'
    *   `active_flag` = `FALSE` (job completed)
    *   `created_ts` is recent
    *   `completed_ts` is recent
    *   `record_count` = `0` (due to placeholder)
3.  **`error_log` Assertion**: `error_log` table should remain empty.

```sql
-- Pass/Fail Criterion (SQL Assertions)
SELECT
    (SELECT COUNT(*) FROM `project.dataset.job_table` WHERE job_kennung = 'JOB_A' AND eintrags_nr = 'ENTRY_123' AND active_flag = FALSE AND record_count = 0) = 1 AS job_table_entry_correct,
    (SELECT COUNT(*) FROM `project.dataset.error_log`) = 0 AS error_log_empty;
```

### Test Case 2: Missing `p_JobKennung` Parameter

**Purpose**: Verify that the migrated procedure correctly handles a missing `p_JobKennung` parameter, logs the error, and exits gracefully without processing. This covers parameter validation and error handling.

**Setup**:
*   `job_table` and `error_log` are empty.

**Action**:
Call the BigQuery Stored Procedure with `p_JobKennung` as `NULL` or an empty string.

```sql
-- Clear tables for test isolation
TRUNCATE TABLE `project.dataset.job_table`;
TRUNCATE TABLE `project.dataset.error_log`;

-- Call the procedure with missing JobKennung
CALL `project.dataset.r_ausd_vertrag_control`(NULL, 'ENTRY_123');
-- Or: CALL `project.dataset.r_ausd_vertrag_control`('', 'ENTRY_123');
```

**Expected Outcome / Pass/Fail Criterion**:
1.  **Output Parity**: The procedure should return an error message.
    *   **Legacy (simulated)**: Console output similar to:
        ```
        FEHLER: 0 E 193 Jobkennung
        Bitte ueber Rahmenscript aufrufen
        # (exit code 193)
        ```
    *   **Migrated**: The `SELECT` statement within the error handling block should return one row:
        ```
        error_code | error_arg  | message
        -----------|------------|-------------------------
        193        | Jobkennung | FEHLER: 0 E 193 Jobkennung
        ```
2.  **`job_table` Assertion**: `job_table` should remain empty. No job should be activated or deactivated.
3.  **`error_log` Assertion**: One entry should be present in `error_log`.
    *   `error_code` = `193`
    *   `error_arg` = 'Jobkennung'
    *   `procedure_name` = 'r_ausd_vertrag_control'
    *   `message` = 'FEHLER: 0 E 193 Jobkennung'

```sql
-- Pass/Fail Criterion (SQL Assertions)
SELECT
    (SELECT COUNT(*) FROM `project.dataset.job_table`) = 0 AS job_table_empty,
    (SELECT COUNT(*) FROM `project.dataset.error_log` WHERE error_code = 193 AND error_arg = 'Jobkennung') = 1 AS error_log_entry_correct;
```

### Test Case 3: Missing `p_EintragsNr` Parameter

**Purpose**: Verify that the migrated procedure correctly handles a missing `p_EintragsNr` parameter, logs the error, and exits gracefully.

**Setup**:
*   `job_table` and `error_log` are empty.

**Action**:
Call the BigQuery Stored Procedure with `p_EintragsNr` as `NULL` or an empty string.

```sql
-- Clear tables for test isolation
TRUNCATE TABLE `project.dataset.job_table`;
TRUNCATE TABLE `project.dataset.error_log`;

-- Call the procedure with missing EintragsNr
CALL `project.dataset.r_ausd_vertrag_control`('JOB_A', NULL);
-- Or: CALL `project.dataset.r_ausd_vertrag_control`('JOB_A', '');
```

**Expected Outcome / Pass/Fail Criterion**:
1.  **Output Parity**: The procedure should return an error message.
    *   **Legacy (simulated)**: Console output similar to:
        ```
        FEHLER: 0 E 193 EintragsNr
        Bitte ueber Rahmenscript aufrufen
        # (exit code 193)
        ```
    *   **Migrated**: The `SELECT` statement within the error handling block should return one row:
        ```
        error_code | error_arg  | message
        -----------|------------|-------------------------
        193        | EintragsNr | FEHLER: 0 E 193 EintragsNr
        ```
2.  **`job_table` Assertion**: `job_table` should remain empty.
3.  **`error_log` Assertion**: One entry should be present in `error_log`.
    *   `error_code` = `193`
    *   `error_arg` = 'EintragsNr'
    *   `procedure_name` = 'r_ausd_vertrag_control'
    *   `message` = 'FEHLER: 0 E 193 EintragsNr'

```sql
-- Pass/Fail Criterion (SQL Assertions)
SELECT
    (SELECT COUNT(*) FROM `project.dataset.job_table`) = 0 AS job_table_empty,
    (SELECT COUNT(*) FROM `project.dataset.error_log` WHERE error_code = 193 AND error_arg = 'EintragsNr') = 1 AS error_log_entry_correct;
```

### Test Case 4: Job Deactivation - Existing Active Job

**Purpose**: Verify that the procedure correctly deactivates any previously active jobs for the same `p_JobKennung` before activating the current job. This tests the job status management logic.

**Setup**:
*   `job_table` contains an active entry for `JOB_A` and a different `eintrags_nr`.
*   `error_log` is empty.
*   Assume the `d_ausd_v_ta_period.sql` logic (placeholder) would result in `v_records = 0`.

**Action**:
1.  Insert a pre-existing active job into `job_table`.
2.  Call the BigQuery Stored Procedure with `p_JobKennung = 'JOB_A'` and a *new* `p_EintragsNr`.

```sql
-- Clear tables for test isolation
TRUNCATE TABLE `project.dataset.job_table`;
TRUNCATE TABLE `project.dataset.error_log`;

-- Setup: Insert a pre-existing active job
INSERT INTO `project.dataset.job_table` (job_kennung, eintrags_nr, tab_name, active_flag, created_ts, updated_ts, completed_ts, record_count)
VALUES ('JOB_A', 'OLD_ENTRY_001', 'ta_period', TRUE, TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), NULL, NULL);

-- Call the procedure
CALL `project.dataset.r_ausd_vertrag_control`('JOB_A', 'NEW_ENTRY_002');
```

**Expected Outcome / Pass/Fail Criterion**:
1.  **Output Parity**: Similar to Test Case 1.
2.  **`job_table` Assertion**:
    *   The entry for `OLD_ENTRY_001` should have `active_flag = FALSE` and `updated_ts` should be recent.
    *   A new entry for `NEW_ENTRY_002` should exist with `active_flag = FALSE` (completed), `record_count = 0`, and recent `created_ts`/`completed_ts`.
    *   Total two entries in `job_table`.
3.  **`error_log` Assertion**: `error_log` table should remain empty.

```sql
-- Pass/Fail Criterion (SQL Assertions)
SELECT
    (SELECT COUNT(*) FROM `project.dataset.job_table`) = 2 AS total_job_entries_correct,
    (SELECT COUNT(*) FROM `project.dataset.job_table` WHERE job_kennung = 'JOB_A' AND eintrags_nr = 'OLD_ENTRY_001' AND active_flag = FALSE) = 1 AS old_job_deactivated,
    (SELECT COUNT(*) FROM `project.dataset.job_table` WHERE job_kennung = 'JOB_A' AND eintrags_nr = 'NEW_ENTRY_002' AND active_flag = FALSE AND record_count = 0) = 1 AS new_job_completed,
    (SELECT COUNT(*) FROM `project.dataset.error_log`) = 0 AS error_log_empty;
```

### Test Case 5: Job Deactivation - No Existing Active Job

**Purpose**: Verify that the procedure handles the case where no active job exists for the given `p_JobKennung` without error, and correctly activates and completes the new job.

**Setup**:
*   `job_table` is empty.
*   `error_log` is empty.
*   Assume the `d_ausd_v_ta_period.sql` logic (placeholder) would result in `v_records = 0`.

**Action**:
Call the BigQuery Stored Procedure with `p_JobKennung = 'JOB_B'` and `p_EintragsNr = 'ENTRY_001'`.

```sql
-- Clear tables for test isolation
TRUNCATE TABLE `project.dataset.job_table`;
TRUNCATE TABLE `project.dataset.error_log`;

-- Call the procedure
CALL `project.dataset.r_ausd_vertrag_control`('JOB_B', 'ENTRY_001');
```

**Expected Outcome / Pass/Fail Criterion**:
1.  **Output Parity**: Similar to Test Case 1.
2.  **`job_table` Assertion**: One entry should be present in `job_table` for `JOB_B` and `ENTRY_001`.
    *   `job_kennung` = 'JOB_B'
    *   `eintrags_nr` = 'ENTRY_001'
    *   `active_flag` = `FALSE`
    *   `record_count` = `0`
3.  **`error_log` Assertion**: `error_log` table should remain empty.

```sql
-- Pass/Fail Criterion (SQL Assertions)
SELECT
    (SELECT COUNT(*) FROM `project.dataset.job_table` WHERE job_kennung = 'JOB_B' AND eintrags_nr = 'ENTRY_001' AND active_flag = FALSE AND record_count = 0) = 1 AS job_table_entry_correct,
    (SELECT COUNT(*) FROM `project.dataset.error_log`) = 0 AS error_log_empty;
```

### Test Case 6: Core SQL Logic Failure (Simulated)

**Purpose**: Verify that if the underlying `d_ausd_v_ta_period.sql` logic (represented by the placeholder block) fails, the procedure catches the error, logs it, deactivates the job, and raises an appropriate error. This tests transformation correctness for error handling within the core logic.

**Setup**:
*   `job_table` and `error_log` are empty.
*   **Crucially**: Modify the `r_ausd_vertrag_control` procedure *for this test* to simulate an error within the `BEGIN...EXCEPTION` block for the core SQL logic.

```sql
-- Temporary modification for testing (replace the placeholder block)
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_vertrag_control`(
  p_JobKennung STRING,
  p_EintragsNr STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'ta_period';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_error_code INT64 DEFAULT 0;
  DECLARE v_error_arg STRING DEFAULT NULL;
  DECLARE v_proc_name STRING DEFAULT 'r_ausd_vertrag_control';

  -- Parameter validation (omitted for brevity, assume passed)

  -- Job deactivation / activation handling (omitted for brevity, assume passed)
  UPDATE `project.dataset.job_table`
  SET
    active_flag = FALSE,
    updated_ts = CURRENT_TIMESTAMP()
  WHERE job_kennung = p_JobKennung
    AND active_flag = TRUE;

  INSERT INTO `project.dataset.job_table` (
    job_kennung, eintrags_nr, tab_name, active_flag, created_ts, updated_ts, completed_ts, record_count
  )
  VALUES (
    p_JobKennung, p_EintragsNr, v_TabName, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL, NULL
  );

  -- =========================================================
  -- Core SQL logic migrated from d_ausd_v_ta_period.sql - SIMULATED FAILURE
  -- =========================================================
  BEGIN
    -- Simulate an error during the core SQL execution
    RAISE USING MESSAGE = 'Simulated error during d_ausd_v_ta_period.sql logic';
    -- SET v_records = 100; -- This line will not be reached
  EXCEPTION WHEN ERROR THEN
    INSERT INTO `project.dataset.error_log` (
      error_ts, error_code, error_arg, procedure_name, message
    )
    VALUES (
      CURRENT_TIMESTAMP(), 500, p_EintragsNr, v_proc_name, 'Error during execution of migrated d_ausd_v_ta_period.sql logic'
    );

    UPDATE `project.dataset.job_table`
    SET
      active_flag = FALSE,
      updated_ts = CURRENT_TIMESTAMP()
    WHERE job_kennung = p_JobKennung
      AND eintrags_nr = p_EintragsNr
      AND active_flag = TRUE;

    RAISE USING MESSAGE = 'Error during execution of migrated d_ausd_v_ta_period.sql logic';
  END;

  -- Final job completion update (this section should not be reached on error)
  -- ...
END;
```

**Action**:
Call the (modified) BigQuery Stored Procedure with valid parameters.

```sql
-- Clear tables for test isolation
TRUNCATE TABLE `project.dataset.job_table`;
TRUNCATE TABLE `project.dataset.error_log`;

-- Call the procedure (which will now simulate an internal error)
CALL `project.dataset.r_ausd_vertrag_control`('JOB_C', 'ENTRY_FAIL');
```

**Expected Outcome / Pass/Fail Criterion**:
1.  **Output Parity**: The procedure call should fail and return an error message.
    *   **Legacy (simulated)**: `starteSQLSkript` would likely return a non-zero exit code or log an error.
    *   **Migrated**: The `CALL` statement should result in an error being raised, e.g., `Error during execution of migrated d_ausd_v_ta_period.sql logic`.
2.  **`job_table` Assertion**:
    *   One entry should be present for `JOB_C`, `ENTRY_FAIL`.
    *   `active_flag` should be `FALSE` (deactivated by the inner `EXCEPTION` block).
    *   `record_count` should be `NULL` (as the final update is not reached).
    *   `completed_ts` should be `NULL`.
3.  **`error_log` Assertion**: One entry should be present for the simulated error.
    *   `error_code` = `500`
    *   `error_arg` = 'ENTRY_FAIL'
    *   `procedure_name` = 'r_ausd_vertrag_control'
    *   `message` = 'Error during execution of migrated d_ausd_v_ta_period.sql logic'

```sql
-- Pass/Fail Criterion (SQL Assertions)
SELECT
    (SELECT COUNT(*) FROM `project.dataset.job_table` WHERE job_kennung = 'JOB_C' AND eintrags_nr = 'ENTRY_FAIL' AND active_flag = FALSE AND record_count IS NULL AND completed_ts IS NULL) = 1 AS job_table_entry_correct_on_error,
    (SELECT COUNT(*) FROM `project.dataset.error_log` WHERE error_code = 500 AND error_arg = 'ENTRY_FAIL') = 1 AS error_log_entry_correct_on_error;
```

### Test Case 7: Record Count Correctness (Post-`d_ausd_v_ta_period.sql` Integration)

**Purpose**: Verify that the `v_records` variable correctly captures the number of records processed by the `d_ausd_v_ta_period.sql` logic and that this count is persisted in the `job_table`. This tests data quality and row-count assertions.

**Setup**:
*   `job_table` and `error_log` are empty.
*   **Crucially**: The placeholder `SET v_records = 0;` must be replaced with the actual logic to count records from the `d_ausd_v_ta_period.sql` output. For this test, we'll assume it's replaced with `SET v_records = 123;` for a specific run.

```sql
-- Temporary modification for testing (replace the placeholder block)
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_vertrag_control`(
  p_JobKennung STRING,
  p_EintragsNr STRING
)
BEGIN
  -- ... (parameter validation, job activation/deactivation as in generated code) ...

  -- =========================================================
  -- Core SQL logic migrated from d_ausd_v_ta_period.sql - SIMULATED RECORD COUNT
  -- =========================================================
  BEGIN
    -- Simulate the d_ausd_v_ta_period.sql logic processing 123 records
    SET v_records = 123;
  EXCEPTION WHEN ERROR THEN
    -- ... (error handling as in generated code) ...
    RAISE USING MESSAGE = 'Error during execution of migrated d_ausd_v_ta_period.sql logic';
  END;

  -- =========================================================
  -- Final job completion update (as in generated code)
  -- =========================================================
  UPDATE `project.dataset.job_table`
  SET
    record_count = v_records,
    active_flag = FALSE,
    completed_ts = CURRENT_TIMESTAMP(),
    updated_ts = CURRENT_TIMESTAMP()
  WHERE job_kennung = p_JobKennung
    AND eintrags_nr = p_EintragsNr;

  SELECT
    p_JobKennung AS job_kennung,
    p_EintragsNr AS eintrags_nr,
    v_TabName AS tab_name,
    v_records AS records_processed,
    'COMPLETED' AS status;

EXCEPTION WHEN ERROR THEN
  -- ... (outer error handling as in generated code) ...
  RAISE USING MESSAGE = 'Unexpected error in r_ausd_vertrag_control';
END;
```

**Action**:
Call the (modified) BigQuery Stored Procedure with valid parameters.

```sql
-- Clear tables for test isolation
TRUNCATE TABLE `project.dataset.job_table`;
TRUNCATE TABLE `project.dataset.error_log`;

-- Call the procedure
CALL `project.dataset.r_ausd_vertrag_control`('JOB_D', 'ENTRY_COUNT');
```

**Expected Outcome / Pass/Fail Criterion**:
1.  **Output Parity**: The procedure should return a result set with `records_processed = 123`.
    *   **Legacy (simulated)**: The `eval "v_records=`cat $tmpFile`"` would set `v_records` to 123.
    *   **Migrated**:
        ```
        job_kennung | eintrags_nr | tab_name   | records_processed | status
        ------------|-------------|------------|-------------------|---------
        JOB_D       | ENTRY_COUNT | ta_period  | 123               | COMPLETED
        ```
2.  **`job_table` Assertion**: One entry should be present in `job_table` for `JOB_D` and `ENTRY_COUNT`.
    *   `job_kennung` = 'JOB_D'
    *   `eintrags_nr` = 'ENTRY_COUNT'
    *   `active_flag` = `FALSE`
    *   `record_count` = `123`
    *   `completed_ts` is recent
3.  **`error_log` Assertion**: `error_log` table should remain empty.

```sql
-- Pass/Fail Criterion (SQL Assertions)
SELECT
    (SELECT COUNT(*) FROM `project.dataset.job_table` WHERE job_kennung = 'JOB_D' AND eintrags_nr = 'ENTRY_COUNT' AND active_flag = FALSE AND record_count = 123) = 1 AS job_table_entry_correct_with_count,
    (SELECT COUNT(*) FROM `project.dataset.error_log`) = 0 AS error_log_empty;
```

### Test Case 8: External System Replacement - `tab_name` Value

**Purpose**: Verify that the hardcoded `v_TabName='ta_period'` in the BigQuery procedure correctly replaces the shell script's `v_TabName='ta_period'` assignment, ensuring consistency in metadata.

**Setup**:
*   `job_table` and `error_log` are empty.
*   Assume the `d_ausd_v_ta_period.sql` logic (placeholder) would result in `v_records = 0`.

**Action**:
Call the BigQuery Stored Procedure with valid parameters.

```sql
-- Clear tables for test isolation
TRUNCATE TABLE `project.dataset.job_table`;
TRUNCATE TABLE `project.dataset.error_log`;

-- Call the procedure
CALL `project.dataset.r_ausd_vertrag_control`('JOB_E', 'ENTRY_TABNAME');
```

**Expected Outcome / Pass/Fail Criterion**:
1.  **Output Parity**: The returned result set should include `tab_name = 'ta_period'`.
2.  **`job_table` Assertion**: The entry in `job_table` for `JOB_E` and `ENTRY_TABNAME` should have `tab_name = 'ta_period'`.

```sql
-- Pass/Fail Criterion (SQL Assertions)
SELECT
    (SELECT COUNT(*) FROM `project.dataset.job_table` WHERE job_kennung = 'JOB_E' AND eintrags_nr = 'ENTRY_TABNAME' AND tab_name = 'ta_period') = 1 AS tab_name_correct;
```

---

These test cases cover the primary functionalities and potential failure points of the migrated orchestration logic. Once the `d_ausd_v_ta_period.sql` content is migrated, additional tests would be required to validate its specific data transformations, joins, aggregations, and data quality aspects.