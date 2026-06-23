The migration of `k_ausd_bp_ta_bpr_instance.ksh` to a BigQuery stored procedure `project.dataset.r_ausd_bp_ta_bpr_instance` involves significant changes in technology and execution flow. The tests below focus on validating the orchestration, parameter handling, error logging, and auditing aspects of the migrated BigQuery stored procedure, assuming the core SQL logic (`d_ausd_bp_ta_bpr_instance.sql`) is migrated correctly into `project.dataset.d_ausd_bp_ta_bpr_instance`.

**Assumptions for Testing:**
*   A BigQuery project and dataset (`project.dataset`) are configured.
*   The DDLs for `error_log`, `job_audit`, and `job_control` (if used) have been executed.
*   A mock target table, `project.dataset.mock_target_bpr_instance_table`, exists with at least a `processing_date_col` (DATE type) for simulating record counts.
*   The `project.dataset.d_ausd_bp_ta_bpr_instance` procedure has been modified for testing purposes to allow verification of parameters passed to it and to simulate record counts.

---

## Test Setup: Mock `d_ausd_bp_ta_bpr_instance` and Target Table

Before running the tests, we need to create a mock version of `d_ausd_bp_ta_bpr_instance` and a mock target table. This allows us to isolate and test the `r_ausd_bp_ta_bpr_instance` orchestrator.

```sql
-- Create a mock target table for record counting
CREATE OR REPLACE TABLE `project.dataset.mock_target_bpr_instance_table` (
    id INT64,
    processing_date_col DATE,
    some_data STRING
);

-- Create a table to log parameters passed to the mock d_ausd_bp_ta_bpr_instance
CREATE OR REPLACE TABLE `project.dataset.d_ausd_bp_ta_bpr_instance_log` (
    job_kennung STRING,
    eintrags_nr STRING,
    stichtag_raw STRING,
    stichtag_date DATE,
    wiederanlauf_wert INT64,
    datum_heute DATE,
    datum_gestern DATE,
    execution_timestamp TIMESTAMP
);

-- Create a mock d_ausd_bp_ta_bpr_instance procedure for testing
CREATE OR REPLACE PROCEDURE `project.dataset`.d_ausd_bp_ta_bpr_instance(
    p_JobKennung STRING,
    p_EintragsNr STRING,
    p_Stichtag_Raw STRING,
    p_Stichtag_Date DATE,
    p_wiederanlaufWert INT64,
    p_datum_heute DATE,
    p_datum_gestern DATE
)
BEGIN
    -- Log parameters for verification
    INSERT INTO `project.dataset.d_ausd_bp_ta_bpr_instance_log` (
        job_kennung, eintrags_nr, stichtag_raw, stichtag_date, wiederanlauf_wert,
        datum_heute, datum_gestern, execution_timestamp
    )
    VALUES (
        p_JobKennung, p_EintragsNr, p_Stichtag_Raw, p_Stichtag_Date, p_wiederanlaufWert,
        p_datum_heute, p_datum_gestern, CURRENT_TIMESTAMP()
    );

    -- Simulate data insertion into the mock target table for record counting
    -- This can be adjusted to simulate different record counts for specific tests
    DELETE FROM `project.dataset.mock_target_bpr_instance_table` WHERE processing_date_col = p_Stichtag_Date;
    INSERT INTO `project.dataset.mock_target_bpr_instance_table` (id, processing_date_col, some_data)
    SELECT
        GENERATE_UUID() AS id, -- Using UUID for unique IDs, assuming INT64 is a placeholder
        p_Stichtag_Date AS processing_date_col,
        'mock_data' AS some_data
    FROM
        UNNEST(GENERATE_ARRAY(1, 10)) AS x; -- Simulate 10 records
END;

-- Temporarily modify r_ausd_bp_ta_bpr_instance to use the mock target table
-- In a real scenario, this would be handled by environment variables or a more robust mocking framework.
-- For this exercise, we'll assume the `r_ausd_bp_ta_bpr_instance` procedure is updated to reference
-- `project.dataset.mock_target_bpr_instance_table` for record counting.
-- Specifically, the line:
-- FROM `project.dataset.target_bpr_instance_table`
-- should be changed to:
-- FROM `project.dataset.mock_target_bpr_instance_table`
```

---

## Test Case 1: Happy Path Execution (Output Parity, Data Quality, Row Count)

**Purpose:** Verify that the migrated orchestrator executes successfully with valid parameters, correctly calls the core SQL logic, and logs the expected record count to the `job_audit` table. This covers output parity for the audit log and basic data quality/row count.

**Setup:**
1.  Ensure `project.dataset.error_log`, `project.dataset.job_audit`, `project.dataset.mock_target_bpr_instance_table`, and `project.dataset.d_ausd_bp_ta_bpr_instance_log` tables are empty.
2.  The mock `d_ausd_bp_ta_bpr_instance` procedure is set up to insert 10 records.

**Action:**
Execute the `r_ausd_bp_ta_bpr_instance` procedure with valid parameters.

```sql
CALL `project.dataset`.r_ausd_bp_ta_bpr_instance(
    'JOB_A1',
    'ENTRY_001',
    '01012023',
    0
);
```

**Pass/Fail Criterion:**
1.  The call completes without raising an error.
2.  `project.dataset.job_audit` contains exactly one new row with:
    *   `job_kennung` = 'JOB_A1'
    *   `eintrags_nr` = 'ENTRY_001'
    *   `stichtag` = '01012023'
    *   `records` = 10 (as simulated by the mock `d_ausd_bp_ta_bpr_instance`)
    *   `tab_name` = 'bert_bp_ta_bpr_instance_target'
    *   `created_at` is recent.
3.  `project.dataset.error_log` remains empty.
4.  `project.dataset.d_ausd_bp_ta_bpr_instance_log` contains one row, confirming the call to the core logic.

```python
# Pytest assertion example
def test_happy_path_execution(bigquery_client):
    # Clear audit tables
    bigquery_client.query("TRUNCATE TABLE `project.dataset.job_audit`").result()
    bigquery_client.query("TRUNCATE TABLE `project.dataset.error_log`").result()
    bigquery_client.query("TRUNCATE TABLE `project.dataset.d_ausd_bp_ta_bpr_instance_log`").result()
    bigquery_client.query("TRUNCATE TABLE `project.dataset.mock_target_bpr_instance_table`").result()

    # Action
    bigquery_client.query("""
        CALL `project.dataset`.r_ausd_bp_ta_bpr_instance(
            'JOB_A1',
            'ENTRY_001',
            '01012023',
            0
        );
    """).result()

    # Assertions
    audit_rows = list(bigquery_client.query("SELECT * FROM `project.dataset.job_audit`").result())
    assert len(audit_rows) == 1
    assert audit_rows[0]['job_kennung'] == 'JOB_A1'
    assert audit_rows[0]['eintrags_nr'] == 'ENTRY_001'
    assert audit_rows[0]['stichtag'] == '01012023'
    assert audit_rows[0]['records'] == 10
    assert audit_rows[0]['tab_name'] == 'bert_bp_ta_bpr_instance_target'

    error_rows = list(bigquery_client.query("SELECT * FROM `project.dataset.error_log`").result())
    assert len(error_rows) == 0

    d_instance_log_rows = list(bigquery_client.query("SELECT * FROM `project.dataset.d_ausd_bp_ta_bpr_instance_log`").result())
    assert len(d_instance_log_rows) == 1
```

---

## Test Case 2: Missing `p_JobKennung` (Transformation Correctness - Parameter Validation)

**Purpose:** Verify that the procedure correctly identifies and handles a missing `p_JobKennung` parameter, logging an error and raising an exception, mimicking the legacy script's `pruefeParameterGesetzt` and `DWMSG_MeldeFehler` behavior.

**Setup:**
1.  Ensure `project.dataset.error_log` and `project.dataset.job_audit` tables are empty.

**Action:**
Execute the `r_ausd_bp_ta_bpr_instance` procedure with `p_JobKennung` as NULL.

```sql
-- This will raise an error, so it needs to be caught by the calling environment
-- In BigQuery scripting, you'd use EXCEPTION WHEN ERROR. In Python, a try-except block.
CALL `project.dataset`.r_ausd_bp_ta_bpr_instance(
    NULL,
    'ENTRY_001',
    '01012023',
    0
);
```

**Pass/Fail Criterion:**
1.  The call raises an exception with a message indicating `JobKennung` is missing.
2.  `project.dataset.error_log` contains exactly one new row with:
    *   `process_name` = 'r_ausd_bp_ta_bpr_instance'
    *   `error_nr` = 1001
    *   `error_arg` = 'JobKennung parameter is missing or empty.'
    *   `created_at` is recent.
3.  `project.dataset.job_audit` remains empty (no successful execution).

---

## Test Case 3: Missing `p_EintragsNr` (Transformation Correctness - Parameter Validation)

**Purpose:** Verify that the procedure correctly identifies and handles a missing `p_EintragsNr` parameter, logging an error and raising an exception.

**Setup:**
1.  Ensure `project.dataset.error_log` and `project.dataset.job_audit` tables are empty.

**Action:**
Execute the `r_ausd_bp_ta_bpr_instance` procedure with `p_EintragsNr` as NULL.

```sql
CALL `project.dataset`.r_ausd_bp_ta_bpr_instance(
    'JOB_A1',
    NULL,
    '01012023',
    0
);
```

**Pass/Fail Criterion:**
1.  The call raises an exception with a message indicating `EintragsNr` is missing.
2.  `project.dataset.error_log` contains exactly one new row with:
    *   `process_name` = 'r_ausd_bp_ta_bpr_instance'
    *   `error_nr` = 1002
    *   `error_arg` = 'EintragsNr parameter is missing or empty.'
    *   `created_at` is recent.
3.  `project.dataset.job_audit` remains empty.

---

## Test Case 4: Missing `p_Stichtag` (Transformation Correctness - Parameter Validation)

**Purpose:** Verify that the procedure correctly identifies and handles a missing `p_Stichtag` parameter, logging an error and raising an exception.

**Setup:**
1.  Ensure `project.dataset.error_log` and `project.dataset.job_audit` tables are empty.

**Action:**
Execute the `r_ausd_bp_ta_bpr_instance` procedure with `p_Stichtag` as NULL.

```sql
CALL `project.dataset`.r_ausd_bp_ta_bpr_instance(
    'JOB_A1',
    'ENTRY_001',
    NULL,
    0
);
```

**Pass/Fail Criterion:**
1.  The call raises an exception with a message indicating `Stichtag` is missing.
2.  `project.dataset.error_log` contains exactly one new row with:
    *   `process_name` = 'r_ausd_bp_ta_bpr_instance'
    *   `error_nr` = 1003
    *   `error_arg` = 'Stichtag parameter is missing or empty.'
    *   `created_at` is recent.
3.  `project.dataset.job_audit` remains empty.

---

## Test Case 5: Invalid `p_Stichtag` Format (Transformation Correctness - Type Handling, Edge Case)

**Purpose:** Verify that the procedure correctly validates the `DDMMYYYY` format for `p_Stichtag`, logging an error and raising an exception if the format is incorrect, mimicking `DWDate_Datum_Check`.

**Setup:**
1.  Ensure `project.dataset.error_log` and `project.dataset.job_audit` tables are empty.

**Action:**
Execute the `r_ausd_bp_ta_bpr_instance` procedure with `p_Stichtag` in an invalid format (e.g., `YYYY-MM-DD`).

```sql
CALL `project.dataset`.r_ausd_bp_ta_bpr_instance(
    'JOB_A1',
    'ENTRY_001',
    '2023-01-01', -- Invalid format
    0
);
```

**Pass/Fail Criterion:**
1.  The call raises an exception with a message indicating `Stichtag` has an invalid format.
2.  `project.dataset.error_log` contains exactly one new row with:
    *   `process_name` = 'r_ausd_bp_ta_bpr_instance'
    *   `error_nr` = 1004
    *   `error_arg` = 'Stichtag parameter has invalid format. Expected DDMMYYYY.'
    *   `created_at` is recent.
3.  `project.dataset.job_audit` remains empty.

---

## Test Case 6: `p_wiederanlaufWert` Default Handling (Transformation Correctness - NULL Handling)

**Purpose:** Verify that `p_wiederanlaufWert` correctly defaults to `0` when not explicitly provided, matching the legacy script's `if [[ -z "$p_wiederanlaufWert" ]]` logic.

**Setup:**
1.  Ensure `project.dataset.d_ausd_bp_ta_bpr_instance_log` is empty.
2.  The mock `d_ausd_bp_ta_bpr_instance` procedure logs its parameters.

**Action:**
Execute the `r_ausd_bp_ta_bpr_instance` procedure without providing `p_wiederanlaufWert`.

```sql
CALL `project.dataset`.r_ausd_bp_ta_bpr_instance(
    'JOB_A2',
    'ENTRY_002',
    '02022023'
    -- p_wiederanlaufWert is omitted, so it should use the default 0
);
```

**Pass/Fail Criterion:**
1.  The call completes successfully.
2.  `project.dataset.d_ausd_bp_ta_bpr_instance_log` contains one new row where `wiederanlauf_wert` is `0`.

---

## Test Case 7: Date Derivation (Transformation Correctness)

**Purpose:** Verify that `p_datum_heute` and `p_datum_gestern` are correctly derived using `CURRENT_DATE()` and `DATE_SUB()`, replacing the `gestern.ksh` functionality.

**Setup:**
1.  Ensure `project.dataset.d_ausd_bp_ta_bpr_instance_log` is empty.
2.  The mock `d_ausd_bp_ta_bpr_instance` procedure logs its parameters.

**Action:**
Execute the `r_ausd_bp_ta_bpr_instance` procedure with valid parameters.

```sql
CALL `project.dataset`.r_ausd_bp_ta_bpr_instance(
    'JOB_A3',
    'ENTRY_003',
    '03032023',
    1
);
```

**Pass/Fail Criterion:**
1.  The call completes successfully.
2.  `project.dataset.d_ausd_bp_ta_bpr_instance_log` contains one new row where:
    *   `datum_heute` is equal to `CURRENT_DATE()` at the time of execution.
    *   `datum_gestern` is equal to `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` at the time of execution.

---

## Test Case 8: External System Replacement - Error Logging (`error_log` table)

**Purpose:** Verify that the `error_log` table is correctly populated with detailed error information when an error occurs, replacing the `f_alis_msgerr.ksh` functionality.

**Setup:**
1.  Ensure `project.dataset.error_log` is empty.

**Action:**
Trigger a known validation error (e.g., invalid `p_Stichtag` format).

```sql
-- This will raise an error, which needs to be handled by the test runner.
CALL `project.dataset`.r_ausd_bp_ta_bpr_instance(
    'JOB_A4',
    'ENTRY_004',
    'INVALID_DATE', -- Invalid format
    0
);
```

**Pass/Fail Criterion:**
1.  The call raises an exception.
2.  `project.dataset.error_log` contains exactly one new row with:
    *   `process_name` = 'r_ausd_bp_ta_bpr_instance'
    *   `error_nr` = 1004
    *   `error_arg` = 'Stichtag parameter has invalid format. Expected DDMMYYYY.'
    *   `created_at` is recent and within the execution window.

---

## Test Case 9: External System Replacement - Audit Logging (`job_audit` table)

**Purpose:** Verify that the `job_audit` table is correctly populated with execution metrics (including record count) upon successful completion, replacing the temporary file (`.tmp`) based record count.

**Setup:**
1.  Ensure `project.dataset.job_audit` is empty.
2.  The mock `d_ausd_bp_ta_bpr_instance` procedure is configured to simulate a specific record count (e.g., 10 records).

**Action:**
Execute the `r_ausd_bp_ta_bpr_instance` procedure with valid parameters.

```sql
CALL `project.dataset`.r_ausd_bp_ta_bpr_instance(
    'JOB_A5',
    'ENTRY_005',
    '05052023',
    0
);
```

**Pass/Fail Criterion:**
1.  The call completes successfully.
2.  `project.dataset.job_audit` contains exactly one new row with:
    *   `job_kennung` = 'JOB_A5'
    *   `eintrags_nr` = 'ENTRY_005'
    *   `stichtag` = '05052023'
    *   `records` = 10 (or whatever count the mock `d_ausd_bp_ta_bpr_instance` simulates)
    *   `created_at` is recent.
    *   `tab_name` = 'bert_bp_ta_bpr_instance_target'

---

## Test Case 10: Schema Assertions for Audit Tables

**Purpose:** Verify that the DDLs for `error_log`, `job_audit`, and `job_control` (if reactivated) are correctly applied and their schemas match the design document. This is a data quality/schema assertion.

**Setup:**
Ensure the DDLs for `error_log`, `job_audit`, and `job_control` have been executed.

**Action:**
Query the BigQuery `INFORMATION_SCHEMA` for the table schemas.

```sql
SELECT
    column_name,
    data_type,
    is_nullable
FROM
    `project.dataset`.INFORMATION_SCHEMA.COLUMNS
WHERE
    table_name IN ('error_log', 'job_audit', 'job_control')
ORDER BY
    table_name, ordinal_position;
```

**Pass/Fail Criterion:**
The returned schema information for each table matches the expected DDLs:

*   **`error_log`**:
    *   `process_name` STRING (NULLABLE)
    *   `error_nr` INT64 (NULLABLE)
    *   `error_arg` STRING (NULLABLE)
    *   `created_at` TIMESTAMP (NULLABLE)

*   **`job_audit`**:
    *   `job_kennung` STRING (NULLABLE)
    *   `eintrags_nr` STRING (NULLABLE)
    *   `stichtag` STRING (NULLABLE)
    *   `records` INT64 (NULLABLE)
    *   `created_at` TIMESTAMP (NULLABLE)
    *   `tab_name` STRING (NULLABLE)

*   **`job_control` (if reactivated)**:
    *   `tab_name` STRING (NULLABLE)
    *   `status` STRING (NULLABLE)
    *   `mode` STRING (NULLABLE)
    *   `from_date` DATE (NULLABLE)
    *   `to_date` DATE (NULLABLE)
    *   `job_type` STRING (NULLABLE)
    *   `restart_flag` STRING (NULLABLE)
    *   `records` INT64 (NULLABLE)
    *   `description` STRING (NULLABLE)

---

## Test Case 11: `d_ausd_bp_ta_bpr_instance` Integration (Transformation Correctness)

**Purpose:** Verify that the `r_ausd_bp_ta_bpr_instance` orchestrator correctly passes all derived and input parameters to the `d_ausd_bp_ta_bpr_instance` procedure.

**Setup:**
1.  Ensure `project.dataset.d_ausd_bp_ta_bpr_instance_log` is empty.
2.  The mock `d_ausd_bp_ta_bpr_instance` procedure logs all its input parameters.

**Action:**
Execute `r_ausd_bp_ta_bpr_instance` with a specific set of valid inputs.

```sql
CALL `project.dataset`.r_ausd_bp_ta_bpr_instance(
    'JOB_A6',
    'ENTRY_006',
    '06062023',
    5 -- Specific restart value
);
```

**Pass/Fail Criterion:**
1.  The call completes successfully.
2.  `project.dataset.d_ausd_bp_ta_bpr_instance_log` contains exactly one new row where:
    *   `job_kennung` = 'JOB_A6'
    *   `eintrags_nr` = 'ENTRY_006'
    *   `stichtag_raw` = '06062023'
    *   `stichtag_date` = `PARSE_DATE('%d%m%Y', '06062023')`
    *   `wiederanlauf_wert` = 5
    *   `datum_heute` = `CURRENT_DATE()`
    *   `datum_gestern` = `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`

---

## Test Case 12: Commented-out Job Control (Optional)

**Purpose:** If the `job_control` functionality is reactivated, verify that `r_ausd_bp_ta_bpr_instance` correctly inserts an entry into `project.dataset.job_control` with the appropriate status and parameters.

**Setup:**
1.  **Uncomment the `job_control` INSERT block** in `project.dataset.r_ausd_bp_ta_bpr_instance`.
2.  Ensure `project.dataset.job_control` is empty.

**Action:**
Execute `r_ausd_bp_ta_bpr_instance` with valid parameters, including a non-default `p_wiederanlaufWert`.

```sql
CALL `project.dataset`.r_ausd_bp_ta_bpr_instance(
    'JOB_A7',
    'ENTRY_007',
    '07072023',
    1 -- Simulate a restart
);
```

**Pass/Fail Criterion:**
1.  The call completes successfully.
2.  `project.dataset.job_control` contains exactly one new row with:
    *   `tab_name` = 'bert_bp_ta_bpr_instance_target'
    *   `status` = 'COMPLETED'
    *   `mode` = 'FULL' (or 'INCREMENTAL' if logic dictates)
    *   `from_date` = `PARSE_DATE('%d%m%Y', '07072023')`
    *   `to_date` = `PARSE_DATE('%d%m%Y', '07072023')`
    *   `job_type` = 'ETL'
    *   `restart_flag` = 'Y' (because `p_wiederanlaufWert` was > 0)
    *   `records` = 10 (from mock `d_ausd_bp_ta_bpr_instance`)
    *   `description` matches the expected string.

---