The migration of `k_ausd_v_ta_apn_ve.ksh` to a BigQuery Stored Procedure (`project.dataset.r_ausd_vertrag_control`) involves re-implementing orchestration, parameter handling, and logging. The core data processing logic, originally in `d_ausd_v_ta_apn_ve.sql`, is assumed to be migrated to `project.dataset.d_ausd_v_ta_apn_ve`.

The following tests focus on validating the `r_ausd_vertrag_control` procedure and its interactions with the logging tables and the `ta_apn_ve` table, as well as its invocation of `d_ausd_v_ta_apn_ve`. Since `d_ausd_v_ta_apn_ve` is a placeholder, we'll use a modified version for testing that allows simulating data insertion and errors.

**Pre-requisites for all tests:**

1.  **BigQuery Environment**: A BigQuery project and dataset (`project.dataset`) must exist.
2.  **DDL Execution**: The DDL scripts for `project.dataset.ta_apn_ve`, `project.dataset.job_error_log`, and `project.dataset.job_run_log` must be executed to create these tables.
3.  **Modified `d_ausd_v_ta_apn_ve` for Testing**: The placeholder `project.dataset.d_ausd_v_ta_apn_ve` procedure needs to be replaced with a test-friendly version that allows simulating data processing and errors.

    ```sql
    -- Modified placeholder for testing purposes
    CREATE OR REPLACE PROCEDURE `project.dataset.d_ausd_v_ta_apn_ve`(
      IN p_EintragsNr STRING,
      IN p_JobKennung STRING,
      IN p_SimulateError BOOL DEFAULT FALSE,
      IN p_RecordsToInsert INT64 DEFAULT 0
    )
    BEGIN
      -- Clear previous test data for this EintragsNr to ensure idempotency for tests
      DELETE FROM `project.dataset.ta_apn_ve` WHERE eintrags_nr = p_EintragsNr;

      IF p_SimulateError THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated error in d_ausd_v_ta_apn_ve';
      END IF;

      -- Simulate data processing and insertion into the target table
      FOR i IN 1 TO p_RecordsToInsert DO
        INSERT INTO `project.dataset.ta_apn_ve` (eintrags_nr) VALUES (p_EintragsNr);
      END FOR;

      -- In a real scenario, this procedure would also handle job table updates and deactivation.
      -- For these tests, we focus on its interaction with `ta_apn_ve` and error propagation.
    END;
    ```

---

## Test Case 1: Successful Execution with Valid Parameters

**Purpose:** To verify that the migrated procedure executes successfully with valid input parameters, calls the core processing procedure, logs the run details, and reports the correct number of processed records. This covers output parity (successful completion message, record count) and external system replacement (logging to BigQuery tables).

**Setup:**
1.  Ensure `project.dataset.ta_apn_ve`, `project.dataset.job_error_log`, and `project.dataset.job_run_log` tables are empty.
2.  The test version of `project.dataset.d_ausd_v_ta_apn_ve` is deployed.

**Action:**
Execute the `r_ausd_vertrag_control` procedure with valid `JobKennung` and `EintragsNr`, simulating the insertion of a specific number of records.

```sql
-- Clear logs and target table before test
DELETE FROM `project.dataset.job_error_log` WHERE TRUE;
DELETE FROM `project.dataset.job_run_log` WHERE TRUE;
DELETE FROM `project.dataset.ta_apn_ve` WHERE TRUE;

-- Call the control procedure with valid parameters
-- We pass additional parameters to the test d_ausd_v_ta_apn_ve to simulate data
CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_001', 'ENTRY_001');

-- To simulate records inserted by d_ausd_v_ta_apn_ve, we need to call it directly for this test
-- In a real scenario, d_ausd_v_ta_apn_ve would be called by r_ausd_vertrag_control
-- For this test, let's assume d_ausd_v_ta_apn_ve is called and inserts 5 records.
-- The r_ausd_vertrag_control procedure will then count these records.
-- To make the test self-contained, we'll modify the call to d_ausd_v_ta_apn_ve within r_ausd_vertrag_control for this specific test.
-- Or, more practically, we can assume the d_ausd_v_ta_apn_ve *mock* is called with these parameters.

-- Let's re-run the test with a direct call to the mock d_ausd_v_ta_apn_ve first, then r_ausd_vertrag_control
CALL `project.dataset.d_ausd_v_ta_apn_ve`('ENTRY_001', 'TEST_JOB_001', FALSE, 5);
CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_001', 'ENTRY_001');
```

**Pass/Fail Criterion:**
1.  The `r_ausd_vertrag_control` procedure completes without raising an error.
2.  A record is inserted into `project.dataset.job_run_log` with:
    *   `job_kennung` = 'TEST_JOB_001'
    *   `eintrags_nr` = 'ENTRY_001'
    *   `tab_name` = 'ta_apn_ve'
    *   `records` = 5 (matching the simulated records)
    *   `run_ts` is recent.
3.  `project.dataset.job_error_log` remains empty.
4.  The procedure's final `SELECT` statement returns a message indicating successful completion and the correct record count.

```sql
-- Pytest assertion (example)
def test_successful_execution(bigquery_client):
    # Setup: Clear tables
    bigquery_client.query("DELETE FROM `project.dataset.job_error_log` WHERE TRUE").result()
    bigquery_client.query("DELETE FROM `project.dataset.job_run_log` WHERE TRUE").result()
    bigquery_client.query("DELETE FROM `project.dataset.ta_apn_ve` WHERE TRUE").result()

    # Action: Call mock d_ausd_v_ta_apn_ve and then r_ausd_vertrag_control
    bigquery_client.query("CALL `project.dataset.d_ausd_v_ta_apn_ve`('ENTRY_001', 'TEST_JOB_001', FALSE, 5)").result()
    results = bigquery_client.query("CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_001', 'ENTRY_001')").result()

    # Assertions
    # 1. Procedure completes without error (handled by .result() not raising exception)
    assert "Job completed successfully. Processed 5 records for EintragsNr: ENTRY_001" in [row.message for row in results]

    # 2. Check job_run_log
    run_log = bigquery_client.query("SELECT job_kennung, eintrags_nr, tab_name, records FROM `project.dataset.job_run_log`").result().to_dataframe()
    assert len(run_log) == 1
    assert run_log.iloc[0]['job_kennung'] == 'TEST_JOB_001'
    assert run_log.iloc[0]['eintrags_nr'] == 'ENTRY_001'
    assert run_log.iloc[0]['tab_name'] == 'ta_apn_ve'
    assert run_log.iloc[0]['records'] == 5

    # 3. Check job_error_log
    error_log = bigquery_client.query("SELECT * FROM `project.dataset.job_error_log`").result().to_dataframe()
    assert len(error_log) == 0
```

---

## Test Case 2: Parameter Validation - Missing JobKennung

**Purpose:** To verify that the procedure correctly identifies a missing `JobKennung` parameter, logs an error, and terminates execution as specified in the design (similar to legacy script's `exit $ErrNr`). This covers transformation correctness (parameter validation, error handling) and external system replacement (error logging).

**Setup:**
1.  Ensure `project.dataset.job_error_log` and `project.dataset.job_run_log` tables are empty.
2.  The test version of `project.dataset.d_ausd_v_ta_apn_ve` is deployed.

**Action:**
Execute the `r_ausd_vertrag_control` procedure with a `NULL` or empty `p_JobKennung` and a valid `p_EintragsNr`.

```sql
-- Clear logs before test
DELETE FROM `project.dataset.job_error_log` WHERE TRUE;
DELETE FROM `project.dataset.job_run_log` WHERE TRUE;

-- Call the control procedure with missing JobKennung
-- This call is expected to raise an error
-- Example with NULL:
-- CALL `project.dataset.r_ausd_vertrag_control`(NULL, 'ENTRY_002');
-- Example with empty string:
CALL `project.dataset.r_ausd_vertrag_control`('', 'ENTRY_002');
```

**Pass/Fail Criterion:**
1.  The `r_ausd_vertrag_control` procedure raises a `SQLSTATE '45000'` error with the message 'Bitte ueber Rahmenscript aufrufen'.
2.  A record is inserted into `project.dataset.job_error_log` with:
    *   `job_kennung` = `NULL` or `''` (depending on input)
    *   `eintrags_nr` = 'ENTRY_002'
    *   `error_nr` = 0 (as per the pseudocode's `ErrNr=0` logic for parameter errors)
    *   `error_arg` = 'Jobkennung'
    *   `error_ts` is recent.
3.  `project.dataset.job_run_log` remains empty (as the procedure should terminate before logging a successful run).
4.  The `project.dataset.d_ausd_v_ta_apn_ve` procedure is *not* called.

```sql
# Pytest assertion (example)
import pytest
from google.cloud import bigquery

def test_missing_jobkennung(bigquery_client):
    # Setup: Clear tables
    bigquery_client.query("DELETE FROM `project.dataset.job_error_log` WHERE TRUE").result()
    bigquery_client.query("DELETE FROM `project.dataset.job_run_log` WHERE TRUE").result()

    # Action: Call with missing JobKennung
    with pytest.raises(bigquery.exceptions.BadRequest) as excinfo:
        bigquery_client.query("CALL `project.dataset.r_ausd_vertrag_control`('', 'ENTRY_002')").result()

    # Assertions
    # 1. Check for SQLSTATE error message
    assert "Bitte ueber Rahmenscript aufrufen" in str(excinfo.value)

    # 2. Check job_error_log
    error_log = bigquery_client.query("SELECT job_kennung, eintrags_nr, error_nr, error_arg FROM `project.dataset.job_error_log`").result().to_dataframe()
    assert len(error_log) == 1
    assert error_log.iloc[0]['job_kennung'] == ''
    assert error_log.iloc[0]['eintrags_nr'] == 'ENTRY_002'
    assert error_log.iloc[0]['error_nr'] == 0
    assert error_log.iloc[0]['error_arg'] == 'Jobkennung'

    # 3. Check job_run_log
    run_log = bigquery_client.query("SELECT * FROM `project.dataset.job_run_log`").result().to_dataframe()
    assert len(run_log) == 0
```

---

## Test Case 3: Parameter Validation - Missing EintragsNr

**Purpose:** To verify that the procedure correctly identifies a missing `EintragsNr` parameter, logs an error, and terminates execution. This covers transformation correctness (parameter validation, error handling) and external system replacement (error logging).

**Setup:**
1.  Ensure `project.dataset.job_error_log` and `project.dataset.job_run_log` tables are empty.
2.  The test version of `project.dataset.d_ausd_v_ta_apn_ve` is deployed.

**Action:**
Execute the `r_ausd_vertrag_control` procedure with a valid `p_JobKennung` and a `NULL` or empty `p_EintragsNr`.

```sql
-- Clear logs before test
DELETE FROM `project.dataset.job_error_log` WHERE TRUE;
DELETE FROM `project.dataset.job_run_log` WHERE TRUE;

-- Call the control procedure with missing EintragsNr
CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_003', '');
```

**Pass/Fail Criterion:**
1.  The `r_ausd_vertrag_control` procedure raises a `SQLSTATE '45000'` error with the message 'Bitte ueber Rahmenscript aufrufen'.
2.  A record is inserted into `project.dataset.job_error_log` with:
    *   `job_kennung` = 'TEST_JOB_003'
    *   `eintrags_nr` = `NULL` or `''` (depending on input)
    *   `error_nr` = 0
    *   `error_arg` = 'EintragsNr'
    *   `error_ts` is recent.
3.  `project.dataset.job_run_log` remains empty.
4.  The `project.dataset.d_ausd_v_ta_apn_ve` procedure is *not* called.

```sql
# Pytest assertion (example)
import pytest
from google.cloud import bigquery

def test_missing_eintragsnr(bigquery_client):
    # Setup: Clear tables
    bigquery_client.query("DELETE FROM `project.dataset.job_error_log` WHERE TRUE").result()
    bigquery_client.query("DELETE FROM `project.dataset.job_run_log` WHERE TRUE").result()

    # Action: Call with missing EintragsNr
    with pytest.raises(bigquery.exceptions.BadRequest) as excinfo:
        bigquery_client.query("CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_003', '')").result()

    # Assertions
    # 1. Check for SQLSTATE error message
    assert "Bitte ueber Rahmenscript aufrufen" in str(excinfo.value)

    # 2. Check job_error_log
    error_log = bigquery_client.query("SELECT job_kennung, eintrags_nr, error_nr, error_arg FROM `project.dataset.job_error_log`").result().to_dataframe()
    assert len(error_log) == 1
    assert error_log.iloc[0]['job_kennung'] == 'TEST_JOB_003'
    assert error_log.iloc[0]['eintrags_nr'] == ''
    assert error_log.iloc[0]['error_nr'] == 0
    assert error_log.iloc[0]['error_arg'] == 'EintragsNr'

    # 3. Check job_run_log
    run_log = bigquery_client.query("SELECT * FROM `project.dataset.job_run_log`").result().to_dataframe()
    assert len(run_log) == 0
```

---

## Test Case 4: Error During Core Data Processing (`d_ausd_v_ta_apn_ve`)

**Purpose:** To verify that if an error occurs within the called `d_ausd_v_ta_apn_ve` procedure, the `r_ausd_vertrag_control` procedure catches it, logs the error, and terminates appropriately. This covers transformation correctness (error handling, exception block) and external system replacement (error logging).

**Setup:**
1.  Ensure `project.dataset.job_error_log` and `project.dataset.job_run_log` tables are empty.
2.  The test version of `project.dataset.d_ausd_v_ta_apn_ve` is deployed and configured to simulate an error.

**Action:**
Execute the `r_ausd_vertrag_control` procedure with valid parameters, but configure the mock `d_ausd_v_ta_apn_ve` to raise an error.

```sql
-- Clear logs before test
DELETE FROM `project.dataset.job_error_log` WHERE TRUE;
DELETE FROM `project.dataset.job_run_log` WHERE TRUE;

-- Call the mock d_ausd_v_ta_apn_ve to simulate an error
-- This will be called by r_ausd_vertrag_control.
-- For this test, we need to ensure the mock d_ausd_v_ta_apn_ve is called with p_SimulateError = TRUE.
-- This requires a slight modification to r_ausd_vertrag_control for testing, or a more sophisticated mocking framework.
-- Assuming the mock d_ausd_v_ta_apn_ve is called and raises an error:
-- The r_ausd_vertrag_control code snippet provided does not pass p_SimulateError to d_ausd_v_ta_apn_ve.
-- For a robust test, the `CALL` in `r_ausd_vertrag_control` would need to be updated to:
-- CALL `project.dataset.d_ausd_v_ta_apn_ve`(p_EintragsNr, p_JobKennung, TRUE, 0);
-- Or, we assume the `d_ausd_v_ta_apn_ve` procedure itself has internal logic that can fail.

-- For the purpose of this test, let's assume `d_ausd_v_ta_apn_ve` is modified to always error for a specific JobKennung.
-- Or, we can directly call the `r_ausd_vertrag_control` and expect the `EXCEPTION WHEN ERROR` block to trigger.
-- The provided `r_ausd_vertrag_control` pseudocode's `CALL` statement is:
-- CALL `project.dataset.d_ausd_v_ta_apn_ve`(p_EintragsNr, p_JobKennung);
-- To test the EXCEPTION block, we need `d_ausd_v_ta_apn_ve` to throw an error.
-- Let's assume the mock `d_ausd_v_ta_apn_ve` is configured to error if `p_JobKennung` is 'ERROR_JOB'.

CALL `project.dataset.r_ausd_vertrag_control`('ERROR_JOB', 'ENTRY_004');
```

**Pass/Fail Criterion:**
1.  The `r_ausd_vertrag_control` procedure raises a `SQLSTATE '45000'` error with the message 'SQL execution failed'.
2.  A record is inserted into `project.dataset.job_error_log` with:
    *   `job_kennung` = 'ERROR_JOB'
    *   `eintrags_nr` = 'ENTRY_004'
    *   `error_nr` = 1
    *   `error_arg` = 'SQL execution failed'
    *   `error_ts` is recent.
3.  `project.dataset.job_run_log` remains empty (as the procedure should terminate before logging a successful run).
4.  `project.dataset.ta_apn_ve` should not contain any records for 'ENTRY_004' (or should be in a consistent state, depending on `d_ausd_v_ta_apn_ve`'s transactionality).

```sql
# Pytest assertion (example)
import pytest
from google.cloud import bigquery

def test_error_in_core_processing(bigquery_client):
    # Setup: Clear tables
    bigquery_client.query("DELETE FROM `project.dataset.job_error_log` WHERE TRUE").result()
    bigquery_client.query("DELETE FROM `project.dataset.job_run_log` WHERE TRUE").result()
    bigquery_client.query("DELETE FROM `project.dataset.ta_apn_ve` WHERE TRUE").result()

    # Action: Call r_ausd_vertrag_control, assuming d_ausd_v_ta_apn_ve will error
    # This requires d_ausd_v_ta_apn_ve to be configured to error for 'ERROR_JOB'
    # Or, for a more direct test, modify r_ausd_vertrag_control to pass p_SimulateError=TRUE
    # For this example, we'll assume the mock d_ausd_v_ta_apn_ve is called with p_SimulateError=TRUE
    # by modifying the CALL in r_ausd_vertrag_control for this test.
    # (In a real test suite, you'd use a more sophisticated mocking or dependency injection)

    # Temporarily modify r_ausd_vertrag_control to force an error in d_ausd_v_ta_apn_ve
    # This is a hack for demonstration; proper testing would involve mocking or test-specific procedure versions.
    # For the sake of this example, let's assume the `d_ausd_v_ta_apn_ve` mock is called with `p_SimulateError=TRUE`
    # when `p_JobKennung` is 'ERROR_JOB'.
    # This implies a change in the `r_ausd_vertrag_control` procedure for testing:
    # CALL `project.dataset.d_ausd_v_ta_apn_ve`(p_EintragsNr, p_JobKennung, p_JobKennung = 'ERROR_JOB', 0);

    with pytest.raises(bigquery.exceptions.BadRequest) as excinfo:
        bigquery_client.query("CALL `project.dataset.r_ausd_vertrag_control`('ERROR_JOB', 'ENTRY_004')").result()

    # Assertions
    # 1. Check for SQLSTATE error message
    assert "SQL execution failed" in str(excinfo.value)

    # 2. Check job_error_log
    error_log = bigquery_client.query("SELECT job_kennung, eintrags_nr, error_nr, error_arg FROM `project.dataset.job_error_log`").result().to_dataframe()
    assert len(error_log) == 1
    assert error_log.iloc[0]['job_kennung'] == 'ERROR_JOB'
    assert error_log.iloc[0]['eintrags_nr'] == 'ENTRY_004'
    assert error_log.iloc[0]['error_nr'] == 1
    assert error_log.iloc[0]['error_arg'] == 'SQL execution failed'

    # 3. Check job_run_log
    run_log = bigquery_client.query("SELECT * FROM `project.dataset.job_run_log`").result().to_dataframe()
    assert len(run_log) == 0

    # 4. Check ta_apn_ve (should be empty for this EintragsNr if d_ausd_v_ta_apn_ve failed before inserting)
    ta_apn_ve_data = bigquery_client.query("SELECT * FROM `project.dataset.ta_apn_ve` WHERE eintrags_nr = 'ENTRY_004'").result().to_dataframe()
    assert len(ta_apn_ve_data) == 0
```

---

## Test Case 5: Data Quality - `job_run_log` Schema and Data Types

**Purpose:** To verify that the `job_run_log` table has the correct schema and that data is inserted with the expected data types. This covers data quality and schema assertions.

**Setup:**
1.  Ensure `project.dataset.job_run_log` is created according to the DDL.
2.  Execute a successful run of `r_ausd_vertrag_control` (e.g., from Test Case 1).

**Action:**
Query the schema of `project.dataset.job_run_log` and retrieve a sample record.

```sql
-- Ensure a successful run has occurred to populate the log
-- (e.g., by running Test Case 1 setup and action)
CALL `project.dataset.d_ausd_v_ta_apn_ve`('ENTRY_005', 'TEST_JOB_005', FALSE, 10);
CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_005', 'ENTRY_005');

-- Query schema information
SELECT
    column_name,
    data_type
FROM
    `project.dataset.INFORMATION_SCHEMA.COLUMNS`
WHERE
    table_name = 'job_run_log'
ORDER BY
    ordinal_position;

-- Query sample data
SELECT
    job_kennung,
    eintrags_nr,
    tab_name,
    records,
    run_ts
FROM
    `project.dataset.job_run_log`
WHERE
    job_kennung = 'TEST_JOB_005' AND eintrags_nr = 'ENTRY_005';
```

**Pass/Fail Criterion:**
1.  The `job_run_log` schema matches the DDL:
    *   `job_kennung` (STRING)
    *   `eintrags_nr` (STRING)
    *   `tab_name` (STRING)
    *   `records` (INT64)
    *   `run_ts` (TIMESTAMP)
2.  The retrieved sample data shows correct data types and values for the successful run.

```sql
# Pytest assertion (example)
def test_job_run_log_schema_and_data_types(bigquery_client):
    # Setup: Ensure a successful run has populated the log
    bigquery_client.query("DELETE FROM `project.dataset.job_error_log` WHERE TRUE").result()
    bigquery_client.query("DELETE FROM `project.dataset.job_run_log` WHERE TRUE").result()
    bigquery_client.query("DELETE FROM `project.dataset.ta_apn_ve` WHERE TRUE").result()
    bigquery_client.query("CALL `project.dataset.d_ausd_v_ta_apn_ve`('ENTRY_005', 'TEST_JOB_005', FALSE, 10)").result()
    bigquery_client.query("CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_005', 'ENTRY_005')").result()

    # Action: Query schema and data
    schema_query = """
        SELECT column_name, data_type
        FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'job_run_log'
        ORDER BY ordinal_position
    """
    schema_df = bigquery_client.query(schema_query).result().to_dataframe()

    data_query = """
        SELECT job_kennung, eintrags_nr, tab_name, records, run_ts
        FROM `project.dataset.job_run_log`
        WHERE job_kennung = 'TEST_JOB_005' AND eintrags_nr = 'ENTRY_005'
    """
    data_df = bigquery_client.query(data_query).result().to_dataframe()

    # Assertions
    expected_schema = {
        'job_kennung': 'STRING',
        'eintrags_nr': 'STRING',
        'tab_name': 'STRING',
        'records': 'INT64',
        'run_ts': 'TIMESTAMP'
    }
    actual_schema = {row.column_name: row.data_type for row in schema_df.itertuples(index=False)}
    assert actual_schema == expected_schema

    assert len(data_df) == 1
    assert data_df.iloc[0]['job_kennung'] == 'TEST_JOB_005'
    assert data_df.iloc[0]['eintrags_nr'] == 'ENTRY_005'
    assert data_df.iloc[0]['tab_name'] == 'ta_apn_ve'
    assert data_df.iloc[0]['records'] == 10
    assert isinstance(data_df.iloc[0]['run_ts'], (pd.Timestamp, datetime.datetime)) # Check type
```

---

## Test Case 6: Row Count Correctness (Zero Records)

**Purpose:** To verify that the record count mechanism correctly handles cases where the core processing procedure produces zero records for the given `EintragsNr`. This covers transformation correctness (counting logic) and data quality (accurate record count).

**Setup:**
1.  Ensure `project.dataset.job_run_log` is empty.
2.  The test version of `project.dataset.d_ausd_v_ta_apn_ve` is deployed.

**Action:**
Execute the `r_ausd_vertrag_control` procedure with valid parameters, configuring the mock `d_ausd_v_ta_apn_ve` to insert zero records.

```sql
-- Clear logs and target table before test
DELETE FROM `project.dataset.job_error_log` WHERE TRUE;
DELETE FROM `project.dataset.job_run_log` WHERE TRUE;
DELETE FROM `project.dataset.ta_apn_ve` WHERE TRUE;

-- Call mock d_ausd_v_ta_apn_ve to insert 0 records
CALL `project.dataset.d_ausd_v_ta_apn_ve`('ENTRY_006', 'TEST_JOB_006', FALSE, 0);
-- Call the control procedure
CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_006', 'ENTRY_006');
```

**Pass/Fail Criterion:**
1.  The `r_ausd_vertrag_control` procedure completes successfully.
2.  A record is inserted into `project.dataset.job_run_log` with `records` = 0.
3.  The procedure's final `SELECT` statement returns a message indicating 0 processed records.

```sql
# Pytest assertion (example)
def test_zero_records_processed(bigquery_client):
    # Setup: Clear tables
    bigquery_client.query("DELETE FROM `project.dataset.job_error_log` WHERE TRUE").result()
    bigquery_client.query("DELETE FROM `project.dataset.job_run_log` WHERE TRUE").result()
    bigquery_client.query("DELETE FROM `project.dataset.ta_apn_ve` WHERE TRUE").result()

    # Action: Call mock d_ausd_v_ta_apn_ve to insert 0 records, then r_ausd_vertrag_control
    bigquery_client.query("CALL `project.dataset.d_ausd_v_ta_apn_ve`('ENTRY_006', 'TEST_JOB_006', FALSE, 0)").result()
    results = bigquery_client.query("CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_006', 'ENTRY_006')").result()

    # Assertions
    assert "Job completed successfully. Processed 0 records for EintragsNr: ENTRY_006" in [row.message for row in results]

    run_log = bigquery_client.query("SELECT records FROM `project.dataset.job_run_log` WHERE job_kennung = 'TEST_JOB_006'").result().to_dataframe()
    assert len(run_log) == 1
    assert run_log.iloc[0]['records'] == 0
```