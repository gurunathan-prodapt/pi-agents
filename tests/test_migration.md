The migration of `r_ausd_bp_ta_bpr_instance.ksh` to a BigQuery Stored Procedure requires thorough validation to ensure behavioral equivalence. The tests below cover parameter handling, defaulting logic, error handling, logging, and the correct invocation of the downstream kernel procedure.

**Assumptions:**
*   A BigQuery project and dataset (`project.dataset`) are available.
*   The `job_log`, `job_metadata`, and `job_status` tables have been created as per the migration design document.
*   A mock BigQuery Stored Procedure for `k_ausd_bp_ta_bpr_instance` is deployed to simulate its behavior and allow for testing its invocation and error handling.
*   `pytest` is used as the testing framework, and a `bq_client` fixture is available for BigQuery interactions.

---

## Setup for All Tests

Before running any tests, ensure the necessary BigQuery objects are in place.

**1. Create BigQuery Tables (if not already existing):**

```sql
-- project.dataset.job_log
CREATE TABLE IF NOT EXISTS `project.dataset.job_log` (
  job_name STRING,
  job_nr INT64,
  log_level STRING,
  message STRING,
  stichtag STRING,
  restart_value INT64,
  created_at TIMESTAMP
);

-- project.dataset.job_metadata
CREATE TABLE IF NOT EXISTS `project.dataset.job_metadata` (
  job_name STRING,
  job_nr INT64,
  log_file_name STRING,
  sysdate_ddmmyyyy STRING,
  stichtag_ddmmyyyy STRING,
  restart_value INT64,
  created_at TIMESTAMP
);

-- project.dataset.job_status
CREATE TABLE IF NOT EXISTS `project.dataset.job_status` (
  job_name STRING,
  job_nr INT64,
  status STRING,
  updated_at TIMESTAMP
);
```

**2. Deploy the Migrated Stored Procedure (`ausd_bp_ta_bpr_instance`):**

The provided `r_ausd_bp_ta_bpr_instance.sql` should be deployed as `project.dataset.ausd_bp_ta_bpr_instance`.

**3. Deploy Mock Kernel Stored Procedure (`k_ausd_bp_ta_bpr_instance`):**

This mock procedure will allow us to verify parameters passed to the kernel and simulate errors.

```sql
CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_bp_ta_bpr_instance`(
  IN p_job_kennung STRING,
  IN p_stichtag STRING,
  IN p_eintrags_nr INT64,
  IN p_wiederanlaufwert INT64
)
BEGIN
  -- Log the invocation and parameters for verification
  INSERT INTO `project.dataset.job_log`
    (job_name, job_nr, log_level, message, stichtag, restart_value, created_at)
  VALUES
    (p_job_kennung, p_eintrags_nr, 'D',
     CONCAT('Mock k_ausd_bp_ta_bpr_instance called with: ',
            'JobKennung=', p_job_kennung,
            ', Stichtag=', p_stichtag,
            ', EintragsNr=', CAST(p_eintrags_nr AS STRING),
            ', Wiederanlaufwert=', CAST(p_wiederanlaufwert AS STRING)),
     p_stichtag, p_wiederanlaufwert, CURRENT_TIMESTAMP());

  -- Simulate an error if a specific stichtag is provided
  IF p_stichtag = '01011999' THEN
    RAISE USING MESSAGE = 'Simulated error in k_ausd_bp_ta_bpr_instance for Stichtag 01011999';
  END IF;

  -- Simulate a successful execution otherwise
END;
```

---

## Test Cases

### Test Case 1: Full Parameter Provisioning (Output Parity, Transformation Correctness)

**Purpose:** Verify that the stored procedure correctly processes all provided parameters (`p_stichtag`, `p_wiederanlaufwert`), logs the execution, and calls the kernel procedure with the expected values.

**Setup:**
1.  Clear `job_log`, `job_metadata`, and `job_status` tables for the job `ausd_bp_ta_bpr_instance`.
2.  Define test parameters: `stichtag = '25122023'`, `wiederanlaufwert = 12345`.

**Action:**
Execute the BigQuery Stored Procedure `project.dataset.ausd_bp_ta_bpr_instance` with the defined parameters.

```python
# pytest example
def test_full_parameter_provisioning(bq_client):
    job_name = 'ausd_bp_ta_bpr_instance'
    test_stichtag = '25122023'
    test_wiederanlaufwert = 12345

    # Clear previous logs
    bq_client.query(f"DELETE FROM `project.dataset.job_log` WHERE job_name = '{job_name}'").result()
    bq_client.query(f"DELETE FROM `project.dataset.job_metadata` WHERE job_name = '{job_name}'").result()
    bq_client.query(f"DELETE FROM `project.dataset.job_status` WHERE job_name = '{job_name}'").result()

    # Action: Call the stored procedure
    query = f"""
    CALL `project.dataset.ausd_bp_ta_bpr_instance`(
      p_stichtag => '{test_stichtag}',
      p_wiederanlaufwert => {test_wiederanlaufwert}
    );
    """
    bq_client.query(query).result()

    # Assertions (Pass/Fail Criteria)
    # 1. Verify job_log entries
    log_entries = list(bq_client.query(f"SELECT * FROM `project.dataset.job_log` WHERE job_name = '{job_name}' ORDER BY created_at").result())
    assert len(log_entries) == 3, "Expected 3 log entries (start, kernel call, success)"
    assert log_entries[0]['log_level'] == 'I'
    assert f"Stichtag: {test_stichtag}" in log_entries[0]['message']
    assert f"Wiederanlaufwert: {test_wiederanlaufwert}" in log_entries[0]['message']
    assert log_entries[0]['stichtag'] == test_stichtag
    assert log_entries[0]['restart_value'] == test_wiederanlaufwert

    # 2. Verify kernel call in log
    kernel_log = next((e for e in log_entries if e['log_level'] == 'D' and 'Mock k_ausd_bp_ta_bpr_instance called with' in e['message']), None)
    assert kernel_log is not None, "Kernel procedure call not logged"
    assert f"Stichtag={test_stichtag}" in kernel_log['message']
    assert f"Wiederanlaufwert={test_wiederanlaufwert}" in kernel_log['message']
    assert kernel_log['stichtag'] == test_stichtag
    assert kernel_log['restart_value'] == test_wiederanlaufwert

    # 3. Verify job_metadata entry
    metadata_entry = list(bq_client.query(f"SELECT * FROM `project.dataset.job_metadata` WHERE job_name = '{job_name}'").result())
    assert len(metadata_entry) == 1, "Expected 1 metadata entry"
    assert metadata_entry[0]['stichtag_ddmmyyyy'] == test_stichtag
    assert metadata_entry[0]['restart_value'] == test_wiederanlaufwert

    # 4. Verify job_status entry
    status_entry = list(bq_client.query(f"SELECT * FROM `project.dataset.job_status` WHERE job_name = '{job_name}'").result())
    assert len(status_entry) == 1, "Expected 1 status entry"
    assert status_entry[0]['status'] == 'OK'
```

**Pass/Fail Criterion:**
*   Three entries exist in `job_log`: one for job start (level 'I'), one for the mock kernel call (level 'D'), and one for successful completion (level 'I').
*   The `stichtag` and `restart_value` in `job_log` and `job_metadata` match the input parameters.
*   The `job_status` table shows `status = 'OK'` for the job.
*   The message for the kernel call in `job_log` explicitly contains the correct `Stichtag` and `Wiederanlaufwert` passed to it.

---

### Test Case 2: Missing `p_stichtag` (Transformation Correctness - Defaulting)

**Purpose:** Verify that `p_stichtag` defaults to the current system date (`v_sysdate`) when not provided, and that this default is correctly used in logging and passed to the kernel procedure.

**Setup:**
1.  Clear `job_log`, `job_metadata`, and `job_status` tables.
2.  Define test parameter: `wiederanlaufwert = 54321`.
3.  Determine the expected default `stichtag` (current date in DDMMYYYY format).

**Action:**
Execute the BigQuery Stored Procedure `project.dataset.ausd_bp_ta_bpr_instance` with `p_stichtag` as `NULL` or an empty string.

```python
# pytest example
import datetime

def test_missing_stichtag_default(bq_client):
    job_name = 'ausd_bp_ta_bpr_instance'
    test_wiederanlaufwert = 54321
    expected_stichtag = datetime.datetime.now().strftime('%d%m%Y') # Expected default

    # Clear previous logs
    bq_client.query(f"DELETE FROM `project.dataset.job_log` WHERE job_name = '{job_name}'").result()
    bq_client.query(f"DELETE FROM `project.dataset.job_metadata` WHERE job_name = '{job_name}'").result()
    bq_client.query(f"DELETE FROM `project.dataset.job_status` WHERE job_name = '{job_name}'").result()

    # Action: Call the stored procedure with NULL stichtag
    query = f"""
    CALL `project.dataset.ausd_bp_ta_bpr_instance`(
      p_stichtag => NULL,
      p_wiederanlaufwert => {test_wiederanlaufwert}
    );
    """
    bq_client.query(query).result()

    # Assertions (Pass/Fail Criteria)
    log_entries = list(bq_client.query(f"SELECT * FROM `project.dataset.job_log` WHERE job_name = '{job_name}' ORDER BY created_at").result())
    assert len(log_entries) == 3, "Expected 3 log entries (start, kernel call, success)"
    assert log_entries[0]['log_level'] == 'I'
    assert f"Stichtag: {expected_stichtag}" in log_entries[0]['message']
    assert log_entries[0]['stichtag'] == expected_stichtag

    kernel_log = next((e for e in log_entries if e['log_level'] == 'D' and 'Mock k_ausd_bp_ta_bpr_instance called with' in e['message']), None)
    assert kernel_log is not None, "Kernel procedure call not logged"
    assert f"Stichtag={expected_stichtag}" in kernel_log['message']
    assert kernel_log['stichtag'] == expected_stichtag

    metadata_entry = list(bq_client.query(f"SELECT * FROM `project.dataset.job_metadata` WHERE job_name = '{job_name}'").result())
    assert len(metadata_entry) == 1, "Expected 1 metadata entry"
    assert metadata_entry[0]['stichtag_ddmmyyyy'] == expected_stichtag

    status_entry = list(bq_client.query(f"SELECT * FROM `project.dataset.job_status` WHERE job_name = '{job_name}'").result())
    assert len(status_entry) == 1, "Expected 1 status entry"
    assert status_entry[0]['status'] == 'OK'
```

**Pass/Fail Criterion:**
*   The `stichtag` recorded in `job_log` and `job_metadata` matches the current system date (DDMMYYYY).
*   The mock kernel procedure is called with the current system date as `p_stichtag`.
*   The `job_status` table shows `status = 'OK'`.

---

### Test Case 3: Missing `p_wiederanlaufwert` (Transformation Correctness - Defaulting)

**Purpose:** Verify that `p_wiederanlaufwert` defaults to `0` when not provided, and that this default is correctly used in logging and passed to the kernel procedure.

**Setup:**
1.  Clear `job_log`, `job_metadata`, and `job_status` tables.
2.  Define test parameter: `stichtag = '01012024'`.
3.  Expected default `wiederanlaufwert = 0`.

**Action:**
Execute the BigQuery Stored Procedure `project.dataset.ausd_bp_ta_bpr_instance` with `p_wiederanlaufwert` as `NULL`.

```python
# pytest example
def test_missing_wiederanlaufwert_default(bq_client):
    job_name = 'ausd_bp_ta_bpr_instance'
    test_stichtag = '01012024'
    expected_wiederanlaufwert = 0 # Expected default

    # Clear previous logs
    bq_client.query(f"DELETE FROM `project.dataset.job_log` WHERE job_name = '{job_name}'").result()
    bq_client.query(f"DELETE FROM `project.dataset.job_metadata` WHERE job_name = '{job_name}'").result()
    bq_client.query(f"DELETE FROM `project.dataset.job_status` WHERE job_name = '{job_name}'").result()

    # Action: Call the stored procedure with NULL wiederanlaufwert
    query = f"""
    CALL `project.dataset.ausd_bp_ta_bpr_instance`(
      p_stichtag => '{test_stichtag}',
      p_wiederanlaufwert => NULL
    );
    """
    bq_client.query(query).result()

    # Assertions (Pass/Fail Criteria)
    log_entries = list(bq_client.query(f"SELECT * FROM `project.dataset.job_log` WHERE job_name = '{job_name}' ORDER BY created_at").result())
    assert len(log_entries) == 3, "Expected 3 log entries (start, kernel call, success)"
    assert log_entries[0]['log_level'] == 'I'
    assert f"Wiederanlaufwert: {expected_wiederanlaufwert}" in log_entries[0]['message']
    assert log_entries[0]['restart_value'] == expected_wiederanlaufwert

    kernel_log = next((e for e in log_entries if e['log_level'] == 'D' and 'Mock k_ausd_bp_ta_bpr_instance called with' in e['message']), None)
    assert kernel_log is not None, "Kernel procedure call not logged"
    assert f"Wiederanlaufwert={expected_wiederanlaufwert}" in kernel_log['message']
    assert kernel_log['restart_value'] == expected_wiederanlaufwert

    metadata_entry = list(bq_client.query(f"SELECT * FROM `project.dataset.job_metadata` WHERE job_name = '{job_name}'").result())
    assert len(metadata_entry) == 1, "Expected 1 metadata entry"
    assert metadata_entry[0]['restart_value'] == expected_wiederanlaufwert

    status_entry = list(bq_client.query(f"SELECT * FROM `project.dataset.job_status` WHERE job_name = '{job_name}'").result())
    assert len(status_entry) == 1, "Expected 1 status entry"
    assert status_entry[0]['status'] == 'OK'
```

**Pass/Fail Criterion:**
*   The `restart_value` recorded in `job_log` and `job_metadata` is `0`.
*   The mock kernel procedure is called with `p_wiederanlaufwert = 0`.
*   The `job_status` table shows `status = 'OK'`.

---

### Test Case 4: Invalid `p_stichtag` Format (Transformation Correctness - Validation & Error Handling)

**Purpose:** Verify that the stored procedure correctly validates the `p_stichtag` format, logs an error, updates the job status to 'ERROR', and raises an exception.

**Setup:**
1.  Clear `job_log`, `job_metadata`, and `job_status` tables.
2.  Define test parameters: `stichtag = '2023-12-25'` (invalid format), `wiederanlaufwert = 100`.

**Action:**
Attempt to execute the BigQuery Stored Procedure `project.dataset.ausd_bp_ta_bpr_instance` with the invalid `p_stichtag`. Expect an error to be raised.

```python
# pytest example
import pytest

def test_invalid_stichtag_format(bq_client):
    job_name = 'ausd_bp_ta_bpr_instance'
    invalid_stichtag = '2023-12-25' # Invalid DDMMYYYY format
    test_wiederanlaufwert = 100

    # Clear previous logs
    bq_client.query(f"DELETE FROM `project.dataset.job_log` WHERE job_name = '{job_name}'").result()
    bq_client.query(f"DELETE FROM `project.dataset.job_metadata` WHERE job_name = '{job_name}'").result()
    bq_client.query(f"DELETE FROM `project.dataset.job_status` WHERE job_name = '{job_name}'").result()

    # Action: Call the stored procedure, expecting an error
    query = f"""
    CALL `project.dataset.ausd_bp_ta_bpr_instance`(
      p_stichtag => '{invalid_stichtag}',
      p_wiederanlaufwert => {test_wiederanlaufwert}
    );
    """
    with pytest.raises(Exception) as excinfo:
        bq_client.query(query).result()

    # Assertions (Pass/Fail Criteria)
    assert "AppError: Abbruch. Error Message: Parameter validation failed: Stichtag must be in DDMMYYYY format" in str(excinfo.value)

    log_entries = list(bq_client.query(f"SELECT * FROM `project.dataset.job_log` WHERE job_name = '{job_name}' ORDER BY created_at").result())
    assert len(log_entries) == 1, "Expected 1 log entry for validation error"
    assert log_entries[0]['log_level'] == 'E'
    assert "Parameter validation failed: Stichtag must be in DDMMYYYY format" in log_entries[0]['message']
    assert log_entries[0]['stichtag'] == invalid_stichtag
    assert log_entries[0]['restart_value'] == test_wiederanlaufwert

    # Verify job_metadata is NOT created for failed validation
    metadata_entry = list(bq_client.query(f"SELECT * FROM `project.dataset.job_metadata` WHERE job_name = '{job_name}'").result())
    assert len(metadata_entry) == 0, "Expected no metadata entry for failed validation"

    status_entry = list(bq_client.query(f"SELECT * FROM `project.dataset.job_status` WHERE job_name = '{job_name}'").result())
    assert len(status_entry) == 1, "Expected 1 status entry"
    assert status_entry[0]['status'] == 'ERROR'
```

**Pass/Fail Criterion:**
*   The procedure raises an exception containing the expected error message.
*   Exactly one entry exists in `job_log` with `log_level = 'E'` and the specific validation error message.
*   No entry is created in `job_metadata`.
*   The `job_status` table shows `status = 'ERROR'`.
*   The mock kernel procedure is NOT called (verified by checking `job_log` for 'D' level entries).

---

### Test Case 5: Error During Kernel Procedure Execution (External-system replacements, Error Handling)

**Purpose:** Verify that if the `k_ausd_bp_ta_bpr_instance` (mocked) procedure encounters an error, the orchestrating procedure catches it, logs it, updates the job status, and re-raises the error.

**Setup:**
1.  Clear `job_log`, `job_metadata`, and `job_status` tables.
2.  Define test parameters: `stichtag = '01011999'` (this special value triggers an error in the mock kernel), `wiederanlaufwert = 999`.

**Action:**
Execute the BigQuery Stored Procedure `project.dataset.ausd_bp_ta_bpr_instance` with the special `p_stichtag` that causes the mock kernel to fail. Expect an error to be raised.

```python
# pytest example
import pytest

def test_kernel_procedure_error_handling(bq_client):
    job_name = 'ausd_bp_ta_bpr_instance'
    error_stichtag = '01011999' # Triggers error in mock k_ausd_bp_ta_bpr_instance
    test_wiederanlaufwert = 999

    # Clear previous logs
    bq_client.query(f"DELETE FROM `project.dataset.job_log` WHERE job_name = '{job_name}'").result()
    bq_client.query(f"DELETE FROM `project.dataset.job_metadata` WHERE job_name = '{job_name}'").result()
    bq_client.query(f"DELETE FROM `project.dataset.job_status` WHERE job_name = '{job_name}'").result()

    # Action: Call the stored procedure, expecting an error from the kernel
    query = f"""
    CALL `project.dataset.ausd_bp_ta_bpr_instance`(
      p_stichtag => '{error_stichtag}',
      p_wiederanlaufwert => {test_wiederanlaufwert}
    );
    """
    with pytest.raises(Exception) as excinfo:
        bq_client.query(query).result()

    # Assertions (Pass/Fail Criteria)
    assert "AppError: Abbruch. Error Message: Simulated error in k_ausd_bp_ta_bpr_instance for Stichtag 01011999" in str(excinfo.value)

    log_entries = list(bq_client.query(f"SELECT * FROM `project.dataset.job_log` WHERE job_name = '{job_name}' ORDER BY created_at").result())
    assert len(log_entries) == 3, "Expected 3 log entries (start, kernel call, error)"
    assert log_entries[0]['log_level'] == 'I' # Job start
    assert log_entries[1]['log_level'] == 'D' # Mock kernel call
    assert log_entries[2]['log_level'] == 'E' # Error from kernel
    assert "AppError: Abbruch. Error Message: Simulated error in k_ausd_bp_ta_bpr_instance for Stichtag 01011999" in log_entries[2]['message']
    assert log_entries[2]['stichtag'] == error_stichtag
    assert log_entries[2]['restart_value'] == test_wiederanlaufwert

    metadata_entry = list(bq_client.query(f"SELECT * FROM `project.dataset.job_metadata` WHERE job_name = '{job_name}'").result())
    assert len(metadata_entry) == 1, "Expected 1 metadata entry"
    assert metadata_entry[0]['stichtag_ddmmyyyy'] == error_stichtag

    status_entry = list(bq_client.query(f"SELECT * FROM `project.dataset.job_status` WHERE job_name = '{job_name}'").result())
    assert len(status_entry) == 1, "Expected 1 status entry"
    assert status_entry[0]['status'] == 'ERROR'
```

**Pass/Fail Criterion:**
*   The procedure raises an exception containing the error message from the mock kernel.
*   Three entries exist in `job_log`: job start, mock kernel call, and an error entry (level 'E') with the kernel's error message.
*   One entry exists in `job_metadata`.
*   The `job_status` table shows `status = 'ERROR'`.

---

### Test Case 6: Data Quality and Schema Assertions for Logging Tables

**Purpose:** Verify that the logging and metadata tables conform to the expected schema and data types, and that `job_nr` increments correctly across multiple runs.

**Setup:**
1.  Clear `job_log`, `job_metadata`, and `job_status` tables.
2.  Define expected schema for `job_log`, `job_metadata`, `job_status`.

**Action:**
1.  Execute the main stored procedure twice with different parameters.
2.  Query the schema of the logging tables.
3.  Query the contents of the logging tables.

```python
# pytest example
def test_logging_table_schema_and_job_nr_increment(bq_client):
    job_name = 'ausd_bp_ta_bpr_instance'

    # Clear previous logs
    bq_client.query(f"DELETE FROM `project.dataset.job_log` WHERE job_name = '{job_name}'").result()
    bq_client.query(f"DELETE FROM `project.dataset.job_metadata` WHERE job_name = '{job_name}'").result()
    bq_client.query(f"DELETE FROM `project.dataset.job_status` WHERE job_name = '{job_name}'").result()

    # Action 1: First run
    bq_client.query(f"CALL `project.dataset.ausd_bp_ta_bpr_instance`(p_stichtag => '01012023', p_wiederanlaufwert => 10)").result()

    # Action 2: Second run
    bq_client.query(f"CALL `project.dataset.ausd_bp_ta_bpr_instance`(p_stichtag => '02022023', p_wiederanlaufwert => 20)").result()

    # Assertions (Pass/Fail Criteria)

    # 1. Schema assertions (example for job_log)
    table_ref = bq_client.dataset('dataset').table('job_log')
    table = bq_client.get_table(table_ref)
    schema_fields = {field.name: field.field_type for field in table.schema}
    expected_schema = {
        'job_name': 'STRING',
        'job_nr': 'INT64',
        'log_level': 'STRING',
        'message': 'STRING',
        'stichtag': 'STRING',
        'restart_value': 'INT64',
        'created_at': 'TIMESTAMP'
    }
    assert schema_fields == expected_schema, "job_log schema mismatch"

    # Repeat for job_metadata and job_status

    # 2. Job number increment and data quality
    log_entries = list(bq_client.query(f"SELECT job_nr, stichtag, restart_value FROM `project.dataset.job_log` WHERE job_name = '{job_name}' AND log_level = 'I' AND message LIKE 'Job start%' ORDER BY job_nr").result())
    assert len(log_entries) == 2, "Expected 2 job start entries"
    assert log_entries[0]['job_nr'] == 1
    assert log_entries[0]['stichtag'] == '01012023'
    assert log_entries[0]['restart_value'] == 10
    assert log_entries[1]['job_nr'] == 2
    assert log_entries[1]['stichtag'] == '02022023'
    assert log_entries[1]['restart_value'] == 20

    metadata_entries = list(bq_client.query(f"SELECT job_nr, stichtag_ddmmyyyy, restart_value FROM `project.dataset.job_metadata` WHERE job_name = '{job_name}' ORDER BY job_nr").result())
    assert len(metadata_entries) == 2, "Expected 2 metadata entries"
    assert metadata_entries[0]['job_nr'] == 1
    assert metadata_entries[0]['stichtag_ddmmyyyy'] == '01012023'
    assert metadata_entries[0]['restart_value'] == 10
    assert metadata_entries[1]['job_nr'] == 2
    assert metadata_entries[1]['stichtag_ddmmyyyy'] == '02022023'
    assert metadata_entries[1]['restart_value'] == 20

    status_entries = list(bq_client.query(f"SELECT job_nr, status FROM `project.dataset.job_status` WHERE job_name = '{job_name}' ORDER BY job_nr").result())
    assert len(status_entries) == 2, "Expected 2 status entries"
    assert status_entries[0]['job_nr'] == 1
    assert status_entries[0]['status'] == 'OK'
    assert status_entries[1]['job_nr'] == 2
    assert status_entries[1]['status'] == 'OK'
```

**Pass/Fail Criterion:**
*   The schema of `job_log`, `job_metadata`, and `job_status` tables matches the expected definitions (column names and types).
*   `job_nr` increments correctly for successive job runs.
*   The `stichtag` and `restart_value` stored in `job_log` and `job_metadata` for each run are correct.
*   The `job_status` for both runs is 'OK'.

---