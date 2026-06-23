The following migration validation tests are designed to ensure that the BigQuery stored procedure `project.dataset.BERT_DROP_TEMP_TABLE` (migrated from `r_drop_temp_table.ksh`) is behaviourally equivalent to its legacy counterpart. The tests cover parameter handling, logging, error management, and the correct invocation of the core cleanup logic.

**Prerequisites for Running Tests:**

1.  **Google Cloud Project and BigQuery Dataset:** Replace `your-gcp-project-id` and `your-bq-dataset-id` with your actual project ID and dataset ID.
2.  **BigQuery Tables and Procedures:** Ensure the following BigQuery objects are created in your target dataset:
    *   `project.dataset.job_log` (as defined in `create_log_tables.sql`)
    *   `project.dataset.job_status` (as defined in `create_log_tables.sql`)
    *   `project.dataset.k_drop_temp_table` (the placeholder procedure as provided in `k_drop_temp_table.sql`)
    *   `project.dataset.BERT_DROP_TEMP_TABLE` (the wrapper procedure as provided in `bert_drop_temp_table_wrapper.sql`)
3.  **Python Environment:** Install `pytest` and `google-cloud-bigquery`.
    ```bash
    pip install pytest google-cloud-bigquery
    ```
4.  **Authentication:** Ensure your environment is authenticated to GCP (e.g., `gcloud auth application-default login`).

---

### Test Setup (Pytest Fixtures)

The following Pytest fixtures will be used across the test cases to manage the BigQuery client and ensure a clean state for log tables before each test.

```python
import pytest
from google.cloud import bigquery
from datetime import datetime
import time

# --- Configuration ---
PROJECT_ID = "your-gcp-project-id"  # Replace with your GCP Project ID
DATASET_ID = "your-bq-dataset-id"  # Replace with your BigQuery Dataset ID
# --- End Configuration ---

@pytest.fixture(scope="module")
def bq_client():
    """Provides a BigQuery client for the test module."""
    return bigquery.Client(project=PROJECT_ID)

@pytest.fixture(autouse=True)
def cleanup_log_tables(bq_client):
    """
    Ensures job_log and job_status tables are truncated before each test.
    This provides a clean slate for log assertions.
    """
    print(f"\nCleaning up tables: {PROJECT_ID}.{DATASET_ID}.job_log, {PROJECT_ID}.{DATASET_ID}.job_status")
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_log`").result()
    bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_status`").result()
    yield
    print(f"Finished test, tables cleaned.")

# Helper function to get current date in DDMMYYYY format
def get_current_date_ddmmyyyy():
    return datetime.now().strftime('%d%m%Y')

# Helper function to query logs
def get_logs(bq_client):
    log_query = f"SELECT eintragsnr, job_kennung, log_level, err_nr, err_arg, message, stichtag, restart_value, created_at FROM `{PROJECT_ID}.{DATASET_ID}.job_log` ORDER BY created_at ASC"
    return list(bq_client.query(log_query).result())

# Helper function to query status
def get_status(bq_client):
    status_query = f"SELECT eintragsnr, job_kennung, status, updated_at FROM `{PROJECT_ID}.{DATASET_ID}.job_status` WHERE job_kennung = 'BERT_DROP_TEMP_TABLE' ORDER BY updated_at DESC LIMIT 1"
    return list(bq_client.query(status_query).result())
```

---

### Test Case 1: Successful Execution with Default Parameters

**1. Purpose:**
Validate that the migrated BigQuery stored procedure `BERT_DROP_TEMP_TABLE` executes successfully when no parameters are provided. This test verifies that the procedure correctly applies default values for `p_stichtag` (current system date in `DDMMYYYY` format) and `p_wiederanlaufWert` (0), and accurately logs the execution status and parameters.

**2. Setup:**
*   Ensure `project.dataset.job_log` and `project.dataset.job_status` tables are empty (handled by `cleanup_log_tables` fixture).
*   Ensure `project.dataset.k_drop_temp_table` procedure exists and is in its default placeholder state (i.e., it logs its invocation but does not raise an error).

**3. Action:**
Execute the `BERT_DROP_TEMP_TABLE` stored procedure without any input parameters:
```sql
CALL `your-gcp-project-id.your-bq-dataset-id.BERT_DROP_TEMP_TABLE`(NULL, NULL);
```

**4. Pass/Fail Criterion:**
*   The BigQuery procedure call completes without raising an error.
*   The `project.dataset.job_log` table contains at least three `INFO` entries:
    *   One indicating "Job started".
    *   One detailing the job parameters (e.g., "Job-Nr: ..., JobKennung: ..., Stichtag: ..., Wiederanlaufwert: ..."). This entry's `stichtag` field must match the current system date in `DDMMYYYY` format, and `restart_value` must be '0'.
    *   One from the `k_drop_temp_table` procedure (e.g., "Executing core cleanup logic (placeholder)"). This entry's `stichtag` and `restart_value` must also match the defaults.
    *   One indicating "The processing completed without recognizable errors".
*   The `project.dataset.job_status` table contains exactly one entry for `job_kennung = 'BERT_DROP_TEMP_TABLE'` with `status = 'OK'`.

**5. Runnable Test Code (Pytest):**

```python
# Assuming the fixtures and helper functions from above are defined

def test_default_parameters_success(bq_client):
    # Action
    bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.BERT_DROP_TEMP_TABLE`(NULL, NULL);").result()

    # Assertions
    logs = get_logs(bq_client)
    status_result = get_status(bq_client)

    assert len(logs) >= 3, "Expected at least 3 log entries for successful run"
    assert any("Job started" in log.message for log in logs)
    assert any("The processing completed without recognizable errors" in log.message for log in logs)
    assert any("Executing core cleanup logic (placeholder)" in log.message for log in logs)

    # Check parameters in logs
    current_date_ddmmyyyy = get_current_date_ddmmyyyy()
    param_log_entry = next((log for log in logs if "Job-Nr" in log.message), None)
    assert param_log_entry is not None, "Parameter log entry not found"
    assert param_log_entry.stichtag == current_date_ddmmyyyy, f"Expected stichtag to be {current_date_ddmmyyyy}, got {param_log_entry.stichtag}"
    assert param_log_entry.restart_value == '0', f"Expected restart_value to be '0', got {param_log_entry.restart_value}"

    k_drop_log_entry = next((log for log in logs if "Executing core cleanup logic" in log.message), None)
    assert k_drop_log_entry is not None, "k_drop_temp_table log entry not found"
    assert k_drop_log_entry.stichtag == current_date_ddmmyyyy
    assert k_drop_log_entry.restart_value == '0'

    # Check job_status entry
    assert len(status_result) == 1, "Expected one status entry"
    assert status_result[0].status == 'OK', "Expected job status to be 'OK'"
```

---

### Test Case 2: Successful Execution with Explicit Valid Parameters

**1. Purpose:**
Validate that the migrated BigQuery stored procedure `BERT_DROP_TEMP_TABLE` correctly processes and passes explicit valid `p_stichtag` and `p_wiederanlaufWert` to the core cleanup procedure, and logs them accurately.

**2. Setup:**
*   Ensure `project.dataset.job_log` and `project.dataset.job_status` tables are empty (handled by `cleanup_log_tables` fixture).
*   Ensure `project.dataset.k_drop_temp_table` procedure exists and is in its default placeholder state.

**3. Action:**
Execute the `BERT_DROP_TEMP_TABLE` stored procedure with specific valid parameters:
```sql
CALL `your-gcp-project-id.your-bq-dataset-id.BERT_DROP_TEMP_TABLE`('01012023', '12345');
```

**4. Pass/Fail Criterion:**
*   The BigQuery procedure call completes without raising an error.
*   The `project.dataset.job_log` table contains at least three `INFO` entries:
    *   One indicating "Job started".
    *   One detailing the job parameters. This entry's `stichtag` field must be '01012023' and `restart_value` must be '12345'.
    *   One from the `k_drop_temp_table` procedure. This entry's `stichtag` and `restart_value` must also match '01012023' and '12345' respectively.
    *   One indicating "The processing completed without recognizable errors".
*   The `project.dataset.job_status` table contains exactly one entry for `job_kennung = 'BERT_DROP_TEMP_TABLE'` with `status = 'OK'`.

**5. Runnable Test Code (Pytest):**

```python
# Assuming the fixtures and helper functions from above are defined

def test_explicit_parameters_success(bq_client):
    test_stichtag = '01012023'
    test_wiederanlaufwert = '12345'

    # Action
    bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.BERT_DROP_TEMP_TABLE`('{test_stichtag}', '{test_wiederanlaufwert}');").result()

    # Assertions
    logs = get_logs(bq_client)
    status_result = get_status(bq_client)

    assert len(logs) >= 3, "Expected at least 3 log entries for successful run"
    assert any("Job started" in log.message for log in logs)
    assert any("The processing completed without recognizable errors" in log.message for log in logs)
    assert any("Executing core cleanup logic (placeholder)" in log.message for log in logs)

    param_log_entry = next((log for log in logs if "Job-Nr" in log.message), None)
    assert param_log_entry is not None, "Parameter log entry not found"
    assert param_log_entry.stichtag == test_stichtag, f"Expected stichtag to be {test_stichtag}, got {param_log_entry.stichtag}"
    assert param_log_entry.restart_value == test_wiederanlaufwert, f"Expected restart_value to be {test_wiederanlaufwert}, got {param_log_entry.restart_value}"

    k_drop_log_entry = next((log for log in logs if "Executing core cleanup logic" in log.message), None)
    assert k_drop_log_entry is not None, "k_drop_temp_table log entry not found"
    assert k_drop_log_entry.stichtag == test_stichtag
    assert k_drop_log_entry.restart_value == test_wiederanlaufwert

    # Check job_status entry
    assert len(status_result) == 1, "Expected one status entry"
    assert status_result[0].status == 'OK', "Expected job status to be 'OK'"
```

---

### Test Case 3: Error Handling - Invalid Stichtag Format

**1. Purpose:**
Verify that the migrated procedure correctly identifies and handles an invalid `p_stichtag` format. It should log an error with the specific error code (`193`) and message, update the job status to `ERROR`, and terminate execution without calling the core cleanup procedure.

**2. Setup:**
*   Ensure `project.dataset.job_log` and `project.dataset.job_status` tables are empty (handled by `cleanup_log_tables` fixture).

**3. Action:**
Execute the `BERT_DROP_TEMP_TABLE` stored procedure with an invalid `p_stichtag` format:
```sql
CALL `your-gcp-project-id.your-bq-dataset-id.BERT_DROP_TEMP_TABLE`('2023-01-01', NULL);
```

**4. Pass/Fail Criterion:**
*   The BigQuery procedure call raises an error (e.g., `RAISE USING MESSAGE`) with a message indicating the invalid Stichtag format.
*   The `project.dataset.job_log` table contains:
    *   An `INFO` entry for "Job started".
    *   An `ERROR` entry with `log_level = 'ERROR'`, `err_nr = 193`, and `err_arg` containing a message like "Stichtag (2023-01-01) is not in DDMMYYYY format".
*   The `project.dataset.job_status` table contains exactly one entry for `job_kennung = 'BERT_DROP_TEMP_TABLE'` with `status = 'ERROR'`.
*   There are no log entries from `k_drop_temp_table`, confirming it was not invoked.

**5. Runnable Test Code (Pytest):**

```python
# Assuming the fixtures and helper functions from above are defined

def test_invalid_stichtag_format_error(bq_client):
    invalid_stichtag = '2023-01-01' # YYYY-MM-DD instead of DDMMYYYY

    # Action & Assertion for error
    with pytest.raises(Exception) as excinfo:
        bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.BERT_DROP_TEMP_TABLE`('{invalid_stichtag}', NULL);").result()
    assert f"Stichtag ({invalid_stichtag}) is not in DDMMYYYY format" in str(excinfo.value)

    # Assertions for log and status tables
    logs = get_logs(bq_client)
    status_result = get_status(bq_client)

    assert any("Job started" in log.message for log in logs)
    error_log_entry = next((log for log in logs if log.log_level == 'ERROR'), None)
    assert error_log_entry is not None, "Expected an ERROR log entry"
    assert error_log_entry.err_nr == 193, f"Expected error number 193, got {error_log_entry.err_nr}"
    assert f"Stichtag ({invalid_stichtag}) is not in DDMMYYYY format" in error_log_entry.err_arg

    # Verify k_drop_temp_table was NOT called
    assert not any("Executing core cleanup logic" in log.message for log in logs), "k_drop_temp_table should not have been called"

    # Check job_status entry
    assert len(status_result) == 1, "Expected one status entry"
    assert status_result[0].status == 'ERROR', "Expected job status to be 'ERROR'"
```

---

### Test Case 4: Error Handling - Core Cleanup Procedure Failure

**1. Purpose:**
Verify that if the `k_drop_temp_table` procedure (the core cleanup logic) fails, the wrapper `BERT_DROP_TEMP_TABLE` correctly catches the error, logs it with a generic application error code (`999`), updates the job status to `ERROR`, and re-raises the error to indicate overall job failure.

**2. Setup:**
*   Ensure `project.dataset.job_log` and `project.dataset.job_status` tables are empty (handled by `cleanup_log_tables` fixture).
*   Temporarily modify the `project.dataset.k_drop_temp_table` procedure to intentionally raise an error. This modification should be reverted after the test.

**3. Action:**
Execute the `BERT_DROP_TEMP_TABLE` stored procedure with valid parameters, knowing that `k_drop_temp_table` will fail:
```sql
CALL `your-gcp-project-id.your-bq-dataset-id.BERT_DROP_TEMP_TABLE`('01012023', NULL);
```

**4. Pass/Fail Criterion:**
*   The BigQuery procedure call raises an error (e.g., `RAISE USING MESSAGE`) from the wrapper, containing both the wrapper's error message ("AppError: Abbruch") and the underlying error message from `k_drop_temp_table`.
*   The `project.dataset.job_log` table contains:
    *   An `INFO` entry for "Job started".
    *   An `INFO` entry from `k_drop_temp_table` indicating it started (and then failed).
    *   An `ERROR` entry from the wrapper with `log_level = 'ERROR'`, `err_nr = 999` (generic application error), and `err_arg` containing the message from `k_drop_temp_table`'s simulated failure.
*   The `project.dataset.job_status` table contains exactly one entry for `job_kennung = 'BERT_DROP_TEMP_TABLE'` with `status = 'ERROR'`.

**5. Runnable Test Code (Pytest):**

```python
# Assuming the fixtures and helper functions from above are defined

@pytest.fixture(autouse=True)
def setup_k_drop_temp_table_for_failure(bq_client):
    """
    Temporarily modifies k_drop_temp_table to fail, then restores it.
    """
    fail_proc_sql = f"""
    CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.k_drop_temp_table`(
      IN p_job_kennung STRING,
      IN p_stichtag STRING,
      IN p_eintragsnr INT64,
      IN p_wiederanlaufwert INT64
    )
    BEGIN
      INSERT INTO `{PROJECT_ID}.{DATASET_ID}.job_log` (eintragsnr, job_kennung, log_level, message, stichtag, restart_value, created_at)
      VALUES (p_eintragsnr, p_job_kennung, 'INFO', 'Executing core cleanup logic (placeholder, will fail)', p_stichtag, CAST(p_wiederanlaufwert AS STRING), CURRENT_TIMESTAMP());
      RAISE USING MESSAGE = 'Simulated failure in k_drop_temp_table';
    END;
    """
    print(f"\nModifying k_drop_temp_table to simulate failure...")
    bq_client.query(fail_proc_sql).result()
    yield
    # Restore k_drop_temp_table to its original placeholder state after the test
    restore_proc_sql = f"""
    CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.k_drop_temp_table`(
      IN p_job_kennung STRING,
      IN p_stichtag STRING,
      IN p_eintragsnr INT64,
      IN p_wiederanlaufwert INT64
    )
    BEGIN
      INSERT INTO `{PROJECT_ID}.{DATASET_ID}.job_log` (eintragsnr, job_kennung, log_level, message, stichtag, restart_value, created_at)
      VALUES (p_eintragsnr, p_job_kennung, 'INFO', 'Executing core cleanup logic (placeholder)', p_stichtag, CAST(p_wiederanlaufwert AS STRING), CURRENT_TIMESTAMP());
    END;
    """
    print(f"Restoring k_drop_temp_table to original state.")
    bq_client.query(restore_proc_sql).result()


def test_core_cleanup_procedure_failure(bq_client, setup_k_drop_temp_table_for_failure):
    test_stichtag = '01012023'

    # Action & Assertion for error
    with pytest.raises(Exception) as excinfo:
        bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.BERT_DROP_TEMP_TABLE`('{test_stichtag}', NULL);").result()
    assert "Simulated failure in k_drop_temp_table" in str(excinfo.value)
    assert "AppError: Abbruch" in str(excinfo.value) # Wrapper's error message

    # Assertions for log and status tables
    logs = get_logs(bq_client)
    status_result = get_status(bq_client)

    assert any("Job started" in log.message for log in logs)
    assert any("Executing core cleanup logic (placeholder, will fail)" in log.message for log in logs)

    error_log_entry = next((log for log in logs if log.log_level == 'ERROR'), None)
    assert error_log_entry is not None, "Expected an ERROR log entry from wrapper"
    assert error_log_entry.err_nr == 999, f"Expected error number 999, got {error_log_entry.err_nr}"
    assert "Simulated failure in k_drop_temp_table" in error_log_entry.err_arg

    # Check job_status entry
    assert len(status_result) == 1, "Expected one status entry"
    assert status_result[0].status == 'ERROR', "Expected job status to be 'ERROR'"
```

---

### Test Case 5: Data Quality and Schema Assertions for Log Tables

**1. Purpose:**
Verify that the `job_log` and `job_status` tables conform to their expected schemas and data types as defined in the migration design. This test also checks for basic data quality, ensuring that critical fields are populated and in the correct format.

**2. Setup:**
*   Ensure `project.dataset.job_log` and `project.dataset.job_status` tables are created as per the design (handled by `create_log_tables.sql`).
*   Populate the tables with data by running a mix of successful and failed `BERT_DROP_TEMP_TABLE` executions.

**3. Action:**
*   Query `INFORMATION_SCHEMA.COLUMNS` to retrieve the schema of `job_log` and `job_status`.
*   Query the `job_log` and `job_status` tables directly to inspect data quality.

**4. Pass/Fail Criterion:**
*   The `job_log` table's schema must exactly match:
    *   `eintragsnr` (INT64)
    *   `job_kennung` (STRING)
    *   `log_level` (STRING)
    *   `err_nr` (INT64)
    *   `err_arg` (STRING)
    *   `message` (STRING)
    *   `stichtag` (STRING)
    *   `restart_value` (STRING)
    *   `created_at` (TIMESTAMP)
*   The `job_status` table's schema must exactly match:
    *   `eintragsnr` (INT64)
    *   `job_kennung` (STRING)
    *   `status` (STRING)
    *   `updated_at` (TIMESTAMP)
*   For all entries in `job_log`:
    *   `eintragsnr`, `job_kennung`, `log_level`, `message`, `created_at` must not be NULL.
    *   `log_level` must be either 'INFO' or 'ERROR'.
    *   If `log_level` is 'ERROR', then `err_nr` and `err_arg` must not be NULL.
    *   `stichtag` values, if not NULL, must be 8 characters long, contain only digits, and be parsable as a `DDMMYYYY` date.
    *   `restart_value` values, if not NULL, must be convertible to an `INT64`.
*   For all entries in `job_status`:
    *   `eintragsnr`, `job_kennung`, `status`, `updated_at` must not be NULL.
    *   `status` must be either 'OK' or 'ERROR'.

**5. Runnable Test Code (Pytest):**

```python
# Assuming the fixtures and helper functions from above are defined

def test_log_table_schema_and_data_quality(bq_client):
    # Populate tables first with various scenarios
    print("\nPopulating log tables for schema and data quality checks...")
    bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.BERT_DROP_TEMP_TABLE`('01012023', '123');").result()
    time.sleep(1) # Ensure unique eintragsnr if timestamp-based
    bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.BERT_DROP_TEMP_TABLE`(NULL, NULL);").result()
    time.sleep(1)
    
    # Test invalid stichtag to get an error entry
    with pytest.raises(Exception):
        bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.BERT_DROP_TEMP_TABLE`('invalid_date', NULL);").result()
    print("Log tables populated.")

    # 1. Check job_log schema
    log_schema_query = f"""
    SELECT column_name, data_type
    FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'job_log'
    ORDER BY ordinal_position
    """
    log_schema = {row.column_name: row.data_type for row in bq_client.query(log_schema_query).result()}
    expected_log_schema = {
        'eintragsnr': 'INT64',
        'job_kennung': 'STRING',
        'log_level': 'STRING',
        'err_nr': 'INT64',
        'err_arg': 'STRING',
        'message': 'STRING',
        'stichtag': 'STRING',
        'restart_value': 'STRING',
        'created_at': 'TIMESTAMP'
    }
    assert log_schema == expected_log_schema, f"job_log schema mismatch. Expected: {expected_log_schema}, Got: {log_schema}"

    # 2. Check job_status schema
    status_schema_query = f"""
    SELECT column_name, data_type
    FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'job_status'
    ORDER BY ordinal_position
    """
    status_schema = {row.column_name: row.data_type for row in bq_client.query(status_schema_query).result()}
    expected_status_schema = {
        'eintragsnr': 'INT64',
        'job_kennung': 'STRING',
        'status': 'STRING',
        'updated_at': 'TIMESTAMP'
    }
    assert status_schema == expected_status_schema, f"job_status schema mismatch. Expected: {expected_status_schema}, Got: {status_schema}"

    # 3. Data Quality for job_log
    log_data_query = f"""
    SELECT eintragsnr, job_kennung, log_level, message, stichtag, restart_value, created_at, err_nr, err_arg
    FROM `{PROJECT_ID}.{DATASET_ID}.job_log`
    """
    log_data = list(bq_client.query(log_data_query).result())

    assert len(log_data) > 0, "No data found in job_log for quality check."

    for row in log_data:
        assert row.eintragsnr is not None and isinstance(row.eintragsnr, int), f"eintragsnr is invalid: {row.eintragsnr}"
        assert row.job_kennung == 'BERT_DROP_TEMP_TABLE', f"job_kennung mismatch: {row.job_kennung}"
        assert row.log_level in ['INFO', 'ERROR'], f"Invalid log_level: {row.log_level}"
        assert row.message is not None and isinstance(row.message, str), f"message is invalid: {row.message}"
        assert row.created_at is not None, f"created_at is NULL" # BigQuery client converts TIMESTAMP to datetime object

        # Stichtag format check (DDMMYYYY)
        if row.stichtag is not None:
            assert len(row.stichtag) == 8, f"stichtag length invalid: {row.stichtag}"
            assert row.stichtag.isdigit(), f"stichtag not digit-only: {row.stichtag}"
            try:
                datetime.strptime(row.stichtag, '%d%m%Y')
            except ValueError:
                pytest.fail(f"Invalid stichtag date format: {row.stichtag}")

        # restart_value check (convertible to INT64)
        if row.restart_value is not None:
            try:
                int(row.restart_value)
            except ValueError:
                pytest.fail(f"Invalid restart_value format: {row.restart_value}")
        
        # Error specific fields
        if row.log_level == 'ERROR':
            assert row.err_nr is not None, f"err_nr is NULL for ERROR log: {row}"
            assert row.err_arg is not None and isinstance(row.err_arg, str), f"err_arg is invalid for ERROR log: {row}"
        else: # INFO logs might have NULL for err_nr, err_arg
            pass # BigQuery allows NULLs by default

    # 4. Data Quality for job_status
    status_data_query = f"""
    SELECT eintragsnr, job_kennung, status, updated_at
    FROM `{PROJECT_ID}.{DATASET_ID}.job_status`
    """
    status_data = list(bq_client.query(status_data_query).result())

    assert len(status_data) > 0, "No data found in job_status for quality check."

    for row in status_data:
        assert row.eintragsnr is not None and isinstance(row.eintragsnr, int), f"status eintragsnr is invalid: {row.eintragsnr}"
        assert row.job_kennung == 'BERT_DROP_TEMP_TABLE', f"status job_kennung mismatch: {row.job_kennung}"
        assert row.status in ['OK', 'ERROR'], f"Invalid status value: {row.status}"
        assert row.updated_at is not None, f"status updated_at is NULL"
```