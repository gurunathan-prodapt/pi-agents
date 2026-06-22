The migration of `r_ausd_v_ta_cntrct_crs.ksh` to BigQuery Stored Procedures and Cloud Composer requires comprehensive validation to ensure behavioral equivalence. The tests below focus on the wrapper logic, parameter handling, logging, and error management, as the core business logic (`sp_ausd_v_ta_cntrct_crs`) is currently a placeholder.

We will use `pytest` for test orchestration and `google-cloud-bigquery` for interacting with BigQuery.

**Prerequisites:**
1.  A Google Cloud Project and BigQuery Dataset (e.g., `my_project.my_dataset`).
2.  The `bq_ddl_job_audit_log.sql`, `sp_vertragsdatenabgleich.sql`, and `sp_ausd_v_ta_cntrct_crs.sql` files have been deployed to your BigQuery environment.
3.  Python environment with `pytest` and `google-cloud-bigquery` installed.
4.  Authentication configured for BigQuery (e.g., `gcloud auth application-default login`).

**Helper Functions and Pytest Fixtures (Conceptual):**

```python
# conftest.py or a test_utils.py file

import pytest
from google.cloud import bigquery
import uuid
import re

PROJECT_ID = "my_project"  # Replace with your GCP Project ID
DATASET_ID = "my_dataset"  # Replace with your BigQuery Dataset ID
AUDIT_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.job_audit_log"
WRAPPER_SP_ID = f"{PROJECT_ID}.{DATASET_ID}.sp_vertragsdatenabgleich"
CORE_SP_ID = f"{PROJECT_ID}.{DATASET_ID}.sp_ausd_v_ta_cntrct_crs"

@pytest.fixture(scope="session")
def bq_client():
    """Provides a BigQuery client for the test session."""
    return bigquery.Client(project=PROJECT_ID)

@pytest.fixture(autouse=True)
def cleanup_audit_log(bq_client):
    """Cleans up the job_audit_log table before each test."""
    bq_client.query(f"TRUNCATE TABLE `{AUDIT_TABLE_ID}`").result()
    yield
    bq_client.query(f"TRUNCATE TABLE `{AUDIT_TABLE_ID}`").result()

def execute_wrapper_sp(bq_client, p_s: str = None, p_l: str = None):
    """Executes the sp_vertragsdatenabgleich stored procedure."""
    params = []
    if p_s is not None:
        params.append(f"p_s => '{p_s}'")
    else:
        params.append(f"p_s => NULL") # Explicitly pass NULL if not provided

    if p_l is not None:
        params.append(f"p_l => '{p_l}'")
    else:
        params.append(f"p_l => NULL") # Explicitly pass NULL if not provided

    query = f"CALL `{WRAPPER_SP_ID}`({', '.join(params)});"
    print(f"Executing: {query}")
    try:
        job = bq_client.query(query)
        job.result()  # Wait for the job to complete
        return True, None
    except Exception as e:
        return False, str(e)

def get_audit_log_entries(bq_client):
    """Retrieves all entries from the job_audit_log table."""
    query = f"SELECT * FROM `{AUDIT_TABLE_ID}` ORDER BY start_ts ASC;"
    rows = bq_client.query(query).result()
    return [dict(row) for row in rows]

def update_core_sp_to_raise_error(bq_client, error_message: str):
    """Temporarily modifies the core SP to raise an error."""
    ddl = f"""
    CREATE OR REPLACE PROCEDURE `{CORE_SP_ID}`(
      IN p_job_id STRING,
      IN p_stichtag STRING,
      IN p_laufnummer STRING
    )
    BEGIN
      RAISE USING MESSAGE = '{error_message}';
    END;
    """
    bq_client.query(ddl).result()

def restore_core_sp_placeholder(bq_client):
    """Restores the core SP to its original placeholder state."""
    ddl = f"""
    CREATE OR REPLACE PROCEDURE `{CORE_SP_ID}`(
      IN p_job_id STRING,
      IN p_stichtag STRING,
      IN p_laufnummer STRING
    )
    BEGIN
      -- Placeholder logic
    END;
    """
    bq_client.query(ddl).result()

```

---

## 1. Test Case: `job_audit_log` Schema Validation

*   **Purpose:** Verify that the `job_audit_log` table exists and its schema matches the design document's specification. This ensures the foundation for logging and auditing is correctly laid out.
*   **Setup:** The `bq_ddl_job_audit_log.sql` script must have been executed, creating the `job_audit_log` table in the target BigQuery dataset.
*   **Action:** Query BigQuery's `INFORMATION_SCHEMA.COLUMNS` view for the `job_audit_log` table.
*   **Pass/Fail Criterion:** The query returns exactly 10 columns with the specified names and data types.

```python
# test_migration.py
import pytest
from google.cloud import bigquery
from conftest import PROJECT_ID, DATASET_ID, AUDIT_TABLE_ID, bq_client

def test_audit_log_schema(bq_client: bigquery.Client):
    """Verifies the schema of the job_audit_log table."""
    expected_schema = {
        "job_id": "STRING",
        "job_name": "STRING",
        "script_name": "STRING",
        "status": "STRING",
        "message": "STRING",
        "start_ts": "TIMESTAMP",
        "end_ts": "TIMESTAMP",
        "run_date": "DATE",
        "error_code": "INT64",
        "error_detail": "STRING",
    }

    query = f"""
        SELECT column_name, data_type
        FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'job_audit_log'
        ORDER BY ordinal_position;
    """
    rows = bq_client.query(query).result()
    actual_schema = {row.column_name: row.data_type for row in rows}

    assert len(actual_schema) == len(expected_schema), "Number of columns mismatch"
    for col_name, data_type in expected_schema.items():
        assert col_name in actual_schema, f"Missing column: {col_name}"
        assert actual_schema[col_name] == data_type, \
            f"Data type mismatch for column {col_name}: Expected {data_type}, Got {actual_schema[col_name]}"
```

---

## 2. Test Case: Successful Execution (Happy Path)

*   **Purpose:** Verify that the migrated wrapper stored procedure executes successfully with valid inputs, logs the job's lifecycle (start and success), and correctly calls the core reconciliation procedure. This covers output parity for successful runs and basic transformation correctness for logging.
*   **Setup:** The `sp_vertragsdatenabgleich` and `sp_ausd_v_ta_cntrct_crs` (placeholder) procedures are deployed. The `job_audit_log` table is empty.
*   **Action:** Call `sp_vertragsdatenabgleich` with valid `p_s` (Stichtag) and `p_l` (Laufnummer) parameters.
*   **Pass/Fail Criterion:**
    *   The wrapper SP completes without raising an error.
    *   The `job_audit_log` table contains exactly two entries for the same `job_id`.
    *   The first entry (`RUNNING` status) has `end_ts` as `NULL`.
    *   The second entry (`SUCCESS` status) has `end_ts` populated and greater than `start_ts`.
    *   `job_id` is a valid UUID.
    *   `run_date` is correctly derived from `p_s`.
    *   `message` and `status` fields match the expected success messages.

```python
# test_migration.py
import pytest
from google.cloud import bigquery
from conftest import execute_wrapper_sp, get_audit_log_entries
import re

def test_successful_execution(bq_client: bigquery.Client):
    """Tests the successful execution of the wrapper SP."""
    stichtag = "2023-01-15"
    laufnummer = "JOB123"

    success, error_message = execute_wrapper_sp(bq_client, p_s=stichtag, p_l=laufnummer)

    assert success, f"Wrapper SP failed unexpectedly: {error_message}"

    entries = get_audit_log_entries(bq_client)
    assert len(entries) == 2, f"Expected 2 audit log entries, got {len(entries)}"

    # Entry 1: RUNNING status
    running_entry = entries[0]
    assert re.match(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$', running_entry['job_id'])
    assert running_entry['job_name'] == 'r_ausd_v_ta_cntrct_crs'
    assert running_entry['script_name'] == 'sp_vertragsdatenabgleich'
    assert running_entry['status'] == 'RUNNING'
    assert running_entry['message'] == 'Job started.'
    assert running_entry['start_ts'] is not None
    assert running_entry['end_ts'] is None
    assert str(running_entry['run_date']) == stichtag
    assert running_entry['error_code'] is None
    assert running_entry['error_detail'] is None

    # Entry 2: SUCCESS status
    success_entry = entries[1]
    assert success_entry['job_id'] == running_entry['job_id'] # Same job_id
    assert success_entry['status'] == 'SUCCESS'
    assert success_entry['message'] == 'Die Abarbeitung wurde ohne erkennbare Fehler beendet.'
    assert success_entry['start_ts'] is not None
    assert success_entry['end_ts'] is not None
    assert success_entry['end_ts'] > success_entry['start_ts']
    assert str(success_entry['run_date']) == stichtag
    assert success_entry['error_code'] is None
    assert success_entry['error_detail'] is None
```

---

## 3. Test Case: Missing Parameter (`p_s` or `p_l`)

*   **Purpose:** Verify that the wrapper SP correctly handles missing required parameters, mirroring the legacy script's `getopts` error code `193` and corresponding error message. This tests transformation correctness for parameter validation and error handling.
*   **Setup:** The `sp_vertragsdatenabgleich` procedure is deployed. The `job_audit_log` table is empty.
*   **Action:**
    1.  Call `sp_vertragsdatenabgleich` with `p_s` as `NULL`.
    2.  Call `sp_vertragsdatenabgleich` with `p_l` as `NULL`.
*   **Pass/Fail Criterion:**
    *   Both calls to the wrapper SP raise an error with the message 'ERROR: Missing required argument (-s or -l).'.
    *   For each failed call, the `job_audit_log` contains exactly one entry.
    *   This entry has `status='FAILED'`, `message='ERROR: Missing required argument (-s or -l).'`, and `error_code=193`.
    *   `start_ts` and `end_ts` are populated.

```python
# test_migration.py
import pytest
from google.cloud import bigquery
from conftest import execute_wrapper_sp, get_audit_log_entries
import re

def test_missing_parameter_s(bq_client: bigquery.Client):
    """Tests wrapper SP behavior when p_s is missing."""
    success, error_message = execute_wrapper_sp(bq_client, p_s=None, p_l="JOB123")

    assert not success, "Wrapper SP succeeded unexpectedly with missing p_s"
    assert "ERROR: Missing required argument (-s or -l)." in error_message

    entries = get_audit_log_entries(bq_client)
    assert len(entries) == 1, f"Expected 1 audit log entry, got {len(entries)}"

    failed_entry = entries[0]
    assert failed_entry['status'] == 'FAILED'
    assert failed_entry['message'] == 'ERROR: Missing required argument (-s or -l).'
    assert failed_entry['error_code'] == 193
    assert failed_entry['start_ts'] is not None
    assert failed_entry['end_ts'] is not None
    assert failed_entry['run_date'] is None # run_date cannot be parsed if p_s is NULL

def test_missing_parameter_l(bq_client: bigquery.Client):
    """Tests wrapper SP behavior when p_l is missing."""
    success, error_message = execute_wrapper_sp(bq_client, p_s="2023-01-15", p_l=None)

    assert not success, "Wrapper SP succeeded unexpectedly with missing p_l"
    assert "ERROR: Missing required argument (-s or -l)." in error_message

    entries = get_audit_log_entries(bq_client)
    assert len(entries) == 1, f"Expected 1 audit log entry, got {len(entries)}"

    failed_entry = entries[0]
    assert failed_entry['status'] == 'FAILED'
    assert failed_entry['message'] == 'ERROR: Missing required argument (-s or -l).'
    assert failed_entry['error_code'] == 193
    assert failed_entry['start_ts'] is not None
    assert failed_entry['end_ts'] is not None
    assert str(failed_entry['run_date']) == "2023-01-15" # p_s was provided and valid
```

---

## 4. Test Case: Invalid Date Format for `p_s`

*   **Purpose:** Verify that the wrapper SP correctly handles an invalid date format for the `p_s` parameter, mapping to the legacy script's `getopts` error code `192` (unknown parameter) for format issues. This tests type handling and error propagation.
*   **Setup:** The `sp_vertragsdatenabgleich` procedure is deployed. The `job_audit_log` table is empty.
*   **Action:** Call `sp_vertragsdatenabgleich` with `p_s` in an incorrect date format (e.g., `YYYY/MM/DD`).
*   **Pass/Fail Criterion:**
    *   The wrapper SP raises an error with the message 'ERROR: Invalid date format for -s parameter. Expected YYYY-MM-DD.'.
    *   The `job_audit_log` contains exactly one entry.
    *   This entry has `status='FAILED'`, `message='ERROR: Invalid date format for -s parameter. Expected YYYY-MM-DD.'`, and `error_code=192`.
    *   `start_ts` and `end_ts` are populated. `run_date` is `NULL`.

```python
# test_migration.py
import pytest
from google.cloud import bigquery
from conftest import execute_wrapper_sp, get_audit_log_entries

def test_invalid_date_format_s(bq_client: bigquery.Client):
    """Tests wrapper SP behavior with invalid date format for p_s."""
    invalid_stichtag = "2023/01/15" # Incorrect format
    laufnummer = "JOB123"

    success, error_message = execute_wrapper_sp(bq_client, p_s=invalid_stichtag, p_l=laufnummer)

    assert not success, "Wrapper SP succeeded unexpectedly with invalid p_s format"
    assert "ERROR: Invalid date format for -s parameter. Expected YYYY-MM-DD." in error_message

    entries = get_audit_log_entries(bq_client)
    assert len(entries) == 1, f"Expected 1 audit log entry, got {len(entries)}"

    failed_entry = entries[0]
    assert failed_entry['status'] == 'FAILED'
    assert failed_entry['message'] == 'ERROR: Invalid date format for -s parameter. Expected YYYY-MM-DD.'
    assert failed_entry['error_code'] == 192
    assert failed_entry['start_ts'] is not None
    assert failed_entry['end_ts'] is not None
    assert failed_entry['run_date'] is None # Should be NULL as parsing failed
```

---

## 5. Test Case: Core Script Failure

*   **Purpose:** Verify that the wrapper SP correctly catches and logs errors originating from the called core reconciliation procedure (`sp_ausd_v_ta_cntrct_crs`), mimicking the legacy script's `ERR` trap and `DWMSG_Fehlerbehandlung`. This tests external-system replacements (error handling framework) and transformation correctness for error propagation.
*   **Setup:**
    1.  Temporarily modify `sp_ausd_v_ta_cntrct_crs` to `RAISE` an error.
    2.  The `sp_vertragsdatenabgleich` procedure is deployed. The `job_audit_log` table is empty.
*   **Action:** Call `sp_vertragsdatenabgleich` with valid `p_s` and `p_l` parameters.
*   **Pass/Fail Criterion:**
    *   The wrapper SP raises an error with a message indicating core procedure failure and including the `error_detail` from the core SP.
    *   The `job_audit_log` contains exactly two entries for the same `job_id`.
    *   The first entry (`RUNNING` status) is as expected.
    *   The second entry (`FAILED` status) has `message='AppError: Abbruch. Core procedure failed.'`, `error_code=1` (generic for core SP failure), and `error_detail` matching the error raised by the core SP.
    *   `end_ts` is populated for the `FAILED` entry.
*   **Cleanup:** Restore `sp_ausd_v_ta_cntrct_crs` to its placeholder state after the test.

```python
# test_migration.py
import pytest
from google.cloud import bigquery
from conftest import execute_wrapper_sp, get_audit_log_entries, \
                     update_core_sp_to_raise_error, restore_core_sp_placeholder
import re

def test_core_script_failure(bq_client: bigquery.Client):
    """Tests wrapper SP behavior when the core SP fails."""
    stichtag = "2023-01-15"
    laufnummer = "JOB123"
    core_error_msg = "Simulated core script error: Division by zero."

    # Setup: Modify core SP to raise an error
    update_core_sp_to_raise_error(bq_client, core_error_msg)

    try:
        success, error_message = execute_wrapper_sp(bq_client, p_s=stichtag, p_l=laufnummer)

        assert not success, "Wrapper SP succeeded unexpectedly when core SP should fail"
        assert "AppError: Abbruch. Core procedure failed." in error_message
        assert core_error_msg in error_message

        entries = get_audit_log_entries(bq_client)
        assert len(entries) == 2, f"Expected 2 audit log entries, got {len(entries)}"

        # Entry 1: RUNNING status
        running_entry = entries[0]
        assert running_entry['status'] == 'RUNNING'
        assert running_entry['message'] == 'Job started.'
        assert running_entry['end_ts'] is None

        # Entry 2: FAILED status
        failed_entry = entries[1]
        assert failed_entry['job_id'] == running_entry['job_id']
        assert failed_entry['status'] == 'FAILED'
        assert failed_entry['message'] == 'AppError: Abbruch. Core procedure failed.'
        assert failed_entry['error_code'] == 1 # Generic error code for core SP failure
        assert failed_entry['error_detail'] == core_error_msg
        assert failed_entry['start_ts'] is not None
        assert failed_entry['end_ts'] is not None
        assert failed_entry['end_ts'] > failed_entry['start_ts']
        assert str(failed_entry['run_date']) == stichtag

    finally:
        # Cleanup: Restore core SP
        restore_core_sp_placeholder(bq_client)
```

---

## 6. Test Case: Data Quality - `job_id` and Timestamps

*   **Purpose:** Verify the data quality of generated `job_id`s (UUID format) and the accuracy of `start_ts`, `end_ts`, and `run_date` fields in the `job_audit_log`. This covers data quality assertions.
*   **Setup:** The `sp_vertragsdatenabgleich` procedure is deployed. The `job_audit_log` table is empty.
*   **Action:** Execute `sp_vertragsdatenabgleich` successfully multiple times with different `p_s` values.
*   **Pass/Fail Criterion:**
    *   Each `job_id` is a unique, valid UUID.
    *   `start_ts` and `end_ts` are valid `TIMESTAMP` values, and `end_ts` is always greater than `start_ts` for completed jobs.
    *   `run_date` is correctly parsed and stored as a `DATE` type, matching the input `p_s`.

```python
# test_migration.py
import pytest
from google.cloud import bigquery
from conftest import execute_wrapper_sp, get_audit_log_entries
import re
from datetime import date

def test_data_quality_job_id_and_timestamps(bq_client: bigquery.Client):
    """Verifies data quality for job_id, timestamps, and run_date."""
    test_runs = [
        ("2023-01-01", "RUN001"),
        ("2023-02-28", "RUN002"),
        ("2024-03-01", "RUN003"), # Leap year test
    ]

    for stichtag, laufnummer in test_runs:
        success, error_message = execute_wrapper_sp(bq_client, p_s=stichtag, p_l=laufnummer)
        assert success, f"Run for {stichtag} failed: {error_message}"

    entries = get_audit_log_entries(bq_client)
    assert len(entries) == len(test_runs) * 2, f"Expected {len(test_runs)*2} entries, got {len(entries)}"

    processed_job_ids = set()
    for i in range(0, len(entries), 2):
        running_entry = entries[i]
        success_entry = entries[i+1]

        # Check job_id uniqueness and format
        job_id = running_entry['job_id']
        assert job_id not in processed_job_ids, f"Duplicate job_id found: {job_id}"
        processed_job_ids.add(job_id)
        assert re.match(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$', job_id), \
            f"Invalid UUID format for job_id: {job_id}"
        assert success_entry['job_id'] == job_id

        # Check timestamps
        assert running_entry['start_ts'] is not None
        assert running_entry['end_ts'] is None
        assert success_entry['start_ts'] == running_entry['start_ts']
        assert success_entry['end_ts'] is not None
        assert success_entry['end_ts'] > success_entry['start_ts']

        # Check run_date
        expected_run_date = date.fromisoformat(test_runs[i//2][0])
        assert running_entry['run_date'] == expected_run_date
        assert success_entry['run_date'] == expected_run_date
```