The migration of `r_drop_temp_table.ksh` to BigQuery involves replacing a KornShell wrapper script with a BigQuery Stored Procedure (`k_drop_temp_table_wrapper`), which in turn calls another BigQuery Stored Procedure (`k_drop_temp_table_core`) that encapsulates the actual cleanup logic. Logging is transitioned from file-based to dedicated BigQuery tables (`job_audit_log`, `job_status_log`), and orchestration moves from UC4 to Airflow.

The following tests validate the behavioral equivalence of the migrated BigQuery solution, focusing on the `k_drop_temp_table_wrapper` procedure and its interaction with the logging tables and the placeholder `k_drop_temp_table_core`.

---

## Test Environment Setup

Before running the tests, ensure the following:

1.  **BigQuery Project and Dataset**: Replace `your-gcp-project-id` and `your_bigquery_dataset_id` with your actual GCP project ID and BigQuery dataset ID in the configuration.
2.  **BigQuery Tables and Stored Procedures**: The DDL for `job_audit_log`, `job_status_log`, and the stored procedures `k_drop_temp_table_core`, `k_drop_temp_table_wrapper` must be deployed to your BigQuery dataset.
3.  **Python Environment**: Install `pytest` and `google-cloud-bigquery`.
    ```bash
    pip install pytest google-cloud-bigquery
    ```
4.  **Authentication**: Ensure your environment is authenticated to GCP (e.g., `gcloud auth application-default login`).

```python
# test_r_drop_temp_table_migration.py
import pytest
from google.cloud import bigquery
import datetime
import uuid
import re

# --- Configuration ---
PROJECT_ID = "your-gcp-project-id"  # <<< REPLACE WITH YOUR GCP PROJECT ID
DATASET_ID = "your_bigquery_dataset_id" # <<< REPLACE WITH YOUR BIGQUERY DATASET ID
WRAPPER_SP_ID = "k_drop_temp_table_wrapper"
CORE_SP_ID = "k_drop_temp_table_core"
AUDIT_LOG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_audit_log"
STATUS_LOG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_status_log"

@pytest.fixture(scope="module")
def bq_client():
    """Provides a BigQuery client for tests."""
    return bigquery.Client(project=PROJECT_ID)

@pytest.fixture(autouse=True)
def cleanup_log_tables(bq_client):
    """Cleans up log tables before and after each test."""
    # Before test
    print(f"\nCleaning up {AUDIT_LOG_TABLE} and {STATUS_LOG_TABLE}...")
    bq_client.query(f"TRUNCATE TABLE `{AUDIT_LOG_TABLE}`").result()
    bq_client.query(f"TRUNCATE TABLE `{STATUS_LOG_TABLE}`").result()
    yield
    # After test (optional, uncomment if you want to inspect logs manually after a run)
    # print(f"Cleaning up {AUDIT_LOG_TABLE} and {STATUS_LOG_TABLE} after test...")
    # bq_client.query(f"TRUNCATE TABLE `{AUDIT_LOG_TABLE}`").result()
    # bq_client.query(f"TRUNCATE TABLE `{STATUS_LOG_TABLE}`").result()

def call_wrapper_sp(bq_client, p_stichtag_in, p_wiederanlauf_wert_in):
    """Helper to call the wrapper stored procedure."""
    stichtag_arg = f"'{p_stichtag_in}'" if p_stichtag_in is not None else "NULL"
    wiederanlauf_arg = str(p_wiederanlauf_wert_in) if p_wiederanlauf_wert_in is not None else "NULL"
    query = f"CALL `{PROJECT_ID}.{DATASET_ID}.{WRAPPER_SP_ID}`({stichtag_arg}, {wiederanlauf_arg});"
    print(f"Executing: {query}")
    return bq_client.query(query)

def get_all_log_entries(bq_client, table_name):
    """Helper to fetch all log entries from a table."""
    query = f"SELECT * FROM `{table_name}` ORDER BY created_at ASC;"
    return list(bq_client.query(query).result())

def assert_audit_log_entry_properties(entry, expected_level, expected_message_part, expected_stichtag, expected_restart_value):
    """Helper to assert properties of an audit log entry."""
    assert entry.log_level == expected_level, f"Expected log_level '{expected_level}', got '{entry.log_level}'"
    assert expected_message_part in entry.message, f"Expected message part '{expected_message_part}' not in '{entry.message}' (Full message: '{entry.message}')"
    assert entry.stichtag == expected_stichtag, f"Expected stichtag '{expected_stichtag}', got '{entry.stichtag}'"
    assert entry.restart_value == expected_restart_value, f"Expected restart_value '{expected_restart_value}', got '{entry.restart_value}'"
    assert entry.job_kennung == "BERT_DROP_TEMP_TABLE", f"Expected job_kennung 'BERT_DROP_TEMP_TABLE', got '{entry.job_kennung}'"
    assert entry.job_entry_nr is not None, "job_entry_nr should not be None"

def assert_status_log_entry_properties(entry, expected_status, expected_stichtag):
    """Helper to assert properties of a status log entry."""
    assert entry.status == expected_status, f"Expected status '{expected_status}', got '{entry.status}'"
    assert entry.stichtag == expected_stichtag, f"Expected stichtag '{expected_stichtag}', got '{entry.stichtag}'"
    assert entry.job_kennung == "BERT_DROP_TEMP_TABLE", f"Expected job_kennung 'BERT_DROP_TEMP_TABLE', got '{entry.job_kennung}'"
    assert entry.job_entry_nr is not None, "job_entry_nr should not be None"

```

---

### Test Case 1: Successful execution with all parameters provided

*   **Purpose**: Verify that the `k_drop_temp_table_wrapper` procedure correctly accepts and passes both `p_stichtag` and `p_wiederanlaufWert` to the core procedure, and logs the job's start, core call, and successful completion. This covers output parity and transformation correctness for parameter handling.
*   **Setup**:
    *   Ensure `job_audit_log` and `job_status_log` tables are empty (handled by `cleanup_log_tables` fixture).
    *   Define a specific `p_stichtag` ('01012023') and `p_wiederanlaufWert` (12345).
*   **Action**: Call `k_drop_temp_table_wrapper` with these specific parameters.
    ```python
    # In test_r_drop_temp_table_migration.py
    def test_successful_execution_with_all_params(bq_client):
        p_stichtag = '01012023'
        p_wiederanlauf_wert = 12345

        call_wrapper_sp(bq_client, p_stichtag, p_wiederanlauf_wert).result()

        audit_logs = get_all_log_entries(bq_client, AUDIT_LOG_TABLE)
        status_logs = get_all_log_entries(bq_client, STATUS_LOG_TABLE)

        # All logs should belong to the same job_entry_nr
        job_entry_nr = audit_logs[0].job_entry_nr
        assert all(log.job_entry_nr == job_entry_nr for log in audit_logs)
        assert all(log.job_entry_nr == job_entry_nr for log in status_logs)

        # Assert audit log entries
        assert len(audit_logs) == 4 # Start, Wrapper-Debug, Core-Info, Success
        assert_audit_log_entry_properties(audit_logs[0], 'INFO', 'Job BERT_DROP_TEMP_TABLE started', p_stichtag, p_wiederanlauf_wert)
        assert_audit_log_entry_properties(audit_logs[1], 'DEBUG', 'Calling core cleanup', p_stichtag, p_wiederanlauf_wert)
        assert_audit_log_entry_properties(audit_logs[2], 'INFO', 'Core script received parameters', p_stichtag, p_wiederanlauf_wert) # From k_drop_temp_table_core
        assert_audit_log_entry_properties(audit_logs[3], 'INFO', 'Job completed successfully.', p_stichtag, p_wiederanlauf_wert)

        # Assert status log entry
        assert len(status_logs) == 1
        assert_status_log_entry_properties(status_logs[0], 'OK', p_stichtag)
    ```
*   **Pass/Fail Criterion**:
    *   The `CALL` statement completes without error.
    *   `job_audit_log` contains exactly 4 entries for the same `job_entry_nr`:
        1.  `log_level='INFO'`, message indicating job start, `stichtag='01012023'`, `restart_value=12345`.
        2.  `log_level='DEBUG'`, message indicating core call, `stichtag='01012023'`, `restart_value=12345`.
        3.  `log_level='INFO'`, message from `k_drop_temp_table_core` confirming received parameters, `stichtag='01012023'`, `restart_value=12345`.
        4.  `log_level='INFO'`, message 'Job completed successfully.', `stichtag='01012023'`, `restart_value=12345`.
    *   `job_status_log` contains exactly 1 entry for the same `job_entry_nr`: `status='OK'`, `stichtag='01012023'`.

---

### Test Case 2: Successful execution with default `p_wiederanlaufWert`

*   **Purpose**: Verify that `p_wiederanlaufWert` correctly defaults to `0` when not provided, and the job executes successfully. This covers transformation correctness for NULL handling and defaulting logic.
*   **Setup**:
    *   Ensure `job_audit_log` and `job_status_log` tables are empty.
    *   Define a specific `p_stichtag` ('15062023').
*   **Action**: Call `k_drop_temp_table_wrapper` with `p_stichtag` and `NULL` for `p_wiederanlauf_wert_in`.
    ```python
    # In test_r_drop_temp_table_migration.py
    def test_successful_execution_default_wiederanlaufwert(bq_client):
        p_stichtag = '15062023'
        p_wiederanlauf_wert = None # Should default to 0

        call_wrapper_sp(bq_client, p_stichtag, p_wiederanlauf_wert).result()

        audit_logs = get_all_log_entries(bq_client, AUDIT_LOG_TABLE)
        status_logs = get_all_log_entries(bq_client, STATUS_LOG_TABLE)

        job_entry_nr = audit_logs[0].job_entry_nr

        # Assert audit log entries
        assert len(audit_logs) == 4
        assert_audit_log_entry_properties(audit_logs[0], 'INFO', 'Job BERT_DROP_TEMP_TABLE started', p_stichtag, 0)
        assert_audit_log_entry_properties(audit_logs[1], 'DEBUG', 'Calling core cleanup', p_stichtag, 0)
        assert_audit_log_entry_properties(audit_logs[2], 'INFO', 'Core script received parameters', p_stichtag, 0)
        assert_audit_log_entry_properties(audit_logs[3], 'INFO', 'Job completed successfully.', p_stichtag, 0)

        # Assert status log entry
        assert len(status_logs) == 1
        assert_status_log_entry_properties(status_logs[0], 'OK', p_stichtag)
    ```
*   **Pass/Fail Criterion**:
    *   The `CALL` statement completes without error.
    *   `job_audit_log` contains exactly 4 entries for the same `job_entry_nr`, with `restart_value=0` in all relevant entries.
    *   `job_status_log` contains exactly 1 entry for the same `job_entry_nr`, with `status='OK'` and the provided `p_stichtag`.

---

### Test Case 3: Successful execution with default `p_stichtag`

*   **Purpose**: Verify that `p_stichtag` correctly defaults to the current date (DDMMYYYY) when not provided, and the job executes successfully. This covers transformation correctness for NULL handling and defaulting logic.
*   **Setup**:
    *   Ensure `job_audit_log` and `job_status_log` tables are empty.
    *   Define a specific `p_wiederanlaufWert` (54321).
    *   Determine the current date in DDMMYYYY format for assertion.
*   **Action**: Call `k_drop_temp_table_wrapper` with `NULL` for `p_stichtag_in` and the specific `p_wiederanlauf_wert_in`.
    ```python
    # In test_r_drop_temp_table_migration.py
    def test_successful_execution_default_stichtag(bq_client):
        p_stichtag = None # Should default to current date DDMMYYYY
        p_wiederanlauf_wert = 54321
        expected_stichtag = datetime.datetime.now().strftime('%d%m%Y')

        call_wrapper_sp(bq_client, p_stichtag, p_wiederanlauf_wert).result()

        audit_logs = get_all_log_entries(bq_client, AUDIT_LOG_TABLE)
        status_logs = get_all_log_entries(bq_client, STATUS_LOG_TABLE)

        job_entry_nr = audit_logs[0].job_entry_nr

        # Assert audit log entries
        assert len(audit_logs) == 4
        assert_audit_log_entry_properties(audit_logs[0], 'INFO', 'Job BERT_DROP_TEMP_TABLE started', expected_stichtag, p_wiederanlauf_wert)
        assert_audit_log_entry_properties(audit_logs[1], 'DEBUG', 'Calling core cleanup', expected_stichtag, p_wiederanlauf_wert)
        assert_audit_log_entry_properties(audit_logs[2], 'INFO', 'Core script received parameters', expected_stichtag, p_wiederanlauf_wert)
        assert_audit_log_entry_properties(audit_logs[3], 'INFO', 'Job completed successfully.', expected_stichtag, p_wiederanlauf_wert)

        # Assert status log entry
        assert len(status_logs) == 1
        assert_status_log_entry_properties(status_logs[0], 'OK', expected_stichtag)
    ```
*   **Pass/Fail Criterion**:
    *   The `CALL` statement completes without error.
    *   `job_audit_log` contains exactly 4 entries for the same `job_entry_nr`, with `stichtag` matching the current date in DDMMYYYY format.
    *   `job_status_log` contains exactly 1 entry for the same `job_entry_nr`, with `status='OK'` and `stichtag` matching the current date.

---

### Test Case 4: Successful execution with both parameters defaulted

*   **Purpose**: Verify that both `p_stichtag` and `p_wiederanlaufWert` default correctly when neither is provided, and the job executes successfully. This covers comprehensive defaulting logic.
*   **Setup**:
    *   Ensure `job_audit_log` and `job_status_log` tables are empty.
    *   Determine the current date in DDMMYYYY format for assertion.
*   **Action**: Call `k_drop_temp_table_wrapper` with both parameters as `NULL`.
    ```python
    # In test_r_drop_temp_table_migration.py
    def test_successful_execution_both_params_default(bq_client):
        p_stichtag = None # Should default to current date DDMMYYYY
        p_wiederanlauf_wert = None # Should default to 0
        expected_stichtag = datetime.datetime.now().strftime('%d%m%Y')

        call_wrapper_sp(bq_client, p_stichtag, p_wiederanlauf_wert).result()

        audit_logs = get_all_log_entries(bq_client, AUDIT_LOG_TABLE)
        status_logs = get_all_log_entries(bq_client, STATUS_LOG_TABLE)

        job_entry_nr = audit_logs[0].job_entry_nr

        # Assert audit log entries
        assert len(audit_logs) == 4
        assert_audit_log_entry_properties(audit_logs[0], 'INFO', 'Job BERT_DROP_TEMP_TABLE started', expected_stichtag, 0)
        assert_audit_log_entry_properties(audit_logs[1], 'DEBUG', 'Calling core cleanup', expected_stichtag, 0)
        assert_audit_log_entry_properties(audit_logs[2], 'INFO', 'Core script received parameters', expected_stichtag, 0)
        assert_audit_log_entry_properties(audit_logs[3], 'INFO', 'Job completed successfully.', expected_stichtag, 0)

        # Assert status log entry
        assert len(status_logs) == 1
        assert_status_log_entry_properties(status_logs[0], 'OK', expected_stichtag)
    ```
*   **Pass/Fail Criterion**:
    *   The `CALL` statement completes without error.
    *   `job_audit_log` contains exactly 4 entries for the same `job_entry_nr`, with `stichtag` matching the current date in DDMMYYYY format and `restart_value=0`.
    *   `job_status_log` contains exactly 1 entry for the same `job_entry_nr`, with `status='OK'` and `stichtag` matching the current date.

---

### Test Case 5: Error handling during core script execution

*   **Purpose**: Verify that the wrapper's error handling (`BEGIN...EXCEPTION`) correctly catches errors from the core procedure, logs the failure, and updates the status table. This covers transformation correctness for error handling.
*   **Setup**:
    *   Ensure `job_audit_log` and `job_status_log` tables are empty.
    *   **Temporarily modify `k_drop_temp_table_core` to `RAISE` an error.**
        ```sql
        -- Re-deploy this version of k_drop_temp_table_core for the test
        CREATE OR REPLACE PROCEDURE `project.dataset.k_drop_temp_table_core`(
          p_job_kennung STRING,
          p_stichtag STRING,
          p_job_entry_nr INT64,
          p_wiederanlauf_wert INT64
        )
        BEGIN
          INSERT INTO `project.dataset.job_audit_log` (
            job_kennung, job_entry_nr, log_level, message, stichtag, restart_value, created_at
          )
          VALUES (
            p_job_kennung,
            p_job_entry_nr,
            'INFO',
            FORMAT(
              'Core script received parameters: stichtag=%s, wiederanlaufWert=%d. (Simulating error)',
              p_stichtag, p_wiederanlauf_wert
            ),
            p_stichtag,
            p_wiederanlauf_wert,
            CURRENT_TIMESTAMP()
          );
          RAISE USING MESSAGE 'Simulated error from core cleanup procedure.';
        END;
        ```
    *   Define `p_stichtag` ('01012024') and `p_wiederanlaufWert` (999).
*   **Action**: Call `k_drop_temp_table_wrapper` with the chosen parameters.
    ```python
    # In test_r_drop_temp_table_migration.py
    def test_error_handling_in_core_script(bq_client):
        p_stichtag = '01012024'
        p_wiederanlauf_wert = 999

        # Expect the call to fail
        with pytest.raises(Exception) as excinfo:
            call_wrapper_sp(bq_client, p_stichtag, p_wiederanlauf_wert).result()
        assert "Simulated error from core cleanup procedure." in str(excinfo.value)

        audit_logs = get_all_log_entries(bq_client, AUDIT_LOG_TABLE)
        status_logs = get_all_log_entries(bq_client, STATUS_LOG_TABLE)

        job_entry_nr = audit_logs[0].job_entry_nr

        # Assert audit log entries
        assert len(audit_logs) == 4 # Start, Wrapper-Debug, Core-Info (before raise), Error
        assert_audit_log_entry_properties(audit_logs[0], 'INFO', 'Job BERT_DROP_TEMP_TABLE started', p_stichtag, p_wiederanlauf_wert)
        assert_audit_log_entry_properties(audit_logs[1], 'DEBUG', 'Calling core cleanup', p_stichtag, p_wiederanlauf_wert)
        assert_audit_log_entry_properties(audit_logs[2], 'INFO', 'Core script received parameters', p_stichtag, p_wiederanlauf_wert)
        assert_audit_log_entry_properties(audit_logs[3], 'ERROR', 'Job failed with error:', p_stichtag, p_wiederanlauf_wert)
        assert "Simulated error from core cleanup procedure." in audit_logs[3].message

        # Assert status log entry
        assert len(status_logs) == 1
        assert_status_log_entry_properties(status_logs[0], 'FAILED', p_stichtag)

        # Cleanup: Revert k_drop_temp_table_core to its original placeholder state
        # This would typically be done in a separate setup/teardown for the test suite
        # or manually after this specific test run.
    ```
*   **Pass/Fail Criterion**:
    *   The `CALL` statement fails and propagates an error containing "Simulated error from core cleanup procedure.".
    *   `job_audit_log` contains exactly 4 entries for the same `job_entry_nr`:
        1.  `log_level='INFO'`, message indicating job start.
        2.  `log_level='DEBUG'`, message indicating core call.
        3.  `log_level='INFO'`, message from `k_drop_temp_table_core` (before it raises).
        4.  `log_level='ERROR'`, message containing 'Job failed with error:' and 'Simulated error from core cleanup procedure.'.
    *   `job_status_log` contains exactly 1 entry for the same `job_entry_nr`: `status='FAILED'`, `stichtag='01012024'`.
*   **Cleanup**: **Crucially, revert `k_drop_temp_table_core` to its original placeholder state after this test.**

---

### Test Case 6: Schema and data quality of log tables

*   **Purpose**: Verify that the `job_audit_log` and `job_status_log` tables adhere to their defined schemas and that data types and constraints are correctly handled. This covers data quality and schema assertions.
*   **Setup**:
    *   Run Test Case 1 (or any successful execution) to populate the log tables with valid data.
*   **Action**: Query the BigQuery `INFORMATION_SCHEMA` and the log tables directly.
    ```python
    # In test_r_drop_temp_table_migration.py
    def test_log_table_schema_and_data_quality(bq_client):
        # First, run a successful job to populate logs
        call_wrapper_sp(bq_client, '01012023', 12345).result()

        # --- Schema Assertions ---
        # Query INFORMATION_SCHEMA for job_audit_log
        audit_schema_query = f"""
            SELECT column_name, data_type, is_nullable
            FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = 'job_audit_log'
            ORDER BY ordinal_position;
        """
        audit_schema = {row.column_name: (row.data_type, row.is_nullable) for row in bq_client.query(audit_schema_query).result()}

        expected_audit_schema = {
            'job_kennung': ('STRING', 'NO'),
            'job_entry_nr': ('INT64', 'NO'),
            'log_level': ('STRING', 'NO'),
            'message': ('STRING', 'YES'),
            'stichtag': ('STRING', 'YES'),
            'restart_value': ('INT64', 'YES'),
            'created_at': ('TIMESTAMP', 'NO')
        }
        assert audit_schema == expected_audit_schema, "job_audit_log schema mismatch"

        # Query INFORMATION_SCHEMA for job_status_log
        status_schema_query = f"""
            SELECT column_name, data_type, is_nullable
            FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = 'job_status_log'
            ORDER BY ordinal_position;
        """
        status_schema = {row.column_name: (row.data_type, row.is_nullable) for row in bq_client.query(status_schema_query).result()}

        expected_status_schema = {
            'job_kennung': ('STRING', 'NO'),
            'job_entry_nr': ('INT64', 'NO'),
            'status': ('STRING', 'NO'),
            'stichtag': ('STRING', 'YES'),
            'created_at': ('TIMESTAMP', 'NO')
        }
        assert status_schema == expected_status_schema, "job_status_log schema mismatch"

        # --- Data Quality Assertions ---
        audit_dq_query = f"""
            SELECT
                COUNT(1) AS total_entries,
                COUNTIF(job_kennung = 'BERT_DROP_TEMP_TABLE') AS correct_job_kennung,
                COUNTIF(job_entry_nr IS NOT NULL AND job_entry_nr >= 0) AS valid_job_entry_nr,
                COUNTIF(log_level IN ('INFO', 'DEBUG', 'ERROR')) AS valid_log_level,
                COUNTIF(stichtag IS NULL OR REGEXP_CONTAINS(stichtag, r'^\\d{{8}}$')) AS valid_stichtag_format,
                COUNTIF(restart_value IS NULL OR restart_value >= 0) AS valid_restart_value,
                COUNTIF(created_at IS NOT NULL) AS valid_created_at
            FROM `{AUDIT_LOG_TABLE}`;
        """
        audit_dq_result = list(bq_client.query(audit_dq_query).result())[0]
        assert audit_dq_result.total_entries > 0
        assert audit_dq_result.total_entries == audit_dq_result.correct_job_kennung
        assert audit_dq_result.total_entries == audit_dq_result.valid_job_entry_nr
        assert audit_dq_result.total_entries == audit_dq_result.valid_log_level
        assert audit_dq_result.total_entries == audit_dq_result.valid_stichtag_format
        assert audit_dq_result.total_entries == audit_dq_result.valid_restart_value
        assert audit_dq_result.total_entries == audit_dq_result.valid_created_at

        status_dq_query = f"""
            SELECT
                COUNT(1) AS total_entries,
                COUNTIF(job_kennung = 'BERT_DROP_TEMP_TABLE') AS correct_job_kennung,
                COUNTIF(job_entry_nr IS NOT NULL AND job_entry_nr >= 0) AS valid_job_entry_nr,
                COUNTIF(status IN ('OK', 'FAILED')) AS valid_status,
                COUNTIF(stichtag IS NULL OR REGEXP_CONTAINS(stichtag, r'^\\d{{8}}$')) AS valid_stichtag_format,
                COUNTIF(created_at IS NOT NULL) AS valid_created_at
            FROM `{STATUS_LOG_TABLE}`;
        """
        status_dq_result = list(bq_client.query(status_dq_query).result())[0]
        assert status_dq_result.total_entries > 0
        assert status_dq_result.total_entries == status_dq_result.correct_job_kennung
        assert status_dq_result.total_entries == status_dq_result.valid_job_entry_nr
        assert status_dq_result.total_entries == status_dq_result.valid_status
        assert status_dq_result.total_entries == status_dq_result.valid_stichtag_format
        assert status_dq_result.total_entries == status_dq_result.valid_created_at
    ```
*   **Pass/Fail Criterion**:
    *   The `INFORMATION_SCHEMA` queries return the exact column names, data types, and nullability as defined in the DDL for both `job_audit_log` and `job_status_log`.
    *   The data quality queries confirm that:
        *   `job_kennung` is always 'BERT_DROP_TEMP_TABLE'.
        *   `job_entry_nr` is always a positive `INT64`.
        *   `log_level` is one of 'INFO', 'DEBUG', 'ERROR'.
        *   `stichtag` is either `NULL` or a `STRING` in 'DDMMYYYY' format.
        *   `restart_value` is either `NULL` or a non-negative `INT64`.
        *   `created_at` is a valid `TIMESTAMP` and not `NULL`.
        *   `status` in `job_status_log` is 'OK' or 'FAILED'.

---

### Test Case 7: Airflow DAG invocation

*   **Purpose**: Verify that the Airflow DAG correctly triggers the BigQuery stored procedure and handles parameter passing (or defaulting) as expected. This tests the external system replacement (UC4 -> Airflow).
*   **Setup**:
    *   Deploy the `dags/bert_drop_temp_table_dag.py` to an Airflow environment (e.g., Cloud Composer).
    *   Ensure `job_audit_log` and `job_status_log` tables are empty before each run.
*   **Action**:
    1.  **Trigger the Airflow DAG manually without any `conf` parameters.** This tests the full defaulting behavior.
    2.  **Trigger the Airflow DAG with `conf` parameters:** `{"p_stichtag_in": "01012025", "p_wiederanlauf_wert_in": 789}`. This tests explicit parameter passing.
*   **Pass/Fail Criterion**:
    *   **For Action 1 (no parameters):**
        *   The Airflow task `call_k_drop_temp_table_wrapper` completes successfully.
        *   Query `job_audit_log` and `job_status_log` in BigQuery. The entries should be consistent with Test Case 4 (both parameters defaulted: `stichtag` = current date DDMMYYYY, `restart_value` = 0).
    *   **For Action 2 (with parameters):**
        *   The Airflow task `call_k_drop_temp_table_wrapper` completes successfully.
        *   Query `job_audit_log` and `job_status_log` in BigQuery. The entries should be consistent with Test Case 1 (`stichtag='01012025'`, `restart_value=789`).
    *   Airflow task logs should not show any errors related to BigQuery connection or procedure invocation.

---