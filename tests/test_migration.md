As a senior data-migration QA engineer, I have analyzed the migration design document and the provided BigQuery code for `r_ausd_v_ta_p_vertrag.ksh`. The following test cases are designed to validate the behavioral equivalence of the migrated BigQuery stored procedure (`sp_vertragsdatenabgleich_wrapper`) against the legacy KornShell script.

These tests cover output parity, transformation correctness, external-system replacements, and data quality/schema assertions. They are presented using a `pytest` framework with BigQuery client interactions, which is a common approach for testing BigQuery stored procedures.

---

**Assumptions for Test Execution:**

*   A Google Cloud Project and BigQuery dataset (`project.dataset`) are configured.
*   The DDL for `project.dataset.job_control`, `project.dataset.job_runtime_log`, and `project.dataset.job_error_log` has been executed.
*   The helper stored procedure `project.dataset.sp_log_runtime_message` is deployed.
*   The core processing stored procedure `project.dataset.sp_k_ausd_v_ta_p_vertrag` is deployed (even if it's the placeholder version).
*   The `pytest` framework is set up with `google-cloud-bigquery` library.
*   A `conftest.py` file (or similar setup) provides the `bq_client` fixture and helper functions for executing stored procedures and querying tables.

**`conftest.py` Example (for context):**

```python
# conftest.py
import pytest
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
from datetime import date

# --- Configuration ---
PROJECT_ID = "your-gcp-project-id" # Replace with your GCP project ID
DATASET_ID = "dataset"             # Replace with your BigQuery dataset ID

# --- Fixtures ---
@pytest.fixture(scope="session")
def bq_client():
    """Provides a BigQuery client for the test session."""
    return bigquery.Client(project=PROJECT_ID)

@pytest.fixture(scope="function", autouse=True)
def cleanup_tables(bq_client):
    """Cleans up logging tables before each test to ensure isolation."""
    tables = ["job_control", "job_runtime_log", "job_error_log"]
    for table_id in tables:
        try:
            bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.{table_id}`").result()
        except GoogleCloudError as e:
            # Handle cases where table might not exist yet, or other transient errors
            print(f"Warning: Could not truncate table {table_id}: {e}")
    yield

# --- Helper Functions ---
def execute_sp(bq_client, sp_name, *args):
    """Helper to execute a BigQuery stored procedure."""
    # Format arguments for SQL CALL statement
    formatted_args = []
    for arg in args:
        if arg is None or arg == 'NULL':
            formatted_args.append('NULL')
        elif isinstance(arg, str):
            formatted_args.append(f"'{arg}'")
        elif isinstance(arg, date):
            formatted_args.append(f"'{arg.isoformat()}'")
        else:
            formatted_args.append(str(arg))
    arg_str = ", ".join(formatted_args)
    
    query = f"CALL `{PROJECT_ID}.{DATASET_ID}.{sp_name}`({arg_str})"
    print(f"\nExecuting: {query}")
    return bq_client.query(query).result()

def query_table(bq_client, table_name, order_by=None):
    """Helper to query a BigQuery table and return rows as a list of Row objects."""
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`"
    if order_by:
        query += f" ORDER BY {order_by}"
    return list(bq_client.query(query).result())

def get_runtime_logs(bq_client, eintragsnr, job_kennung):
    """Helper to get runtime logs for a specific job."""
    query = f"""
        SELECT log_level, message FROM `{PROJECT_ID}.{DATASET_ID}.job_runtime_log`
        WHERE eintragsnr = {eintragsnr} AND job_kennung = '{job_kennung}'
        ORDER BY log_ts
    """
    return list(bq_client.query(query).result())

def get_table_schema(bq_client, table_id):
    """Helper to get schema details for a BigQuery table."""
    table_ref = bq_client.dataset(DATASET_ID).table(table_id)
    table = bq_client.get_table(table_ref)
    return {field.name: {'field_type': field.field_type, 'mode': field.mode} for field in table.schema}

# --- Helper for simulating core SP failure ---
def deploy_failing_core_sp(bq_client):
    """Deploys a version of sp_k_ausd_v_ta_p_vertrag that always fails."""
    failing_sp_sql = f"""
    CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.sp_k_ausd_v_ta_p_vertrag`(
        IN p_jobkennung STRING,
        IN p_eintragsnr INT64,
        IN p_stichtag DATE
    )
    OPTIONS(
        description="Placeholder for the core contract data reconciliation logic, migrating k_ausd_v_ta_p_vertrag.ksh"
    )
    BEGIN
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.job_runtime_log` (eintragsnr, job_kennung, log_level, message, log_ts)
        VALUES (
            p_eintragsnr,
            p_jobkennung,
            'INFO',
            FORMAT("Executing core logic for job_kennung=%s, eintragsnr=%d, stichtag=%t (SIMULATING FAILURE)", p_jobkennung, p_eintragsnr, p_stichtag),
            CURRENT_TIMESTAMP()
        );
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated failure in core logic';
    END;
    """
    bq_client.query(failing_sp_sql).result()
    print(f"Deployed failing sp_k_ausd_v_ta_p_vertrag to {PROJECT_ID}.{DATASET_ID}")

def deploy_original_core_sp(bq_client):
    """Deploys the original placeholder version of sp_k_ausd_v_ta_p_vertrag."""
    original_sp_sql = f"""
    CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.sp_k_ausd_v_ta_p_vertrag`(
        IN p_jobkennung STRING,
        IN p_eintragsnr INT64,
        IN p_stichtag DATE
    )
    OPTIONS(
        description="Placeholder for the core contract data reconciliation logic, migrating k_ausd_v_ta_p_vertrag.ksh"
    )
    BEGIN
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.job_runtime_log` (eintragsnr, job_kennung, log_level, message, log_ts)
        VALUES (
            p_eintragsnr,
            p_jobkennung,
            'INFO',
            FORMAT("Executing core logic for job_kennung=%s, eintragsnr=%d, stichtag=%t", p_jobkennung, p_eintragsnr, p_stichtag),
            CURRENT_TIMESTAMP()
        );
    END;
    """
    bq_client.query(original_sp_sql).result()
    print(f"Deployed original sp_k_ausd_v_ta_p_vertrag to {PROJECT_ID}.{DATASET_ID}")

```

---

### Test Case 1: Happy Path Execution - Output Parity & Transformation Correctness

*   **Purpose:** Verify that the migrated wrapper script executes successfully, logs all expected messages, updates job status correctly, and calls the core processing script with the correct parameters, mirroring the successful execution of the legacy script. This covers output parity (logging) and transformation correctness (flow, variable handling).
*   **Setup:**
    *   Ensure `job_control`, `job_runtime_log`, `job_error_log` tables are empty.
    *   The `sp_k_ausd_v_ta_p_vertrag` placeholder procedure is deployed and configured not to fail.
*   **Action:**
    *   Call `project.dataset.sp_vertragsdatenabgleich_wrapper` with a specific `job_kennung`, `stichtag`, and `log_file_base_name`.
    *   Example: `CALL project.dataset.sp_vertragsdatenabgleich_wrapper('BERT_V_TA_P_VERTRAG', '2023-01-15', 'r_ausd_v_ta_p_vertrag')`
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   The `sp_vertragsdatenabgleich_wrapper` procedure completes without raising an error.
        *   Exactly one entry exists in `project.dataset.job_control` for the given `job_kennung` and `eintragsnr`.
        *   The `job_control` entry has `status = 'OK'`, `script_name` matches (`'r_ausd_v_ta_p_vertrag.ksh'`), `created_ts` and `finished_ts` are populated, and `stichtag_info` reflects the input `p_stichtag` (`'Stichtag=2023-01-15'`).
        *   `project.dataset.job_runtime_log` contains at least 4-5 informational messages, including "Started...", "Stichtag parameter set...", "Executing core logic...", and "Successfully completed...". All log entries should have `log_level = 'INFO'`.
        *   `project.dataset.job_error_log` is empty.
        *   The `eintragsnr` generated is 1 (assuming empty `job_control` initially).

```python
# test_wrapper_happy_path.py
import pytest
from datetime import date

def test_happy_path_execution(bq_client, execute_sp, query_table, get_runtime_logs):
    job_kennung = 'BERT_V_TA_P_VERTRAG'
    stichtag = date(2023, 1, 15)
    log_file_base_name = 'r_ausd_v_ta_p_vertrag'

    # Action
    execute_sp(bq_client, 'sp_vertragsdatenabgleich_wrapper', job_kennung, stichtag, log_file_base_name)

    # Assertions
    job_control_entries = query_table(bq_client, 'job_control')
    assert len(job_control_entries) == 1, "Expected exactly one job_control entry"

    job_entry = job_control_entries[0]
    assert job_entry.eintragsnr == 1, "Expected eintragsnr to be 1 for the first run"
    assert job_entry.job_kennung == job_kennung
    assert job_entry.script_name == 'r_ausd_v_ta_p_vertrag.ksh'
    assert job_entry.status == 'OK'
    assert job_entry.created_ts is not None
    assert job_entry.finished_ts is not None
    assert job_entry.stichtag_info == f'Stichtag={stichtag.isoformat()}'
    assert job_entry.log_name.startswith(f'{log_file_base_name}_{stichtag.strftime("%Y%m%d")}_1.log')

    runtime_logs = get_runtime_logs(bq_client, job_entry.eintragsnr, job_kennung)
    log_messages = [row.message for row in runtime_logs]
    log_levels = [row.log_level for row in runtime_logs]

    assert all(level == 'INFO' for level in log_levels), "Expected all log messages to be INFO level"
    assert any(f"Started r_ausd_v_ta_p_vertrag.ksh version 1.0 for job_kennung={job_kennung} (EintragsNr: 1)" in msg for msg in log_messages)
    assert any(f"Stichtag parameter set: Stichtag={stichtag.isoformat()}" in msg for msg in log_messages)
    assert any(f"Executing core logic for job_kennung={job_kennung}, eintragsnr=1, stichtag={stichtag.isoformat()}" in msg for msg in log_messages)
    assert any(f"Successfully completed r_ausd_v_ta_p_vertrag.ksh for job_kennung={job_kennung}" in msg for msg in log_messages)

    error_logs = query_table(bq_client, 'job_error_log')
    assert len(error_logs) == 0, "Expected no error logs on happy path"

```

---

### Test Case 2: Core Script Failure - Error Handling & External System Replacement

*   **Purpose:** Verify that if the core processing script (`sp_k_ausd_v_ta_p_vertrag`) fails, the wrapper script correctly catches the error, logs it to `job_error_log` and `job_runtime_log`, and updates the `job_control` status to 'ERROR', mimicking the legacy script's `trap ERR` behavior and `DWMSG_Fehlerbehandlung`. This covers error handling (transformation correctness) and external system replacement (DWMSG functions).
*   **Setup:**
    *   Ensure tables are empty.
    *   Temporarily modify `sp_k_ausd_v_ta_p_vertrag` to simulate a failure (e.g., by adding a `SIGNAL SQLSTATE` statement).
*   **Action:**
    *   Call `project.dataset.sp_vertragsdatenabgleich_wrapper` with parameters. The call is expected to raise an error.
    *   Example: `CALL project.dataset.sp_vertragsdatenabgleich_wrapper('BERT_V_TA_P_VERTRAG_FAIL', '2023-01-16', 'r_ausd_v_ta_p_vertrag')`
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   The `sp_vertragsdatenabgleich_wrapper` procedure raises an error (i.e., the `CALL` statement fails in the test runner).
        *   Exactly one entry exists in `project.dataset.job_control` for the given `job_kennung` and `eintragsnr`.
        *   The `job_control` entry has `status = 'ERROR'`, `script_name` matches, `created_ts` and `finished_ts` are populated.
        *   `project.dataset.job_runtime_log` contains messages up to the point of failure, including an 'ERROR' level message about the failure.
        *   `project.dataset.job_error_log` contains exactly one entry for the failed job, with `error_nr = -1` (generic error for BigQuery exceptions), `error_arg` containing the error message, and `source_proc = 'sp_vertragsdatenabgleich_wrapper'`.

```python
# test_wrapper_core_script_failure.py
import pytest
from datetime import date
from google.cloud.exceptions import GoogleCloudError

def test_core_script_failure(bq_client, execute_sp, query_table, get_runtime_logs, deploy_failing_core_sp, deploy_original_core_sp):
    job_kennung = 'BERT_V_TA_P_VERTRAG_FAIL'
    stichtag = date(2023, 1, 16)
    log_file_base_name = 'r_ausd_v_ta_p_vertrag'

    # Setup: Deploy failing core SP
    deploy_failing_core_sp(bq_client)

    try:
        # Action: Expecting the wrapper to raise an error
        with pytest.raises(GoogleCloudError) as excinfo:
            execute_sp(bq_client, 'sp_vertragsdatenabgleich_wrapper', job_kennung, stichtag, log_file_base_name)
        assert "Simulated failure in core logic" in str(excinfo.value)

        # Assertions
        job_control_entries = query_table(bq_client, 'job_control')
        assert len(job_control_entries) == 1, "Expected exactly one job_control entry"

        job_entry = job_control_entries[0]
        assert job_entry.eintragsnr == 1, "Expected eintragsnr to be 1 for the first run"
        assert job_entry.job_kennung == job_kennung
        assert job_entry.script_name == 'r_ausd_v_ta_p_vertrag.ksh'
        assert job_entry.status == 'ERROR'
        assert job_entry.created_ts is not None
        assert job_entry.finished_ts is not None
        assert job_entry.stichtag_info == f'Stichtag={stichtag.isoformat()}'

        runtime_logs = get_runtime_logs(bq_client, job_entry.eintragsnr, job_kennung)
        log_messages = [row.message for row in runtime_logs]
        log_levels = [row.log_level for row in runtime_logs]

        assert any(f"Started r_ausd_v_ta_p_vertrag.ksh version 1.0 for job_kennung={job_kennung} (EintragsNr: 1)" in msg for msg in log_messages)
        assert any(f"Executing core logic for job_kennung={job_kennung}, eintragsnr=1, stichtag={stichtag.isoformat()} (SIMULATING FAILURE)" in msg for msg in log_messages)
        assert any('ERROR' == level for level in log_levels)
        assert any(f"Error in r_ausd_v_ta_p_vertrag.ksh: Simulated failure in core logic" in msg for msg in log_messages)

        error_logs = query_table(bq_client, 'job_error_log')
        assert len(error_logs) == 1, "Expected one error log entry"
        error_entry = error_logs[0]
        assert error_entry.job_kennung == job_kennung
        assert error_entry.eintragsnr == job_entry.eintragsnr
        assert error_entry.error_nr == -1 # Generic error number for BQ exceptions
        assert 'Simulated failure in core logic' in error_entry.error_arg
        assert 'sp_vertragsdatenabgleich_wrapper' in error_entry.source_proc

    finally:
        # Teardown: Deploy original core SP back
        deploy_original_core_sp(bq_client)

```

---

### Test Case 3: `eintragsnr` Generation - Transformation Correctness & Data Quality

*   **Purpose:** Verify that the `eintragsnr` (job entry number) is correctly generated and incremented for subsequent job runs, matching the `DWMSG_ErmittleNr` logic (`MAX(eintragsnr) + 1`). This covers transformation correctness (aggregation/logic) and data quality (unique IDs).
*   **Setup:**
    *   Ensure tables are empty.
    *   The `sp_k_ausd_v_ta_p_vertrag` placeholder procedure is deployed and configured not to fail.
*   **Action:**
    *   Call `project.dataset.sp_vertragsdatenabgleich_wrapper` twice with the same `job_kennung` but different `stichtag` values.
    *   Example:
        1.  `CALL project.dataset.sp_vertragsdatenabgleich_wrapper('BERT_V_TA_P_VERTRAG_SEQ', '2023-01-01', 'r_ausd_v_ta_p_vertrag')`
        2.  `CALL project.dataset.sp_vertragsdatenabgleich_wrapper('BERT_V_TA_P_VERTRAG_SEQ', '2023-01-02', 'r_ausd_v_ta_p_vertrag')`
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   Both calls complete successfully.
        *   Two entries exist in `project.dataset.job_control` for the given `job_kennung`.
        *   The first entry has `eintragsnr = 1`.
        *   The second entry has `eintragsnr = 2`.
        *   All other fields (`status`, `script_name`, `job_kennung`, `stichtag_info`) are correct for both entries.
        *   `job_runtime_log` contains all expected messages for both runs, correctly associated with their `eintragsnr`.
        *   `job_error_log` is empty.

```python
# test_wrapper_eintragsnr_generation.py
import pytest
from datetime import date

def test_eintragsnr_generation(bq_client, execute_sp, query_table, get_runtime_logs):
    job_kennung = 'BERT_V_TA_P_VERTRAG_SEQ'
    log_file_base_name = 'r_ausd_v_ta_p_vertrag'

    # Action 1
    execute_sp(bq_client, 'sp_vertragsdatenabgleich_wrapper', job_kennung, date(2023, 1, 1), log_file_base_name)

    # Action 2
    execute_sp(bq_client, 'sp_vertragsdatenabgleich_wrapper', job_kennung, date(2023, 1, 2), log_file_base_name)

    # Assertions
    job_control_entries = query_table(bq_client, 'job_control', order_by='eintragsnr')
    assert len(job_control_entries) == 2, "Expected two job_control entries"

    # First run
    job_entry_1 = job_control_entries[0]
    assert job_entry_1.eintragsnr == 1
    assert job_entry_1.job_kennung == job_kennung
    assert job_entry_1.status == 'OK'
    assert job_entry_1.stichtag_info == 'Stichtag=2023-01-01'
    runtime_logs_1 = get_runtime_logs(bq_client, job_entry_1.eintragsnr, job_kennung)
    assert any(f"Executing core logic for job_kennung={job_kennung}, eintragsnr=1, stichtag=2023-01-01" in row.message for row in runtime_logs_1)

    # Second run
    job_entry_2 = job_control_entries[1]
    assert job_entry_2.eintragsnr == 2
    assert job_entry_2.job_kennung == job_kennung
    assert job_entry_2.status == 'OK'
    assert job_entry_2.stichtag_info == 'Stichtag=2023-01-02'
    runtime_logs_2 = get_runtime_logs(bq_client, job_entry_2.eintragsnr, job_kennung)
    assert any(f"Executing core logic for job_kennung={job_kennung}, eintragsnr=2, stichtag=2023-01-02" in row.message for row in runtime_logs_2)

    error_logs = query_table(bq_client, 'job_error_log')
    assert len(error_logs) == 0, "Expected no error logs"

```

---

### Test Case 4: Stichtag Handling (NULL `p_stichtag`) - Transformation Correctness (NULL handling)

*   **Purpose:** Verify that the `p_stichtag` parameter is correctly handled, specifically when it is `NULL` (not provided). The legacy script uses `v_sysdate` if no `-s` parameter is given. The migrated script uses `COALESCE(p_stichtag, v_sysdate)` for the core script call and logs a warning.
*   **Setup:**
    *   Ensure tables are empty.
    *   The `sp_k_ausd_v_ta_p_vertrag` placeholder procedure is deployed and configured not to fail.
*   **Action:**
    *   Call `project.dataset.sp_vertragsdatenabgleich_wrapper` without providing a `p_stichtag` value (i.e., pass `NULL`).
    *   Example: `CALL project.dataset.sp_vertragsdatenabgleich_wrapper('BERT_V_TA_P_VERTRAG_NO_STICHTAG', NULL, 'r_ausd_v_ta_p_vertrag')`
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   The call completes successfully.
        *   The `job_control` entry for this run has `stichtag_info` populated with `CURRENT_DATE()` (the `v_sysdate` from the wrapper).
        *   `job_runtime_log` contains a 'WARNING' message indicating no `Stichtag` was provided.
        *   The `sp_k_ausd_v_ta_p_vertrag` call log message shows `stichtag` as `CURRENT_DATE()`.
        *   `job_error_log` is empty.

```python
# test_wrapper_stichtag_handling.py
import pytest
from datetime import date

def test_stichtag_null_handling(bq_client, execute_sp, query_table, get_runtime_logs):
    job_kennung = 'BERT_V_TA_P_VERTRAG_NO_STICHTAG'
    log_file_base_name = 'r_ausd_v_ta_p_vertrag'
    current_date = date.today().isoformat() # This will be the v_sysdate in the SP

    # Action
    execute_sp(bq_client, 'sp_vertragsdatenabgleich_wrapper', job_kennung, 'NULL', log_file_base_name)

    # Assertions
    job_control_entries = query_table(bq_client, 'job_control')
    assert len(job_control_entries) == 1, "Expected exactly one job_control entry"

    job_entry = job_control_entries[0]
    assert job_entry.eintragsnr == 1
    assert job_entry.job_kennung == job_kennung
    assert job_entry.status == 'OK'
    # The stichtag_info in job_control should reflect the v_sysdate used by the wrapper
    assert job_entry.stichtag_info == f'Stichtag={current_date}'

    runtime_logs = get_runtime_logs(bq_client, job_entry.eintragsnr, job_kennung)
    log_messages = [row.message for row in runtime_logs]
    log_levels = [row.log_level for row in runtime_logs]

    assert any('WARNING' == level for level in log_levels)
    assert any("No Stichtag parameter provided. Using default logic if applicable in core script." in msg for msg in log_messages)
    # Verify that the core script was called with the current date
    assert any(f"Executing core logic for job_kennung={job_kennung}, eintragsnr=1, stichtag={current_date}" in msg for msg in log_messages)

    error_logs = query_table(bq_client, 'job_error_log')
    assert len(error_logs) == 0, "Expected no error logs"

```

---

### Test Case 5: Schema and Data Type Assertions - Data Quality

*   **Purpose:** Verify that the schema and data types of the `job_control`, `job_runtime_log`, and `job_error_log` tables conform to the design document and BigQuery best practices. This is a fundamental data quality assertion.
*   **Setup:**
    *   The DDL for `job_control`, `job_runtime_log`, `job_error_log` has been executed.
*   **Action:**
    *   Query the schema information for each table using BigQuery's metadata views or client library functions.
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   `job_control` table has columns: `eintragsnr (INT64, REQUIRED)`, `job_kennung (STRING, REQUIRED)`, `script_name (STRING, NULLABLE)`, `log_name (STRING, NULLABLE)`, `stichtag_info (STRING, NULLABLE)`, `status (STRING, NULLABLE)`, `created_ts (TIMESTAMP, NULLABLE)`, `finished_ts (TIMESTAMP, NULLABLE)`.
        *   `job_runtime_log` table has columns: `eintragsnr (INT64, REQUIRED)`, `job_kennung (STRING, REQUIRED)`, `log_level (STRING, NULLABLE)`, `message (STRING, NULLABLE)`, `log_ts (TIMESTAMP, NULLABLE)`.
        *   `job_error_log` table has columns: `job_kennung (STRING, REQUIRED)`, `eintragsnr (INT64, REQUIRED)`, `error_nr (INT64, NULLABLE)`, `error_arg (STRING, NULLABLE)`, `error_message (STRING, NULLABLE)`, `error_ts (TIMESTAMP, NULLABLE)`, `source_proc (STRING, NULLABLE)`.
        *   All columns are nullable/non-nullable as specified in the DDL.

```python
# test_schema_assertions.py
import pytest
from google.cloud import bigquery

def test_job_control_schema(bq_client, get_table_schema):
    expected_schema = {
        'eintragsnr': {'field_type': 'INT64', 'mode': 'REQUIRED'},
        'job_kennung': {'field_type': 'STRING', 'mode': 'REQUIRED'},
        'script_name': {'field_type': 'STRING', 'mode': 'NULLABLE'},
        'log_name': {'field_type': 'STRING', 'mode': 'NULLABLE'},
        'stichtag_info': {'field_type': 'STRING', 'mode': 'NULLABLE'},
        'status': {'field_type': 'STRING', 'mode': 'NULLABLE'},
        'created_ts': {'field_type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        'finished_ts': {'field_type': 'TIMESTAMP', 'mode': 'NULLABLE'}
    }
    actual_schema = get_table_schema(bq_client, 'job_control')
    assert actual_schema == expected_schema, "job_control schema mismatch"

def test_job_runtime_log_schema(bq_client, get_table_schema):
    expected_schema = {
        'eintragsnr': {'field_type': 'INT64', 'mode': 'REQUIRED'},
        'job_kennung': {'field_type': 'STRING', 'mode': 'REQUIRED'},
        'log_level': {'field_type': 'STRING', 'mode': 'NULLABLE'},
        'message': {'field_type': 'STRING', 'mode': 'NULLABLE'},
        'log_ts': {'field_type': 'TIMESTAMP', 'mode': 'NULLABLE'}
    }
    actual_schema = get_table_schema(bq_client, 'job_runtime_log')
    assert actual_schema == expected_schema, "job_runtime_log schema mismatch"

def test_job_error_log_schema(bq_client, get_table_schema):
    expected_schema = {
        'job_kennung': {'field_type': 'STRING', 'mode': 'REQUIRED'},
        'eintragsnr': {'field_type': 'INT64', 'mode': 'REQUIRED'},
        'error_nr': {'field_type': 'INT64', 'mode': 'NULLABLE'},
        'error_arg': {'field_type': 'STRING', 'mode': 'NULLABLE'},
        'error_message': {'field_type': 'STRING', 'mode': 'NULLABLE'},
        'error_ts': {'field_type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        'source_proc': {'field_type': 'STRING', 'mode': 'NULLABLE'}
    }
    actual_schema = get_table_schema(bq_client, 'job_error_log')
    assert actual_schema == expected_schema, "job_error_log schema mismatch"

```

---

### Test Case 6: Parameter Validation (Missing `p_jobkennung`) - Transformation Correctness / Edge Case

*   **Purpose:** Test the behavior when a critical parameter like `p_jobkennung` is not provided (passed as `NULL`). While the original script hardcodes `JobKennung`, the migrated SP takes it as an input. This tests how the BigQuery SP handles a `NULL` `p_jobkennung` which is used in `WHERE` clauses and `NOT NULL` table constraints. This also highlights a potential gap where explicit parameter validation (as mentioned in the design document) is not fully implemented in the generated code, relying instead on BigQuery's `NOT NULL` constraints.
*   **Setup:**
    *   Ensure tables are empty.
    *   The `sp_k_ausd_v_ta_p_vertrag` placeholder procedure is deployed and configured not to fail.
*   **Action:**
    *   Call `project.dataset.sp_vertragsdatenabgleich_wrapper` with `p_jobkennung` as `NULL`.
    *   Example: `CALL project.dataset.sp_vertragsdatenabgleich_wrapper(NULL, '2023-01-17', 'r_ausd_v_ta_p_vertrag')`
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   The procedure raises an error (e.g., `GoogleCloudError` with a message indicating a `NULL` value for a `REQUIRED` field).
        *   Both `job_control` and `job_error_log` tables remain empty. This is because the initial `INSERT` into `job_control` fails due to `job_kennung NOT NULL`, and subsequently, the error handling block's attempt to `INSERT` into `job_error_log` also fails for the same reason (as `job_kennung` is also `NOT NULL` there).
    *   **Finding:** The migration design document states: "Parameter Handling: ... `IF` conditions and assignments to validate parameters and set `v_errnr`, `v_errarg`." The current BigQuery stored procedure does not implement explicit `IF` conditions to validate `p_jobkennung` before attempting database operations. Instead, it relies on the `NOT NULL` constraint of the target tables, which then causes the error logging mechanism itself to fail for this specific edge case. This could lead to silent failures in error reporting if `p_jobkennung` is `NULL`.

```python
# test_wrapper_null_jobkennung.py
import pytest
from datetime import date
from google.cloud.exceptions import GoogleCloudError

def test_null_jobkennung_handling(bq_client, execute_sp, query_table, deploy_original_core_sp):
    stichtag = date(2023, 1, 17)
    log_file_base_name = 'r_ausd_v_ta_p_vertrag'

    try:
        # Action: Expecting the wrapper to raise an error due to NOT NULL constraint on job_kennung
        with pytest.raises(GoogleCloudError) as excinfo:
            execute_sp(bq_client, 'sp_vertragsdatenabgleich_wrapper', 'NULL', stichtag, log_file_base_name)
        
        # Assert that the error message indicates a NOT NULL violation for job_kennung
        assert "Cannot insert NULL value into column job_kennung" in str(excinfo.value) or \
               "NULL value is not allowed for column job_kennung" in str(excinfo.value)

        # Assertions: Both tables should be empty as the initial insert and subsequent error logging fail
        job_control_entries = query_table(bq_client, 'job_control')
        assert len(job_control_entries) == 0, "Expected no job_control entries due to NULL job_kennung"

        error_logs = query_table(bq_client, 'job_error_log')
        assert len(error_logs) == 0, "Expected no error_log entries due to NULL job_kennung preventing error logging"

    finally:
        # Ensure original core SP is deployed if it was modified by other tests
        deploy_original_core_sp(bq_client)

```