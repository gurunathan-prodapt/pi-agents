As a senior data-migration QA engineer, I've designed a suite of validation tests for the migration of `r_ausd_bp_ta_bpr_instance.ksh` to BigQuery. These tests focus on ensuring behavioral equivalence, covering output parity, transformation correctness, external system replacements, and data quality.

The tests are structured to compare the behavior of the legacy KornShell script with its BigQuery Stored Procedure counterpart (`project.dataset.ausd_bp_ta_bpr_instance`). Where applicable, runnable `pytest` code with BigQuery assertions is provided.

**Assumptions for Test Execution:**
*   A BigQuery project and dataset (`project.dataset`) are configured.
*   The `job_log` table and the stored procedures (`ausd_bp_ta_bpr_instance`, `k_ausd_bp_ta_bpr_instance_sql`) are deployed in the target BigQuery environment.
*   For legacy script comparison, a controlled environment is available to execute the `.ksh` script and capture its output (log files, exit codes).
*   The `k_ausd_bp_ta_bpr_instance_sql` procedure can be temporarily modified for error simulation tests.
*   `pytest` with `google-cloud-bigquery` client is used for BigQuery interactions.

---

## Test Case 1: Default Execution (No Parameters)

**Purpose:** To verify that the migrated job executes successfully when no `stichtag` or `wiederanlaufwert` parameters are provided, correctly applying default values. This tests parameter defaulting and successful orchestration.

**Setup:**
1.  Ensure the `project.dataset.job_log` table is empty or truncated.
2.  Ensure `k_ausd_bp_ta_bpr_instance_sql` is in its default, successful execution state (i.e., it does not `SIGNAL SQLSTATE` an error).

**Action:**

*   **Legacy:** Execute the KornShell script without any arguments:
    ```bash
    ./r_ausd_bp_ta_bpr_instance.ksh
    ```
*   **Migrated:** Call the BigQuery Stored Procedure without providing `p_stichtag_str` or `p_wiederanlaufwert`:
    ```sql
    CALL project.dataset.ausd_bp_ta_bpr_instance(NULL, NULL);
    ```
    Or, via Airflow (simulated):
    ```python
    # In your Airflow DAG test or local run
    # call_main_procedure.execute(context={'params': {'stichtag_str': None, 'wiederanlaufwert': None}})
    ```

**Pass/Fail Criterion:**

*   **Legacy:**
    *   The script exits with code `0`.
    *   The generated log file (`LogDatei`) contains messages indicating job start, the derived `Stichtag` (which should be `v_sysdate` / current date), `Wiederanlaufwert` as `0`, and a final success message (`Die Abarbeitung wurde ohne erkennbare Fehler beendet`).
    *   The kernel script `k_ausd_bp_ta_bpr_instance.ksh` is invoked with the correct default parameters.
*   **Migrated:**
    *   The BigQuery Stored Procedure call completes without raising an error.
    *   The `project.dataset.job_log` table contains exactly two entries for the `job_run_id` (one 'STARTED', one 'SUCCEEDED').
    *   The 'STARTED' entry's `stichtag` column matches the `CURRENT_DATE()` at the time of execution.
    *   The 'STARTED' entry's `wiederanlaufwert` column is `0`.
    *   The 'SUCCEEDED' entry's `status` column is 'SUCCEEDED'.

**Runnable Test Code (Pytest for Migrated):**

```python
import pytest
from google.cloud import bigquery
from datetime import date

@pytest.fixture(scope="module")
def bq_client():
    """Provides a BigQuery client."""
    return bigquery.Client()

@pytest.fixture(autouse=True)
def cleanup_job_log(bq_client):
    """Cleans up the job_log table before each test."""
    bq_client.query("TRUNCATE TABLE project.dataset.job_log").result()
    yield
    bq_client.query("TRUNCATE TABLE project.dataset.job_log").result()

def test_default_execution(bq_client):
    """
    Tests the BigQuery stored procedure with default parameters.
    """
    procedure_call = "CALL project.dataset.ausd_bp_ta_bpr_instance(NULL, NULL);"
    bq_client.query(procedure_call).result()

    # Assertions on job_log table
    query_results = bq_client.query("SELECT * FROM project.dataset.job_log ORDER BY start_time").result()
    logs = list(query_results)

    assert len(logs) == 2, "Expected two log entries (STARTED and SUCCEEDED)"

    started_log = logs[0]
    succeeded_log = logs[1]

    # Check STARTED entry
    assert started_log.status == "STARTED"
    assert started_log.stichtag == date.today()
    assert started_log.wiederanlaufwert == 0
    assert started_log.job_name == "ausd_bp_ta_bpr_instance"
    assert started_log.program_name == "r_ausd_bp_ta_bpr_instance"
    assert started_log.log_message == "Job execution started."
    assert started_log.parameters_json is not None
    assert started_log.parameters_json['p_stichtag_str'] is None
    assert started_log.parameters_json['p_wiederanlaufwert'] is None
    assert started_log.parameters_json['v_stichtag'] == date.today().isoformat()
    assert started_log.parameters_json['v_actual_wiederanlaufwert'] == 0

    # Check SUCCEEDED entry
    assert succeeded_log.status == "SUCCEEDED"
    assert succeeded_log.log_message == "Job completed successfully."
    assert succeeded_log.job_run_id == started_log.job_run_id # Same job run ID
    assert succeeded_log.end_time is not None
```

---

## Test Case 2: Specific Stichtag, Default Wiederanlaufwert

**Purpose:** To verify that the migrated job correctly processes a provided `stichtag` parameter while defaulting `wiederanlaufwert`.

**Setup:**
1.  Ensure the `project.dataset.job_log` table is empty or truncated.
2.  Ensure `k_ausd_bp_ta_bpr_instance_sql` is in its default, successful execution state.

**Action:**

*   **Legacy:** Execute the KornShell script with a specific `stichtag`:
    ```bash
    ./r_ausd_bp_ta_bpr_instance.ksh -s 01012023
    ```
*   **Migrated:** Call the BigQuery Stored Procedure with `p_stichtag_str` and `p_wiederanlaufwert` as NULL:
    ```sql
    CALL project.dataset.ausd_bp_ta_bpr_instance('01012023', NULL);
    ```

**Pass/Fail Criterion:**

*   **Legacy:**
    *   The script exits with code `0`.
    *   The log file shows `Stichtag` as `01012023` and `Wiederanlaufwert` as `0`.
    *   The kernel script is invoked with `01012023` and `0`.
*   **Migrated:**
    *   The BigQuery Stored Procedure call completes without raising an error.
    *   The `project.dataset.job_log` table contains two entries for the `job_run_id`.
    *   The 'STARTED' entry's `stichtag` column is `2023-01-01`.
    *   The 'STARTED' entry's `wiederanlaufwert` column is `0`.
    *   The 'SUCCEEDED' entry's `status` column is 'SUCCEEDED'.

**Runnable Test Code (Pytest for Migrated):**

```python
import pytest
from google.cloud import bigquery
from datetime import date

# Fixtures bq_client and cleanup_job_log are assumed from Test Case 1

def test_specific_stichtag_default_wiederanlaufwert(bq_client):
    """
    Tests the BigQuery stored procedure with a specific stichtag and default wiederanlaufwert.
    """
    test_stichtag_str = "01012023"
    expected_stichtag_date = date(2023, 1, 1)
    procedure_call = f"CALL project.dataset.ausd_bp_ta_bpr_instance('{test_stichtag_str}', NULL);"
    bq_client.query(procedure_call).result()

    query_results = bq_client.query("SELECT * FROM project.dataset.job_log ORDER BY start_time").result()
    logs = list(query_results)

    assert len(logs) == 2, "Expected two log entries (STARTED and SUCCEEDED)"

    started_log = logs[0]
    succeeded_log = logs[1]

    assert started_log.status == "STARTED"
    assert started_log.stichtag == expected_stichtag_date
    assert started_log.wiederanlaufwert == 0
    assert started_log.parameters_json['p_stichtag_str'] == test_stichtag_str
    assert started_log.parameters_json['p_wiederanlaufwert'] is None
    assert started_log.parameters_json['v_stichtag'] == expected_stichtag_date.isoformat()
    assert started_log.parameters_json['v_actual_wiederanlaufwert'] == 0

    assert succeeded_log.status == "SUCCEEDED"
    assert succeeded_log.job_run_id == started_log.job_run_id
```

---

## Test Case 3: Specific Stichtag and Wiederanlaufwert

**Purpose:** To verify that the migrated job correctly processes both `stichtag` and `wiederanlaufwert` parameters.

**Setup:**
1.  Ensure the `project.dataset.job_log` table is empty or truncated.
2.  Ensure `k_ausd_bp_ta_bpr_instance_sql` is in its default, successful execution state.

**Action:**

*   **Legacy:** Execute the KornShell script with both parameters:
    ```bash
    ./r_ausd_bp_ta_bpr_instance.ksh -s 15032024 -l 12345
    ```
*   **Migrated:** Call the BigQuery Stored Procedure with both parameters:
    ```sql
    CALL project.dataset.ausd_bp_ta_bpr_instance('15032024', 12345);
    ```

**Pass/Fail Criterion:**

*   **Legacy:**
    *   The script exits with code `0`.
    *   The log file shows `Stichtag` as `15032024` and `Wiederanlaufwert` as `12345`.
    *   The kernel script is invoked with `15032024` and `12345`.
*   **Migrated:**
    *   The BigQuery Stored Procedure call completes without raising an error.
    *   The `project.dataset.job_log` table contains two entries for the `job_run_id`.
    *   The 'STARTED' entry's `stichtag` column is `2024-03-15`.
    *   The 'STARTED' entry's `wiederanlaufwert` column is `12345`.
    *   The 'SUCCEEDED' entry's `status` column is 'SUCCEEDED'.

**Runnable Test Code (Pytest for Migrated):**

```python
import pytest
from google.cloud import bigquery
from datetime import date

# Fixtures bq_client and cleanup_job_log are assumed from Test Case 1

def test_specific_stichtag_and_wiederanlaufwert(bq_client):
    """
    Tests the BigQuery stored procedure with specific stichtag and wiederanlaufwert.
    """
    test_stichtag_str = "15032024"
    expected_stichtag_date = date(2024, 3, 15)
    test_wiederanlaufwert = 12345
    procedure_call = f"CALL project.dataset.ausd_bp_ta_bpr_instance('{test_stichtag_str}', {test_wiederanlaufwert});"
    bq_client.query(procedure_call).result()

    query_results = bq_client.query("SELECT * FROM project.dataset.job_log ORDER BY start_time").result()
    logs = list(query_results)

    assert len(logs) == 2, "Expected two log entries (STARTED and SUCCEEDED)"

    started_log = logs[0]
    succeeded_log = logs[1]

    assert started_log.status == "STARTED"
    assert started_log.stichtag == expected_stichtag_date
    assert started_log.wiederanlaufwert == test_wiederanlaufwert
    assert started_log.parameters_json['p_stichtag_str'] == test_stichtag_str
    assert started_log.parameters_json['p_wiederanlaufwert'] == test_wiederanlaufwert
    assert started_log.parameters_json['v_stichtag'] == expected_stichtag_date.isoformat()
    assert started_log.parameters_json['v_actual_wiederanlaufwert'] == test_wiederanlaufwert

    assert succeeded_log.status == "SUCCEEDED"
    assert succeeded_log.job_run_id == started_log.job_run_id
```

---

## Test Case 4: Invalid Stichtag Format

**Purpose:** To verify that the migrated job correctly handles and logs errors when an invalid `stichtag` format is provided. This tests transformation correctness (type handling, error handling).

**Setup:**
1.  Ensure the `project.dataset.job_log` table is empty or truncated.

**Action:**

*   **Legacy:** Execute the KornShell script with an invalid `stichtag` format (e.g., `YYYY-MM-DD` instead of `DDMMYYYY`):
    ```bash
    ./r_ausd_bp_ta_bpr_instance.ksh -s 2023-01-01
    ```
*   **Migrated:** Call the BigQuery Stored Procedure with an invalid `p_stichtag_str`:
    ```sql
    CALL project.dataset.ausd_bp_ta_bpr_instance('2023-01-01', NULL);
    ```

**Pass/Fail Criterion:**

*   **Legacy:**
    *   The script exits with a non-zero error code (e.g., `193` or `1`).
    *   The log file contains an error message related to parameter validation or date parsing (e.g., `Notwendiges Argument fehlt` or a date parsing error).
    *   The `usage` message is printed.
*   **Migrated:**
    *   The BigQuery Stored Procedure call fails and raises an error (e.g., `Invalid p_stichtag_str format. Expected DDMMYYYY.`).
    *   The `project.dataset.job_log` table contains exactly two entries for the `job_run_id` (one 'STARTED', one 'FAILED').
    *   The 'FAILED' entry's `status` column is 'FAILED'.
    *   The 'FAILED' entry's `error_details` column contains a message indicating the invalid date format.

**Runnable Test Code (Pytest for Migrated):**

```python
import pytest
from google.cloud import bigquery

# Fixtures bq_client and cleanup_job_log are assumed from Test Case 1

def test_invalid_stichtag_format(bq_client):
    """
    Tests the BigQuery stored procedure with an invalid stichtag format.
    """
    invalid_stichtag_str = "2023-01-01"
    procedure_call = f"CALL project.dataset.ausd_bp_ta_bpr_instance('{invalid_stichtag_str}', NULL);"

    with pytest.raises(Exception) as excinfo:
        bq_client.query(procedure_call).result()

    # Check that the error message indicates invalid format
    assert "Invalid p_stichtag_str format. Expected DDMMYYYY." in str(excinfo.value)

    # Assertions on job_log table
    query_results = bq_client.query("SELECT * FROM project.dataset.job_log ORDER BY start_time").result()
    logs = list(query_results)

    assert len(logs) == 2, "Expected two log entries (STARTED and FAILED)"

    started_log = logs[0]
    failed_log = logs[1]

    # Check STARTED entry
    assert started_log.status == "STARTED"
    assert started_log.parameters_json['p_stichtag_str'] == invalid_stichtag_str

    # Check FAILED entry
    assert failed_log.status == "FAILED"
    assert failed_log.job_run_id == started_log.job_run_id
    assert "Invalid p_stichtag_str format. Expected DDMMYYYY." in failed_log.error_details
    assert failed_log.log_message == "Job failed."
    assert failed_log.end_time is not None
```

---

## Test Case 5: Kernel Script Failure Simulation

**Purpose:** To verify that the orchestrator correctly catches, logs, and propagates errors originating from the invoked kernel script. This tests external system replacement (kernel invocation) and error handling.

**Setup:**
1.  Ensure the `project.dataset.job_log` table is empty or truncated.
2.  **Temporarily modify `project.dataset.k_ausd_bp_ta_bpr_instance_sql`** to simulate a failure. For example, change its content to:
    ```sql
    CREATE OR REPLACE PROCEDURE project.dataset.k_ausd_bp_ta_bpr_instance_sql(
        IN p_stichtag DATE,
        IN p_wiederanlaufwert INT64,
        IN p_job_kennung STRING,
        IN p_dw_eintrags_nr STRING
    )
    BEGIN
        SIGNAL SQLSTATE '22000' SET MESSAGE_TEXT = 'Simulated kernel error: Data processing failed.';
    END;
    ```

**Action:**

*   **Legacy:** Modify `k_ausd_bp_ta_bpr_instance.ksh` to `exit 1` immediately after its start. Then execute `r_ausd_bp_ta_bpr_instance.ksh` with any valid parameters:
    ```bash
    # Inside k_ausd_bp_ta_bpr_instance.ksh:
    # ...
    # print "Simulating error"
    # exit 1
    # ...

    ./r_ausd_bp_ta_bpr_instance.ksh -s 01012023
    ```
*   **Migrated:** Call the BigQuery Stored Procedure with any valid parameters (e.g., default):
    ```sql
    CALL project.dataset.ausd_bp_ta_bpr_instance(NULL, NULL);
    ```

**Pass/Fail Criterion:**

*   **Legacy:**
    *   The `r_ausd_bp_ta_bpr_instance.ksh` script exits with a non-zero error code (e.g., `1`).
    *   The log file contains messages indicating the kernel script's failure and the `DWMSG_Fehlerbehandlung` being invoked (e.g., `AppError: Abbruch`).
*   **Migrated:**
    *   The BigQuery Stored Procedure call fails and raises an error (e.g., `Simulated kernel error: Data processing failed.`).
    *   The `project.dataset.job_log` table contains exactly two entries for the `job_run_id` (one 'STARTED', one 'FAILED').
    *   The 'FAILED' entry's `status` column is 'FAILED'.
    *   The 'FAILED' entry's `error_details` column contains the simulated kernel error message.

**Runnable Test Code (Pytest for Migrated):**

```python
import pytest
from google.cloud import bigquery
from datetime import date

# Fixtures bq_client and cleanup_job_log are assumed from Test Case 1

@pytest.fixture(autouse=True)
def setup_kernel_error_simulation(bq_client):
    """
    Temporarily modifies k_ausd_bp_ta_bpr_instance_sql to simulate an error.
    Restores it after the test.
    """
    # Save original kernel procedure definition (if possible, or assume a known good state)
    # For simplicity, we'll just replace and then restore.
    original_kernel_proc = """
    CREATE OR REPLACE PROCEDURE project.dataset.k_ausd_bp_ta_bpr_instance_sql(
        IN p_stichtag DATE,
        IN p_wiederanlaufwert INT64,
        IN p_job_kennung STRING,
        IN p_dw_eintrags_nr STRING
    )
    BEGIN
        SELECT 'Kernel procedure k_ausd_bp_ta_bpr_instance_sql executed successfully.' AS status_message;
    END;
    """
    
    # Deploy error-simulating kernel procedure
    error_kernel_proc = """
    CREATE OR REPLACE PROCEDURE project.dataset.k_ausd_bp_ta_bpr_instance_sql(
        IN p_stichtag DATE,
        IN p_wiederanlaufwert INT64,
        IN p_job_kennung STRING,
        IN p_dw_eintrags_nr STRING
    )
    BEGIN
        SIGNAL SQLSTATE '22000' SET MESSAGE_TEXT = 'Simulated kernel error: Data processing failed.';
    END;
    """
    bq_client.query(error_kernel_proc).result()
    yield # Run the test
    # Restore original kernel procedure
    bq_client.query(original_kernel_proc).result()

def test_kernel_script_failure_simulation(bq_client):
    """
    Tests that the orchestrator handles and logs errors from the kernel script.
    """
    procedure_call = "CALL project.dataset.ausd_bp_ta_bpr_instance(NULL, NULL);"

    with pytest.raises(Exception) as excinfo:
        bq_client.query(procedure_call).result()

    # Check that the error message indicates the kernel failure
    assert "Simulated kernel error: Data processing failed." in str(excinfo.value)

    # Assertions on job_log table
    query_results = bq_client.query("SELECT * FROM project.dataset.job_log ORDER BY start_time").result()
    logs = list(query_results)

    assert len(logs) == 2, "Expected two log entries (STARTED and FAILED)"

    started_log = logs[0]
    failed_log = logs[1]

    # Check STARTED entry
    assert started_log.status == "STARTED"

    # Check FAILED entry
    assert failed_log.status == "FAILED"
    assert failed_log.job_run_id == started_log.job_run_id
    assert "Simulated kernel error: Data processing failed." in failed_log.error_details
    assert failed_log.log_message == "Job failed."
    assert failed_log.end_time is not None
```

---

## Test Case 6: `job_log` Table Schema and Data Quality

**Purpose:** To verify that the `job_log` table's schema adheres to the design, and that data written to it conforms to expected types and constraints. This tests data quality and schema assertions.

**Setup:**
1.  Ensure the `project.dataset.job_log` table exists.
2.  Run `test_default_execution` (or any successful execution) to populate the `job_log` table with data.

**Action:**
*   **Legacy:** N/A (no equivalent structured log table).
*   **Migrated:** Query the BigQuery information schema to inspect the `job_log` table's structure and query its content to verify data types and non-null constraints.

**Pass/Fail Criterion:**

*   The `job_log` table exists in `project.dataset`.
*   The schema of `job_log` matches the DDL provided in the migration design document (column names, data types).
*   Columns `job_run_id`, `job_name`, `status` are marked as `NOT NULL`.
*   For a successful run, `job_run_id` is a valid UUID string, `stichtag` is a valid DATE, `wiederanlaufwert` is an INT64, and `parameters_json` is valid JSON.

**Runnable Test Code (Pytest for Migrated):**

```python
import pytest
from google.cloud import bigquery
from datetime import date
import json

# Fixtures bq_client and cleanup_job_log are assumed from Test Case 1

def test_job_log_schema_and_data_quality(bq_client):
    """
    Verifies the schema and data quality of the job_log table.
    """
    # First, ensure there's data in the log table
    procedure_call = "CALL project.dataset.ausd_bp_ta_bpr_instance('01012023', 100);"
    bq_client.query(procedure_call).result()

    # 1. Verify table existence and schema
    table_ref = bq_client.dataset("dataset").table("job_log")
    table = bq_client.get_table(table_ref)

    expected_schema = {
        "job_run_id": ("STRING", "REQUIRED"),
        "job_name": ("STRING", "REQUIRED"),
        "program_name": ("STRING", "NULLABLE"),
        "program_version": ("STRING", "NULLABLE"),
        "status": ("STRING", "REQUIRED"),
        "start_time": ("TIMESTAMP", "NULLABLE"),
        "end_time": ("TIMESTAMP", "NULLABLE"),
        "stichtag": ("DATE", "NULLABLE"),
        "wiederanlaufwert": ("INT64", "NULLABLE"),
        "log_message": ("STRING", "NULLABLE"),
        "error_details": ("STRING", "NULLABLE"),
        "parameters_json": ("JSON", "NULLABLE"),
    }

    actual_schema = {field.name: (field.field_type, field.mode) for field in table.schema}

    assert actual_schema == expected_schema, "job_log table schema mismatch"

    # 2. Verify data types and non-null constraints for actual data
    query_results = bq_client.query("SELECT * FROM project.dataset.job_log WHERE status = 'STARTED' LIMIT 1").result()
    log_entry = next(iter(query_results), None)

    assert log_entry is not None, "No 'STARTED' log entry found for data quality check."

    assert isinstance(log_entry.job_run_id, str) and len(log_entry.job_run_id) > 0
    assert isinstance(log_entry.job_name, str) and len(log_entry.job_name) > 0
    assert isinstance(log_entry.status, str) and len(log_entry.status) > 0
    assert isinstance(log_entry.start_time, type(bq_client.query_and_wait("SELECT CURRENT_TIMESTAMP()").result().next()[0]))
    assert isinstance(log_entry.stichtag, date)
    assert isinstance(log_entry.wiederanlaufwert, int)
    assert isinstance(log_entry.log_message, str)

    # Verify parameters_json content
    assert log_entry.parameters_json is not None
    assert isinstance(log_entry.parameters_json, dict) # BigQuery JSON type is returned as dict in Python client
    assert log_entry.parameters_json['p_stichtag_str'] == '01012023'
    assert log_entry.parameters_json['p_wiederanlaufwert'] == 100
    assert log_entry.parameters_json['v_stichtag'] == '2023-01-01'
    assert log_entry.parameters_json['v_actual_wiederanlaufwert'] == 100
```

---

## Test Case 7: Airflow DAG Integration

**Purpose:** To verify that the Airflow DAG correctly triggers the BigQuery Stored Procedure and passes parameters as expected, demonstrating the orchestration replacement.

**Setup:**
1.  Deploy the `r_ausd_bp_ta_bpr_instance_dag.py` to an Airflow environment.
2.  Ensure the `project.dataset.job_log` table is empty or truncated.
3.  Ensure `k_ausd_bp_ta_bpr_instance_sql` is in its default, successful execution state.

**Action:**

*   **Legacy:** N/A (shell script directly executed).
*   **Migrated:** Trigger the `r_ausd_bp_ta_bpr_instance_dag` in Airflow, providing specific `stichtag_str` and `wiederanlaufwert` parameters via the Airflow UI or API.

**Pass/Fail Criterion:**

*   The Airflow DAG run completes successfully.
*   The `call_ausd_bp_ta_bpr_instance_sp` task within the DAG completes successfully.
*   The `project.dataset.job_log` table contains two entries for the `job_run_id` corresponding to the Airflow run.
*   The 'STARTED' entry's `stichtag` and `wiederanlaufwert` columns in `job_log` match the parameters passed via Airflow.
*   The 'SUCCEEDED' entry's `status` column is 'SUCCEEDED'.

**Runnable Test Code (Conceptual - Airflow DAG testing is typically done with Airflow's own test utilities or by observing actual runs):**

```python
# This is a conceptual test. Airflow DAGs are typically tested using
# Airflow's built-in testing utilities (e.g., `dag.test()`) or by
# deploying and observing runs in a test Airflow environment.

# Example of how you might trigger and check in a test Airflow environment:

# 1. Trigger the DAG via Airflow CLI or API:
#    airflow dags trigger r_ausd_bp_ta_bpr_instance_dag \
#        -c '{"stichtag_str": "20052023", "wiederanlaufwert": 500}'

# 2. Wait for the DAG run to complete (e.g., poll Airflow API for status).

# 3. Use BigQuery client to verify job_log entries:
import pytest
from google.cloud import bigquery
from datetime import date
import time

# Fixtures bq_client and cleanup_job_log are assumed from Test Case 1

def test_airflow_dag_integration(bq_client):
    """
    Verifies that the Airflow DAG correctly triggers the BigQuery SP.
    This test assumes manual or external triggering of the Airflow DAG.
    """
    # --- Manual/External Step: Trigger Airflow DAG ---
    # For a real automated test, you'd use Airflow's test utilities
    # or a programmatic trigger here.
    # Example:
    # from airflow.models.dagbag import DagBag
    # dagbag = DagBag(dag_folder='dags', include_examples=False)
    # dag = dagbag.get_dag('r_ausd_bp_ta_bpr_instance_dag')
    # dr = dag.create_dagrun(
    #     state=State.RUNNING,
    #     execution_date=pendulum.now(),
    #     conf={'stichtag_str': '20052023', 'wiederanlaufwert': 500}
    # )
    # dr.task_instances[0].run() # This would run the task directly

    # For this example, we'll simulate the effect of the DAG by directly
    # calling the BigQuery SP with the expected Airflow parameters.
    # In a true integration test, you'd trigger Airflow and then query BQ.

    test_stichtag_str = "20052023"
    expected_stichtag_date = date(2023, 5, 20)
    test_wiederanlaufwert = 500
    
    # Simulate Airflow calling the SP
    procedure_call = f"CALL project.dataset.ausd_bp_ta_bpr_instance('{test_stichtag_str}', {test_wiederanlaufwert});"
    bq_client.query(procedure_call).result()

    # --- Verification Step: Query BigQuery job_log ---
    query_results = bq_client.query("SELECT * FROM project.dataset.job_log ORDER BY start_time").result()
    logs = list(query_results)

    assert len(logs) == 2, "Expected two log entries (STARTED and SUCCEEDED) from Airflow run"

    started_log = logs[0]
    succeeded_log = logs[1]

    assert started_log.status == "STARTED"
    assert started_log.stichtag == expected_stichtag_date
    assert started_log.wiederanlaufwert == test_wiederanlaufwert
    assert started_log.parameters_json['p_stichtag_str'] == test_stichtag_str
    assert started_log.parameters_json['p_wiederanlaufwert'] == test_wiederanlaufwert
    assert started_log.parameters_json['v_stichtag'] == expected_stichtag_date.isoformat()
    assert started_log.parameters_json['v_actual_wiederanlaufwert'] == test_wiederanlaufwert

    assert succeeded_log.status == "SUCCEEDED"
    assert succeeded_log.job_run_id == started_log.job_run_id
```