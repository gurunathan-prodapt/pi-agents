Here is a comprehensive suite of migration-validation tests designed to prove that the migrated Airflow tasks behave identically to the legacy UC4 JOBI scripts.

---

# Test Suite: `DW.DWH_ADM_JOB_MONITOR` Migration Validation

## Section 1: Output Parity & Log Literal Assertions

### Test Case 1.1: Start Script Verbatim Log Output
* **Purpose**: Verify that the migrated start logic produces the exact German log output matching the legacy UC4 script when a job is registered.
* **Setup**: 
  * Mock the Airflow Variable `dwh_monitored_dags` to return `{"test_dag": "J"}`.
  * Mock `PostgresHook` to prevent actual database writes during this unit test.
* **Action**: Call `register_job_monitoring_start_logic` with `dag_id="test_dag"`, `task_id="test_task"`, and `run_id="manual__2023-10-27T12:00:00+00:00"`. Capture standard logging output.
* **Pass/Fail Criterion**: The test passes if the log output contains the exact literal string: `"Added test_task with manual__2023-10-27T12:00:00+00:00"`.

```python
import logging
import pytest
from unittest.mock import MagicMock, patch
from utils.job_monitor_utils import register_job_monitoring_start_logic

def test_start_script_verbatim_log(caplog):
    caplog.set_level(logging.INFO)
    
    # Mock Airflow Variables and DB Hook
    with patch("utils.job_monitor_utils.Variable.get") as mock_var_get, \
         patch("utils.job_monitor_utils.PostgresHook") as mock_pg_hook:
        
        mock_var_get.return_value = {"test_dag": "J"}
        mock_pg_hook.return_value = MagicMock()
        
        register_job_monitoring_start_logic(
            dag_id="test_dag",
            task_id="test_task",
            run_id="manual__2023-10-27T12:00:00+00:00"
        )
        
        # Assert verbatim output rule mapping
        assert any(
            "Added test_task with manual__2023-10-27T12:00:00+00:00" in record.message 
            for record in caplog.records
        )
```

### Test Case 1.2: End Script Verbatim Log Output
* **Purpose**: Verify that the migrated end logic produces the exact German log output matching the legacy UC4 script when a job finishes.
* **Setup**:
  * Mock the Airflow Variable `Variable.set` to prevent writing to the metadata database.
* **Action**: Call `register_job_monitoring_end_logic` with `dag_id="test_dag"`, `task_id="test_task"`, and `dag_run_conf={"dwh_job_kennung": "KENNUNG_ABC"}`. Capture standard logging output.
* **Pass/Fail Criterion**: The test passes if the log output contains the exact literal string: `"Jobkennung KENNUNG_ABC eingetragen für test_task"`.

```python
import logging
import pytest
from unittest.mock import patch
from utils.job_monitor_utils import register_job_monitoring_end_logic

def test_end_script_verbatim_log(caplog):
    caplog.set_level(logging.INFO)
    
    with patch("utils.job_monitor_utils.Variable.set") as mock_var_set:
        register_job_monitoring_end_logic(
            dag_id="test_dag",
            task_id="test_task",
            dag_run_conf={"dwh_job_kennung": "KENNUNG_ABC"}
        )
        
        # Assert verbatim output rule mapping
        assert any(
            "Jobkennung KENNUNG_ABC eingetragen für test_task" in record.message 
            for record in caplog.records
        )
```

---

## Section 2: Transformation Correctness & Routing Logic

### Test Case 2.1: Monitoring Flag Evaluation Matrix
* **Purpose**: Prove that the start logic correctly evaluates the monitoring configuration matrix (Specific DAG match, "ALL" fallback, and "N" skip) exactly as the legacy nested `:IF` statements did.
* **Setup**:
  * Prepare a parameterized matrix of inputs and expected outcomes.
* **Action**: Execute `register_job_monitoring_start_logic` across the parameter matrix.
* **Pass/Fail Criterion**: 
  * If the resolved flag is `"J"`, the database insert must be called.
  * If the resolved flag is `"N"`, an `AirflowSkipException` must be raised, and the database insert must not be called.

```python
import pytest
from unittest.mock import MagicMock, patch
from airflow.exceptions import AirflowSkipException
from utils.job_monitor_utils import register_job_monitoring_start_logic

@pytest.mark.parametrize(
    "monitored_dags_var, dag_id, should_register",
    [
        # Case A: Specific DAG is explicitly monitored
        ({"dw_example_dag": "J", "ALL": "N"}, "dw_example_dag", True),
        # Case B: Specific DAG is explicitly disabled, overriding ALL
        ({"dw_example_dag": "N", "ALL": "J"}, "dw_example_dag", False),
        # Case C: DAG not listed, falls back to ALL = J
        ({"ALL": "J"}, "dw_unlisted_dag", True),
        # Case D: DAG not listed, falls back to ALL = N
        ({"ALL": "N"}, "dw_unlisted_dag", False),
        # Case E: Empty configuration, defaults to N
        ({}, "dw_any_dag", False),
    ]
)
def test_monitoring_flag_evaluation(monitored_dags_var, dag_id, should_register):
    with patch("utils.job_monitor_utils.Variable.get") as mock_var_get, \
         patch("utils.job_monitor_utils.PostgresHook") as mock_pg_hook:
        
        mock_var_get.return_value = monitored_dags_var
        mock_hook_instance = MagicMock()
        mock_pg_hook.return_value = mock_hook_instance
        
        if should_register:
            register_job_monitoring_start_logic(dag_id=dag_id, task_id="task_1", run_id="run_1")
            mock_hook_instance.run.assert_called_once()
        else:
            with pytest.raises(AirflowSkipException):
                register_job_monitoring_start_logic(dag_id=dag_id, task_id="task_1", run_id="run_1")
            mock_hook_instance.run.assert_not_called()
```

### Test Case 2.2: End Script Default Parameter Handling
* **Purpose**: Verify that if `dwh_job_kennung` is missing from the DAG run configuration, the system gracefully falls back to `"DEFAULT_KENNUNG"` without throwing an exception.
* **Setup**:
  * Mock `Variable.set` to track calls.
* **Action**: Call `register_job_monitoring_end_logic` with an empty `dag_run_conf` dictionary.
* **Pass/Fail Criterion**: The test passes if the variable is registered with the value `"DEFAULT_KENNUNG"`.

```python
from unittest.mock import patch
from utils.job_monitor_utils import register_job_monitoring_end_logic

def test_end_script_fallback_handling():
    with patch("utils.job_monitor_utils.Variable.set") as mock_var_set:
        register_job_monitoring_end_logic(
            dag_id="test_dag",
            task_id="test_task",
            dag_run_conf={} # Missing dwh_job_kennung
        )
        
        mock_var_set.assert_called_once_with(
            key="dw_dwh_adm_job_monitor_jobkennung_var_test_task",
            value="DEFAULT_KENNUNG"
        )
```

---

## Section 3: External-System Replacements (Database & State Writes)

### Test Case 3.1: Database Write Execution & SQL Correctness
* **Purpose**: Verify that the start script correctly formats and executes the upsert SQL query against the target metadata database.
* **Setup**:
  * Mock `PostgresHook` and capture the SQL statement and parameters passed to it.
* **Action**: Call `register_job_monitoring_start_logic` with `dag_id="test_dag"`, `task_id="test_task"`, and `run_id="run_123"`.
* **Pass/Fail Criterion**: The test passes if:
  1. The SQL query contains `INSERT INTO dwh_running_jobs`.
  2. The SQL query contains an `ON CONFLICT (job_name) DO UPDATE` clause.
  3. The parameters passed are exactly `('test_task', 'run_123')`.

```python
from unittest.mock import MagicMock, patch
from utils.job_monitor_utils import register_job_monitoring_start_logic

def test_database_write_sql_and_params():
    with patch("utils.job_monitor_utils.Variable.get") as mock_var_get, \
         patch("utils.job_monitor_utils.PostgresHook") as mock_pg_hook:
        
        mock_var_get.return_value = {"ALL": "J"}
        mock_hook_instance = MagicMock()
        mock_pg_hook.return_value = mock_hook_instance
        
        register_job_monitoring_start_logic(
            dag_id="test_dag",
            task_id="test_task",
            run_id="run_123"
        )
        
        # Extract arguments passed to pg_hook.run()
        called_args, called_kwargs = mock_hook_instance.run.call_args
        sql_query = called_args[0]
        sql_params = called_kwargs.get("parameters") or called_args[1]
        
        # Assert SQL structure and parameters
        assert "INSERT INTO dwh_running_jobs" in sql_query
        assert "ON CONFLICT (job_name)" in sql_query
        assert "DO UPDATE SET" in sql_query
        assert sql_params == ("test_task", "run_123")
```

### Test Case 3.2: Airflow Variable Registry Write (PUT_VAR Emulation)
* **Purpose**: Verify that the end script emulates the UC4 `:PUT_VAR` command by correctly writing the job key to the Airflow Variable store.
* **Setup**:
  * Mock `Variable.set` to intercept the write.
* **Action**: Call `register_job_monitoring_end_logic` with `task_id="my_etl_task"` and `dwh_job_kennung="KENN_999"`.
* **Pass/Fail Criterion**: The test passes if `Variable.set` is called with key `"dw_dwh_adm_job_monitor_jobkennung_var_my_etl_task"` and value `"KENN_999"`.

```python
from unittest.mock import patch
from utils.job_monitor_utils import register_job_monitoring_end_logic

def test_airflow_variable_registry_write():
    with patch("utils.job_monitor_utils.Variable.set") as mock_var_set:
        register_job_monitoring_end_logic(
            dag_id="parent_dag",
            task_id="my_etl_task",
            dag_run_conf={"dwh_job_kennung": "KENN_999"}
        )
        
        mock_var_set.assert_called_once_with(
            key="dw_dwh_adm_job_monitor_jobkennung_var_my_etl_task",
            value="KENN_999"
        )
```

---

## Section 4: Data-Quality & Schema Assertions

### Test Case 4.1: Target Table Schema Validation
* **Purpose**: Ensure that the target database table `dwh_running_jobs` matches the schema expected by the Python code.
* **Setup**:
  * Establish a connection to the target database environment (or test database).
* **Action**: Query the database schema catalog for the `dwh_running_jobs` table.
* **Pass/Fail Criterion**: The test passes if the table exists and contains the following columns with compatible types:
  * `job_name` (character/varchar, Primary Key or Unique)
  * `run_number` (character/varchar)
  * `registration_timestamp` (timestamp with or without timezone)
  * `status` (character/varchar)

```sql
-- SQL Assertion: Run against target database to verify schema integrity
SELECT 
    column_name, 
    data_type, 
    is_nullable
FROM 
    information_schema.columns
WHERE 
    table_name = 'dwh_running_jobs'
ORDER BY 
    ordinal_position;

-- Expected Output Verification:
-- job_name               | character varying | NO (Must be Primary Key or Unique)
-- run_number             | character varying | NO
-- registration_timestamp | timestamp...      | NO
-- status                 | character varying | NO
```

### Test Case 4.2: Database Connection Resolution Integrity
* **Purpose**: Ensure that the environment-specific connection variables resolve correctly and do not fall back to defaults silently if configured otherwise.
* **Setup**:
  * Set environment variable `GCP_PROJECT` to `"prod-gcp-project"`.
  * Set Airflow Variable `METADATA_AUDIT_DB_CONN` to `"prod_postgres_conn"`.
* **Action**: Call `get_gcp_project()` and `get_metadata_db_conn()`.
* **Pass/Fail Criterion**: The test passes if the resolved values match the environment configurations exactly.

```python
import os
from unittest.mock import patch
from utils.job_monitor_utils import get_gcp_project, get_metadata_db_conn

def test_environment_variable_resolution():
    with patch.dict(os.environ, {"GCP_PROJECT": "prod-gcp-project"}), \
         patch("utils.job_monitor_utils.Variable.get") as mock_var_get:
        
        mock_var_get.return_value = "prod_postgres_conn"
        
        assert get_gcp_project() == "prod-gcp-project"
        assert get_metadata_db_conn() == "prod_postgres_conn"
```