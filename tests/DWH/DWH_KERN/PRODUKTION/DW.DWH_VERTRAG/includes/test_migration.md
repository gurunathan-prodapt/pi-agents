Here is the comprehensive migration-validation test suite designed to verify that the migrated Python modules behave identically to their legacy UC4 XML counterparts.

---

# Test Suite: Shared Files — DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes

## Section 1: Unit & Integration Tests for `dw_hole_pfad_vtrg.py`

### Test Case 1.1: Successful Variable Retrieval (Output Parity)
* **Purpose:** Verify that when the Airflow Variable `dw_variablen` is correctly configured in the metastore, `load_dw_variables()` successfully retrieves and maps the keys to match the legacy UC4 variable resolution (`&DWH_HOME`, `&HOME`, `&PMS_HOME`).
* **Setup:**
  * Mock the Airflow `Variable.get` method to return a valid JSON string containing the target paths.
* **Action:**
  * Execute `load_dw_variables()`.
* **Pass/Fail Criterion:**
  * **Pass:** The function returns a dictionary with keys `DWH_HOME`, `HOME`, and `PMS_HOME` containing the exact values from the mock.
  * **Fail:** Any key is missing, mapped incorrectly, or an exception is raised.

```python
import pytest
from unittest.mock import patch
from airflow.exceptions import AirflowException
from dags.includes.dw_hole_pfad_vtrg import load_dw_variables

def test_load_dw_variables_success():
    mock_json = {
        "dwh_home": "gs://prod-dwh-bucket/dwh",
        "home": "/home/airflow",
        "pms_home": "gs://prod-dwh-bucket/pms"
    }
    
    with patch("airflow.models.Variable.get") as mock_get:
        mock_get.return_value = mock_json
        
        result = load_dw_variables()
        
        mock_get.assert_called_once_with("dw_variablen", deserialize_json=True)
        assert result["DWH_HOME"] == "gs://prod-dwh-bucket/dwh"
        assert result["HOME"] == "/home/airflow"
        assert result["PMS_HOME"] == "gs://prod-dwh-bucket/pms"
```

---

### Test Case 1.2: Missing Airflow Variable Container (Error Handling)
* **Purpose:** Verify that the function raises a descriptive `AirflowException` if the parent variable container `dw_variablen` does not exist in the environment.
* **Setup:**
  * Mock `Variable.get` to raise a `KeyError` (the standard behavior of Airflow when a variable is missing).
* **Action:**
  * Execute `load_dw_variables()` inside an exception-assertion block.
* **Pass/Fail Criterion:**
  * **Pass:** An `AirflowException` is raised containing the message `"The Airflow Variable 'dw_variablen' does not exist in the environment."`
  * **Fail:** Any other exception is raised, or the function exits silently.

```python
def test_load_dw_variables_missing_container():
    with patch("airflow.models.Variable.get", side_effect=KeyError("dw_variablen")):
        with pytest.raises(AirflowException) as exc_info:
            load_dw_variables()
        assert "The Airflow Variable 'dw_variablen' does not exist in the environment." in str(exc_info.value)
```

---

### Test Case 1.3: Incomplete JSON Keys (Data Quality & Schema Assertion)
* **Purpose:** Verify that if the JSON object exists but is missing one or more required keys (e.g., `pms_home`), the function fails safely instead of returning partial or `None` values.
* **Setup:**
  * Mock `Variable.get` to return a JSON payload missing the `pms_home` key.
* **Action:**
  * Execute `load_dw_variables()` inside an exception-assertion block.
* **Pass/Fail Criterion:**
  * **Pass:** An `AirflowException` is raised explicitly stating: `"Missing required key(s) in 'dw_variablen' Variable: pms_home"`.
  * **Fail:** The function returns a dictionary containing `None` or does not identify the specific missing key.

```python
def test_load_dw_variables_missing_keys():
    incomplete_json = {
        "dwh_home": "gs://prod-dwh-bucket/dwh",
        "home": "/home/airflow"
        # "pms_home" is missing
    }
    
    with patch("airflow.models.Variable.get", return_value=incomplete_json):
        with pytest.raises(AirflowException) as exc_info:
            load_dw_variables()
        assert "Missing required key(s) in 'dw_variablen' Variable: pms_home" in str(exc_info.value)
```

---

## Section 2: Unit & Integration Tests for `dw_lese_log_vtrg.py`

### Test Case 2.1: Log Output and Verbatim Language Parity
* **Purpose:** Verify that the log utility extracts the Airflow context variables (DAG ID and Task ID) and formats them into the exact German string format used by the legacy UC4 `:PRINT` statement: `Protokolleintrag: &ADMJOB innerhalb &ADMJP`.
* **Setup:**
  * Create mock objects for the Airflow `dag` and `task` context.
  * Set up a `logging.getLogger` spy or use pytest's `caplog` fixture to capture standard output logs.
* **Action:**
  * Execute `log_vtrg_context_executable` passing the mocked context dictionary.
* **Pass/Fail Criterion:**
  * **Pass:** The function returns and logs the exact string: `"Protokolleintrag: test_task innerhalb test_dag"`.
  * **Fail:** The string is formatted incorrectly, uses English, or fails to resolve the context keys.

```python
def test_log_vtrg_context_executable_output(caplog):
    # Mocking Airflow context structures
    class MockDag:
        dag_id = "test_dag"

    class MockTask:
        task_id = "test_task"

    context = {
        "dag": MockDag(),
        "task": MockTask()
    }

    with caplog.at_level("INFO"):
        output_message = log_vtrg_context_executable(**context)
        
        # Assert return value
        assert output_message == "Protokolleintrag: test_task innerhalb test_dag"
        
        # Assert standard logging output
        assert any(
            "Protokolleintrag: test_task innerhalb test_dag" in record.message 
            for record in caplog.records
        )
```

---

### Test Case 2.2: DAG Structure and Task Integrity
* **Purpose:** Ensure that the standalone helper DAG is correctly initialized with the specified parameters and contains the `log_vtrg_context` task.
* **Setup:**
  * Import the `dag` object from `dags.includes.dw_lese_log_vtrg`.
* **Action:**
  * Inspect the DAG's properties and task list.
* **Pass/Fail Criterion:**
  * **Pass:** 
    * `dag.dag_id` is `"dw_lese_log_vtrg_helper"`.
    * `dag.schedule_interval` is `None`.
    * The DAG contains a task with ID `"log_vtrg_context"` which is an instance of `PythonOperator`.
  * **Fail:** The DAG is misconfigured or the task is missing.

```python
def test_dag_structure():
    from dags.includes.dw_lese_log_vtrg import dag as helper_dag
    
    assert helper_dag.dag_id == "dw_lese_log_vtrg_helper"
    assert helper_dag.schedule_interval is None
    assert "log_vtrg_context" in helper_dag.task_ids
    
    task = helper_dag.get_task("log_vtrg_context")
    assert task.python_callable.__name__ == "log_vtrg_context_executable"
```

---

## Section 3: End-to-End Integration Validation (Composer Environment)

### Test Case 3.1: Live Airflow Metastore Integration Test
* **Purpose:** Verify that the helper modules function correctly when executed inside a real/local Airflow database context (e.g., during a local CI/CD run using a test database).
* **Setup:**
  * Initialize a local Airflow metadata database.
  * Programmatically create the Airflow Variable `dw_variablen` using the Airflow models API.
* **Action:**
  * Call `load_dw_variables()` within the active database session.
* **Pass/Fail Criterion:**
  * **Pass:** The function successfully queries the database and returns the correct path mappings without mocking.
  * **Fail:** Database lookup fails, or JSON deserialization fails.

```python
import json
from airflow.utils.session import create_session

def test_live_metadata_variable_resolution(add_airflow_var):
    """
    Integration test requiring an active Airflow DB context.
    'add_airflow_var' is a fixture that writes to the test DB.
    """
    test_payload = {
        "dwh_home": "gs://integration-test-bucket/dwh",
        "home": "/opt/airflow",
        "pms_home": "gs://integration-test-bucket/pms"
    }
    
    # Write directly to the running test database
    from airflow.models import Variable
    Variable.set("dw_variablen", test_payload, serialize_json=True)
    
    try:
        # Execute target code
        resolved_paths = load_dw_variables()
        
        assert resolved_paths["DWH_HOME"] == "gs://integration-test-bucket/dwh"
        assert resolved_paths["PMS_HOME"] == "gs://integration-test-bucket/pms"
    finally:
        # Cleanup
        Variable.delete("dw_variablen")
```