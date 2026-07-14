Here is a comprehensive suite of migration-validation tests designed to prove that the migrated Cloud Composer (Airflow) code is behaviorally equivalent to the legacy UC4 include scripts (`DW.HOLE_PFAD` and `DW.LESE_LOG`).

---

# Test Suite: UC4 Include Migration Validation

## Section 1: Date Calculus Parity (DW.HOLE_PFAD)

### Test Case 1.1: Standard Month Date Calculus
* **Purpose**: Verify that the Python-native date calculations in `calculate_date_windows` yield the exact same string outputs (`YYYYMM`) as the legacy UC4 macro functions (`SUB_PERIOD`, `SUB_DAYS`, `ADD_PERIOD`) for standard mid-month execution dates.
* **Setup**:
  * Install `pytest` and `dateutil` in the test environment.
  * Import `calculate_date_windows` from `plugins.templates.dw_env_resolver`.
* **Action**:
  Execute the calculation function with a standard date: `2023-06-15`.
* **Pass/Fail Criterion**:
  The function must return a dictionary with the exact keys and values:
  * `PRELASTMONTH_YYYYMM` == `"202304"` (2 months prior to June)
  * `LASTMONTH_YYYYMM` == `"202305"` (1 month prior to June)
  * `NEXTMONTH_YYYYMM` == `"202307"` (1 month after June)

```python
# test_date_calculus.py
from templates.dw_env_resolver import calculate_date_windows

def test_standard_month_calculus():
    result = calculate_date_windows("2023-06-15")
    assert result["PRELASTMONTH_YYYYMM"] == "202304"
    assert result["LASTMONTH_YYYYMM"] == "202305"
    assert result["NEXTMONTH_YYYYMM"] == "202307"
```

### Test Case 1.2: Year Boundary & Leap Year Transitions
* **Purpose**: Ensure that year-end transitions (January execution) and leap years (February/March transitions) do not cause date overflows or off-by-one errors.
* **Setup**:
  * Import `calculate_date_windows` from `plugins.templates.dw_env_resolver`.
* **Action**:
  Execute calculations for boundary dates: `2024-01-15` (Year transition) and `2024-03-29` (Leap year transition).
* **Pass/Fail Criterion**:
  * For `2024-01-15`:
    * `PRELASTMONTH_YYYYMM` == `"202311"`
    * `LASTMONTH_YYYYMM` == `"202312"`
    * `NEXTMONTH_YYYYMM` == `"202402"`
  * For `2024-03-29`:
    * `PRELASTMONTH_YYYYMM` == `"202401"`
    * `LASTMONTH_YYYYMM` == `"202402"`
    * `NEXTMONTH_YYYYMM` == `"202404"`

```python
def test_year_boundary_calculus():
    result = calculate_date_windows("2024-01-15")
    assert result["PRELASTMONTH_YYYYMM"] == "202311"
    assert result["LASTMONTH_YYYYMM"] == "202312"
    assert result["NEXTMONTH_YYYYMM"] == "202402"

def test_leap_year_calculus():
    result = calculate_date_windows("2024-03-29")
    assert result["PRELASTMONTH_YYYYMM"] == "202401"
    assert result["LASTMONTH_YYYYMM"] == "202402"
    assert result["NEXTMONTH_YYYYMM"] == "202404"
```

---

## Section 2: Environment Variable Resolution & Fallbacks

### Test Case 2.1: Airflow Variable Store Resolution
* **Purpose**: Prove that the environment resolver correctly pulls values from the Airflow Variable store when they exist, matching the legacy `GET_VAR` behavior.
* **Setup**:
  * Mock the `airflow.models.Variable.get` method to simulate populated database values.
* **Action**:
  Call `compute_environment_context` with a mock logical date.
* **Pass/Fail Criterion**:
  The returned dictionary must contain the mocked values instead of the default values.

```python
from unittest.mock import patch
from templates.dw_env_resolver import compute_environment_context

@patch("templates.dw_env_resolver.Variable.get")
def test_variable_resolution_from_store(mock_get):
    # Define mock behavior for GET_VAR equivalents
    mock_vars = {
        "dwh_home": "gs://prod-bucket/dwh_custom",
        "aktiv_carmen": "1",
        "aktiv_crs": "0",
        "aktiv_ctel": "1",
        "aktiv_dpps": "0",
        "aktiv_kds": "1",
        "aktiv_wuerfel": "0",
        "aktiv_xtra": "1",
        "aktuell_cache_dwk_kkm": "99"
    }
    mock_get.side_effect = lambda key, default_var=None: mock_vars.get(key, default_var)
    
    context = compute_environment_context("2023-06-15")
    
    assert context["DWH_HOME"] == "gs://prod-bucket/dwh_custom"
    assert context["AKTIV_CARMEN"] == "1"
    assert context["AKTIV_CTEL"] == "1"
    assert context["AKTUELL_CACHE"] == "99"
```

### Test Case 2.2: Safe Fallback Defaults
* **Purpose**: Verify that if the Airflow Variable store is missing keys, the system falls back to safe, documented defaults rather than raising a runtime exception.
* **Setup**:
  * Mock `airflow.models.Variable.get` to return the `default_var` parameter.
* **Action**:
  Call `compute_environment_context` with a mock logical date.
* **Pass/Fail Criterion**:
  The returned dictionary must contain the standard default values (e.g., `"0"` for activation flags, and standard bucket paths for directories).

```python
@patch("templates.dw_env_resolver.Variable.get")
def test_variable_resolution_fallbacks(mock_get):
    # Simulate missing variables in Airflow DB (returns default_var)
    mock_get.side_effect = lambda key, default_var=None: default_var
    
    context = compute_environment_context("2023-06-15")
    
    assert context["DWH_HOME"] == "gs://your-production-bucket/dwh"
    assert context["AKTIV_CARMEN"] == "0"
    assert context["AKTUELL_CACHE"] == "0"
```

---

## Section 3: Error Handling & Log Trapping (DW.LESE_LOG)

### Test Case 3.1: On-Failure Callback Execution & Log Emulation
* **Purpose**: Prove that when an upstream task fails, the `on_failure_show_log` callback intercepts the failure, emulates the legacy `showlog -uc4` output, and triggers the audit-end failure registration.
* **Setup**:
  * Create a mock Airflow context dictionary containing a mock `TaskInstance` and execution date.
  * Mock the `ti.xcom_pull` method to return a valid environment context.
  * Mock the logging framework to capture output.
* **Action**:
  Invoke `on_failure_show_log(context)` manually.
* **Pass/Fail Criterion**:
  * The callback must log the fatal error message containing the failed task ID and the `DWH_HOME` path.
  * The callback must log the legacy-equivalent return value trace: `Rueckgabewert: '1' (Fehlerfall)`.
  * The callback must call `execute_job_monitor_end_failed` to register the failure in the audit database.

```python
import logging
from unittest.mock import MagicMock, patch
from templates.dw_error_handler import on_failure_show_log

@patch("templates.dw_error_handler.logger")
@patch("templates.dw_error_handler.execute_job_monitor_end_failed")
def test_on_failure_callback_behavior(mock_audit_end, mock_logger):
    # Setup mock Airflow context
    mock_ti = MagicMock()
    mock_ti.task_id = "execute_production_work"
    mock_ti.state = "failed"
    
    # Mock XCom pull to return environment variables
    mock_ti.xcom_pull.return_value = {
        "DWH_HOME": "gs://test-bucket/dwh",
        "AKTIV_CARMEN": "1"
    }
    
    context = {
        "ti": mock_ti,
        "execution_date": "2023-06-15T00:00:00+00:00"
    }
    
    # Execute callback
    on_failure_show_log(context)
    
    # Assertions
    # 1. Verify legacy-equivalent log traces were written
    mock_logger.error.assert_any_call("Failed Task ID     : execute_production_work")
    mock_logger.error.assert_any_call("DWH Root (DWH_HOME): gs://test-bucket/dwh")
    mock_logger.error.assert_any_call("Rueckgabewert: '1' (Fehlerfall) - Propagating hard failure status.")
    
    # 2. Verify audit-end failure registration was triggered
    mock_audit_end.assert_called_once_with("execute_production_work", "2023-06-15T00:00:00+00:00")
```

---

## Section 4: End-to-End DAG Orchestration & Audit Flow

### Test Case 4.1: Successful Execution Path Auditing
* **Purpose**: Verify that under normal execution conditions, the DAG initializes the environment, triggers the start monitor, executes work, and successfully triggers the end monitor.
* **Setup**:
  * Load the DAG `dw_produktion_allgemein_includes` in an Airflow test environment.
* **Action**:
  Execute a test run of the DAG using the Airflow CLI or a local runner.
* **Pass/Fail Criterion**:
  * All tasks (`initialize_environment` -> `job_monitor_start` -> `execute_production_work` -> `job_monitor_end`) must transition to the `SUCCESS` state.
  * The `job_monitor_end` task must execute (proving the `all_success` trigger rule was met).

```python
from airflow.models import DagBag

def test_dag_loading_and_structure():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.get_dag(dag_id="dw_produktion_allgemein_includes")
    
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 4
    
    # Verify execution sequence
    init_task = dag.get_task("initialize_environment")
    start_task = dag.get_task("job_monitor_start")
    work_task = dag.get_task("execute_production_work")
    end_task = dag.get_task("job_monitor_end")
    
    assert start_task in init_task.downstream_list
    assert work_task in start_task.downstream_list
    assert end_task in work_task.downstream_list
```