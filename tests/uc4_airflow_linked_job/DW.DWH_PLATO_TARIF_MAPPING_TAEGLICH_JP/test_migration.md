# Migration Validation Test Suite
**Job Name:** DW.DWH_DUMMY_ABSD_PLATO_TARIFE  
**Target DAG ID:** `dw_dwh_plato_tarif_mapping_taeglich_jp`

This test suite contains automated validation tests to prove that the migrated Airflow DAG behaves identically to the legacy UC4 Unix job. Since the legacy job is a dummy synchronization anchor, the validation focuses on print-literal compliance, DAG structure, environment variable integration, and execution integrity.

---

## Test Case 1: Print Literal and Output Parity Validation

### Purpose
To verify that the migrated task outputs the exact string `"Doing nothinig"` (preserving the original spelling mistake character-for-character) to both standard output and the Airflow task logs, matching the legacy UC4 `:print Doing nothinig` action.

### Setup
* A Python testing environment with `pytest` and `pytest-mock` installed.
* The target DAG file `DW.DWH_DUMMY_ABSD_PLATO_TARIFE.py` must be in the Python path.
* Airflow variables mocked to prevent initialization errors.

### Action
Execute the Python callable `execute_dummy_job` inside a captured standard output and logging context, then assert the output contents.

### Code Implementation
```python
import logging
import sys
from io import StringIO
import pytest
from unittest.mock import patch

# Mock Airflow Variables before importing the DAG to avoid KeyError
with patch('airflow.models.Variable.get') as mock_variable_get:
    mock_variable_get.side_effect = lambda key: f"mocked_{key.lower()}"
    from uc4_airflow_linked_job.DW_DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.DW_DWH_DUMMY_ABSD_PLATO_TARIFE import execute_dummy_job

def test_print_literal_parity(caplog):
    """
    Verify that the dummy job prints and logs the exact string 'Doing nothinig'
    preserving the original legacy spelling mistake.
    """
    # Capture standard output
    captured_stdout = StringIO()
    sys.stdout = captured_stdout

    try:
        with caplog.at_level(logging.INFO):
            execute_dummy_job()
    finally:
        # Restore standard output
        sys.stdout = sys.__stdout__

    # 1. Assert Standard Output Parity
    stdout_val = captured_stdout.getvalue().strip()
    assert stdout_val == "Doing nothinig", f"Expected stdout 'Doing nothinig', got '{stdout_val}'"

    # 2. Assert Logging Output Parity
    log_messages = [record.message for record in caplog.records if record.levelno == logging.INFO]
    assert "Doing nothinig" in log_messages, f"Expected 'Doing nothinig' in logs, found: {log_messages}"
```

### Pass/Fail Criterion
* **Pass:** The standard output and the log records contain the exact string `"Doing nothinig"`.
* **Fail:** The string is missing, modified, or corrected (e.g., "Doing nothing").

---

## Test Case 2: Airflow DAG Structure and Metadata Validation

### Purpose
To verify that the DAG is correctly configured with the specified metadata (ID, schedule, catchup, active status) and that the task dependency chain matches the legacy design (`start >> dw_dwh_dummy_absd_plato_tarife >> end`).

### Setup
* Access to the Airflow DAG parsing context.
* Airflow Variables mocked to return dummy values.

### Action
Load the DAG using Airflow's `DagBag` and inspect its properties and task relationships.

### Code Implementation
```python
import pytest
from unittest.mock import patch
from airflow.models import DagBag

@pytest.fixture(scope="module")
def dagbag():
    # Mock Airflow Variables during DAG parsing
    with patch('airflow.models.Variable.get') as mock_variable_get:
        mock_variable_get.side_effect = lambda key: f"mocked_{key.lower()}"
        # Load the DAG file
        db = DagBag(dag_folder="uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP", include_examples=False)
        return db

def test_dag_structure_and_metadata(dagbag):
    """
    Verify DAG properties, task IDs, and execution flow.
    """
    dag_id = 'dw_dwh_plato_tarif_mapping_taeglich_jp'
    dag = dagbag.get_dag(dag_id)
    
    # 1. Verify DAG exists
    assert dag is not None, f"DAG {dag_id} failed to load. Errors: {dagbag.import_errors}"
    
    # 2. Verify Metadata
    assert dag.schedule_interval is None, "DAG schedule must be None (triggered dynamically)"
    assert dag.catchup is False, "DAG catchup must be False"
    assert dag.max_active_runs == 1, "DAG max_active_runs must be 1"
    assert dag.is_paused_upon_creation is False, "DAG must not be paused upon creation (Active=1 in UC4)"
    
    # 3. Verify Task Inventory
    expected_tasks = {'start', 'dw_dwh_dummy_absd_plato_tarife', 'end'}
    actual_tasks = set(dag.task_ids)
    assert actual_tasks == expected_tasks, f"Task mismatch. Expected {expected_tasks}, got {actual_tasks}"
    
    # 4. Verify Task Dependencies (start >> dummy >> end)
    start_task = dag.get_task('start')
    dummy_task = dag.get_task('dw_dwh_dummy_absd_plato_tarife')
    end_task = dag.get_task('end')
    
    assert dummy_task.task_id in [t.task_id for t in start_task.downstream_list], "start must precede dummy task"
    assert end_task.task_id in [t.task_id for t in dummy_task.downstream_list], "dummy task must precede end"
```

### Pass/Fail Criterion
* **Pass:** The DAG loads without import errors, matches all metadata assertions, and preserves the exact linear execution sequence.
* **Fail:** Any metadata mismatch, missing tasks, or incorrect dependency ordering.

---

## Test Case 3: Environment Variable Integration Validation

### Purpose
To verify that the DAG does not contain hardcoded environment values or prose placeholders (e.g., `"YOUR_GCP_PROJECT_ID"`), and instead dynamically retrieves global environment variables from Airflow Variables at runtime.

### Setup
* A clean test environment where Airflow Variables are not pre-configured.

### Action
Attempt to load the DAG and assert that it dynamically calls `Variable.get` for `GCP_PROJECT`, `GCP_REGION`, and `GCS_BUCKET`.

### Code Implementation
```python
import pytest
from unittest.mock import patch, MagicMock

def test_environment_variable_resolution():
    """
    Verify that the DAG dynamically retrieves GCP configurations from Airflow Variables
    and does not contain hardcoded fallback values.
    """
    # Track calls to Variable.get
    with patch('airflow.models.Variable.get') as mock_get:
        mock_get.side_effect = lambda key: f"env_value_for_{key}"
        
        # Import the module to trigger global variable evaluation
        import uc4_airflow_linked_job.DW_DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.DW_DWH_DUMMY_ABSD_PLATO_TARIFE as dag_module
        
        # Assert that the variables were fetched dynamically
        mock_get.assert_any_call("GCP_PROJECT")
        mock_get.assert_any_call("GCP_REGION")
        mock_get.assert_any_call("GCS_BUCKET")
        
        # Assert that the module-level variables hold the dynamically fetched values
        assert dag_module.GCP_PROJECT_ID == "env_value_for_GCP_PROJECT"
        assert dag_module.GCP_REGION == "env_value_for_GCP_REGION"
        assert dag_module.GCS_BUCKET_NAME == "env_value_for_GCS_BUCKET"
```

### Pass/Fail Criterion
* **Pass:** The DAG successfully resolves `GCP_PROJECT_ID`, `GCP_REGION`, and `GCS_BUCKET_NAME` via `Variable.get()` calls without hardcoded fallbacks.
* **Fail:** The DAG uses hardcoded strings or fails to call `Variable.get()`.

---

## Test Case 4: Task Execution and Idempotency Validation

### Purpose
To verify that the `dw_dwh_dummy_absd_plato_tarife` task executes successfully, behaves idempotently (can be run repeatedly without side effects), and completes within a minimal runtime (verifying that it does not spin up expensive Dataproc clusters).

### Setup
* Initialize a local Airflow metadata database or mock the execution context.
* Create a dummy DAG run and Task Instance.

### Action
Run the `dw_dwh_dummy_absd_plato_tarife` task instance twice in succession and measure execution status and duration.

### Code Implementation
```python
import time
import pytest
from unittest.mock import patch
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from airflow.utils import timezone

@pytest.mark.integration
def test_task_execution_and_idempotency():
    """
    Verify that executing the PythonOperator task is successful, fast, and idempotent.
    """
    with patch('airflow.models.Variable.get') as mock_variable_get:
        mock_variable_get.side_effect = lambda key: f"mocked_{key.lower()}"
        from uc4_airflow_linked_job.DW_DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP.DW_DWH_DUMMY_ABSD_PLATO_TARIFE import dag
        
    task = dag.get_task('dw_dwh_dummy_absd_plato_tarife')
    
    # Create a mock DagRun and TaskInstance context
    now = timezone.utcnow()
    dag_run = dag.create_dagrun(
        state=State.RUNNING,
        execution_date=now,
        data_interval=(now, now),
        start_date=now,
        run_type=DagRunType.MANUAL
    )
    
    ti = dag_run.get_task_instance(task_id='dw_dwh_dummy_absd_plato_tarife')
    ti.task = task
    
    # Run 1: Verify Success and Speed
    start_time = time.time()
    ti.run(ignore_ti_state=True, ignore_all_deps=True)
    duration_run_1 = time.time() - start_time
    
    assert ti.state == State.SUCCESS, "First execution failed"
    assert duration_run_1 < 2.0, f"Execution took too long ({duration_run_1}s). Ensure no Dataproc clusters are being provisioned."
    
    # Run 2: Verify Idempotency (running again has no side effects and succeeds)
    start_time_2 = time.time()
    ti.run(ignore_ti_state=True, ignore_all_deps=True)
    duration_run_2 = time.time() - start_time_2
    
    assert ti.state == State.SUCCESS, "Second execution failed"
    assert duration_run_2 < 2.0, "Second execution took too long"
```

### Pass/Fail Criterion
* **Pass:** Both task executions complete with a state of `SUCCESS` in under 2 seconds.
* **Fail:** The task fails, throws an exception, or takes a significant amount of time (indicating cluster provisioning or external network calls).