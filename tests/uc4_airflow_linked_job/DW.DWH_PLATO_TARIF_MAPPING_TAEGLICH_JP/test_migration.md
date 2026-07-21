Here is the migration-validation test suite designed to verify that the migrated Airflow DAG `dw_dwh_dummy_absd_plato_tarife` behaves identically to the legacy UC4 job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE`.

---

# Test Suite: Migration Validation for `DW.DWH_DUMMY_ABSD_PLATO_TARIFE`

This test suite ensures that the migrated Airflow DAG is structurally sound, preserves all metadata, handles environment variables correctly, and executes the exact character-for-character log output of the legacy UC4 job.

---

## Test Case 1: DAG Import and Structural Integrity

### Purpose
Verify that the migrated Airflow DAG file is syntactically correct, can be parsed by the Airflow `DagBag` without import errors, and maintains the exact task structure, dependencies, and default configurations defined in the migration design.

### Setup
*   Ensure the target file `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW_DWH_DUMMY_ABSD_PLATO_TARIFE.py` is in the Airflow DAGs search path.
*   Install `pytest` and `apache-airflow` in the test environment.

### Action
Run a pytest execution that loads the DAG via `DagBag` and asserts its structural properties.

```python
import pytest
from airflow.models import DagBag

def test_dag_import_and_structure():
    # Load the DAG bag
    dag_bag = DagBag(dag_folder="uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP", include_examples=False)
    
    # 1. Assert no import errors occurred
    assert len(dag_bag.import_errors) == 0, f"DAG import failures: {dag_bag.import_errors}"
    
    # 2. Assert DAG exists
    dag_id = "dw_dwh_dummy_absd_plato_tarife"
    assert dag_id in dag_bag.dags, f"DAG {dag_id} not found in DagBag"
    
    dag = dag_bag.get_dag(dag_id)
    
    # 3. Assert DAG Properties
    assert dag.schedule_interval is None, "Schedule interval should be None (manual execution)"
    assert dag.catchup is False, "Catchup should be set to False"
    assert dag.max_active_runs == 1, "Max active runs should be constrained to 1"
    assert "migrated_uc4" in dag.tags, "Missing 'migrated_uc4' tag"
    assert "dummy_task" in dag.tags, "Missing 'dummy_task' tag"
    
    # 4. Assert Task Inventory
    expected_tasks = {"start", "dwh_dummy_absd_plato_tarife", "end"}
    actual_tasks = set(dag.task_ids)
    assert actual_tasks == expected_tasks, f"Task mismatch. Expected {expected_tasks}, got {actual_tasks}"
    
    # 5. Assert Task Dependencies (start >> dwh_dummy_absd_plato_tarife >> end)
    start_task = dag.get_task("start")
    dummy_task = dag.get_task("dwh_dummy_absd_plato_tarife")
    end_task = dag.get_task("end")
    
    assert dummy_task.task_id in [t.task_id for t in start_task.downstream_list]
    assert end_task.task_id in [t.task_id for t in dummy_task.downstream_list]
```

### Pass/Fail Criterion
*   **Pass**: The DAG imports with zero errors, contains exactly the tasks `start`, `dwh_dummy_absd_plato_tarife`, and `end` in sequential order, and matches all metadata properties.
*   **Fail**: Any import errors are raised, tasks are missing, or dependencies are incorrectly wired.

---

## Test Case 2: Output Parity (Log Execution Verification)

### Purpose
Verify that the `dwh_dummy_absd_plato_tarife` task executes and outputs the exact string `"Doing nothinig"` (preserving the legacy typo) to standard output, matching the legacy UC4 `:print Doing nothinig` command.

### Setup
*   Initialize a local Airflow metadata database or mock the execution context.
*   Set up a test runner to execute the specific task instance.

### Action
Execute the `dwh_dummy_absd_plato_tarife` task locally and capture standard output to verify the print literal.

```python
import pytest
from datetime import datetime
from airflow.models import DagBag, TaskInstance
from airflow.utils.state import TaskInstanceState

def test_output_parity_log_execution(capsys):
    dag_bag = DagBag(dag_folder="uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP", include_examples=False)
    dag = dag_bag.get_dag("dw_dwh_dummy_absd_plato_tarife")
    task = dag.get_task("dwh_dummy_absd_plato_tarife")
    
    # Create a dummy execution context
    execution_date = datetime(2026, 3, 30)
    ti = TaskInstance(task=task, execution_date=execution_date)
    
    # Run the task directly
    ti.run(ignore_ti_state=True, ignore_all_deps=True, test_mode=True)
    
    # Capture standard output/error
    captured = capsys.readouterr()
    
    # Assert task completed successfully
    assert ti.state == TaskInstanceState.SUCCESS
    
    # Assert the exact print literal is present in the execution output
    # Note: BashOperator outputs the command execution to logs
    assert "Doing nothinig" in captured.out or "Doing nothinig" in captured.err, \
        "The exact legacy print statement 'Doing nothinig' was not found in the execution logs."
```

### Pass/Fail Criterion
*   **Pass**: The task runs successfully and outputs the exact string `"Doing nothinig"` to the execution log.
*   **Fail**: The task fails to execute, or the output string does not match the legacy typo character-for-character.

---

## Test Case 3: Metadata and Recovery Documentation Verification

### Purpose
Verify that the German recovery documentation (`Wiederanlauf ohne weitere Maßnahmen möglich`) is preserved exactly inside the DAG's markdown documentation (`doc_md`) to comply with the operational preservation rules.

### Setup
*   Load the DAG via `DagBag`.

### Action
Assert that the `doc_md` attribute of the DAG object contains the exact recovery string.

```python
import pytest
from airflow.models import DagBag

def test_recovery_documentation_preservation():
    dag_bag = DagBag(dag_folder="uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP", include_examples=False)
    dag = dag_bag.get_dag("dw_dwh_dummy_absd_plato_tarife")
    
    expected_recovery_phrase = "Wiederanlauf ohne weitere Maßnahmen möglich"
    
    assert dag.doc_md is not None, "DAG doc_md is empty or missing"
    assert expected_recovery_phrase in dag.doc_md, \
        f"Expected recovery phrase '{expected_recovery_phrase}' not found in DAG doc_md."
```

### Pass/Fail Criterion
*   **Pass**: The DAG's `doc_md` contains the exact German recovery phrase.
*   **Fail**: The `doc_md` is missing or does not contain the exact phrase.

---

## Test Case 4: Environment Variable Sourcing & Fallback Robustness

### Purpose
Verify that the DAG retrieves environment variables (`GCP_PROJECT`, `GCP_REGION`, `DATAPROC_CLUSTER`, `GCS_BUCKET`) via `Variable.get` and handles missing variables gracefully using `default_var=None` without throwing parsing exceptions.

### Setup
*   Clear any existing Airflow Variables in the test environment to simulate a clean bootstrap environment.

### Action
Parse the DAG file and verify that the variables default to `None` instead of raising a `KeyError`. Then, set the variables and verify they are correctly read.

```python
import pytest
from unittest.mock import patch
from airflow.models import DagBag, Variable

def test_variable_sourcing_and_fallbacks():
    # 1. Test with missing variables (should fallback to None gracefully)
    with patch.object(Variable, 'get', return_value=None) as mock_get:
        dag_bag = DagBag(dag_folder="uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP", include_examples=False)
        assert len(dag_bag.import_errors) == 0, "DAG failed to import when Airflow Variables were missing"
        
        # Verify Variable.get was called for the expected environment keys
        called_keys = [call[0][0] for call in mock_get.call_args_list]
        assert "GCP_PROJECT" in called_keys
        assert "GCP_REGION" in called_keys
        assert "DATAPROC_CLUSTER" in called_keys
        assert "GCS_BUCKET" in called_keys

    # 2. Test with populated variables
    mock_vars = {
        "GCP_PROJECT": "prod-gcp-project",
        "GCP_REGION": "europe-west3",
        "DATAPROC_CLUSTER": "dwh-dataproc-cluster",
        "GCS_BUCKET": "dwh-gcs-bucket"
    }
    
    def side_effect(key, default_var=None):
        return mock_vars.get(key, default_var)
        
    with patch.object(Variable, 'get', side_effect=side_effect):
        dag_bag = DagBag(dag_folder="uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP", include_examples=False)
        assert len(dag_bag.import_errors) == 0
```

### Pass/Fail Criterion
*   **Pass**: The DAG parses successfully both when variables are completely absent (returning `None`) and when they are present.
*   **Fail**: The DAG raises a `KeyError` or fails to import when variables are missing from the environment.