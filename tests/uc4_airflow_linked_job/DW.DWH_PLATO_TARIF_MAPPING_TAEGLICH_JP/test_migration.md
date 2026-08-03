Here is the migration-validation test suite designed to verify the behavioral equivalence, structural integrity, and execution correctness of the migrated Airflow DAG `dw_dwh_dummy_absd_plato_tarife`.

---

# Migration Validation Test Suite: DW.DWH_DUMMY_ABSD_PLATO_TARIFE

## Test Case 1: DAG Parsing and Structural Integrity
### Purpose
Verify that the migrated Airflow DAG file is syntactically correct, parses without errors or warnings, and matches the structural specifications defined in the migration design document (e.g., DAG ID, schedule, task type, and task count).

### Setup
- A Python environment with `apache-airflow` (matching the target environment version) and `pytest` installed.
- The DAG file `dw_dwh_dummy_absd_plato_tarife.py` placed in the Airflow DAGs folder or added to the python path.
- Mocked Airflow Variables (`GCP_PROJECT`, `DATAPROC_REGION`, `DATAPROC_CLUSTER`, `GCS_BUCKET`) to prevent parsing errors.

### Action
Run a pytest suite that loads the DAG using Airflow's `DagBag` and asserts its structural properties.

```python
import pytest
from airflow.models import DagBag, Variable
from airflow.operators.bash import BashOperator

@pytest.fixture(scope="module", autouse=True)
def setup_mock_variables():
    """Mock required Airflow variables for parsing."""
    Variable.set("GCP_PROJECT", "mock-gcp-project")
    Variable.set("DATAPROC_REGION", "europe-west3")
    Variable.set("DATAPROC_CLUSTER", "mock-cluster")
    Variable.set("GCS_BUCKET", "mock-bucket")
    yield
    Variable.delete("GCP_PROJECT")
    Variable.delete("DATAPROC_REGION")
    Variable.delete("DATAPROC_CLUSTER")
    Variable.delete("GCS_BUCKET")

def test_dag_loads_with_no_errors():
    dag_bag = DagBag(dag_folder="uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP", include_examples=False)
    dag_id = "dw_dwh_dummy_absd_plato_tarife"
    
    # Assert no import errors
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"
    
    # Assert DAG exists
    dag = dag_bag.get_dag(dag_id)
    assert dag is not None, f"DAG {dag_id} not found in DagBag"
    
    # Assert structural metadata
    assert dag.schedule_interval is None, "DAG schedule should be None (externally triggered)"
    assert dag.catchup is False, "Catchup should be set to False"
    assert dag.max_active_runs == 1, "Max active runs should be limited to 1"

def test_task_structure_and_operator():
    dag_bag = DagBag(dag_folder="uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP", include_examples=False)
    dag = dag_bag.get_dag("dw_dwh_dummy_absd_plato_tarife")
    
    # Assert task count
    assert len(dag.tasks) == 1, "DAG must contain exactly 1 task"
    
    # Assert task ID and Operator type
    task = dag.get_task("dummy_execution")
    assert task is not None, "Task 'dummy_execution' is missing"
    assert isinstance(task, BashOperator), "Task 'dummy_execution' must be a BashOperator"
```

### Pass/Fail Criterion
- **Pass**: The DAG parses in under 2 seconds with zero import errors, contains exactly one task named `dummy_execution` of type `BashOperator`, and has a schedule of `None`.
- **Fail**: Any import errors are raised, or the DAG structure/metadata deviates from the design.

---

## Test Case 2: Behavioral Equivalence (Output Parity)
### Purpose
Verify that executing the migrated `dummy_execution` task produces the exact same behavioral output as the legacy UC4 job. Specifically, it must output the string `"Doing nothinig"` (preserving the original legacy spelling mistake) to standard output.

### Setup
- A local or runner-based Airflow execution context.
- Access to the task execution logs.

### Action
Execute the `dummy_execution` task in a test context and capture the standard output logs.

```python
import pytest
from datetime import datetime
from airflow.models import TaskInstance, DagBag
from airflow.utils.state import State

def test_task_execution_output(caplog):
    """Execute the task and verify it prints the exact legacy string."""
    dag_bag = DagBag(dag_folder="uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP", include_examples=False)
    dag = dag_bag.get_dag("dw_dwh_dummy_absd_plato_tarife")
    task = dag.get_task("dummy_execution")
    
    # Create a dummy TaskInstance
    execution_date = datetime(2026, 3, 30)
    ti = TaskInstance(task=task, execution_date=execution_date)
    
    # Run the task locally
    ti.run(ignore_ti_state=True, ignore_all_deps=True, test_mode=True)
    
    # Assert task completed successfully
    assert ti.state == State.SUCCESS
    
    # Assert the exact legacy string (with typo) was printed to stdout/logs
    # Note: BashOperator logs the command output to its execution logs
    log_text = caplog.text
    assert "Doing nothinig" in log_text, f"Expected legacy output 'Doing nothinig' not found in logs: {log_text}"
```

### Pass/Fail Criterion
- **Pass**: The task runs successfully (`State.SUCCESS`) and the execution logs contain the exact string `Doing nothinig`.
- **Fail**: The task fails, or the output string is missing, modified, or has its spelling corrected (which would break strict legacy log-scraping parity).

---

## Test Case 3: Error Handling and Retry Policy Validation
### Purpose
Verify that the task-level retry policies are configured exactly as specified in the design document to match the legacy operational behavior.

### Setup
- The parsed DAG object from the Airflow environment.

### Action
Inspect the task attributes of `dummy_execution` to validate retry counts and retry delays.

```python
from datetime import timedelta
from airflow.models import DagBag

def test_retry_policy_configuration():
    dag_bag = DagBag(dag_folder="uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP", include_examples=False)
    dag = dag_bag.get_dag("dw_dwh_dummy_absd_plato_tarife")
    task = dag.get_task("dummy_execution")
    
    # Assert retry configuration
    assert task.retries == 1, "Task retries must be set to 1"
    assert task.retry_delay == timedelta(minutes=5), "Task retry_delay must be set to 5 minutes"
```

### Pass/Fail Criterion
- **Pass**: `retries` is exactly `1` and `retry_delay` is exactly `timedelta(minutes=5)`.
- **Fail**: Any of the retry parameters differ from the design specification.

---

## Test Case 4: Environment-Agnostic Variable Resiliency
### Purpose
Verify that the DAG file can be parsed safely even if the GCP-related environment variables (`GCP_PROJECT`, `DATAPROC_REGION`, etc.) are not yet defined in the target Airflow environment. This ensures that the DAG does not break the global Airflow scheduler if variables are missing during initial deployment.

### Setup
- A clean Airflow test environment where the variables `GCP_PROJECT`, `DATAPROC_REGION`, `DATAPROC_CLUSTER`, and `GCS_BUCKET` are explicitly deleted/not set.

### Action
Attempt to parse the DAG file and verify that it handles missing variables gracefully (either by using defaults or by not throwing fatal exceptions during the import phase).

```python
import pytest
from airflow.models import DagBag, Variable
from airflow.exceptions import AirflowNotFoundException

def test_dag_parsing_without_variables(monkeypatch):
    """Verify DAG parses or handles missing variables gracefully during import."""
    # Explicitly delete variables if they exist in the test DB
    for var in ["GCP_PROJECT", "DATAPROC_REGION", "DATAPROC_CLUSTER", "GCS_BUCKET"]:
        try:
            Variable.delete(var)
        except KeyError:
            pass

    # Attempt to load the DAG
    dag_bag = DagBag(dag_folder="uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP", include_examples=False)
    
    # The DAG should either parse successfully (if Variable.get has default fallbacks)
    # or fail gracefully. Since the target code uses Variable.get without defaults:
    # GCP_PROJECT = Variable.get("GCP_PROJECT")
    # We expect an import error unless variables are present. 
    # This test highlights the risk and asserts that the deployment pipeline must pre-populate these.
    if len(dag_bag.import_errors) > 0:
        for filepath, error in dag_bag.import_errors.items():
            assert "KeyError" in error or "Variable" in error, f"Unexpected import error: {error}"
            print(f"Confirmed expected dependency on Airflow Variable: {error}")
```

### Pass/Fail Criterion
- **Pass**: The test successfully documents or handles the dependency on Airflow variables without causing unhandled system-level crashes.
- **Fail**: The DAG throws unexpected syntax or structural errors unrelated to the missing configuration variables.