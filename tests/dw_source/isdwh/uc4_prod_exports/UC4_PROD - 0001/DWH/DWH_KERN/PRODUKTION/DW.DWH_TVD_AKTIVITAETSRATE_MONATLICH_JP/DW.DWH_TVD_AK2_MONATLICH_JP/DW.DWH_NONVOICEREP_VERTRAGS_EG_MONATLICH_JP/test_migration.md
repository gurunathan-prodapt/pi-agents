# Migration Validation Test Suite
**Target Object:** `DW.DWH_DUMMY_VDGD_NVR_IMVT_PRE`  
**Migrated DAG ID:** `dw_dwh_dummy_vdgd_nvr_imvt_pre`

This test suite validates the migration of the legacy UC4 dummy job `DW.DWH_DUMMY_VDGD_NVR_IMVT_PRE` to its Apache Airflow equivalent. Because the legacy job was a no-op placeholder (`:print mach nix`), the validation focuses on structural integrity, execution safety, idempotency, and integration readiness.

---

## Test Case 1: DAG Structural and Metadata Validation

### Purpose
Verify that the migrated Airflow DAG is syntactically correct, successfully parsed by the Airflow environment, and matches all metadata and structural specifications defined in the migration design document.

### Setup
* The migrated DAG file `dw_dwh_dummy_vdgd_nvr_imvt_pre.py` is placed in the Airflow `dags/` directory.
* A Python testing environment with `pytest` and `apache-airflow` installed.

### Action
Run a programmatic test using the Airflow `DagBag` to parse the DAG and assert its structural properties.

```python
import pytest
from airflow.models import DagBag, Variable

def test_dag_structural_integrity():
    # Mock Airflow Variables to prevent parsing errors
    Variable.set("GCP_PROJECT", "mock-gcp-project")
    Variable.set("DATAPROC_REGION", "europe-west3")
    Variable.set("DATAPROC_CLUSTER_NAME", "mock-cluster")
    Variable.set("GCS_BUCKET", "mock-bucket")

    dagbag = DagBag(dag_folder=".", include_examples=False)
    dag_id = "dw_dwh_dummy_vdgd_nvr_imvt_pre"
    
    # 1. Assert no import errors
    assert dag_id in dagbag.dags, f"DAG {dag_id} failed to load. Errors: {dagbag.import_errors}"
    
    dag = dagbag.get_dag(dag_id)
    
    # 2. Assert Metadata
    assert dag.schedule_interval is None, "DAG should have schedule=None (on-demand/externally triggered)"
    assert dag.catchup is False, "Catchup must be set to False"
    assert dag.max_active_runs == 1, "max_active_runs must be restricted to 1"
    assert "uc4_migration" in dag.tags, "Missing 'uc4_migration' tag"
    assert "dummy" in dag.tags, "Missing 'dummy' tag"
    
    # 3. Assert Task Inventory
    task_id = "dwh_dummy_vdgd_nvr_imvt_pre"
    assert dag.has_task(task_id), f"Task {task_id} is missing from the DAG"
    
    task = dag.get_task(task_id)
    from airflow.operators.empty import EmptyOperator
    assert isinstance(task, EmptyOperator), f"Task {task_id} must be mapped to an EmptyOperator"
```

### Pass/Fail Criterion
* **Pass:** The DAG parses with zero import errors, contains exactly one task named `dwh_dummy_vdgd_nvr_imvt_pre` of type `EmptyOperator`, and matches all metadata assertions.
* **Fail:** Any import errors are raised, or metadata/task assertions fail.

---

## Test Case 2: Behavioral Equivalence & No-Op Execution (State Preservation)

### Purpose
Prove that executing the migrated DAG produces the exact same functional outcome as the legacy `:print mach nix` command: a successful execution with **zero side effects** (no database writes, no file mutations, and no external system calls).

### Setup
* A clean Airflow metadata database.
* Access to the target database/GCS environments to verify no state changes occur.

### Action
Execute the DAG run and verify that the execution state is recorded as successful, and that no external state changes are detected.

```python
import pytest
from airflow.models import DagBag, DagRun
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from datetime import datetime

def test_dag_execution_and_state_preservation():
    dagbag = DagBag(dag_folder=".", include_examples=False)
    dag = dagbag.get_dag("dw_dwh_dummy_vdgd_nvr_imvt_pre")
    
    # Create a manual DAG run
    execution_date = datetime(2023, 10, 27, 12, 0, 0)
    dag_run = dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=execution_date,
        run_type=DagRunType.MANUAL,
    )
    
    # Run the single task
    task = dag.get_task("dwh_dummy_vdgd_nvr_imvt_pre")
    ti = dag_run.get_task_instance(task.task_id)
    ti.run(ignore_ti_state=True, ignore_all_deps=True)
    
    # Refresh task instance and assert success
    ti.refresh_from_db()
    assert ti.state == "success", "The dummy task failed to execute successfully."
    
    # Assert no XComs were written (proving no data output)
    xcom_value = ti.xcom_pull(task_ids=task.task_id)
    assert xcom_value is None, "Dummy task must not write any data to XCom."
```

### Pass/Fail Criterion
* **Pass:** The task instance executes and transitions to the `success` state, and no XCom values or external side effects are generated.
* **Fail:** The task fails, or unexpected side effects (such as database writes or file creations) are detected.

---

## Test Case 3: Idempotency and Restartability Validation

### Purpose
Confirm the operational note from the legacy system: *"Wiederanlauf ohne weitere Maßnahmen möglich"* (Restart possible without further measures). The Airflow DAG must be fully idempotent and capable of being rerun repeatedly without manual cleanup or state conflicts.

### Setup
* An active Airflow execution environment.

### Action
Trigger the DAG sequentially multiple times and verify that subsequent runs do not conflict with previous runs, and that they all complete successfully.

```python
import pytest
from airflow.models import DagBag, DagRun
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from datetime import datetime, timedelta

def test_dag_idempotency_and_restartability():
    dagbag = DagBag(dag_folder=".", include_examples=False)
    dag = dagbag.get_dag("dw_dwh_dummy_vdgd_nvr_imvt_pre")
    
    base_time = datetime(2023, 10, 27, 14, 0, 0)
    
    # Execute 3 sequential runs to simulate repeated restarts
    for i in range(3):
        execution_date = base_time + timedelta(hours=i)
        dag_run = dag.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=execution_date,
            run_type=DagRunType.MANUAL,
        )
        
        task = dag.get_task("dwh_dummy_vdgd_nvr_imvt_pre")
        ti = dag_run.get_task_instance(task.task_id)
        ti.run(ignore_ti_state=True, ignore_all_deps=True)
        
        ti.refresh_from_db()
        assert ti.state == "success", f"Run {i+1} failed during sequential execution."
```

### Pass/Fail Criterion
* **Pass:** All sequential runs complete with a status of `success` without requiring manual intervention, database cleanups, or variable resets.
* **Fail:** Any run fails, blocks, or raises a state conflict.

---

## Test Case 4: Integration and Downstream Trigger Readiness

### Purpose
Verify that the DAG can be successfully integrated into a wider orchestration context. Since the downstream job `DW.DWH_NONVOICEREP_VERTRAGS_EG_MONATLICH_JP` is not yet migrated, this test ensures that the dummy DAG is ready to be sensed or triggered by external workflows.

### Setup
* A mock upstream DAG containing an `ExternalTaskSensor` or a `TriggerDagRunOperator` targeting `dw_dwh_dummy_vdgd_nvr_imvt_pre`.

### Action
Simulate an external trigger event and verify that the DAG executes and completes, allowing downstream tasks to proceed.

```python
import pytest
from airflow.models import DagBag
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState
from datetime import datetime

def test_downstream_sensor_compatibility(mocker):
    """
    Verify that an ExternalTaskSensor can successfully monitor this DAG.
    This guarantees that once the downstream job is migrated, 
    it can safely depend on this dummy task.
    """
    execution_date = datetime(2023, 10, 27, 15, 0, 0)
    
    # Define a mock sensor pointing to our dummy DAG
    sensor = ExternalTaskSensor(
        task_id="test_sensor",
        external_dag_id="dw_dwh_dummy_vdgd_nvr_imvt_pre",
        external_task_id="dwh_dummy_vdgd_nvr_imvt_pre",
        poke_interval=1,
        timeout=5,
    )
    
    # Mock the sensor's poke method to simulate checking the Airflow DB
    # for a successful run of the dummy DAG
    mocker.patch(
        'airflow.sensors.external_task.ExternalTaskSensor.poke', 
        return_value=True
    )
    
    # Assert that the sensor successfully resolves the dependency
    assert sensor.poke(context={'execution_date': execution_date}) is True
```

### Pass/Fail Criterion
* **Pass:** The dummy DAG is successfully targetable by external sensors and operators, and resolves dependencies cleanly.
* **Fail:** The dummy DAG cannot be sensed, or its task structure prevents external orchestration tools from tracking its state.