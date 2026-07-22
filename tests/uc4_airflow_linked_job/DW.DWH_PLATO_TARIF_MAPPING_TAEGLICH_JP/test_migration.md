# Migration Validation Test Suite: DW.DWH_DUMMY_ABSD_PLATO_TARIFE

This document defines the migration-validation test suite to verify that the migrated Airflow DAG and PySpark job behave identically to the legacy UC4 UNIX job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE`.

---

## Test Case 1: DAG Structure and Metadata Validation

### Purpose
To verify that the migrated Airflow DAG is parsed without errors, contains the correct task sequence, matches the legacy metadata constraints (e.g., no retries, active status), and correctly maps dependencies.

### Setup
* A Python environment with `apache-airflow` installed.
* The DAG file `dw_dwh_dummy_absd_plato_tarife.py` placed in the Airflow DAGs directory or added to the python path.
* Mocked Airflow Variables to prevent database lookup failures during parsing.

### Action
Run a pytest suite that loads the DAG using Airflow’s `DagBag` and asserts its structure, parameters, and task dependencies.

```python
import pytest
from airflow.models import DagBag, Variable
from airflow.utils.dag_cycle_tester import check_cycle

@pytest.fixture(autouse=True)
def mock_airflow_variables(monkeypatch):
    """Mock Airflow Variables required by the DAG during import."""
    mock_vars = {
        "GCP_PROJECT": "test-gcp-project",
        "GCP_REGION": "europe-west3",
        "DATAPROC_CLUSTER": "test-dataproc-cluster",
        "GCS_BUCKET": "test-gcs-bucket"
    }
    monkeypatch.setattr(Variable, "get", lambda key, default_var=None: mock_vars.get(key, default_var))

def test_dag_imports_and_structure():
    # Load the DAG
    dagbag = DagBag(dag_folder="uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP", include_examples=False)
    
    # 1. Assert no import errors
    assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"
    
    dag_id = "dw_dwh_dummy_absd_plato_tarife"
    dag = dagbag.get_dag(dag_id)
    
    # 2. Assert DAG exists
    assert dag is not None, f"DAG {dag_id} not found in DagBag"
    
    # 3. Assert cycle-free
    check_cycle(dag)
    
    # 4. Assert Metadata Parity
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.schedule_interval is None  # Handled by parent workflow
    
    # 5. Assert Default Args
    assert dag.default_args.get('retries') == 0
    assert dag.default_args.get('owner') == 'airflow'
    
    # 6. Assert Task Inventory
    expected_tasks = {"start", "dw_dwh_dummy_absd_plato_tarife", "end"}
    assert set(dag.task_ids) == expected_tasks
    
    # 7. Assert Dependency Map (start -> dummy -> end)
    start_task = dag.get_task("start")
    dummy_task = dag.get_task("dw_dwh_dummy_absd_plato_tarife")
    end_task = dag.get_task("end")
    
    assert dummy_task in start_task.downstream_list
    assert end_task in dummy_task.downstream_list
```

### Pass/Fail Criterion
* **Pass**: The test suite executes successfully with zero import errors, confirming the DAG structure, task dependencies, and metadata match the legacy specification exactly.
* **Fail**: Any import error occurs, tasks are missing, or dependencies do not match `start >> dw_dwh_dummy_absd_plato_tarife >> end`.

---

## Test Case 2: PySpark Script Execution and Output Parity

### Purpose
To verify that the migrated PySpark script executes successfully and outputs the exact misspelled literal `"Doing nothinig"` to standard output, preserving the legacy execution behavior.

### Setup
* A Python execution environment.
* Access to the script `dw_dwh_dummy_absd_plato_tarife_job.py`.

### Action
Execute the PySpark script as a subprocess, capturing standard output (`stdout`) and standard error (`stderr`), and assert the exit code and output string.

```python
import subprocess
import sys
import os

def test_pyspark_script_output_parity():
    script_path = "uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife_job.py"
    
    # Ensure script exists
    assert os.path.exists(script_path), f"Script not found at {script_path}"
    
    # Execute the script using the current Python interpreter
    result = subprocess.run(
        [sys.executable, script_path],
        capture_output=True,
        text=True
    )
    
    # Assert exit code is 0 (success)
    assert result.returncode == 0, f"Script failed with exit code {result.returncode}. Stderr: {result.stderr}"
    
    # Assert exact output parity (including the legacy typo "nothinig")
    expected_output = "Doing nothinig\n"
    assert result.stdout == expected_output, f"Expected output '{expected_output}', but got '{result.stdout}'"
```

### Pass/Fail Criterion
* **Pass**: The script exits with code `0` and writes exactly `"Doing nothinig\n"` to standard output.
* **Fail**: The script exits with a non-zero code, or the output does not match the legacy string verbatim.

---

## Test Case 3: Dataproc Operator Configuration & Variable Resolution

### Purpose
To verify that the `DataprocSubmitJobOperator` resolves Airflow Variables correctly and builds the exact job configuration payload required to run on GCP Dataproc.

### Setup
* A Python environment with `apache-airflow` and `google-cloud-dataproc` installed.
* Mocked Airflow Variables representing target environment configurations.

### Action
Instantiate the DAG and inspect the `pyspark_job` configuration of the `DataprocSubmitJobOperator` task.

```python
from airflow.models import DagBag, Variable

def test_dataproc_operator_configuration(monkeypatch):
    # Mock environment variables
    mock_vars = {
        "GCP_PROJECT": "prod-gcp-project-123",
        "GCP_REGION": "europe-west1",
        "DATAPROC_CLUSTER": "dwh-dataproc-cluster-prod",
        "GCS_BUCKET": "dwh-migration-bucket-prod"
    }
    monkeypatch.setattr(Variable, "get", lambda key, default_var=None: mock_vars.get(key, default_var))
    
    dagbag = DagBag(dag_folder="uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP", include_examples=False)
    dag = dagbag.get_dag("dw_dwh_dummy_absd_plato_tarife")
    task = dag.get_task("dw_dwh_dummy_absd_plato_tarife")
    
    # Verify operator properties
    assert task.project_id == "prod-gcp-project-123"
    assert task.region == "europe-west1"
    
    # Verify job configuration payload
    job_config = task.job
    assert job_config['reference']['project_id'] == "prod-gcp-project-123"
    assert job_config['placement']['cluster_name'] == "dwh-dataproc-cluster-prod"
    
    pyspark_job = job_config['pyspark_job']
    expected_uri = "gs://dwh-migration-bucket-prod/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife_job.py"
    assert pyspark_job['main_python_file_uri'] == expected_uri
    assert pyspark_job['args'] == ['--note', 'placeholder_for_dummy_execution']
```

### Pass/Fail Criterion
* **Pass**: The operator resolves all global variables correctly and constructs the `main_python_file_uri` pointing to the correct GCS path.
* **Fail**: Any variable fails to resolve, or the job configuration contains incorrect paths or parameters.

---

## Test Case 4: End-to-End Integration & Log Verification

### Purpose
To verify that when the DAG is executed in a Cloud Composer environment, the Dataproc job completes successfully and the driver logs capture the exact legacy print statement.

### Setup
* Access to a GCP test environment with Cloud Composer and Dataproc configured.
* The DAG and PySpark script deployed to their respective GCS paths.

### Action
1. Trigger the DAG `dw_dwh_dummy_absd_plato_tarife` via the Airflow CLI or UI.
2. Wait for the DAG run to complete.
3. Retrieve the execution logs for the task `dw_dwh_dummy_absd_plato_tarife`.
4. Assert the task state and verify the log output.

```python
# Note: This integration test is designed to run against a live GCP environment
import pytest
import time
from google.cloud import storage
from google.cloud import dataproc_v1 as dataproc

# This test is marked as integration and skipped in local unit test runs
@pytest.mark.integration
def test_gcp_e2e_execution_and_logs():
    # These should be retrieved from the test environment configuration
    project_id = "YOUR_TEST_GCP_PROJECT"
    region = "YOUR_TEST_GCP_REGION"
    
    # Initialize Dataproc Job Controller Client
    job_client = dataproc.JobControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com"}
    )
    
    # List jobs in the region to find the executed dummy task
    # (Assuming the DAG was triggered and completed)
    jobs = job_client.list_jobs(project_id=project_id, region=region)
    
    dummy_job = None
    for job in jobs:
        if "dw_dwh_dummy_absd_plato_tarife" in job.reference.job_id:
            dummy_job = job
            break
            
    assert dummy_job is not None, "No executed dummy job found on Dataproc cluster."
    
    # Verify Dataproc Job Status
    assert dummy_job.status.state == dataproc.JobStatus.State.DONE, \
        f"Job state is not DONE: {dummy_job.status.state}"
        
    # Retrieve driver output from GCS
    driver_output_uri = dummy_job.driver_output_resource_uri
    # Parse bucket and blob from URI (e.g., gs://bucket/google-dataproc-media/...)
    bucket_name = driver_output_uri.split("/")[2]
    blob_name = "/".join(driver_output_uri.split("/")[3:]) + ".000000000"
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    logs = blob.download_as_text()
    
    # Assert the exact output is present in the Dataproc driver logs
    assert "Doing nothinig" in logs, f"Legacy print statement not found in logs: {logs}"
```

### Pass/Fail Criterion
* **Pass**: The Dataproc job completes with state `DONE` and the driver logs contain the exact string `"Doing nothinig"`.
* **Fail**: The job fails, times out, or the driver logs do not contain the expected print statement.