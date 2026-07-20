Here is the migration-validation test suite designed to verify the behavioral equivalence, structural integrity, and deployment correctness of the migrated `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` job.

---

# Test Suite: DW.DWH_DUMMY_ABSD_PLATO_TARIFE Migration Validation

## Test Case 1: Verbatim Output Parity (Companion Script Validation)

### Purpose
To prove that the companion Python script `dw_dwh_dummy_absd_plato_tarife.py` produces the exact output of the legacy UC4 script, preserving the specific typo `"Doing nothinig"`, and exits cleanly with code `0`.

### Setup
* A Python 3.x environment.
* The companion script `dw_dwh_dummy_absd_plato_tarife.py` is available locally at the path `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py`.

### Action
Execute the companion script as a subprocess, capturing standard output (`stdout`), standard error (`stderr`), and the exit code.

```python
import subprocess
import sys
from pathlib import Path

def test_companion_script_output_parity():
    # Path to the migrated companion script
    script_path = Path("uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py")
    
    assert script_path.exists(), f"Companion script not found at {script_path}"

    # Execute the script
    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True
    )

    # Assertions
    assert result.returncode == 0, f"Script failed with exit code {result.returncode}. Stderr: {result.stderr}"
    assert result.stdout == "Doing nothinig\n", f"Output mismatch. Expected 'Doing nothinig\\n', got {repr(result.stdout)}"
    assert result.stderr == "", f"Expected empty stderr, got: {result.stderr}"
```

### Pass/Fail Criterion
* **Pass**: The script exits with code `0`, writes exactly `"Doing nothinig\n"` to `stdout`, and writes nothing to `stderr`.
* **Fail**: Any non-zero exit code, any output in `stderr`, or any deviation from the literal string `"Doing nothinig\n"` in `stdout`.

---

## Test Case 2: Airflow DAG Structural Integrity & Variable Resolution

### Purpose
To verify that the Airflow DAG `dw_dwh_plato_tarif_mapping_taeglich_jp` parses without syntax or import errors, correctly resolves environment variables, and maintains the expected task dependency structure.

### Setup
* A Python environment with `apache-airflow` installed.
* Airflow Variables mocked to simulate runtime environment resolution.

### Action
Load the DAG file dynamically using `pytest` and assert its structural properties.

```python
import pytest
from unittest.mock import patch
from airflow.models import DagBag, Variable

@pytest.fixture(autouse=True)
def mock_airflow_variables():
    """Mock Airflow Variables to prevent runtime resolution errors during parsing."""
    mock_vars = {
        "GCP_PROJECT": "test-gcp-project-1234",
        "GCP_REGION": "europe-west3",
        "DATAPROC_CLUSTER": "test-dataproc-cluster",
        "GCS_BUCKET": "test-dwh-migration-bucket"
    }
    with patch.object(Variable, 'get', side_effect=lambda key, default_var=None: mock_vars[key]):
        yield

def test_dag_structural_integrity():
    # Load the DAG file
    dag_file_path = "uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife_dag.py"
    dag_bag = DagBag(dag_folder=dag_file_path, include_examples=False)
    
    # Assert no import errors occurred
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"

    dag_id = "dw_dwh_plato_tarif_mapping_taeglich_jp"
    assert dag_id in dag_bag.dags, f"DAG {dag_id} not found in DagBag"
    
    dag = dag_bag.get_dag(dag_id)
    
    # Verify DAG Schedule and Attributes
    assert dag.schedule_interval == "0 5 * * *", f"Expected daily schedule '0 5 * * *', got {dag.schedule_interval}"
    assert dag.catchup is False, "Catchup should be disabled"
    
    # Verify Task List and Dependencies
    expected_tasks = {"start", "dw_dwh_dummy_absd_plato_tarife", "end"}
    actual_tasks = set(dag.task_ids)
    assert actual_tasks == expected_tasks, f"Task mismatch. Expected {expected_tasks}, got {actual_tasks}"
    
    # Verify Lineage: start -> dw_dwh_dummy_absd_plato_tarife -> end
    start_task = dag.get_task("start")
    dummy_task = dag.get_task("dw_dwh_dummy_absd_plato_tarife")
    end_task = dag.get_task("end")
    
    assert dummy_task.task_id in start_task.downstream_task_ids, "Dependency 'start -> dw_dwh_dummy_absd_plato_tarife' is missing"
    assert end_task.task_id in dummy_task.downstream_task_ids, "Dependency 'dw_dwh_dummy_absd_plato_tarife -> end' is missing"
```

### Pass/Fail Criterion
* **Pass**: The DAG parses with zero import errors, contains exactly the three expected tasks, and enforces the linear execution order `start >> dw_dwh_dummy_absd_plato_tarife >> end`.
* **Fail**: Any import errors, missing tasks, incorrect schedule, or broken dependency links.

---

## Test Case 3: Dataproc Operator Configuration Validation

### Purpose
To verify that the `DataprocSubmitJobOperator` task is configured with the correct runtime variables, project IDs, cluster names, and points to the correct GCS URI for the companion script.

### Setup
* A Python environment with `apache-airflow` and `google-cloud-dataproc` installed.
* Airflow Variables mocked to simulate runtime environment resolution.

### Action
Parse the DAG and inspect the configuration dictionary of the `DataprocSubmitJobOperator` task.

```python
import pytest
from unittest.mock import patch
from airflow.models import DagBag, Variable

@pytest.fixture
def mock_variables():
    mock_vars = {
        "GCP_PROJECT": "prod-dwh-project",
        "GCP_REGION": "europe-west3",
        "DATAPROC_CLUSTER": "dwh-dataproc-cluster-01",
        "GCS_BUCKET": "dwh-gcs-bucket-prod"
    }
    with patch.object(Variable, 'get', side_effect=lambda key, default_var=None: mock_vars[key]):
        yield mock_vars

def test_dataproc_operator_configuration(mock_variables):
    dag_file_path = "uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife_dag.py"
    dag_bag = DagBag(dag_folder=dag_file_path, include_examples=False)
    dag = dag_bag.get_dag("dw_dwh_plato_tarif_mapping_taeglich_jp")
    
    task = dag.get_task("dw_dwh_dummy_absd_plato_tarife")
    
    # Verify Operator Type
    assert task.__class__.__name__ == "DataprocSubmitJobOperator", "Task is not a DataprocSubmitJobOperator"
    
    # Verify Region and Project configurations
    assert task.region == mock_variables["GCP_REGION"]
    assert task.project_id == mock_variables["GCP_PROJECT"]
    
    # Verify Job Configuration details
    job_config = task.job
    assert job_config['reference']['project_id'] == mock_variables["GCP_PROJECT"]
    assert job_config['placement']['cluster_name'] == mock_variables["DATAPROC_CLUSTER"]
    
    # Verify PySpark Script URI
    expected_gcs_uri = f"gs://{mock_variables['GCS_BUCKET']}/pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py"
    actual_gcs_uri = job_config['pyspark_job']['main_python_file_uri']
    assert actual_gcs_uri == expected_gcs_uri, f"Expected GCS URI '{expected_gcs_uri}', got '{actual_gcs_uri}'"
```

### Pass/Fail Criterion
* **Pass**: The operator's project, region, cluster, and GCS URI configurations dynamically resolve to match the mocked Airflow Variables exactly.
* **Fail**: Any configuration mismatch, hardcoded string placeholders (e.g., `<PROJECT_ID>`), or incorrect GCS pathing.

---

## Test Case 4: Deployment & GCS Artifact Verification

### Purpose
To verify that the companion script `dw_dwh_dummy_absd_plato_tarife.py` has been successfully deployed to the target Google Cloud Storage (GCS) bucket and matches the local source code.

### Setup
* Google Cloud SDK configured with access to the target GCP environment.
* `google-cloud-storage` library installed.
* Airflow Variable `GCS_BUCKET` set in the target environment.

### Action
Retrieve the file from GCS and compare its MD5 checksum against the local companion script.

```python
import hashlib
from google.cloud import storage
from airflow.models import Variable

def get_md5_checksum(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()

def test_gcs_artifact_deployment():
    # Resolve bucket from Airflow Variable
    gcs_bucket_name = Variable.get("GCS_BUCKET")
    gcs_blob_name = "pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py"
    local_script_path = "uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py"
    
    # Read local file
    with open(local_script_path, "rb") as f:
        local_content = f.read()
    local_checksum = get_md5_checksum(local_content)
    
    # Fetch from GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket_name)
    blob = bucket.blob(gcs_blob_name)
    
    assert blob.exists(), f"Artifact missing from GCS: gs://{gcs_bucket_name}/{gcs_blob_name}"
    
    gcs_content = blob.download_as_bytes()
    gcs_checksum = get_md5_checksum(gcs_content)
    
    # Assert parity
    assert local_checksum == gcs_checksum, "GCS deployed script does not match local script checksum"
```

### Pass/Fail Criterion
* **Pass**: The file exists at `gs://{GCS_BUCKET}/pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py` and its MD5 checksum is identical to the local copy.
* **Fail**: The file is missing from GCS, or the checksums do not match (indicating an outdated or corrupted deployment).

---

## Test Case 5: End-to-End Execution Trace (Dry Run / Integration)

### Purpose
To verify that executing the task in a test Airflow environment successfully triggers the Dataproc job, prints `"Doing nothinig"`, and completes with a `SUCCESS` state.

### Setup
* A running Cloud Composer / Airflow integration environment.
* A running Dataproc cluster matching the `DATAPROC_CLUSTER` variable.

### Action
Trigger the task `dw_dwh_dummy_absd_plato_tarife` using the Airflow CLI or API, and inspect the task execution logs.

```bash
# 1. Trigger a test run of the specific task
airflow tasks test dw_dwh_plato_tarif_mapping_taeglich_jp dw_dwh_dummy_absd_plato_tarife 2026-03-30

# 2. Verify the output in the task execution logs
# (This can be automated via a CI/CD assertion script checking the log output)
```

### Pass/Fail Criterion
* **Pass**: 
  1. The task execution completes with state `SUCCESS`.
  2. The Dataproc driver logs contain the exact string:
     ```
     Doing nothinig
     ```
* **Fail**: The task fails, times out, or the driver logs do not contain the exact string `"Doing nothinig"`.