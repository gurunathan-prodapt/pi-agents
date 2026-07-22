# Migration Validation Test Suite: DW.CFG_LOAD_PARAMS

This document defines the migration-validation test suite for the `DW.CFG_LOAD_PARAMS` workflow. These tests verify behavioral equivalence between the legacy Oracle/KornShell implementation and the modernized Google Cloud Platform (Cloud Composer, BigQuery, and Dataform) implementation.

---

## Test Suite Overview

The validation strategy is divided into four automated test cases:
1. **Staging File Parsing & Ingestion (Output Parity)**: Validates that `r_load_params.py` parses property files (including comments, whitespace, and edge cases) identically to the legacy script and loads them into BigQuery staging.
2. **Dataform MERGE Logic (Transformation Correctness)**: Validates that the BigQuery `MERGE` statement correctly performs upserts (inserts new keys, updates existing keys, and handles NULLs).
3. **Error Handling & Missing Inputs (Resiliency)**: Validates that missing configuration files or database connection failures trigger appropriate alerts and exit codes.
4. **DAG Orchestration & Lineage (Integration)**: Validates that the Airflow DAG structure, task dependencies, and variable resolutions are correct.

---

## Test Case 1: Staging File Parsing & Ingestion (Output Parity)

### Purpose
Verify that `r_load_params.py` correctly parses configuration files from Google Cloud Storage (GCS) and loads them into the BigQuery staging table (`DWH_STG.PARAM_LOAD`). The test ensures that:
* Comments (lines starting with `#` or `--`) are ignored.
* Empty lines are ignored.
* Key-value pairs separated by `=` or whitespace are correctly parsed.
* The staging table is truncated before loading (matching SQL*Loader behavior).

### Setup
1. Create a temporary GCS bucket or use the configured test bucket.
2. Upload a test properties file `config/d_param_load.properties` with the following content:
   ```properties
   # This is a comment line
   db.host=10.0.0.1
   db.sid = DWH1P
   stage.table  PARAM_LOAD
   -- Another comment line
   empty_val=
   spaced_key = spaced_val
   ```
3. Ensure the BigQuery staging table `DWH_STG.PARAM_LOAD` exists and contains 3 dummy records (to verify truncation).

### Action
Execute the Python staging script using `pytest` with mocked GCS and BigQuery clients, or run it against a sandbox GCP environment.

### Concrete Pass/Fail Criterion
* **Pass**: The staging table contains exactly 5 records with the correct keys and values. The pre-existing dummy records are completely removed (verified truncation). The `loaded_at` column is populated with a valid UTC timestamp.
* **Fail**: Any comment lines are parsed as keys, the table is not truncated, or the row count does not equal 5.

### Test Code (`test_staging_load.py`)

```python
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from google.cloud import bigquery
from config_env_linked_job.iscfg.bin.r_load_params import load_parameters

@patch('google.cloud.storage.Client')
@patch('google.cloud.bigquery.Client')
def test_load_parameters_parsing_and_truncation(mock_bq_client, mock_storage_client):
    # 1. Arrange
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_storage_client.return_value.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob
    
    # Simulate the properties file content
    mock_blob.exists.return_value = True
    mock_blob.download_as_text.return_value = (
        "# This is a comment line\n"
        "db.host=10.0.0.1\n"
        "db.sid = DWH1P\n"
        "stage.table  PARAM_LOAD\n"
        "-- Another comment line\n"
        "empty_val=\n"
        "spaced_key = spaced_val\n"
    )
    
    mock_bq_inst = MagicMock()
    mock_bq_client.return_value = mock_bq_inst
    
    # Capture the rows sent to BigQuery load job
    captured_records = []
    def mock_load_table_from_json(json_rows, table_ref, job_config):
        nonlocal captured_records
        captured_records = json_rows
        mock_job = MagicMock()
        mock_job.result.return_value = True
        return mock_job
        
    mock_bq_inst.load_table_from_json.side_effect = mock_load_table_from_json

    # 2. Act
    load_parameters(
        gcs_bucket="test-bucket",
        source_blob="config/d_param_load.properties",
        target_project="test-project",
        target_dataset="DWH_STG",
        target_table="PARAM_LOAD"
    )

    # 3. Assert
    # Verify GCS download was called
    mock_storage_client.return_value.bucket.assert_called_once_with("test-bucket")
    mock_bucket.blob.assert_called_once_with("config/d_param_load.properties")
    
    # Verify Truncate Query was executed (SQL*Loader behavior)
    mock_bq_inst.query.assert_called_once_with("TRUNCATE TABLE `test-project.DWH_STG.PARAM_LOAD`")
    
    # Verify parsed records
    assert len(captured_records) == 5
    
    # Verify specific key-value mappings
    assert captured_records[0]["param_key"] == "db.host"
    assert captured_records[0]["param_value"] == "10.0.0.1"
    
    assert captured_records[1]["param_key"] == "db.sid"
    assert captured_records[1]["param_value"] == "DWH1P"
    
    assert captured_records[2]["param_key"] == "stage.table"
    assert captured_records[2]["param_value"] == "PARAM_LOAD"
    
    assert captured_records[3]["param_key"] == "empty_val"
    assert captured_records[3]["param_value"] == ""
    
    assert captured_records[4]["param_key"] == "spaced_key"
    assert captured_records[4]["param_value"] == "spaced_val"
    
    # Verify timestamp format
    for record in captured_records:
        assert "loaded_at" in record
        # Ensure it parses as ISO format
        datetime.fromisoformat(record["loaded_at"])
```

---

## Test Case 2: Dataform MERGE Logic (Transformation Correctness)

### Purpose
Verify that the BigQuery `MERGE` statement in `d_param_load.sqlx` correctly updates existing keys, inserts new keys, and leaves unaffected keys untouched.

### Setup
1. Populate the target table `DWH_ADM.JOB_PARAMS` with the following baseline data:
   | param_key | param_value | updated_at |
   | :--- | :--- | :--- |
   | `db.host` | `192.168.1.1` | `2026-01-01 00:00:00 UTC` |
   | `db.sid` | `OLD_SID` | `2026-01-01 00:00:00 UTC` |
   | `unaffected.key` | `keep_me` | `2026-01-01 00:00:00 UTC` |

2. Populate the staging table `DWH_STG.PARAM_LOAD` with the following update payload:
   | param_key | param_value | loaded_at |
   | :--- | :--- | :--- |
   | `db.host` | `10.0.0.1` | `2026-04-21 12:00:00 UTC` |  *(Update)*
   | `db.sid` | `NEW_SID` | `2026-04-21 12:00:00 UTC` |  *(Update)*
   | `new.parameter` | `hello_world` | `2026-04-21 12:00:00 UTC` |  *(Insert)*

### Action
Execute the `MERGE` query against a BigQuery sandbox environment.

### Concrete Pass/Fail Criterion
* **Pass**: After execution, `DWH_ADM.JOB_PARAMS` contains exactly 4 records with the following states:
  * `db.host` is updated to `10.0.0.1` and `updated_at` is `2026-04-21 12:00:00 UTC`.
  * `db.sid` is updated to `NEW_SID` and `updated_at` is `2026-04-21 12:00:00 UTC`.
  * `new.parameter` is inserted with value `hello_world` and `updated_at` is `2026-04-21 12:00:00 UTC`.
  * `unaffected.key` remains `keep_me` with its original timestamp `2026-01-01 00:00:00 UTC`.
* **Fail**: Any record is missing, updated incorrectly, or the unaffected key is modified.

### Test Code (`test_merge_logic.py`)

```python
import pytest
from google.cloud import bigquery

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client()

def test_bigquery_merge_logic(bq_client):
    project = bq_client.project
    dataset_stg = f"{project}.DWH_STG"
    dataset_adm = f"{project}.DWH_ADM"
    
    # 1. Clean and Setup Tables
    bq_client.query(f"CREATE OR REPLACE TABLE `{dataset_stg}.PARAM_LOAD` (param_key STRING, param_value STRING, loaded_at TIMESTAMP)").result()
    bq_client.query(f"CREATE OR REPLACE TABLE `{dataset_adm}.JOB_PARAMS` (param_key STRING, param_value STRING, updated_at TIMESTAMP)").result()
    
    # Insert Baseline Target Data
    bq_client.query(f"""
        INSERT INTO `{dataset_adm}.JOB_PARAMS` (param_key, param_value, updated_at) VALUES
        ('db.host', '192.168.1.1', TIMESTAMP('2026-01-01 00:00:00 UTC')),
        ('db.sid', 'OLD_SID', TIMESTAMP('2026-01-01 00:00:00 UTC')),
        ('unaffected.key', 'keep_me', TIMESTAMP('2026-01-01 00:00:00 UTC'))
    """).result()
    
    # Insert Staging Payload
    bq_client.query(f"""
        INSERT INTO `{dataset_stg}.PARAM_LOAD` (param_key, param_value, loaded_at) VALUES
        ('db.host', '10.0.0.1', TIMESTAMP('2026-04-21 12:00:00 UTC')),
        ('db.sid', 'NEW_SID', TIMESTAMP('2026-04-21 12:00:00 UTC')),
        ('new.parameter', 'hello_world', TIMESTAMP('2026-04-21 12:00:00 UTC'))
    """).result()

    # 2. Execute the MERGE Query (verbatim from d_param_load.sqlx)
    merge_query = f"""
    MERGE INTO `{dataset_adm}.JOB_PARAMS` tgt
    USING (
        SELECT param_key, param_value, loaded_at
        FROM   `{dataset_stg}.PARAM_LOAD`
    ) src
    ON (tgt.param_key = src.param_key)
    WHEN MATCHED THEN UPDATE SET
        tgt.param_value = src.param_value,
        tgt.updated_at  = src.loaded_at
    WHEN NOT MATCHED THEN INSERT (param_key, param_value, updated_at)
    VALUES (src.param_key, src.param_value, src.loaded_at);
    """
    bq_client.query(merge_query).result()

    # 3. Assert Results
    results_query = f"SELECT param_key, param_value, updated_at FROM `{dataset_adm}.JOB_PARAMS` ORDER BY param_key"
    rows = list(bq_client.query(results_query).result())
    
    results = {row['param_key']: (row['param_value'], row['updated_at'].isoformat()) for row in rows}
    
    assert len(results) == 4
    assert results['db.host'] == ('10.0.0.1', '2026-04-21T12:00:00+00:00')
    assert results['db.sid'] == ('NEW_SID', '2026-04-21T12:00:00+00:00')
    assert results['new.parameter'] == ('hello_world', '2026-04-21T12:00:00+00:00')
    assert results['unaffected.key'] == ('keep_me', '2026-01-01T00:00:00+00:00')
```

---

## Test Case 3: Error Handling & Resiliency (Edge Cases)

### Purpose
Verify that the Python staging script handles missing GCS files gracefully by raising an error and exiting with status `8` (matching the legacy KornShell script's `exit 8` behavior when the properties file was missing).

### Setup
Ensure that the target properties file does *not* exist in the GCS bucket.

### Action
Run the `load_parameters` function pointing to the non-existent GCS path.

### Concrete Pass/Fail Criterion
* **Pass**: The script raises a `SystemExit` exception with code `8` and logs the error message: `"FEHLER: Parameterdatei gs://{bucket}/{path} nicht gefunden"` to `sys.stderr`.
* **Fail**: The script exits with code `0`, raises an unhandled generic exception, or fails to log the error message.

### Test Code (`test_error_handling.py`)

```python
import pytest
import sys
from unittest.mock import MagicMock, patch
from config_env_linked_job.iscfg.bin.r_load_params import load_parameters

@patch('google.cloud.storage.Client')
def test_missing_properties_file_exits_8(mock_storage_client, capsys):
    # Arrange
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_storage_client.return_value.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob
    
    # Simulate file not existing in GCS
    mock_blob.exists.return_value = False

    # Act & Assert
    with pytest.raises(SystemExit) as exc_info:
        load_parameters(
            gcs_bucket="test-bucket",
            source_blob="config/missing_file.properties",
            target_project="test-project",
            target_dataset="DWH_STG",
            target_table="PARAM_LOAD"
        )
    
    # Verify exit code is 8 (matching legacy KSH exit 8)
    assert exc_info.value.code == 8
    
    # Verify German error message is printed to stderr
    stderr_output = capsys.readouterr().err
    assert "FEHLER: Parameterdatei gs://test-bucket/config/missing_file.properties nicht gefunden" in stderr_output
```

---

## Test Case 4: DAG Orchestration & Lineage (Integration)

### Purpose
Verify that the Airflow DAG parses without errors, contains the correct task sequence, and resolves variables properly.

### Setup
Mock the Airflow environment variables:
* `GCP_PROJECT` = `test-project`
* `GCS_BUCKET` = `test-bucket`
* `DATAFORM_REPOSITORY_ID` = `test-repo`

### Action
Load the DAG file programmatically and inspect its structure.

### Concrete Pass/Fail Criterion
* **Pass**: The DAG is loaded successfully with no import errors. The task dependency chain is exactly:
  `load_staging_params` >> `create_compilation` >> `execute_dataform_merge`.
* **Fail**: The DAG fails to parse, tasks are missing, or the execution order is incorrect.

### Test Code (`test_dag_integrity.py`)

```python
import pytest
from airflow.models import DagBag, Variable

@pytest.fixture(autouse=True)
def setup_airflow_variables(monkeypatch):
    # Mock Airflow Variables before importing the DAG
    mock_vars = {
        "GCP_PROJECT": "test-project",
        "GCP_REGION": "us-central1",
        "GCS_BUCKET": "test-bucket",
        "DATAFORM_REPOSITORY_ID": "test-repo"
    }
    def mock_get(key, default_var=None):
        return mock_vars.get(key, default_var)
    
    monkeypatch.setattr(Variable, "get", mock_get)

def test_dag_structure_and_dependencies():
    # 1. Load the DAG
    dagbag = DagBag(dag_folder="dags/config_env_linked_job/DWH_CFG_JOB", include_examples=False)
    dag_id = "dw_cfg_load_params_dag"
    
    dag = dagbag.get_dag(dag_id)
    
    # Assert no import errors
    assert dagbag.import_errors == {}
    assert dag is not None
    
    # 2. Verify Task List
    expected_tasks = ["load_staging_params", "create_compilation", "execute_dataform_merge"]
    actual_tasks = [task.task_id for task in dag.tasks]
    assert set(expected_tasks) == set(actual_tasks)
    
    # 3. Verify Sequential Lineage
    load_task = dag.get_task("load_staging_params")
    compile_task = dag.get_task("create_compilation")
    merge_task = dag.get_task("execute_dataform_merge")
    
    assert compile_task in load_task.downstream_list
    assert merge_task in compile_task.downstream_list
    assert merge_task.upstream_list == [compile_task]
```