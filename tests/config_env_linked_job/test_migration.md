# Migration Validation Test Suite: `DW.CFG_LOAD_PARAMS`

This document defines the migration-validation test suite for the `DW.CFG_LOAD_PARAMS` job. It ensures behavioral equivalence between the legacy Oracle/KornShell implementation and the migrated Google Cloud Platform (Cloud Composer + Python + Dataform + BigQuery) implementation.

---

## Test Suite Overview

The validation strategy is divided into four distinct test cases:
1. **Output Parity & Log Verification**: Validates that the Python script produces the exact German log outputs and exit codes as the legacy shell script under success and failure scenarios.
2. **Ingestion Correctness & Type Handling**: Validates that the properties file parser correctly handles comments, whitespace, empty lines, and special characters, and loads them into `DWH_STG.PARAM_LOAD` with correct types.
3. **Transformation Correctness (MERGE Logic)**: Validates that the BigQuery MERGE statement in `d_param_load.sqlx` behaves identically to the legacy Oracle MERGE statement (upserting matched keys, inserting new keys, updating timestamps).
4. **End-to-End Orchestration & Variable Resolution**: Validates that the Airflow DAG compiles without syntax errors, resolves global variables correctly, and establishes the correct task dependency chain.

---

## Test Case 1: Output Parity & Log Verification

### Purpose
To verify that the migrated Python script `r_load_params.py` preserves the exact terminal signals, exit codes, and verbatim German logging statements of the legacy KornShell script (`r_load_params.ksh`).

### Setup
1. A Python test environment with `pytest` and `mock` installed.
2. Mocked Google Cloud Storage (`google.cloud.storage.Client`) and BigQuery (`google.cloud.bigquery.Client`) interfaces to isolate local execution.

### Action
Execute the Python script under two scenarios:
* **Scenario A (Failure)**: The properties file does not exist in GCS.
* **Scenario B (Success)**: The properties file exists in GCS and is successfully loaded.

### Concrete Pass/Fail Criteria
* **Scenario A Pass**: The script exits with code `1` (or `8` depending on the entry point) and prints exactly:  
  `FEHLER: Parameterdatei gs://<bucket>/config/param_load.properties nicht gefunden` to `stderr`.
* **Scenario B Pass**: The script exits with code `0` and prints exactly:  
  `Lade Parameter nach DWH_STG.PARAM_LOAD ...` followed by  
  `Parameterladen erfolgreich abgeschlossen` to `stdout`.
* **Fail**: Any deviation in the German text, incorrect stream routing (stdout vs. stderr), or mismatched exit codes.

### Test Code (`test_r_load_params_logs.py`)

```python
import pytest
import sys
from unittest.mock import MagicMock, patch
from config_env_linked_job.iscfg.bin.r_load_params import load_parameters

@patch("google.cloud.storage.Client")
@patch("google.cloud.bigquery.Client")
def test_load_parameters_file_not_found(mock_bq_client, mock_storage_client, capsys):
    # Setup mock for non-existent file
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_blob.exists.return_value = False
    mock_bucket.blob.return_value = mock_blob
    mock_storage_client.return_value.bucket.return_value = mock_bucket

    param_file_path = "gs://test-bucket/config/param_load.properties"
    
    # Action & Assertion for Exit Code
    with pytest.raises(SystemExit) as excinfo:
        load_parameters(
            param_file_path=param_file_path,
            project_id="test-project",
            dataset_id="DWH_STG",
            table_id="PARAM_LOAD"
        )
    
    assert excinfo.value.code == 1

    # Assert Verbatim German Error Message on stderr/stdout
    captured = capsys.readouterr()
    expected_err = f"FEHLER: Parameterdatei {param_file_path} nicht gefunden"
    assert expected_err in captured.out or expected_err in captured.err


@patch("google.cloud.storage.Client")
@patch("google.cloud.bigquery.Client")
def test_load_parameters_success(mock_bq_client, mock_storage_client, capsys):
    # Setup mock for successful file read
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_blob.exists.return_value = True
    
    # Mock properties file content
    properties_content = """
    # This is a comment
    db.host=10.150.20.11
    db.sid=DWH1P
    stage.table=PARAM_LOAD
    """
    mock_blob.download_as_text.return_value = properties_content
    mock_bucket.blob.return_value = mock_blob
    mock_storage_client.return_value.bucket.return_value = mock_bucket

    # Mock BigQuery load job
    mock_job = MagicMock()
    mock_job.result.return_value = True
    mock_bq_client.return_value.load_table_from_json.return_value = mock_job

    # Action
    load_parameters(
        param_file_path="gs://test-bucket/config/param_load.properties",
        project_id="test-project",
        dataset_id="DWH_STG",
        table_id="PARAM_LOAD"
    )

    # Assert Verbatim German Success Messages
    captured = capsys.readouterr()
    assert "Lade Parameter nach DWH_STG.PARAM_LOAD ..." in captured.out
    assert "Parameterladen erfolgreich abgeschlossen" in captured.out
```

---

## Test Case 2: Ingestion Correctness & Type Handling

### Purpose
To verify that the properties file parser correctly handles edge cases (comments, whitespace, empty lines, multiple `=` signs) and loads the parsed records into BigQuery with correct schema types.

### Setup
1. A temporary BigQuery dataset `DWH_STG_VAL` and table `PARAM_LOAD_VAL` created in the test environment.
2. Schema of `PARAM_LOAD_VAL`:
   * `param_key`: `STRING`
   * `param_value`: `STRING`
   * `loaded_at`: `TIMESTAMP`

### Action
1. Upload a properties file containing edge cases to a test GCS bucket.
2. Run `r_load_params.load_parameters` pointing to this file and the validation table.
3. Query the validation table to assert row count, key-value parsing, and schema types.

### Concrete Pass/Fail Criteria
* **Pass**: 
  * Comments (`#`) and empty lines are completely ignored.
  * Leading/trailing whitespaces around keys and values are trimmed.
  * Keys containing multiple `=` signs (e.g., `key=val=with=equals`) are split correctly on the *first* occurrence.
  * `loaded_at` is populated with a valid, current UTC timestamp.
  * Total loaded rows match the expected count of active properties.
* **Fail**: Any parsing error, unhandled exception, or schema mismatch (e.g., truncated values, missing rows).

### Test Code (`test_ingestion_correctness.py`)

```python
import os
import pytest
from google.cloud import bigquery
from google.cloud import storage
from config_env_linked_job.iscfg.bin.r_load_params import load_parameters

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client()

@pytest.fixture(scope="module")
def storage_client():
    return storage.Client()

def test_properties_parsing_and_ingestion(bq_client, storage_client):
    project_id = bq_client.project
    bucket_name = os.environ.get("GCS_BUCKET", f"{project_id}-migration-test")
    dataset_id = "DWH_STG_VAL"
    table_id = "PARAM_LOAD_VAL"
    
    # 1. Create Test Dataset and Table
    dataset_ref = bq_client.dataset(dataset_id)
    bq_client.create_dataset(bigquery.Dataset(dataset_ref), exists_ok=True)
    
    schema = [
        bigquery.SchemaField("param_key", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("param_value", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
    ]
    table = bigquery.Table(dataset_ref.table(table_id), schema=schema)
    bq_client.create_table(table, exists_ok=True)

    # 2. Upload Edge-Case Properties File to GCS
    edge_case_content = """# Verification Properties File
    db.host = 10.150.20.11
    
    # Empty line above and spaces around equals
    db.sid=DWH1P
    
    complex.formula = a=b+c
    empty.property = 
    """
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob("config/test_edge_cases.properties")
    blob.upload_from_string(edge_case_content, content_type="text/plain")

    # 3. Execute Ingestion
    param_file_path = f"gs://{bucket_name}/config/test_edge_cases.properties"
    load_parameters(
        param_file_path=param_file_path,
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id
    )

    # 4. Assertions against BigQuery Staging Table
    query = f"SELECT param_key, param_value, loaded_at FROM `{project_id}.{dataset_id}.{table_id}`"
    query_job = bq_client.query(query)
    results = {row["param_key"]: row["param_value"] for row in query_job.result()}

    # Assertions
    assert len(results) == 4, f"Expected 4 rows, got {len(results)}"
    assert results["db.host"] == "10.150.20.11", "Failed to trim whitespace"
    assert results["db.sid"] == "DWH1P", "Failed to parse standard key-value"
    assert results["complex.formula"] == "a=b+c", "Failed to split on first '=' only"
    assert results["empty.property"] == "", "Failed to handle empty property value"

    # Cleanup
    bq_client.delete_table(table, not_found_ok=True)
    blob.delete()
```

---

## Test Case 3: Transformation Correctness (MERGE Logic)

### Purpose
To verify that the BigQuery MERGE statement in `d_param_load.sqlx` behaves identically to the legacy Oracle MERGE statement, ensuring correct upsert behavior (updates existing keys, inserts new keys, and updates timestamps).

### Setup
1. Create target table `DWH_ADM_VAL.JOB_PARAMS_VAL` and seed it with initial parameters.
2. Create staging table `DWH_STG_VAL.PARAM_LOAD_VAL` and seed it with a mix of updates, inserts, and unchanged rows.

### Action
Execute the MERGE statement against the validation tables.

### Concrete Pass/Fail Criteria
* **Pass**:
  * Existing keys (`param_key`) have their `param_value` updated to the staging value.
  * Existing keys have their `updated_at` timestamp updated to the staging `loaded_at` timestamp.
  * New keys are inserted with correct values and timestamps.
  * Unmatched keys in the target table remain completely unaffected.
* **Fail**: Any duplicate keys, incorrect values, or unmodified timestamps for updated rows.

### Test Code (SQL Assertions)

```sql
-- =====================================================================
-- 1. SETUP: Create and Seed Validation Tables
-- =====================================================================
CREATE OR REPLACE TABLE `DWH_ADM_VAL.JOB_PARAMS_VAL` (
    param_key STRING,
    param_value STRING,
    updated_at TIMESTAMP
);

CREATE OR REPLACE TABLE `DWH_STG_VAL.PARAM_LOAD_VAL` (
    param_key STRING,
    param_value STRING,
    loaded_at TIMESTAMP
);

-- Seed Target Table (Initial State)
INSERT INTO `DWH_ADM_VAL.JOB_PARAMS_VAL` (param_key, param_value, updated_at)
VALUES 
    ('db.host', '10.100.10.1', TIMESTAMP('2026-01-01 00:00:00 UTC')),
    ('db.sid', 'DWH_OLD', TIMESTAMP('2026-01-01 00:00:00 UTC')),
    ('keep.unaffected', 'stable_val', TIMESTAMP('2026-01-01 00:00:00 UTC'));

-- Seed Staging Table (Delta State)
INSERT INTO `DWH_STG_VAL.PARAM_LOAD_VAL` (param_key, param_value, loaded_at)
VALUES 
    ('db.host', '10.150.20.11', TIMESTAMP('2026-04-21 12:00:00 UTC')), -- Update
    ('db.sid', 'DWH1P', TIMESTAMP('2026-04-21 12:00:00 UTC')),         -- Update
    ('new.parameter', 'inserted_val', TIMESTAMP('2026-04-21 12:00:00 UTC')); -- Insert

-- =====================================================================
-- 2. ACTION: Execute Migrated MERGE Statement
-- =====================================================================
MERGE INTO `DWH_ADM_VAL.JOB_PARAMS_VAL` tgt
USING (
    SELECT 
        CAST(param_key AS STRING) AS param_key, 
        CAST(param_value AS STRING) AS param_value, 
        CAST(loaded_at AS TIMESTAMP) AS loaded_at
    FROM `DWH_STG_VAL.PARAM_LOAD_VAL`
) src
ON (tgt.param_key = src.param_key)
WHEN MATCHED THEN UPDATE SET
    tgt.param_value = src.param_value,
    tgt.updated_at  = src.loaded_at
WHEN NOT MATCHED THEN INSERT (param_key, param_value, updated_at)
VALUES (src.param_key, src.param_value, src.loaded_at);

-- =====================================================================
-- 3. ASSERTIONS: Verify Post-Merge State
-- =====================================================================

-- Assertion 1: Verify Updates
ASSERT (
    SELECT param_value = '10.150.20.11' AND updated_at = TIMESTAMP('2026-04-21 12:00:00 UTC')
    FROM `DWH_ADM_VAL.JOB_PARAMS_VAL`
    WHERE param_key = 'db.host'
) AS "ERROR: db.host was not updated correctly";

ASSERT (
    SELECT param_value = 'DWH1P' AND updated_at = TIMESTAMP('2026-04-21 12:00:00 UTC')
    FROM `DWH_ADM_VAL.JOB_PARAMS_VAL`
    WHERE param_key = 'db.sid'
) AS "ERROR: db.sid was not updated correctly";

-- Assertion 2: Verify Inserts
ASSERT (
    SELECT param_value = 'inserted_val' AND updated_at = TIMESTAMP('2026-04-21 12:00:00 UTC')
    FROM `DWH_ADM_VAL.JOB_PARAMS_VAL`
    WHERE param_key = 'new.parameter'
) AS "ERROR: new.parameter was not inserted correctly";

-- Assertion 3: Verify Unaffected Rows
ASSERT (
    SELECT param_value = 'stable_val' AND updated_at = TIMESTAMP('2026-01-01 00:00:00 UTC')
    FROM `DWH_ADM_VAL.JOB_PARAMS_VAL`
    WHERE param_key = 'keep.unaffected'
) AS "ERROR: Unaffected row was modified";

-- Assertion 4: Verify Row Count
ASSERT (
    SELECT COUNT(1) = 4 FROM `DWH_ADM_VAL.JOB_PARAMS_VAL`
) AS "ERROR: Target table row count mismatch";
```

---

## Test Case 4: End-to-End Orchestration & Variable Resolution

### Purpose
To verify that the Airflow DAG `dw_cfg_load_params` compiles without syntax errors, correctly resolves global variables (`GCP_PROJECT`, `GCS_BUCKET`, `DATAFORM_REPOSITORY`), and establishes the correct task dependency chain.

### Setup
An Airflow environment (or local unit-testing environment) with the DAG file placed in the `dags/` folder.

### Action
Parse the DAG file using Airflow's internal validation utilities and inspect the task structure.

### Concrete Pass/Fail Criteria
* **Pass**:
  * The DAG is successfully parsed with zero import errors.
  * The DAG contains exactly three tasks: `load_parameters_to_staging`, `create_compilation_result`, and `run_dataform_merge`.
  * The execution sequence is strictly: `load_parameters_to_staging` -> `create_compilation_result` -> `run_dataform_merge`.
  * `max_active_runs` is set to `1` to prevent concurrent parameter-loading conflicts.
* **Fail**: Any import error, missing task, incorrect dependency order, or failure to resolve Airflow variables.

### Test Code (`test_dag_validation.py`)

```python
import pytest
from airflow.models import DagBag, Variable
from airflow.utils.dag_cycle_tester import check_cycle

@pytest.fixture(scope="module", autouse=True)
def setup_airflow_variables():
    # Mock Airflow Variables required by the DAG
    Variable.set("GCP_PROJECT", "test-gcp-project")
    Variable.set("GCP_REGION", "europe-west3")
    Variable.set("GCS_BUCKET", "test-gcs-bucket")
    Variable.set("DATAFORM_REPOSITORY", "test-dataform-repo")
    yield
    # Cleanup
    Variable.delete("GCP_PROJECT")
    Variable.delete("GCP_REGION")
    Variable.delete("GCS_BUCKET")
    Variable.delete("DATAFORM_REPOSITORY")

def test_dag_imports_and_structure():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    
    # Assert no import errors
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"
    
    dag_id = "dw_cfg_load_params"
    assert dag_id in dag_bag.dags, f"DAG {dag_id} not found in DagBag"
    
    dag = dag_bag.get_dag(dag_id)
    
    # Assert no cycles
    check_cycle(dag)
    
    # Assert DAG properties
    assert dag.max_active_runs == 1, "max_active_runs must be set to 1 to prevent parameter conflicts"
    assert dag.catchup is False, "catchup must be False"
    
    # Assert Task Existence
    expected_tasks = ["load_parameters_to_staging", "create_compilation_result", "run_dataform_merge"]
    actual_tasks = [task.task_id for task in dag.tasks]
    assert set(expected_tasks) == set(actual_tasks), f"Expected tasks {expected_tasks}, but got {actual_tasks}"
    
    # Assert Task Dependencies (load_parameters_to_staging -> create_compilation_result -> run_dataform_merge)
    load_task = dag.get_task("load_parameters_to_staging")
    compile_task = dag.get_task("create_compilation_result")
    merge_task = dag.get_task("run_dataform_merge")
    
    assert compile_task in load_task.downstream_list, "create_compilation_result must run after load_parameters_to_staging"
    assert merge_task in compile_task.downstream_list, "run_dataform_merge must run after create_compilation_result"
```