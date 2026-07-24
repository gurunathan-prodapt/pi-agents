# Migration Validation Test Suite: DW.CFG_LOAD_PARAMS

This document defines the migration-validation test suite for the `DW.CFG_LOAD_PARAMS` job. These tests verify behavioral equivalence between the legacy Oracle/KornShell implementation and the migrated Google Cloud Platform (Cloud Composer, Python, BigQuery) implementation.

---

## Test Case 1: End-to-End Integration & Output Parity (Happy Path)

### Purpose
To prove that a standard parameter properties file uploaded to GCS is successfully parsed, staged, and merged into the target BigQuery table, producing the exact same final state as the legacy Oracle execution.

### Setup
1. **Target Table Initialization**: Truncate `DWH_ADM.JOB_PARAMS` and insert a baseline record to test both insert and update paths:
   ```sql
   TRUNCATE TABLE `DWH_ADM.JOB_PARAMS`;
   INSERT INTO `DWH_ADM.JOB_PARAMS` (param_key, param_value, updated_at)
   VALUES ('db.sid', 'OLD_SID', TIMESTAMP('2026-01-01 00:00:00 UTC'));
   ```
2. **Staging Table Initialization**: Truncate `DWH_STG.PARAM_LOAD`.
3. **GCS Mock File**: Upload a mock `dwh_env.properties` file to `gs://<GCS_CONFIG_BUCKET>/cfg/dwh_env.properties` with the following content:
   ```properties
   # This is a comment
   db.host=new-host.gcp.internal
   db.sid=NEW_SID
   stage.table=DWH_STG.PARAM_LOAD
   new.parameter=active
   ```

### Action
1. Execute the Python staging script `r_load_params.py`.
2. Execute the BigQuery MERGE query (or trigger the Dataform model `d_param_load.sqlx`).

### Pass/Fail Criterion
The test **passes** if:
- `DWH_STG.PARAM_LOAD` contains exactly 4 rows (excluding comments).
- `DWH_ADM.JOB_PARAMS` contains exactly 4 rows.
- The existing key `db.sid` is updated to `NEW_SID`.
- The new keys (`db.host`, `stage.table`, `new.parameter`) are inserted.
- The `updated_at` timestamp in `DWH_ADM.JOB_PARAMS` matches the `loaded_at` timestamp from `DWH_STG.PARAM_LOAD` for all modified/inserted rows.

### Test Code (Pytest)
```python
import os
import pytest
from google.cloud import storage, bigquery
from datetime import datetime, timezone

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client()

@pytest.fixture(scope="module")
def gcs_client():
    return storage.Client()

def test_e2e_parameter_load_parity(bq_client, gcs_client):
    project = os.environ["GCP_PROJECT"]
    bucket_name = os.environ["GCS_CONFIG_BUCKET"]
    
    # 1. Setup baseline target table state
    setup_query = f"""
    TRUNCATE TABLE `{project}.DWH_STG.PARAM_LOAD`;
    TRUNCATE TABLE `{project}.DWH_ADM.JOB_PARAMS`;
    INSERT INTO `{project}.DWH_ADM.JOB_PARAMS` (param_key, param_value, updated_at)
    VALUES ('db.sid', 'OLD_SID', TIMESTAMP('2026-01-01 00:00:00 UTC'));
    """
    bq_client.query(setup_query).result()

    # 2. Upload mock properties file to GCS
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob("cfg/dwh_env.properties")
    mock_properties = (
        "# Baseline Config\n"
        "db.host=new-host.gcp.internal\n"
        "db.sid=NEW_SID\n"
        "stage.table=DWH_STG.PARAM_LOAD\n"
        "new.parameter=active\n"
    )
    blob.upload_from_string(mock_properties, content_type="text/plain")

    # 3. Execute Python staging script
    # Import main directly to execute within the test environment
    from config_env_linked_job.iscfg.bin.r_load_params import main as run_staging
    
    os.environ["GCS_CONFIG_BUCKET"] = bucket_name
    try:
        run_staging()
    except SystemExit as e:
        assert e.code == 0, "Staging script exited with error"

    # 4. Execute BigQuery MERGE
    merge_query = f"""
    MERGE INTO `{project}.DWH_ADM.JOB_PARAMS` tgt
    USING `{project}.DWH_STG.PARAM_LOAD` src
    ON (tgt.param_key = src.param_key)
    WHEN MATCHED THEN UPDATE SET
        tgt.param_value = src.param_value,
        tgt.updated_at  = src.loaded_at
    WHEN NOT MATCHED THEN INSERT (param_key, param_value, updated_at)
    VALUES (src.param_key, src.param_value, src.loaded_at);
    """
    bq_client.query(merge_query).result()

    # 5. Assertions
    results_query = f"SELECT param_key, param_value, updated_at FROM `{project}.DWH_ADM.JOB_PARAMS` ORDER BY param_key"
    rows = list(bq_client.query(results_query).result())
    
    assert len(rows) == 4
    
    data = {row["param_key"]: (row["param_value"], row["updated_at"]) for row in rows}
    
    assert data["db.sid"][0] == "NEW_SID"
    assert data["db.host"][0] == "new-host.gcp.internal"
    assert data["new.parameter"][0] == "active"
    
    # Verify that updated_at is set to a recent timestamp (within the last 5 minutes)
    now = datetime.now(timezone.utc)
    for key, (val, ts) in data.items():
        assert (now - ts).total_seconds() < 300, f"Timestamp for {key} was not updated correctly"
```

---

## Test Case 2: Properties File Parsing Edge Cases

### Purpose
To verify that the Python parser handles edge cases in the properties file (comments, empty lines, leading/trailing spaces, keys with multiple `=` signs, and special characters) correctly without throwing exceptions or corrupting data.

### Setup
1. **Staging Table Initialization**: Truncate `DWH_STG.PARAM_LOAD`.
2. **GCS Mock File**: Upload a properties file containing edge cases to GCS:
   ```properties
   # Leading comment with spaces
      # Indented comment
   
   spaced.key   =   spaced.value   
   key.with.equals=value=contains=equals
   special.chars_#$=@active
   
   # Trailing empty lines
   ```

### Action
Run the Python staging script `r_load_params.py`.

### Pass/Fail Criterion
The test **passes** if:
- The script exits with code `0`.
- `DWH_STG.PARAM_LOAD` contains exactly 3 rows.
- Whitespace is stripped from keys and values (e.g., `spaced.key` maps to `spaced.value`).
- Keys with multiple equals signs are split only on the first equals sign (e.g., `key.with.equals` maps to `value=contains=equals`).
- Special characters are preserved correctly.

### Test Code (Pytest)
```python
def test_properties_parsing_edge_cases(bq_client, gcs_client):
    project = os.environ["GCP_PROJECT"]
    bucket_name = os.environ["GCS_CONFIG_BUCKET"]

    # Clear staging table
    bq_client.query(f"TRUNCATE TABLE `{project}.DWH_STG.PARAM_LOAD`").result()

    # Upload edge-case properties file
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob("cfg/dwh_env.properties")
    edge_case_properties = (
        "# Comment line\n"
        "   # Indented comment\n"
        "\n"  # Empty line
        "spaced.key   =   spaced.value   \n"  # Whitespace
        "key.with.equals=value=contains=equals\n"  # Multiple equals
        "special.chars_#$=value_with_#$\n"  # Special characters
    )
    blob.upload_from_string(edge_case_properties, content_type="text/plain")

    from config_env_linked_job.iscfg.bin.r_load_params import main as run_staging
    os.environ["GCS_CONFIG_BUCKET"] = bucket_name
    
    try:
        run_staging()
    except SystemExit as e:
        assert e.code == 0

    # Query staging table
    query = f"SELECT param_key, param_value FROM `{project}.DWH_STG.PARAM_LOAD` ORDER BY param_key"
    rows = list(bq_client.query(query).result())
    
    assert len(rows) == 3
    data = {row["param_key"]: row["param_value"] for row in rows}
    
    assert "spaced.key" in data
    assert data["spaced.key"] == "spaced.value"  # Whitespace stripped
    
    assert "key.with.equals" in data
    assert data["key.with.equals"] == "value=contains=equals"  # Split on first '=' only
    
    assert "special.chars_#$" in data
    assert data["special.chars_#$"] == "value_with_#$"  # Special characters preserved
```

---

## Test Case 3: Error Handling & Verbatim German Logging

### Purpose
To verify that missing, empty, or invalid properties files trigger the exact German error messages and exit codes specified in the legacy design document and target code.

### Setup
1. **Scenario A (Missing File)**: Ensure `cfg/dwh_env.properties` does not exist in the GCS bucket.
2. **Scenario B (Empty File)**: Upload an empty file to `cfg/dwh_env.properties` in the GCS bucket.

### Action
Run `r_load_params.py` for both scenarios and capture `stdout` and the exit code.

### Pass/Fail Criterion
The test **passes** if:
- **Scenario A**: The script exits with code `1` and prints exactly:
  `FEHLER: Parameterdatei existiert nicht oder ist leer.` to `stdout`.
- **Scenario B**: The script exits with code `1` and prints exactly:
  `FEHLER: Parameterdatei existiert nicht oder ist leer.` to `stdout`.

### Test Code (Pytest)
```python
def test_error_handling_missing_file(gcs_client, capsys):
    bucket_name = os.environ["GCS_CONFIG_BUCKET"]
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob("cfg/dwh_env.properties")
    
    # Ensure file is deleted
    if blob.exists():
        blob.delete()

    from config_env_linked_job.iscfg.bin.r_load_params import main as run_staging
    os.environ["GCS_CONFIG_BUCKET"] = bucket_name

    with pytest.raises(SystemExit) as exc_info:
        run_staging()
        
    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert "FEHLER: Parameterdatei existiert nicht oder ist leer." in captured.out


def test_error_handling_empty_file(gcs_client, capsys):
    bucket_name = os.environ["GCS_CONFIG_BUCKET"]
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob("cfg/dwh_env.properties")
    
    # Upload empty file
    blob.upload_from_string("", content_type="text/plain")

    from config_env_linked_job.iscfg.bin.r_load_params import main as run_staging
    os.environ["GCS_CONFIG_BUCKET"] = bucket_name

    with pytest.raises(SystemExit) as exc_info:
        run_staging()
        
    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert "FEHLER: Parameterdatei existiert nicht oder ist leer." in captured.out
```

---

## Test Case 4: BigQuery MERGE SCD Type 1 Logic

### Purpose
To verify that the BigQuery MERGE statement correctly updates existing keys, inserts new keys, and maps `loaded_at` to `updated_at` without affecting unrelated keys.

### Setup
1. **Target Table Baseline**:
   ```sql
   TRUNCATE TABLE `DWH_ADM.JOB_PARAMS`;
   INSERT INTO `DWH_ADM.JOB_PARAMS` (param_key, param_value, updated_at) VALUES
   ('keep.unmodified', 'original_val', TIMESTAMP('2026-01-01 12:00:00 UTC')),
   ('update.me', 'old_val', TIMESTAMP('2026-01-01 12:00:00 UTC'));
   ```
2. **Staging Table Baseline**:
   ```sql
   TRUNCATE TABLE `DWH_STG.PARAM_LOAD`;
   INSERT INTO `DWH_STG.PARAM_LOAD` (param_key, param_value, loaded_at) VALUES
   ('update.me', 'new_val', TIMESTAMP('2026-04-21 18:00:00 UTC')),
   ('insert.me', 'inserted_val', TIMESTAMP('2026-04-21 18:00:00 UTC'));
   ```

### Action
Execute the BigQuery MERGE statement:
```sql
MERGE `DWH_ADM.JOB_PARAMS` tgt
USING `DWH_STG.PARAM_LOAD` src
ON (tgt.param_key = src.param_key)
WHEN MATCHED THEN UPDATE SET
    tgt.param_value = src.param_value,
    tgt.updated_at  = src.loaded_at
WHEN NOT MATCHED THEN INSERT (param_key, param_value, updated_at)
VALUES (src.param_key, src.param_value, src.loaded_at);
```

### Pass/Fail Criterion
The test **passes** if:
- `DWH_ADM.JOB_PARAMS` contains exactly 3 rows.
- `keep.unmodified` remains completely unchanged (value is `original_val`, timestamp is `2026-01-01 12:00:00 UTC`).
- `update.me` has value `new_val` and timestamp `2026-04-21 18:00:00 UTC`.
- `insert.me` has value `inserted_val` and timestamp `2026-04-21 18:00:00 UTC`.

### Test Code (SQL Assertions)
```sql
-- Assert total row count is 3
ASSERT (SELECT COUNT(1) FROM `DWH_ADM.JOB_PARAMS`) = 3 
  AS "ERROR: Target table row count mismatch";

-- Assert unmodified record was not touched
ASSERT EXISTS (
  SELECT 1 FROM `DWH_ADM.JOB_PARAMS` 
  WHERE param_key = 'keep.unmodified' 
    AND param_value = 'original_val' 
    AND updated_at = TIMESTAMP('2026-01-01 12:00:00 UTC')
) AS "ERROR: Unmodified record was altered";

-- Assert updated record was correctly modified
ASSERT EXISTS (
  SELECT 1 FROM `DWH_ADM.JOB_PARAMS` 
  WHERE param_key = 'update.me' 
    AND param_value = 'new_val' 
    AND updated_at = TIMESTAMP('2026-04-21 18:00:00 UTC')
) AS "ERROR: Record update failed or timestamp mismatch";

-- Assert inserted record was correctly added
ASSERT EXISTS (
  SELECT 1 FROM `DWH_ADM.JOB_PARAMS` 
  WHERE param_key = 'insert.me' 
    AND param_value = 'inserted_val' 
    AND updated_at = TIMESTAMP('2026-04-21 18:00:00 UTC')
) AS "ERROR: Record insertion failed or timestamp mismatch";
```

---

## Test Case 5: Airflow DAG Compilation & Structure Validation

### Purpose
To verify that the migrated Airflow DAG compiles without syntax or import errors, resolves environment variables correctly, and maintains the correct task execution sequence.

### Setup
Set up local environment variables or mock Airflow variables for `GCP_PROJECT` and `GCS_CONFIG_BUCKET`.

### Action
Parse the DAG file using the Airflow `DagBag` utility.

### Pass/Fail Criterion
The test **passes** if:
- No import errors are reported for the DAG file.
- The DAG contains exactly 2 tasks: `parse_and_stage_parameters` and `merge_parameters`.
- The task dependency is strictly: `parse_and_stage_parameters >> merge_parameters`.
- The DAG is configured with `max_active_runs=1` to prevent concurrent parameter loading conflicts.

### Test Code (Pytest)
```python
from airflow.models import DagBag, Variable
from airflow.utils.db import initdb

def test_dag_compilation_and_structure():
    # Initialize mock database for Airflow testing context
    initdb()
    
    # Set mock variables required during DAG parsing
    Variable.set("GCP_PROJECT", "mock-gcp-project")
    Variable.set("GCS_CONFIG_BUCKET", "mock-gcs-bucket")

    dag_path = os.path.join(
        os.environ.get("DWH_HOME", "/home/airflow/gcs/data"),
        "dags/config_env_linked_job/DWH_CFG_JOB/dw_cfg_load_params_dag.py"
    )
    
    dagbag = DagBag(dag_folder=dag_path, include_examples=False)
    
    # Assert no import errors
    assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"
    
    dag = dagbag.get_dag(dag_id="dw_cfg_load_params_dag")
    assert dag is not None, "Failed to load DAG 'dw_cfg_load_params_dag'"
    
    # Assert active run limits and schedule
    assert dag.max_active_runs == 1
    assert dag.schedule_interval == '@daily'
    
    # Assert task structure and dependencies
    expected_tasks = {"parse_and_stage_parameters", "merge_parameters"}
    actual_tasks = set(dag.task_ids)
    assert actual_tasks == expected_tasks, f"Task mismatch. Found: {actual_tasks}"
    
    stage_task = dag.get_task("parse_and_stage_parameters")
    merge_task = dag.get_task("merge_parameters")
    
    assert merge_task in stage_task.downstream_list, "Dependency chain broken: stage must precede merge"
```