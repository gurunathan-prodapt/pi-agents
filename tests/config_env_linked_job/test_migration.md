# Migration Validation Test Suite: DW.CFG_LOAD_PARAMS

This document defines the migration-validation test suite for the `DW.CFG_LOAD_PARAMS` job migration. It ensures behavioral equivalence between the legacy Oracle/KornShell implementation and the target Cloud Composer/BigQuery architecture.

---

## Test Case 1: Properties File Validation & Missing File Handling

### Purpose
Verify that the migrated Python script (`r_load_params.py`) behaves identically to the legacy KornShell script when the configuration properties file is missing or inaccessible. It must exit with code `8` and output the exact German error message to `stderr`.

### Setup
1. Ensure the environment variable `DWH_HOME` is set to a temporary test directory.
2. Ensure **no** file exists at `${DWH_HOME}/cfg/dwh_env.properties`.

### Action
Execute the migrated Python script:
```bash
export GCP_PROJECT="test-gcp-project"
export DWH_HOME="/tmp/non_existent_dwh_home"
python3 config_env_linked_job/iscfg/bin/r_load_params.py
```

### Pass/Fail Criterion
* **Pass**: The script exits with exit code `8` and prints exactly `FEHLER: Parameterdatei /tmp/non_existent_dwh_home/cfg/dwh_env.properties nicht gefunden` to `stderr`.
* **Fail**: The script exits with any other code, or the error message does not match the legacy German literal character-for-character.

### Test Code (pytest)
```python
import os
import subprocess
import pytest

def test_missing_properties_file(tmp_path):
    # Setup empty DWH_HOME directory with no config file
    dwh_home = tmp_path / "dwh_home"
    os.makedirs(dwh_home / "cfg", exist_ok=True)
    
    script_path = os.path.abspath("config_env_linked_job/iscfg/bin/r_load_params.py")
    
    # Run script with missing properties file
    env = os.environ.copy()
    env["DWH_HOME"] = str(dwh_home)
    env["GCP_PROJECT"] = "mock-project"
    
    result = subprocess.run(
        [sys.executable, script_path],
        env=env,
        capture_output=True,
        text=True
    )
    
    expected_err = f"FEHLER: Parameterdatei {dwh_home}/cfg/dwh_env.properties nicht gefunden\n"
    
    assert result.returncode == 8
    assert result.stderr == expected_err
```

---

## Test Case 2: Properties Parsing & BigQuery Ingestion Parity (SQL*Loader Replacement)

### Purpose
Verify that the Python script correctly parses key-value pairs from `dwh_env.properties` (including handling comments, whitespace, and special characters like German umlauts) and loads them into the BigQuery staging table `PARAM_LOAD` with a `WRITE_TRUNCATE` disposition.

### Setup
1. Create a mock `dwh_env.properties` containing standard parameters, comments, blank lines, and German characters.
2. Pre-populate the BigQuery staging table `PARAM_LOAD` with dummy records to verify truncation.

### Action
1. Write the following content to `${DWH_HOME}/cfg/dwh_env.properties`:
   ```properties
   # This is a comment
   db.host=dw-oracle-host.local
   db.sid=DWHDWH1P
   stage.table=PARAM_LOAD
   
   # Parameter with German Umlaut
   system.status=aktiviert_über_prüfstand
   ```
2. Execute the Python script.
3. Query the BigQuery staging table `PARAM_LOAD`.

### Pass/Fail Criterion
* **Pass**: 
  * The staging table is truncated (old dummy records are gone).
  * The parsed keys and values are loaded exactly as defined.
  * Special characters (e.g., `ü`) are preserved without encoding corruption.
  * Metadata columns are populated correctly.
* **Fail**: Legacy records persist, keys/values are misaligned, or encoding issues occur.

### Test Code (pytest / BigQuery Validation)
```python
import os
import pytest
from google.cloud import bigquery

@pytest.fixture
def bq_client():
    return bigquery.Client()

def test_properties_parsing_and_ingestion(bq_client, tmp_path):
    # 1. Setup mock properties file
    dwh_home = tmp_path / "dwh_home"
    cfg_dir = dwh_home / "cfg"
    os.makedirs(cfg_dir, exist_ok=True)
    
    props_file = cfg_dir / "dwh_env.properties"
    props_content = (
        "# Test properties\n"
        "db.host=dw-oracle-host.local\n"
        "db.sid=DWHDWH1P\n"
        "stage.table=PARAM_LOAD\n"
        "system.status=aktiviert_über_prüfstand\n"
    )
    props_file.write_text(props_content, encoding="utf-8")
    
    # Pre-populate target table with a stale record to test WRITE_TRUNCATE
    project = os.environ["GCP_PROJECT"]
    dataset = os.environ.get("BQ_DATASET_STG", "DW_STG")
    table_id = f"{project}.{dataset}.PARAM_LOAD"
    
    # Ensure table exists and has stale data
    bq_client.query(f"CREATE OR REPLACE TABLE `{table_id}` (param_key STRING, param_value STRING)").result()
    bq_client.query(f"INSERT INTO `{table_id}` VALUES ('stale_key', 'stale_val')").result()
    
    # 2. Run the ingestion script
    script_path = os.path.abspath("config_env_linked_job/iscfg/bin/r_load_params.py")
    env = os.environ.copy()
    env["DWH_HOME"] = str(dwh_home)
    
    result = subprocess.run(
        [sys.executable, script_path],
        env=env,
        capture_output=True,
        text=True
    )
    
    assert result.returncode == 0
    
    # 3. Assert BigQuery Table State
    query = f"SELECT param_key, param_value FROM `{table_id}` ORDER BY param_key"
    rows = list(bq_client.query(query).result())
    
    # Verify truncation (stale_key must be gone)
    keys = [row["param_key"] for row in rows]
    assert "stale_key" not in keys
    
    # Verify exact parsed values
    expected_data = {
        "db.host": "dw-oracle-host.local",
        "db.sid": "DWHDWH1P",
        "stage.table": "PARAM_LOAD",
        "system.status": "aktiviert_über_prüfstand"
    }
    
    actual_data = {row["param_key"]: row["param_value"] for row in rows}
    assert actual_data == expected_data
```

---

## Test Case 3: Transformation Correctness (SQL Merge Parity)

### Purpose
Verify that the SQL merge statement (`d_param_load.sql`) correctly upserts parameters from the staging table (`PARAM_LOAD`) into the master parameter table (`JOB_PARAMS`). It must:
1. Update values and `updated_at` timestamps for existing keys.
2. Insert new keys with their corresponding values and timestamps.
3. Handle NULL values correctly without breaking the merge join.

### Setup
1. Create the target table `DWH_ADM.JOB_PARAMS` with pre-existing records.
2. Populate the staging table `DWH_STG.PARAM_LOAD` with a mix of updates, inserts, and NULL values.

### Action
Execute the merge query using the BigQuery client.

### Pass/Fail Criterion
* **Pass**:
  * Existing keys are updated with the new staging values.
  * New keys are inserted.
  * `updated_at` is updated to match the staging `loaded_at` timestamp.
  * Unmatched target records remain completely unaffected.
* **Fail**: Duplicate keys are created, timestamps are misaligned, or unaffected rows are modified.

### Test Code (SQL Assertions)
```sql
-- Setup Target Table
CREATE OR REPLACE TABLE `DWH_ADM.JOB_PARAMS` (
    param_key STRING NOT NULL,
    param_value STRING,
    updated_at TIMESTAMP
);

INSERT INTO `DWH_ADM.JOB_PARAMS` (param_key, param_value, updated_at) VALUES
('keep_unchanged', 'original_val', TIMESTAMP('2026-01-01 00:00:00 UTC')),
('update_me', 'old_val', TIMESTAMP('2026-01-01 00:00:00 UTC'));

-- Setup Staging Table
CREATE OR REPLACE TABLE `DWH_STG.PARAM_LOAD` (
    param_key STRING NOT NULL,
    param_value STRING,
    loaded_at TIMESTAMP
);

INSERT INTO `DWH_STG.PARAM_LOAD` (param_key, param_value, loaded_at) VALUES
('update_me', 'new_val', TIMESTAMP('2026-04-21 12:00:00 UTC')),
('insert_me', 'inserted_val', TIMESTAMP('2026-04-21 12:00:00 UTC')),
('null_val_key', NULL, TIMESTAMP('2026-04-21 12:00:00 UTC'));

-- Execute Converted Merge Statement
MERGE INTO `DWH_ADM.JOB_PARAMS` tgt
USING (
    SELECT param_key, param_value, loaded_at
    FROM   `DWH_STG.PARAM_LOAD`
) src
ON (tgt.param_key = src.param_key)
WHEN MATCHED THEN UPDATE SET
    param_value = src.param_value,
    updated_at  = src.loaded_at
WHEN NOT MATCHED THEN INSERT (param_key, param_value, updated_at)
VALUES (src.param_key, src.param_value, src.loaded_at);

-- VALIDATION ASSERTIONS
-- Assertion 1: Verify 'update_me' was updated correctly
ASSERT (
    SELECT param_value = 'new_val' AND updated_at = TIMESTAMP('2026-04-21 12:00:00 UTC')
    FROM `DWH_ADM.JOB_PARAMS`
    WHERE param_key = 'update_me'
) AS "Error: 'update_me' was not updated correctly";

-- Assertion 2: Verify 'insert_me' was inserted correctly
ASSERT (
    SELECT param_value = 'inserted_val' AND updated_at = TIMESTAMP('2026-04-21 12:00:00 UTC')
    FROM `DWH_ADM.JOB_PARAMS`
    WHERE param_key = 'insert_me'
) AS "Error: 'insert_me' was not inserted";

-- Assertion 3: Verify 'null_val_key' was inserted with NULL value
ASSERT (
    SELECT param_value IS NULL AND updated_at = TIMESTAMP('2026-04-21 12:00:00 UTC')
    FROM `DWH_ADM.JOB_PARAMS`
    WHERE param_key = 'null_val_key'
) AS "Error: 'null_val_key' was not inserted with NULL value";

-- Assertion 4: Verify 'keep_unchanged' was not modified
ASSERT (
    SELECT param_value = 'original_val' AND updated_at = TIMESTAMP('2026-01-01 00:00:00 UTC')
    FROM `DWH_ADM.JOB_PARAMS`
    WHERE param_key = 'keep_unchanged'
) AS "Error: Unmatched target row was modified";
```

---

## Test Case 4: Error Handling & Exit Code Parity (German Log Literals)

### Purpose
Verify that if the post-load SQL execution fails (e.g., due to a syntax error, schema mismatch, or database connection failure), the Python script catches the exception, prints the exact legacy German error message to `stderr`, and exits with code `1`.

### Setup
1. Create a valid properties file so the script passes initial validation.
2. Inject a malformed SQL statement into `d_param_load.sql` to force a BigQuery execution failure.

### Action
Execute the Python script:
```bash
python3 config_env_linked_job/iscfg/bin/r_load_params.py
```

### Pass/Fail Criterion
* **Pass**:
  * The script exits with exit code `1`.
  * The output to `stderr` contains exactly: `FEHLER: d_param_load.sql beendet mit RC=1`.
* **Fail**: The script exits with code `0`, or the exact German error message is missing from the logs.

### Test Code (pytest)
```python
import os
import sys
import subprocess
import pytest

def test_sql_execution_failure_handling(tmp_path):
    # 1. Setup valid properties file
    dwh_home = tmp_path / "dwh_home"
    cfg_dir = dwh_home / "cfg"
    os.makedirs(cfg_dir, exist_ok=True)
    
    props_file = cfg_dir / "dwh_env.properties"
    props_file.write_text("stage.table=PARAM_LOAD\n", encoding="utf-8")
    
    # 2. Setup malformed SQL file to force BigQuery failure
    sql_file = cfg_dir / "d_param_load.sql"
    sql_file.write_text("MERGE INTO INVALID_TABLE_NAME USING (SELECT 1) ON FALSE;", encoding="utf-8")
    
    script_path = os.path.abspath("config_env_linked_job/iscfg/bin/r_load_params.py")
    
    env = os.environ.copy()
    env["DWH_HOME"] = str(dwh_home)
    env["GCP_PROJECT"] = os.environ.get("GCP_PROJECT", "mock-project")
    
    # 3. Execute script
    result = subprocess.run(
        [sys.executable, script_path],
        env=env,
        capture_output=True,
        text=True
    )
    
    # Verify exit code and legacy error message
    assert result.returncode == 1
    assert "FEHLER: d_param_load.sql beendet mit RC=1" in result.stderr
```

---

## Test Case 5: End-to-End DAG Orchestration Validation

### Purpose
Verify that the Cloud Composer Airflow DAG (`dw_cfg_load_params_dag.py`) compiles without syntax errors, has the correct task structure, and passes the required environment variables to the execution task.

### Setup
Place the DAG file in a local Airflow testing environment or parse it using the `DAGBag` utility.

### Action
Run a Python test using the Airflow `DagBag` library to validate the DAG structure.

### Pass/Fail Criterion
* **Pass**:
  * The DAG is loaded with zero import errors.
  * The DAG contains the single execution task `r_load_params`.
  * The task is configured with the correct `BashOperator` command pointing to the Python script.
* **Fail**: Import errors are raised, or task dependencies/configurations are incorrect.

### Test Code (pytest / Airflow)
```python
import pytest
from airflow.models import DagBag, Variable
from airflow.utils.db import initdb

@pytest.fixture(scope="module", autouse=True)
def setup_airflow_db():
    # Initialize temporary Airflow DB for testing
    initdb()
    # Set required global Airflow variables
    Variable.set("GCP_PROJECT", "test-gcp-project")
    Variable.set("GCS_BUCKET", "test-gcs-bucket")
    Variable.set("DWH_HOME", "/tmp/dwh_home")
    Variable.set("DWH_LOG_DIR", "/tmp/dwh_logs")

def test_dag_compiles_and_has_correct_structure():
    dag_bag = DagBag(dag_folder="config_env_linked_job", include_examples=False)
    
    # Verify no import errors
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"
    
    dag = dag_bag.get_dag(dag_id="dw_cfg_load_params")
    assert dag is not None
    
    # Verify task existence
    assert "r_load_params" in dag.task_ids
    task = dag.get_task("r_load_params")
    
    # Verify task configuration
    assert task.bash_command == "python3 /tmp/dwh_home/config_env_linked_job/iscfg/bin/r_load_params.py"
    assert task.env["GCP_PROJECT"] == "test-gcp-project"
    assert task.env["GCS_BUCKET"] == "test-gcs-bucket"
```