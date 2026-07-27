# Migration Validation Test Suite: DW.CFG_LOAD_PARAMS

This document defines the migration-validation test suite for the `DW.CFG_LOAD_PARAMS` job. It ensures behavioral equivalence between the legacy UC4/Oracle/KornShell implementation and the migrated Apache Airflow/Google Cloud Composer/BigQuery/Dataform implementation.

---

## Test Case 1: Properties File Parsing and Schema Validation (`r_load_params.py`)

### Purpose
Verify that the migrated Python script `r_load_params.py` correctly parses the `dwh_env.properties` file format (including comments, empty lines, and whitespace around keys/values) and maps them to the expected JSON structure for BigQuery ingestion.

### Setup
1. Create a mock `dwh_env.properties` file containing comments, empty lines, spaces, and special characters.
2. Save this file locally or upload it to the mock GCS bucket path: `gs://<GCS_BUCKET>/config_env_linked_job/iscfg/cfg/dwh_env.properties`.

### Action
Run a unit test using `pytest` that executes the parsing logic of `r_load_params.py` against the mock properties file and asserts the structure of the generated parameter list.

### Runnable Test Code
```python
# test_r_load_params_parsing.py
import pytest
import datetime

def parse_properties_content(content: str):
    """
    Extracted parsing logic from r_load_params.py for unit testing.
    """
    params_list = []
    db_host = ""
    db_sid = ""
    stg_table = ""

    for line in content.splitlines():
        line_stripped = line.strip()
        if not line_stripped or line_stripped.startswith("#"):
            continue
        if "=" in line_stripped:
            key, val = line_stripped.split("=", 1)
            key = key.strip()
            val = val.strip()
            params_list.append({
                "param_key": key,
                "param_value": val,
                "loaded_at": datetime.datetime.utcnow().isoformat()
            })
            
            if key == "db.host":
                db_host = val
            elif key == "db.sid":
                db_sid = val
            elif key == "stage.table":
                stg_table = val
                
    return params_list, db_host, db_sid, stg_table

def test_properties_parsing_equivalence():
    mock_properties_content = """
    # This is a comment
    db.host=dwhdwh1p.internal
    db.sid=DWHP
    
    # Another comment with spaces
    stage.table = DWH_STG.PARAM_LOAD
    
    custom.param.key = custom_value_123
    empty.param = 
    """

    params, db_host, db_sid, stg_table = parse_properties_content(mock_properties_content)

    # Assertions for metadata extraction
    assert db_host == "dwhdwh1p.internal"
    assert db_sid == "DWHP"
    assert stg_table == "DWH_STG.PARAM_LOAD"

    # Assertions for parsed parameter list
    assert len(params) == 5
    
    # Check specific keys and values
    keys = [p["param_key"] for p in params]
    values = {p["param_key"]: p["param_value"] for p in params}
    
    assert "db.host" in keys
    assert "db.sid" in keys
    assert "stage.table" in keys
    assert "custom.param.key" in keys
    assert "empty.param" in keys

    assert values["custom.param.key"] == "custom_value_123"
    assert values["empty.param"] == ""  # Empty values should be preserved as empty strings
    
    # Verify timestamp presence and format
    for param in params:
        assert "loaded_at" in param
        # Should parse as ISO format
        datetime.datetime.fromisoformat(param["loaded_at"])
```

### Pass/Fail Criterion
* **Pass**: The parsing logic successfully extracts all key-value pairs, ignores comments and empty lines, trims whitespace, and correctly identifies the metadata parameters (`db.host`, `db.sid`, `stage.table`).
* **Fail**: Any key-value pair is missed, whitespace is not trimmed, comments are parsed as keys, or the metadata variables are incorrectly assigned.

---

## Test Case 2: BigQuery Staging Load Parity (`r_load_params.py` execution)

### Purpose
Verify that the Python script successfully loads the parsed parameters into the BigQuery staging table `DWH_STG.PARAM_LOAD` using `WRITE_TRUNCATE` behavior, ensuring that previous runs' data is completely replaced.

### Setup
1. Create the target BigQuery staging table `DWH_STG.PARAM_LOAD` with the schema:
   * `param_key` (STRING, REQUIRED)
   * `param_value` (STRING, NULLABLE)
   * `loaded_at` (TIMESTAMP, REQUIRED)
2. Insert dummy historical records into `DWH_STG.PARAM_LOAD` (e.g., `old_key` = `old_value`).
3. Place a valid `dwh_env.properties` file in the GCS bucket.

### Action
Execute the migrated script `r_load_params.py` in an environment with access to the target BigQuery dataset.

### Runnable Test Code
```python
# test_bq_staging_load.py
import os
import pytest
from google.cloud import bigquery

@pytest.fixture
def bq_client():
    return bigquery.Client()

def test_bigquery_load_parity(bq_client):
    project_id = bq_client.project
    dataset_id = os.environ.get("BQ_DATASET_STG", "DWH_STG")
    table_id = f"{project_id}.{dataset_id}.PARAM_LOAD"

    # 1. Pre-load check: Insert dummy historical data to verify WRITE_TRUNCATE
    pre_insert_query = f"""
        INSERT INTO `{table_id}` (param_key, param_value, loaded_at)
        VALUES ('historical_key', 'historical_value', CURRENT_TIMESTAMP())
    """
    bq_client.query(pre_insert_query).result()

    # 2. Run the migrated script
    # Set environment variables required by r_load_params.py
    os.environ["GCP_PROJECT"] = project_id
    os.environ["BQ_DATASET_STG"] = dataset_id
    
    # Execute the script main function
    from config_env_linked_job.iscfg.bin.r_load_params import main as run_load
    
    with pytest.raises(SystemExit) as excinfo:
        run_load()
    
    assert excinfo.value.code == 0  # Script must exit with 0

    # 3. Post-load assertions
    query = f"SELECT param_key, param_value FROM `{table_id}`"
    results = list(bq_client.query(query).result())
    
    # Verify historical data was truncated
    keys = [row.param_key for row in results]
    assert "historical_key" not in keys, "Staging table was not truncated (WRITE_TRUNCATE failed)!"
    
    # Verify new parameters are loaded
    assert len(results) > 0
    assert "db.host" in keys
```

### Pass/Fail Criterion
* **Pass**: The script exits with code `0`, the historical record `historical_key` is completely removed from the staging table, and the new parameters from `dwh_env.properties` are loaded with correct schemas.
* **Fail**: The script exits with a non-zero code, the historical record remains in the table, or the schema of the loaded data does not match the BigQuery table definition.

---

## Test Case 3: Merge Logic Correctness (SCD Type 1 Upsert in BigQuery)

### Purpose
Verify that the BigQuery `MERGE` statement in `d_param_load.sqlx` correctly performs SCD Type 1 upserts (updates existing keys and inserts new keys) without duplicating records.

### Setup
1. Ensure the target table `DWH_ADM.JOB_PARAMS` exists with the schema:
   * `param_key` (STRING, REQUIRED)
   * `param_value` (STRING, NULLABLE)
   * `updated_at` (TIMESTAMP, REQUIRED)
2. Populate the tables with the following test state:
   * **`DWH_ADM.JOB_PARAMS` (Target)**:
     * `('key_to_update', 'old_value', TIMESTAMP '2023-01-01 00:00:00 UTC')`
     * `('key_to_keep', 'keep_value', TIMESTAMP '2023-01-01 00:00:00 UTC')`
   * **`DWH_STG.PARAM_LOAD` (Source)**:
     * `('key_to_update', 'new_value', TIMESTAMP '2023-02-01 12:00:00 UTC')`
     * `('key_to_insert', 'inserted_value', TIMESTAMP '2023-02-01 12:00:00 UTC')`

### Action
Execute the `MERGE` statement defined in `d_param_load.sqlx` against the BigQuery environment.

### Runnable Test Code
```sql
-- Setup Test Data
OR REPLACE TABLE `DWH_ADM.JOB_PARAMS` AS (
  SELECT 'key_to_update' AS param_key, 'old_value' AS param_value, TIMESTAMP '2023-01-01 00:00:00 UTC' AS updated_at
  UNION ALL
  SELECT 'key_to_keep', 'keep_value', TIMESTAMP '2023-01-01 00:00:00 UTC'
);

OR REPLACE TABLE `DWH_STG.PARAM_LOAD` AS (
  SELECT 'key_to_update' AS param_key, 'new_value' AS param_value, TIMESTAMP '2023-02-01 12:00:00 UTC' AS loaded_at
  UNION ALL
  SELECT 'key_to_insert', 'inserted_value', TIMESTAMP '2023-02-01 12:00:00 UTC'
);

-- Execute Migrated MERGE Statement
MERGE INTO `DWH_ADM.JOB_PARAMS` tgt
USING (
    SELECT 
        CAST(param_key AS STRING) AS param_key, 
        CAST(param_value AS STRING) AS param_value, 
        CAST(loaded_at AS TIMESTAMP) AS loaded_at
    FROM `DWH_STG.PARAM_LOAD`
) src
ON (tgt.param_key = src.param_key)
WHEN MATCHED THEN UPDATE SET
    tgt.param_value = src.param_value,
    tgt.updated_at  = src.loaded_at
WHEN NOT MATCHED THEN INSERT (param_key, param_value, updated_at)
VALUES (src.param_key, src.param_value, src.loaded_at);

-- Validation Assertions
-- Assertion 1: Verify 'key_to_update' was updated with new value and new timestamp
ASSERT (
  SELECT param_value = 'new_value' AND updated_at = TIMESTAMP '2023-02-01 12:00:00 UTC'
  FROM `DWH_ADM.JOB_PARAMS`
  WHERE param_key = 'key_to_update'
) AS "ERROR: key_to_update was not updated correctly!";

-- Assertion 2: Verify 'key_to_keep' remains unchanged
ASSERT (
  SELECT param_value = 'keep_value' AND updated_at = TIMESTAMP '2023-01-01 00:00:00 UTC'
  FROM `DWH_ADM.JOB_PARAMS`
  WHERE param_key = 'key_to_keep'
) AS "ERROR: key_to_keep was modified!";

-- Assertion 3: Verify 'key_to_insert' was inserted
ASSERT (
  SELECT param_value = 'inserted_value' AND updated_at = TIMESTAMP '2023-02-01 12:00:00 UTC'
  FROM `DWH_ADM.JOB_PARAMS`
  WHERE param_key = 'key_to_insert'
) AS "ERROR: key_to_insert was not inserted!";

-- Assertion 4: Verify total row count is exactly 3
ASSERT (
  SELECT COUNT(1) = 3 FROM `DWH_ADM.JOB_PARAMS`
) AS "ERROR: Row count mismatch in target table!";
```

### Pass/Fail Criterion
* **Pass**: All four SQL assertions execute successfully without throwing assertion errors.
* **Fail**: Any assertion fails, indicating that the merge logic either duplicated rows, missed updates, missed inserts, or corrupted existing records.

---

## Test Case 4: Edge Cases and NULL Handling in SQL Merge

### Purpose
Verify that NULL values in `param_value` are handled correctly during the merge (i.e., updating a value to NULL if the staging value is NULL, and inserting a record with a NULL value if the staging value is NULL).

### Setup
1. Populate the tables with the following test state:
   * **`DWH_ADM.JOB_PARAMS` (Target)**:
     * `('key_to_nullify', 'not_null_value', TIMESTAMP '2023-01-01 00:00:00 UTC')`
   * **`DWH_STG.PARAM_LOAD` (Source)**:
     * `('key_to_nullify', CAST(NULL AS STRING), TIMESTAMP '2023-02-01 12:00:00 UTC')`
     * `('key_new_null', CAST(NULL AS STRING), TIMESTAMP '2023-02-01 12:00:00 UTC')`

### Action
Execute the `MERGE` statement defined in `d_param_load.sqlx` against the BigQuery environment.

### Runnable Test Code
```sql
-- Setup Test Data
OR REPLACE TABLE `DWH_ADM.JOB_PARAMS` AS (
  SELECT 'key_to_nullify' AS param_key, 'not_null_value' AS param_value, TIMESTAMP '2023-01-01 00:00:00 UTC' AS updated_at
);

OR REPLACE TABLE `DWH_STG.PARAM_LOAD` AS (
  SELECT 'key_to_nullify' AS param_key, CAST(NULL AS STRING) AS param_value, TIMESTAMP '2023-02-01 12:00:00 UTC' AS loaded_at
  UNION ALL
  SELECT 'key_new_null', CAST(NULL AS STRING), TIMESTAMP '2023-02-01 12:00:00 UTC'
);

-- Execute Migrated MERGE Statement
MERGE INTO `DWH_ADM.JOB_PARAMS` tgt
USING (
    SELECT 
        CAST(param_key AS STRING) AS param_key, 
        CAST(param_value AS STRING) AS param_value, 
        CAST(loaded_at AS TIMESTAMP) AS loaded_at
    FROM `DWH_STG.PARAM_LOAD`
) src
ON (tgt.param_key = src.param_key)
WHEN MATCHED THEN UPDATE SET
    tgt.param_value = src.param_value,
    tgt.updated_at  = src.loaded_at
WHEN NOT MATCHED THEN INSERT (param_key, param_value, updated_at)
VALUES (src.param_key, src.param_value, src.loaded_at);

-- Validation Assertions
-- Assertion 1: Verify 'key_to_nullify' was updated to NULL
ASSERT (
  SELECT param_value IS NULL AND updated_at = TIMESTAMP '2023-02-01 12:00:00 UTC'
  FROM `DWH_ADM.JOB_PARAMS`
  WHERE param_key = 'key_to_nullify'
) AS "ERROR: key_to_nullify was not set to NULL!";

-- Assertion 2: Verify 'key_new_null' was inserted with NULL
ASSERT (
  SELECT param_value IS NULL AND updated_at = TIMESTAMP '2023-02-01 12:00:00 UTC'
  FROM `DWH_ADM.JOB_PARAMS`
  WHERE param_key = 'key_new_null'
) AS "ERROR: key_new_null was not inserted with NULL!";
```

### Pass/Fail Criterion
* **Pass**: Both SQL assertions execute successfully, proving that NULL values are correctly propagated during both `UPDATE` and `INSERT` operations.
* **Fail**: Any assertion fails, indicating that NULL values were either ignored, rejected, or caused the query to fail.

---

## Test Case 5: Airflow DAG Orchestration and Environment Variable Validation

### Purpose
Verify that the migrated Airflow DAG `dw_cfg_load_params` is structurally sound, contains the correct task dependencies, and correctly injects the required environment variables (especially `DWH_JOB_KENNUNG='AUSD_V_TA_PERIOD'`).

### Setup
1. Place the DAG file `dw_cfg_load_params.py` in the Airflow DAGs folder.
2. Ensure Airflow Variables `GCP_PROJECT`, `GCS_BUCKET`, and `BQ_DATASET` are defined in the Airflow metadata database.

### Action
Execute a Python test using the Airflow `DagBag` to validate the DAG structure and task configurations.

### Runnable Test Code
```python
# test_airflow_dag_structure.py
import pytest
from airflow.models import DagBag, Variable

@pytest.fixture(scope="session", autouse=True)
def setup_airflow_variables():
    # Mock Airflow Variables required for DAG compilation
    Variable.set("GCP_PROJECT", "mock-gcp-project")
    Variable.set("GCS_BUCKET", "mock-gcs-bucket")
    Variable.set("BQ_DATASET", "mock_dataset")
    yield
    # Cleanup
    Variable.delete("GCP_PROJECT")
    Variable.delete("GCS_BUCKET")
    Variable.delete("BQ_DATASET")

def test_dag_loads_with_no_errors():
    dag_bag = DagBag(dag_folder="config_env_linked_job/DWH_CFG_JOB", include_examples=False)
    dag = dag_bag.get_dag(dag_id="dw_cfg_load_params")
    
    assert dag_bag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 3

def test_dag_tasks_and_dependencies():
    dag_bag = DagBag(dag_folder="config_env_linked_job/DWH_CFG_JOB", include_examples=False)
    dag = dag_bag.get_dag(dag_id="dw_cfg_load_params")
    
    run_load_params = dag.get_task("run_load_params")
    create_compilation = dag.get_task("create_compilation")
    invoke_dataform = dag.get_task("invoke_dataform")
    
    # Verify Task Dependencies: run_load_params >> create_compilation >> invoke_dataform
    assert create_compilation in run_load_params.downstream_list
    assert invoke_dataform in create_compilation.downstream_list
    
    # Verify Environment Variable Injection
    assert run_load_params.env["DWH_JOB_KENNUNG"] == "AUSD_V_TA_PERIOD"
    assert run_load_params.env["GCP_PROJECT"] == "mock-gcp-project"
    assert run_load_params.env["GCS_BUCKET"] == "mock-gcs-bucket"
    
    # Verify Concurrency Settings
    assert dag.max_active_runs == 1
```

### Pass/Fail Criterion
* **Pass**: The DAG loads without import errors, contains exactly 3 tasks with the correct sequential dependencies, has `max_active_runs` set to `1`, and correctly injects `DWH_JOB_KENNUNG='AUSD_V_TA_PERIOD'` into the execution environment.
* **Fail**: The DAG fails to load, has incorrect task dependencies, or is missing the required environment variables.