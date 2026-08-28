# Migration Validation Test Suite: DW.DWH_PFNW_ILV_FLAGTEST

This document defines the migration-validation test suite for the migrated job `DW.DWH_PFNW_ILV_FLAGTEST`. The test suite ensures behavioral equivalence between the legacy Oracle/KornShell execution and the migrated Google Cloud Platform (BigQuery/Airflow/Python) implementation.

---

## Test Case 1: End-to-End Validation — Flag Not Set (Success Path)

### Purpose
Prove that when the Maximo "Fill-Flag" is **not** set to `0` (meaning no rows match the query criteria), the Python script `k_pfnw_ilv_flagtest.py` exits with code `0`. This indicates to the orchestrator that the validation passed and downstream jobs are allowed to proceed.

### Setup
1. **Database State**: Populate the BigQuery table `is_maint_schema.dwh_ta_k_ilv_abr_ilv` such that **no rows** match the criteria `quellsystem = 'MAXIMO' AND ilv_teilschritt = 'FILL' AND job_starten_flag = 0`.
   * *Option A*: The table is completely empty.
   * *Option B*: The table contains a row where `job_starten_flag = 1` (flag is not active).
2. **Environment Variables**:
   * Set `DWH_JOB_KENNUNG` to `PFNW_ILV_FLAGTEST`.
   * Set `GOOGLE_APPLICATION_CREDENTIALS` to point to a service account with read access to the BigQuery dataset.

### Action
Execute the migrated Python script passing the migrated SQL file:
```bash
python3 k_pfnw_ilv_flagtest.py \
  -q d_pfnw_ilv_flagtest.sql \
  -j PFNW_ILV_FLAGTEST \
  -v
```

### Pass/Fail Criterion
* **Pass**: 
  * The script exits with status code `0`.
  * The generated log file `/tmp/validation_PFNW_ILV_FLAGTEST_0.log` contains the line: `Query returned 0 rows. Validation succeeded.`
* **Fail**: 
  * The script exits with a non-zero status code, or the log indicates that rows were found.

---

## Test Case 2: End-to-End Validation — Flag Set (Block Path)

### Purpose
Prove that when the Maximo "Fill-Flag" **is** set to `0` (meaning a row matches the query criteria), the Python script exits with code `1`. This indicates to the orchestrator that the validation failed (or a block condition is active), halting downstream execution.

### Setup
1. **Database State**: Insert a matching row into the BigQuery table `is_maint_schema.dwh_ta_k_ilv_abr_ilv`:
   ```sql
   INSERT INTO `is_maint_schema.dwh_ta_k_ilv_abr_ilv` (quellsystem, ilv_teilschritt, job_starten_flag)
   VALUES ('MAXIMO', 'FILL', 0);
   ```
2. **Environment Variables**:
   * Set `DWH_JOB_KENNUNG` to `PFNW_ILV_FLAGTEST`.

### Action
Execute the migrated Python script:
```bash
python3 k_pfnw_ilv_flagtest.py \
  -q d_pfnw_ilv_flagtest.sql \
  -j PFNW_ILV_FLAGTEST \
  -v
```

### Pass/Fail Criterion
* **Pass**:
  * The script exits with status code `1`.
  * `stderr` outputs: `Query reported an error - script aborts`.
  * The log file contains: `Query returned 1 rows. Validation failed.`
* **Fail**:
  * The script exits with status code `0`, allowing downstream jobs to run despite the active block flag.

---

## Test Case 3: SQL Dialect and Schema Assertion

### Purpose
Verify that the migrated SQL query `d_pfnw_ilv_flagtest.sql` is syntactically valid in BigQuery, references the correct sanitized table name (replacing the legacy Oracle `$` with `_`), and matches the target schema types.

### Setup
* Access to the target BigQuery environment with the migrated schema deployed.

### Action
Run a programmatic test using `pytest` to dry-run the query and assert schema compatibility:

```python
import pytest
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

def test_sql_syntax_and_schema():
    client = bigquery.Client()
    project_id = client.project
    
    # Read the migrated SQL file
    with open("d_pfnw_ilv_flagtest.sql", "r") as f:
        raw_sql = f.read()
    
    # Render Jinja template parameters if present
    rendered_sql = raw_sql.replace("{{project_id}}", project_id)
    
    # 1. Syntax & Dry-Run Validation
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    try:
        query_job = client.query(rendered_sql, job_config=job_config)
        assert query_job.state == "DONE"
    except GoogleCloudError as e:
        pytest.fail(f"SQL Dry-run failed: {e}")
        
    # 2. Schema Type Assertions
    # Retrieve target table metadata
    table_ref = client.dataset("is_maint_schema").table("dwh_ta_k_ilv_abr_ilv")
    table = client.get_table(table_ref)
    
    schema_dict = {field.name: field.field_type for field in table.schema}
    
    assert "quellsystem" in schema_dict, "Column 'quellsystem' is missing"
    assert schema_dict["quellsystem"] == "STRING", f"Expected STRING, got {schema_dict['quellsystem']}"
    
    assert "ilv_teilschritt" in schema_dict, "Column 'ilv_teilschritt' is missing"
    assert schema_dict["ilv_teilschritt"] == "STRING", f"Expected STRING, got {schema_dict['ilv_teilschritt']}"
    
    assert "job_starten_flag" in schema_dict, "Column 'job_starten_flag' is missing"
    assert schema_dict["job_starten_flag"] in ["INTEGER", "INT64"], f"Expected INTEGER, got {schema_dict['job_starten_flag']}"
```

### Pass/Fail Criterion
* **Pass**: The dry-run completes successfully without syntax errors, and all schema assertions pass.
* **Fail**: Any database exception is raised, or column names/types do not match the expected BigQuery schema.

---

## Test Case 4: Airflow DAG Integrity and Parameter Mapping

### Purpose
Verify that the Airflow DAG `dw_dwh_pfnw_ilv_flagtest` parses without syntax errors, contains the correct task structure, and correctly exposes the required job parameters.

### Setup
* Airflow environment or local development environment with `apache-airflow` installed.

### Action
Run a programmatic DAG validation test:

```python
import pytest
from airflow.models import DagBag

def test_dag_integrity():
    dag_bag = DagBag(dag_folder=".", include_examples=False)
    
    # 1. Assert no import errors
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"
    
    # 2. Assert DAG exists
    dag_id = "dw_dwh_pfnw_ilv_flagtest"
    dag = dag_bag.get_dag(dag_id)
    assert dag is not None, f"DAG {dag_id} not found"
    
    # 3. Assert Task Structure
    task_id = "dw_dwh_pfnw_ilv_flagtest_task"
    assert dag.has_task(task_id), f"Task {task_id} is missing from DAG"
    
    # 4. Assert Parameter Mapping
    assert "DWH_JOB_KENNUNG" in dag.params, "DWH_JOB_KENNUNG parameter is missing from DAG"
    assert dag.params["DWH_JOB_KENNUNG"] == "PFNW_ILV_FLAGTEST ", "Incorrect default value for DWH_JOB_KENNUNG"
```

### Pass/Fail Criterion
* **Pass**: The DAG is successfully parsed with zero import errors, and all structural assertions pass.
* **Fail**: Import errors are detected, or tasks/parameters are missing.

---

## Test Case 5: Robustness and Error Handling (Database Outage)

### Purpose
Prove that the Python execution harness gracefully handles database connection failures or query execution timeouts, logs the error details, and exits with code `1` to prevent downstream jobs from running in an unverified state.

### Setup
1. **Network State**: Simulate a database outage by pointing the BigQuery client to an invalid project or using invalid credentials.

### Action
Execute the Python script:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/non_existent_creds.json"
python3 k_pfnw_ilv_flagtest.py \
  -q d_pfnw_ilv_flagtest.sql \
  -j PFNW_ILV_FLAGTEST \
  -v
```

### Pass/Fail Criterion
* **Pass**:
  * The script exits with status code `1`.
  * The log file `/tmp/validation_PFNW_ILV_FLAGTEST_0.log` captures the exception details (e.g., authentication or connection failure).
  * `stderr` outputs: `ERROR: Query reported an error`.
* **Fail**:
  * The script hangs indefinitely, exits with code `0`, or fails to log the error.