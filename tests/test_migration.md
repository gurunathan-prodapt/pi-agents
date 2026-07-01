# Migration Validation Test Suite: `k_ausd_v_ta_apn_ve.ksh`

This document defines the migration-validation test suite for the migrated `k_ausd_v_ta_apn_ve.ksh` pipeline. It ensures behavioral equivalence between the legacy KornShell/Oracle environment and the target Google Cloud Platform (Airflow/BigQuery) environment.

---

## Test Case 1: Parameter Validation — Missing `p_JobKennung`

### Purpose
Verify that the stored procedure rejects executions missing the required `p_JobKennung` parameter, logs the correct error code (`193`), and records the failure in the `job_error_log` table, matching legacy shell behavior.

### Setup
Ensure the operational tables exist and are cleared of any previous test entries for the test identifier.
```sql
-- Clean up previous test runs
DELETE FROM `dw_isbert_dev.job_error_log` WHERE job_kennung = '';
DELETE FROM `dw_isbert_dev.job_error_log` WHERE job_kennung IS NULL;
```

### Action
Execute the stored procedure with an empty string for `p_JobKennung`.
```sql
DECLARE v_msg STRING;
CALL `dw_isbert_dev.sp_k_ausd_v_ta_apn_ve`('', '10001');
```

### Pass/Fail Criterion
* **Pass:** 
  1. The procedure returns a message matching `'FEHLER: 0 E 193 Jobkennung'`.
  2. A row is inserted into `dw_isbert_dev.job_error_log` with `err_nr = 193` and `err_arg = 'Jobkennung'`.
  3. No active job is registered in `job_table`.
* **Fail:** The procedure executes without error, or fails to log the error to `job_error_log`.

### Automated Test Code (Python / pytest)
```python
import pytest
from google.cloud import bigquery

@pytest.fixture
def bq_client():
    return bigquery.Client()

def test_missing_job_kennung(bq_client):
    project = "gcp-proj-dw-dev"
    dataset = "dw_isbert_dev"
    
    # Clean up
    bq_client.query(f"DELETE FROM `{project}.{dataset}.job_error_log` WHERE eintrags_nr = 'TC1_ERR'").result()
    
    # Execute SP
    query = f"CALL `{project}.{dataset}.sp_k_ausd_v_ta_apn_ve`('', 'TC1_ERR')"
    query_job = bq_client.query(query)
    results = list(query_job.result())
    
    # Assert return message
    assert len(results) == 1
    assert "FEHLER: 0 E 193 Jobkennung" in results[0][0]
    
    # Assert log table entry
    log_query = f"""
        SELECT err_nr, err_arg 
        FROM `{project}.{dataset}.job_error_log` 
        WHERE eintrags_nr = 'TC1_ERR'
    """
    log_results = list(bq_client.query(log_query).result())
    assert len(log_results) == 1
    assert log_results[0]['err_nr'] == 193
    assert log_results[0]['err_arg'] == 'Jobkennung'
```

---

## Test Case 2: Parameter Validation — Missing `p_EintragsNr`

### Purpose
Verify that the stored procedure rejects executions missing the required `p_EintragsNr` parameter, logs the correct error code (`193`), and records the failure in the `job_error_log` table.

### Setup
Clear any previous test entries for the test identifier.
```sql
DELETE FROM `dw_isbert_dev.job_error_log` WHERE job_kennung = 'TC2_JOB';
```

### Action
Execute the stored procedure with an empty string for `p_EintragsNr`.
```sql
CALL `dw_isbert_dev.sp_k_ausd_v_ta_apn_ve`('TC2_JOB', '');
```

### Pass/Fail Criterion
* **Pass:** 
  1. The procedure returns a message matching `'FEHLER: 0 E 193 EintragsNr'`.
  2. A row is inserted into `dw_isbert_dev.job_error_log` with `err_nr = 193` and `err_arg = 'EintragsNr'`.
  3. No active job is registered in `job_table`.
* **Fail:** The procedure executes without error, or fails to log the error to `job_error_log`.

### Automated Test Code (Python / pytest)
```python
def test_missing_eintrags_nr(bq_client):
    project = "gcp-proj-dw-dev"
    dataset = "dw_isbert_dev"
    
    # Clean up
    bq_client.query(f"DELETE FROM `{project}.{dataset}.job_error_log` WHERE job_kennung = 'TC2_JOB'").result()
    
    # Execute SP
    query = f"CALL `{project}.{dataset}.sp_k_ausd_v_ta_apn_ve`('TC2_JOB', '')"
    query_job = bq_client.query(query)
    results = list(query_job.result())
    
    # Assert return message
    assert len(results) == 1
    assert "FEHLER: 0 E 193 EintragsNr" in results[0][0]
    
    # Assert log table entry
    log_query = f"""
        SELECT err_nr, err_arg 
        FROM `{project}.{dataset}.job_error_log` 
        WHERE job_kennung = 'TC2_JOB'
    """
    log_results = list(bq_client.query(log_query).result())
    assert len(log_results) == 1
    assert log_results[0]['err_nr'] == 193
    assert log_results[0]['err_arg'] == 'EintragsNr'
```

---

## Test Case 3: Active Job Registration & Deactivation (State Management)

### Purpose
Verify the state management logic:
1. A new job run is registered as `ACTIVE`.
2. Pre-existing active runs for the same `job_kennung` are updated to `INACTIVE`.
3. The operation is executed atomically inside a transaction.

### Setup
Insert a pre-existing active job run for `job_kennung = 'TC3_STATE'`.
```sql
DELETE FROM `dw_isbert_dev.job_table` WHERE job_kennung = 'TC3_STATE';

INSERT INTO `dw_isbert_dev.job_table` (job_kennung, eintrags_nr, tab_name, status, created_at, updated_at)
VALUES ('TC3_STATE', 'OLD_RUN_01', 'ta_apn_ve', 'ACTIVE', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR));
```

### Action
Execute the stored procedure with the same `job_kennung` but a new `eintrags_nr`.
```sql
CALL `dw_isbert_dev.sp_k_ausd_v_ta_apn_ve`('TC3_STATE', 'NEW_RUN_02');
```

### Pass/Fail Criterion
* **Pass:**
  1. The old run (`OLD_RUN_01`) has its status updated to `'INACTIVE'`.
  2. The new run (`NEW_RUN_02`) is registered with status `'ACTIVE'`.
  3. Both records exist in the `job_table`.
* **Fail:** The old run remains `'ACTIVE'`, or the new run is not registered as `'ACTIVE'`.

### Automated Test Code (SQL Assertions)
```sql
-- Assertions to be run post-execution
SELECT 
  status, 
  eintrags_nr,
  ASSERT_ROWS_MODIFIED(1) -- Helper check if running in a test harness
FROM `dw_isbert_dev.job_table`
WHERE job_kennung = 'TC3_STATE';

-- Verification Query
SELECT 
  SUM(CASE WHEN eintrags_nr = 'OLD_RUN_01' AND status = 'INACTIVE' THEN 1 ELSE 0 END) as old_deactivated,
  SUM(CASE WHEN eintrags_nr = 'NEW_RUN_02' AND status = 'ACTIVE' THEN 1 ELSE 0 END) as new_activated
FROM `dw_isbert_dev.job_table`
WHERE job_kennung = 'TC3_STATE';
-- EXPECTED RESULT: old_deactivated = 1, new_activated = 1
```

---

## Test Case 4: Record Count Capture (`@@row_count`)

### Purpose
Verify that the stored procedure accurately captures the number of rows processed by the core business logic and persists this count to the `job_run_summary` table.

### Setup
Since the core business logic is a placeholder, we will temporarily mock the core logic inside a test version of the stored procedure or verify the behavior of the placeholder. For validation, we verify that the `records_processed` column in `job_run_summary` matches the actual rows affected by the DML statement inside the transaction.

```sql
DELETE FROM `dw_isbert_dev.job_run_summary` WHERE job_kennung = 'TC4_COUNT';
DELETE FROM `dw_isbert_dev.job_table` WHERE job_kennung = 'TC4_COUNT';
```

### Action
Execute the stored procedure.
```sql
CALL `dw_isbert_dev.sp_k_ausd_v_ta_apn_ve`('TC4_COUNT', 'RUN_01');
```

### Pass/Fail Criterion
* **Pass:**
  1. A record is written to `dw_isbert_dev.job_run_summary`.
  2. The `records_processed` value matches the number of rows modified by the core business logic (for the placeholder, this is typically `1` or `0` depending on the exact DML executed).
* **Fail:** No summary record is written, or `records_processed` is `NULL` or incorrect.

### Automated Test Code (Python / pytest)
```python
def test_record_count_capture(bq_client):
    project = "gcp-proj-dw-dev"
    dataset = "dw_isbert_dev"
    
    # Clean up
    bq_client.query(f"DELETE FROM `{project}.{dataset}.job_run_summary` WHERE job_kennung = 'TC4_COUNT'").result()
    
    # Execute SP
    query = f"CALL `{project}.{dataset}.sp_k_ausd_v_ta_apn_ve`('TC4_COUNT', 'RUN_01')"
    bq_client.query(query).result()
    
    # Verify summary entry
    summary_query = f"""
        SELECT records_processed 
        FROM `{project}.{dataset}.job_run_summary` 
        WHERE job_kennung = 'TC4_COUNT' AND eintrags_nr = 'RUN_01'
    """
    results = list(bq_client.query(summary_query).result())
    assert len(results) == 1
    # The placeholder DML inserts 1 row or captures @@row_count. 
    # Ensure it is a valid non-negative integer.
    assert results[0]['records_processed'] >= 0
```

---

## Test Case 5: Airflow DAG Parameter Passing & Dry Run

### Purpose
Verify that the Airflow DAG `dag_k_ausd_v_ta_apn_ve` correctly parses parameters, resolves the environment-specific project and dataset, and passes them to the BigQuery stored procedure.

### Setup
Load the DAG in a local Airflow unit testing environment.

### Action
Trigger a dry run of the DAG using the Airflow CLI or a Python unit test, passing the parameters `p_JobKennung` and `p_EintragsNr`.

### Pass/Fail Criterion
* **Pass:**
  1. The DAG compiles without syntax errors.
  2. The Jinja templates in the `BigQueryInsertJobOperator` resolve correctly to the target environment's project and dataset (e.g., `gcp-proj-dw-dev.dw_isbert_dev` when `env` is `dev`).
  3. The generated SQL query string contains the correct parameters.
* **Fail:** DAG compilation fails, or Jinja templating resolves to incorrect datasets.

### Automated Test Code (Python / pytest)
```python
from airflow.models import DagBag, DagRun, TaskInstance
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType
import pytest

@pytest.fixture
def dagbag():
    return DagBag(dag_folder="dags", include_examples=False)

def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id="dag_k_ausd_v_ta_apn_ve")
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 3  # start, call_stored_procedure, end

def test_dag_template_rendering():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.get_dag(dag_id="dag_k_ausd_v_ta_apn_ve")
    
    # Create a dummy DAG run
    execution_date = datetime(2024, 1, 1)
    dag_run = dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=execution_date,
        run_type=DagRunType.MANUAL,
        conf={
            "env": "dev",
            "p_JobKennung": "TEST_JOB_DAG",
            "p_EintragsNr": "TEST_ENTRY_DAG"
        }
    )
    
    ti = TaskInstance(task=dag.get_task("call_stored_procedure"), run_id=dag_run.run_id)
    ti.render_templates()
    
    rendered_query = ti.task.configuration["query"]["query"]
    
    # Assert that parameters are correctly injected into the SQL wrapper
    assert "DECLARE env_name STRING DEFAULT 'dev';" in rendered_query
    assert "DECLARE job_kennung STRING DEFAULT 'TEST_JOB_DAG';" in rendered_query
    assert "DECLARE eintrags_nr STRING DEFAULT 'TEST_ENTRY_DAG';" in rendered_query
```