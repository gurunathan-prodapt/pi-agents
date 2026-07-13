# Migration Validation Test Suite: `d_alis_spaufruf_p0.sql`

This document contains the migration-validation tests designed to verify that the migrated Airflow DAG (`dags/d_al_is_spaufruf_p0.py`) and BigQuery environment behave identically to the legacy Oracle SQL*Plus wrapper script (`d_alis_spaufruf_p0.sql`).

---

## Test Suite Overview

The legacy script is a dynamic wrapper designed to execute any arbitrary stored procedure passed via positional parameters (`&1` and `&2`). To prove behavioral equivalence, we must validate:
1. **Dynamic Parameter Parsing**: Airflow correctly maps `dag_run.conf` parameters to the BigQuery `CALL` statement.
2. **Transactional Integrity**: BigQuery handles commits and rollbacks identically to Oracle's `COMMIT` and `WHENEVER OSERROR EXIT FAILURE ROLLBACK`.
3. **Session Initialization**: The absence of `d_alis_init.sql` and SQL*Plus formatting commands does not alter data outcomes.
4. **Error Propagation**: Failures inside the BigQuery stored procedure correctly bubble up to fail the Airflow task, matching Oracle's exit codes.

---

## Section 1: Dynamic Parameter Parsing & SQL Compilation

### Purpose
Verify that the Airflow DAG correctly parses dynamic parameters passed via `dag_run.conf` (mimicking Oracle's `&1` and `&2`) and compiles them into a syntactically valid BigQuery `CALL` statement.

### Setup
* A mock BigQuery stored procedure is registered in the target dataset:
  ```sql
  CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset.test_sp_parsing`(
    param_int INT64, 
    param_str STRING
  )
  BEGIN
    -- No-op for parsing validation
  END;
  ```
* Airflow environment variables configured:
  * `gcp_project_id` = `your-gcp-project-id`
  * `bq_dataset` = `your_dataset`

### Action
Trigger the Airflow DAG manually or via API with the following `conf` payload:
```json
{
  "sp_name": "test_sp_parsing",
  "sp_args": "42, 'test_value'"
}
```

### Pass/Fail Criterion
* **Pass**: The Airflow task `execute_migrated_stored_procedure` compiles the SQL query exactly as:
  ```sql
  -- Translated from legacy Oracle EXEC Wrapper
  CALL `your-gcp-project-id.your_dataset.test_sp_parsing`(
      42, 'test_value'
  );
  ```
  The task executes successfully (`SUCCESS` state).
* **Fail**: The Jinja template fails to render, references incorrect default values, or the generated SQL causes a BigQuery syntax error.

---

## Section 2: Transactional Integrity & Rollback Behavior

### Purpose
Verify that if a stored procedure fails midway, BigQuery rolls back any uncommitted DML statements, matching Oracle's `WHENEVER OSERROR EXIT FAILURE ROLLBACK` behavior.

### Setup
1. Create a target tracking table:
   ```sql
   CREATE OR REPLACE TABLE `your_project_id.your_dataset.tx_test_table` (
       id INT64,
       val STRING
   );
   ```
2. Deploy a test stored procedure containing an explicit transaction block that fails midway:
   ```sql
   CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset.test_sp_rollback`()
   BEGIN
     BEGIN TRANSACTION;
     
     INSERT INTO `your_project_id.your_dataset.tx_test_table` (id, val) 
     VALUES (1, 'Committed before failure');
     
     -- Force a runtime error (Division by zero)
     SELECT 1 / 0;
     
     COMMIT TRANSACTION;
   EXCEPTION WHEN ERROR THEN
     ROLLBACK TRANSACTION;
     SELECT @@error.message;
   END;
   ```

### Action
1. Truncate the tracking table:
   ```sql
   TRUNCATE TABLE `your_project_id.your_dataset.tx_test_table`;
   ```
2. Trigger the Airflow DAG with:
   ```json
   {
     "sp_name": "test_sp_rollback",
     "sp_args": ""
   }
   ```

### Pass/Fail Criterion
* **Pass**: The Airflow task completes (or fails depending on whether the exception is re-raised), and a query to `tx_test_table` returns **0 rows**, proving that the partial insert was successfully rolled back.
* **Fail**: The row `(1, 'Committed before failure')` is found in `tx_test_table`, indicating a split-brain or auto-commit state that violates transactional parity.

---

## Section 3: Error Propagation & Exit Code Parity

### Purpose
Ensure that runtime errors inside the BigQuery stored procedure are propagated back to Airflow, causing the task and DAG to mark themselves as `FAILED` (matching Oracle's `EXIT FAILURE`).

### Setup
Deploy a stored procedure designed to raise a user-defined exception:
```sql
CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset.test_sp_error`()
BEGIN
  -- Raise a targeted exception to simulate a business logic failure
  ERROR('Business rule validation failed. Aborting execution.');
END;
```

### Action
1. Trigger the Airflow DAG with:
   ```json
   {
     "sp_name": "test_sp_error",
     "sp_args": ""
   }
   ```
2. Monitor the Airflow Task Instance state.

### Pass/Fail Criterion
* **Pass**: 
  * The task `execute_migrated_stored_procedure` transitions to the `FAILED` state.
  * The Airflow task logs capture the BigQuery error message: `Business rule validation failed. Aborting execution.`.
* **Fail**: The task is marked as `SUCCESS` despite the internal procedure failure, or the error message is swallowed.

---

## Section 4: Automated Integration Test (Pytest)

The following `pytest` script automates the validation of the Airflow DAG's structure, parameter rendering, and execution against a BigQuery sandbox environment.

```python
import pytest
from datetime import datetime
from airflow.models import DagBag, DagRun, TaskInstance
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType
from google.cloud import bigquery

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client()

@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="dags", include_examples=False)

def test_dag_loaded(dagbag):
    """Verify the DAG is loaded correctly without import errors."""
    dag = dagbag.get_dag(dag_id="d_al_is_spaufruf_p0")
    assert dag is not None
    assert len(dag.tasks) == 3
    assert dag.has_task("start_pipeline")
    assert dag.has_task("execute_migrated_stored_procedure")
    assert dag.has_task("end_pipeline")

def test_jinja_rendering(dagbag):
    """Verify that Jinja templates render correctly with dag_run.conf parameters."""
    dag = dagbag.get_dag(dag_id="d_al_is_spaufruf_p0")
    task = dag.get_task("execute_migrated_stored_procedure")
    
    # Create a mock DagRun with specific configuration parameters
    dag_run = DagRun(
        dag_id=dag.dag_id,
        run_id="test_run_1",
        run_type=DagRunType.MANUAL,
        execution_date=datetime.utcnow(),
        conf={"sp_name": "my_target_sp", "sp_args": "100, 'ABC'"}
    )
    
    ti = TaskInstance(task=task, run_id=dag_run.run_id)
    ti.dag_run = dag_run
    
    # Render the templates
    ti.render_templates()
    
    rendered_sql = ti.task.sql
    assert "CALL `your-gcp-project-id.your_dataset.my_target_sp`(" in rendered_sql
    assert "100, 'ABC'" in rendered_sql

def test_e2e_execution_success(bq_client, dagbag):
    """Deploys a dummy procedure, runs the DAG, and asserts success."""
    # 1. Create a dummy procedure in BigQuery
    dataset_id = "your_dataset"
    proc_name = "test_e2e_dummy"
    
    create_proc_sql = f"""
    CREATE OR REPLACE PROCEDURE `{bq_client.project}.{dataset_id}.{proc_name}`(IN val INT64)
    BEGIN
      SELECT val * 2;
    END;
    """
    bq_client.query(create_proc_sql).result()
    
    # 2. Trigger the DAG locally
    dag = dagbag.get_dag(dag_id="d_al_is_spaufruf_p0")
    dag_run = dag.create_dagrun(
        state=DagRunState.RUNNING,
        run_id="test_e2e_run",
        run_type=DagRunType.MANUAL,
        execution_date=datetime.utcnow(),
        conf={"sp_name": proc_name, "sp_args": "21"}
    )
    
    # 3. Run the task
    ti = dag_run.get_task_instance(task_id="execute_migrated_stored_procedure")
    ti.refresh_from_db()
    ti.run(ignore_ti_state=True)
    
    # 4. Assertions
    assert ti.state == TaskInstanceState.SUCCESS
```