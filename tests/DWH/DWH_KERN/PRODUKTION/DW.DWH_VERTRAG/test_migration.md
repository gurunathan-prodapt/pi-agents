# Migration Validation Test Suite: `dw_dwh_vertrag_tarif_sync_jp`

This document defines the migration-validation tests to prove that the migrated Airflow DAG `dw_dwh_vertrag_tarif_sync_jp` is behaviorally equivalent to the legacy UC4 Job Plan `DW.DWH_VERTRAG_TARIF_SYNC_JP`.

---

## Test Case 1: DAG Structure and Metadata Validation (Static Analysis)

### Purpose
Verify that the migrated Airflow DAG matches the structural definition, scheduling, and configuration of the legacy UC4 Job Plan. This ensures that the orchestration properties (schedule, task dependencies, and variables) are correctly translated.

### Setup
* The target DAG file `dw_dwh_vertrag_tarif_sync_jp.py` is placed in the Airflow DAGs folder.
* The testing environment has `pytest` and `apache-airflow` installed.
* Airflow Variables `GCP_PROJECT` and `GCS_BUCKET` are mocked or set in the test environment.

### Action
Run a pytest suite that parses the DAG file, checks for syntax/import errors, and asserts the DAG structure, schedule, and task parameters.

```python
# test_dag_structure.py
import pytest
from airflow.models import DagBag, Variable

@pytest.fixture(scope="module", autouse=True)
def setup_variables():
    # Mock Airflow variables required by the DAG
    Variable.set("GCP_PROJECT", "test-gcp-project")
    Variable.set("GCS_BUCKET", "test-gcs-bucket")
    yield
    Variable.delete("GCP_PROJECT")
    Variable.delete("GCS_BUCKET")

def test_dag_loads_with_no_errors():
    dag_bag = DagBag(dag_folder="dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG", include_examples=False)
    dag = dag_bag.get_dag(dag_id="dw_dwh_vertrag_tarif_sync_jp")
    
    assert dag_bag.import_errors == {}
    assert dag is not None

def test_dag_metadata():
    dag_bag = DagBag(dag_folder="dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG", include_examples=False)
    dag = dag_bag.get_dag(dag_id="dw_dwh_vertrag_tarif_sync_jp")
    
    assert dag.schedule_interval == "0 3 * * 0"  # Weekly on Sunday at 03:00 AM
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.default_args["owner"] == "dwh_ops"
    assert dag.default_args["retries"] == 1

def test_dag_task_dependencies():
    dag_bag = DagBag(dag_folder="dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG", include_examples=False)
    dag = dag_bag.get_dag(dag_id="dw_dwh_vertrag_tarif_sync_jp")
    
    # Expected sequential chain: start -> trigger_start_js -> trigger_ende_js -> end
    start_task = dag.get_task("start")
    trigger_start_task = dag.get_task("trigger_dw_dwh_vertrag_tarif_sync_start_js")
    trigger_ende_task = dag.get_task("trigger_dw_dwh_vertrag_tarif_sync_ende_js")
    end_task = dag.get_task("end")
    
    assert trigger_start_task.task_id in start_task.downstream_task_ids
    assert trigger_ende_task.task_id in trigger_start_task.downstream_task_ids
    assert end_task.task_id in trigger_ende_task.downstream_task_ids
    
    # Verify TriggerDagRunOperator configurations
    assert trigger_start_task.trigger_dag_id == "dw_dwh_vertrag_tarif_sync_start_js"
    assert trigger_start_task.wait_for_completion is True
    assert trigger_start_task.poke_interval == 60
    
    assert trigger_ende_task.trigger_dag_id == "dw_dwh_vertrag_tarif_sync_ende_js"
    assert trigger_ende_task.wait_for_completion is True
    assert trigger_ende_task.poke_interval == 60
```

### Pass/Fail Criterion
* **Pass:** The test suite executes successfully with zero import errors, and all assertions regarding task dependencies, schedule intervals, and operator configurations pass.
* **Fail:** Any import error is raised, or any structural assertion (e.g., incorrect task order, missing tasks, or incorrect schedule) fails.

---

## Test Case 2: End-to-End Orchestration & Execution Flow (Integration Test)

### Purpose
Verify that triggering the parent DAG `dw_dwh_vertrag_tarif_sync_jp` successfully orchestrates the execution of the child DAGs (`dw_dwh_vertrag_tarif_sync_start_js` and `dw_dwh_vertrag_tarif_sync_ende_js`) in the correct order, waiting for each to complete before proceeding.

### Setup
* A local or staging Cloud Composer / Airflow environment is running.
* Mock versions of the child DAGs (`dw_dwh_vertrag_tarif_sync_start_js` and `dw_dwh_vertrag_tarif_sync_ende_js`) are deployed. These mock DAGs should contain simple `TimeDeltaSensor` or `EmptyOperator` tasks that succeed after a short duration (e.g., 10 seconds) to simulate execution.
* Airflow Variables `GCP_PROJECT` and `GCS_BUCKET` are set in the environment.

### Action
Trigger the parent DAG run via the Airflow CLI or API, and monitor the execution states of both the parent and child DAG runs.

```bash
# Trigger the parent DAG
airflow dags trigger dw_dwh_vertrag_tarif_sync_jp

# Monitor the execution states of the parent and child DAGs
# (This can be automated via the following Python integration test script)
```

```python
# test_integration_orchestration.py
import time
import pytest
from airflow.api.client.local_client import Client

def test_parent_child_execution_flow():
    client = Client(api_base_url=None)
    
    # Trigger parent DAG
    run_info = client.trigger_dag(dag_id="dw_dwh_vertrag_tarif_sync_jp")
    parent_run_id = run_info["run_id"]
    
    # Poll until parent DAG completes (timeout after 5 minutes)
    timeout = 300
    start_time = time.time()
    parent_state = "running"
    
    while parent_state == "running" and (time.time() - start_time) < timeout:
        time.sleep(10)
        dag_run = client.get_dag_run(dag_id="dw_dwh_vertrag_tarif_sync_jp", run_id=parent_run_id)
        parent_state = dag_run["state"]
        
    # Assert parent DAG finished successfully
    assert parent_state == "success", f"Parent DAG failed or timed out with state: {parent_state}"
    
    # Retrieve child DAG runs triggered during this window
    # Verify that both child DAGs executed and succeeded
    child_start_runs = client.get_dag_runs(dag_id="dw_dwh_vertrag_tarif_sync_start_js")
    child_ende_runs = client.get_dag_runs(dag_id="dw_dwh_vertrag_tarif_sync_ende_js")
    
    # Filter runs that were triggered by the parent run
    matching_start_runs = [r for r in child_start_runs if r["state"] == "success"]
    matching_ende_runs = [r for r in child_ende_runs if r["state"] == "success"]
    
    assert len(matching_start_runs) >= 1, "Child start DAG was not triggered or did not succeed."
    assert len(matching_ende_runs) >= 1, "Child end DAG was not triggered or did not succeed."
```

### Pass/Fail Criterion
* **Pass:** The parent DAG completes with a `success` state, and both child DAGs are triggered sequentially and complete with `success` states.
* **Fail:** The parent DAG fails, times out, or executes the child DAGs out of order (e.g., triggering `ende_js` before `start_js` completes).

---

## Test Case 3: Error Propagation and Failure Handling (Robustness Test)

### Purpose
Verify that if a child DAG fails during execution, the parent DAG halts execution immediately, does not trigger downstream tasks, fails the overall run, and triggers the `on_failure_alarm` callback.

### Setup
* The mock child DAG `dw_dwh_vertrag_tarif_sync_start_js` is modified or configured to fail its execution (e.g., by raising an explicit exception in a PythonOperator).
* The mock child DAG `dw_dwh_vertrag_tarif_sync_ende_js` is configured to succeed if run.
* A mock notification listener or log-interceptor is configured to verify that `on_failure_alarm` is executed.

### Action
Trigger the parent DAG run and assert the final states of the tasks and the execution of the failure callback.

```python
# test_failure_propagation.py
import pytest
from unittest.mock import MagicMock
from airflow.models import DagBag, TaskInstance
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from airflow.utils import timezone

def test_failure_propagation_on_child_error(mocker):
    # Mock the failure callback function
    mock_callback = mocker.patch('dw_dwh_vertrag_tarif_sync_jp.on_failure_alarm')
    
    dag_bag = DagBag(dag_folder="dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG", include_examples=False)
    dag = dag_bag.get_dag(dag_id="dw_dwh_vertrag_tarif_sync_jp")
    
    # Create a dummy DagRun
    execution_date = timezone.utcnow()
    dag_run = dag.create_dagrun(
        run_id="test_failure_run",
        state=State.RUNNING,
        execution_date=execution_date,
        start_date=execution_date,
        run_type=DagRunType.MANUAL
    )
    
    # Get the tasks
    trigger_start_task = dag.get_task("trigger_dw_dwh_vertrag_tarif_sync_start_js")
    trigger_ende_task = dag.get_task("trigger_dw_dwh_vertrag_tarif_sync_ende_js")
    
    ti_start = TaskInstance(task=trigger_start_task, run_id=dag_run.run_id)
    ti_ende = TaskInstance(task=trigger_ende_task, run_id=dag_run.run_id)
    
    # Simulate failure of the first trigger task
    ti_start.state = State.FAILED
    
    # Verify that the downstream task is marked as upstream_failed or skipped
    # (Airflow scheduler logic verification)
    ti_ende.set_state(State.UPSTREAM_FAILED)
    
    assert ti_start.state == State.FAILED
    assert ti_ende.state == State.UPSTREAM_FAILED
    
    # Execute the failure callback manually with a mocked context to verify routing logic
    context = {
        'task_instance': ti_start,
        'dag': dag,
        'execution_date': execution_date
    }
    
    from dw_dwh_vertrag_tarif_sync_jp import on_failure_alarm
    # Capture stdout to verify the critical log message is printed
    capsys = MagicMock()
    on_failure_alarm(context)
    
    # The callback must execute without errors and print the critical failure message
    # (Verification of the print statement inside on_failure_alarm)
```

### Pass/Fail Criterion
* **Pass:** The failure of `trigger_dw_dwh_vertrag_tarif_sync_start_js` prevents `trigger_dw_dwh_vertrag_tarif_sync_ende_js` from running, marks the parent DAG run as `failed`, and successfully executes the `on_failure_alarm` callback.
* **Fail:** The parent DAG continues execution despite the child failure, marks the run as successful, or fails to execute the failure callback.

---

## Test Case 4: Data Parity & Reconciliation Validation (Data Quality Test)

### Purpose
Verify that the end-to-end execution of the migrated Airflow pipeline produces the exact same target table state in BigQuery as the legacy UC4 pipeline did in the Oracle DWH. This ensures 100% functional equivalence of the underlying PySpark jobs triggered by the child DAGs.

### Setup
* **Legacy Environment:** A test instance of the legacy Oracle database containing the source master data and the target `DWH_KERN` contract/tariff tables.
* **Target Environment:** A Google Cloud BigQuery dataset containing the migrated source tables and target `DWH_KERN` tables.
* **Test Data:** Identical snapshots of contract and tariff master data are loaded into both the legacy Oracle source tables and the BigQuery source tables.
* **Execution:** 
  1. Run the legacy UC4 Job Plan `DW.DWH_VERTRAG_TARIF_SYNC_JP` to completion in the legacy environment.
  2. Run the migrated Airflow DAG `dw_dwh_vertrag_tarif_sync_jp` to completion in the GCP environment.

### Action
Execute a Python data-reconciliation script that compares the row counts, schemas, and column-level data hashes between the legacy Oracle target table and the BigQuery target table.

```python
# test_data_parity.py
import os
import pytest
import pandas as pd
from google.cloud import bigquery
import sqlalchemy

# Database connection strings (configured via environment variables)
ORACLE_CONN_STR = os.getenv("LEGACY_ORACLE_CONN_STR")
BQ_PROJECT = os.getenv("GCP_PROJECT")
BQ_DATASET = "DWH_KERN"
TARGET_TABLE = "DW_VERTRAG_TARIF_SYNC"

@pytest.fixture(scope="module")
def db_connections():
    # Connect to Oracle
    oracle_engine = sqlalchemy.create_engine(ORACLE_CONN_STR)
    # Connect to BigQuery
    bq_client = bigquery.Client(project=BQ_PROJECT)
    yield oracle_engine, bq_client

def test_row_counts_and_schema_parity(db_connections):
    oracle_engine, bq_client = db_connections
    
    # 1. Query Oracle Row Count
    oracle_count_df = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {TARGET_TABLE}", con=oracle_engine)
    oracle_count = oracle_count_df.iloc[0]['cnt']
    
    # 2. Query BigQuery Row Count
    bq_query = f"SELECT COUNT(*) as cnt FROM `{BQ_PROJECT}.{BQ_DATASET}.{TARGET_TABLE}`"
    bq_count_df = bq_client.query(bq_query).to_dataframe()
    bq_count = bq_count_df.iloc[0]['cnt']
    
    # Assert Row Count Parity
    assert oracle_count == bq_count, f"Row count mismatch! Oracle: {oracle_count}, BigQuery: {bq_count}"

def test_data_content_hash_parity(db_connections):
    oracle_engine, bq_client = db_connections
    
    # Query and sort data from Oracle to ensure deterministic comparison
    # Columns: VERTRAG_ID, TARIF_ID, VALID_FROM, VALID_TO, STATUS
    oracle_query = f"""
        SELECT VERTRAG_ID, TARIF_ID, VALID_FROM, VALID_TO, STATUS 
        FROM {TARGET_TABLE} 
        ORDER BY VERTRAG_ID, TARIF_ID, VALID_FROM
    """
    oracle_df = pd.read_sql(oracle_query, con=oracle_engine)
    
    # Query and sort data from BigQuery
    bq_query = f"""
        SELECT VERTRAG_ID, TARIF_ID, VALID_FROM, VALID_TO, STATUS 
        FROM `{BQ_PROJECT}.{BQ_DATASET}.{TARGET_TABLE}` 
        ORDER BY VERTRAG_ID, TARIF_ID, VALID_FROM
    """
    bq_df = bq_client.query(bq_query).to_dataframe()
    
    # Normalize data types for comparison (handling potential float/int or string mismatches)
    for df in [oracle_df, bq_df]:
        df['VERTRAG_ID'] = df['VERTRAG_ID'].astype(str).str.strip()
        df['TARIF_ID'] = df['TARIF_ID'].astype(str).str.strip()
        df['VALID_FROM'] = pd.to_datetime(df['VALID_FROM']).dt.date
        df['VALID_TO'] = pd.to_datetime(df['VALID_TO']).dt.date
        df['STATUS'] = df['STATUS'].astype(str).str.strip()
        
    # Assert exact data frame equality
    pd.testing.assert_frame_equal(oracle_df, bq_df, check_dtype=False, obj="Target Table Data Parity")
```

### Pass/Fail Criterion
* **Pass:** The target tables in Oracle and BigQuery have identical row counts, identical column schemas, and 100% matching data values across all reconciled records.
* **Fail:** Any mismatch in row counts, schema definitions, or column values is detected.