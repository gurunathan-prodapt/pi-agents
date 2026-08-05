# Migration Validation Test Suite: `DW.EXTTEST_LEGACY_DWH`

This document defines the migration-validation test suite for transitioning the legacy UC4 job `DW.EXTTEST_LEGACY_DWH` and its associated shell script `r_legacy_ksh_dwh` to Apache Airflow. 

---

## Test Case 1: DAG Structure and Operator Validation (Static Analysis)

### Purpose
To verify that the migrated Airflow DAG is syntactically correct, contains no import errors, and that the placeholder `EmptyOperator` stub has been successfully replaced with an active execution operator (e.g., `BashOperator`, `SSHOperator`, or `GKEStartPodOperator`) configured with the correct environment variables.

### Setup
1. Place the migrated DAG file `dw_exttest_legacy_dwh.py` into the Airflow `dags/` directory.
2. Ensure the Airflow environment has the required variables defined (`GCP_PROJECT`, `GCP_REGION`, `HOME_DIR`).

### Action
Run a `pytest` suite against the Airflow DagBag to validate the DAG structure, task types, and environment variable configurations.

```python
# test_dag_structure.py
import pytest
from airflow.models import DagBag, Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.cncf.kubernetes.operators.pod import GKEStartPodOperator

@pytest.fixture(scope="module")
def dagbag():
    # Set up mock Airflow variables for testing
    Variable.set("GCP_PROJECT", "test-gcp-project")
    Variable.set("GCP_REGION", "europe-west3")
    Variable.set("HOME_DIR", "/home/airflow")
    return DagBag(dag_folder="vobs/dw_source/isxtst/scheduler/DW.EXTTEST_JP", include_examples=False)

def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id="dw_exttest_legacy_dwh")
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 1

def test_task_operator_not_empty(dagbag):
    dag = dagbag.get_dag(dag_id="dw_exttest_legacy_dwh")
    task = dag.get_task("dw_exttest_legacy_dwh_task")
    
    # CRITICAL: Ensure the EmptyOperator stub has been replaced
    assert not isinstance(task, EmptyOperator), (
        "FAIL: Task 'dw_exttest_legacy_dwh_task' is still an EmptyOperator stub. "
        "It must be migrated to BashOperator, SSHOperator, or GKEStartPodOperator."
    )
    assert isinstance(task, (BashOperator, SSHOperator, GKEStartPodOperator))

def test_environment_variables_configured(dagbag):
    dag = dagbag.get_dag(dag_id="dw_exttest_legacy_dwh")
    task = dag.get_task("dw_exttest_legacy_dwh_task")
    
    # Verify that the tracking identifier is passed to the execution environment
    if isinstance(task, BashOperator):
        assert task.env is not None
        assert task.env.get("DWH_JOB_KENNUNG") == "EXTTEST_LEGACY_DWH"
    elif isinstance(task, SSHOperator):
        assert "DWH_JOB_KENNUNG=EXTTEST_LEGACY_DWH" in task.command or task.environment.get("DWH_JOB_KENNUNG") == "EXTTEST_LEGACY_DWH"
    elif isinstance(task, GKEStartPodOperator):
        env_vars = {env.name: env.value for env in task.env_vars}
        assert env_vars.get("DWH_JOB_KENNUNG") == "EXTTEST_LEGACY_DWH"
```

### Pass/Fail Criterion
* **Pass**: The DAG loads without import errors, the task is not an `EmptyOperator`, and `DWH_JOB_KENNUNG` is correctly injected into the task's execution environment.
* **Fail**: Any import errors occur, the task remains an `EmptyOperator`, or the required environment variables are missing.

---

## Test Case 2: End-to-End Execution & Output Parity

### Purpose
To prove behavioral equivalence by executing both the legacy shell script and the migrated Airflow task against identical source databases, asserting that they produce identical data transformations and row counts in the target database.

### Setup
1. **Source Database**: An Oracle database instance containing the tables read by `/vobs/dw_source/isxtst/sql/d_legacy_ksh_dwh.sql`.
2. **Legacy Run**: Run the legacy script `r_legacy_ksh_dwh` on the legacy host `dwhdwh2p` pointing to the source database. Capture the target table state.
3. **Target Database**: A target database (e.g., BigQuery or a migrated Oracle instance) populated by the migrated Airflow task execution.
4. **Clean Slate**: Truncate target tables before execution.

### Action
1. Execute the legacy script:
   ```bash
   export HOME="/home/legacy_user"
   /vobs/dw_source/isxtst/scripts/r_legacy_ksh_dwh
   ```
2. Execute the migrated Airflow DAG task:
   ```bash
   airflow dags trigger -e 2026-08-05 dw_exttest_legacy_dwh
   ```
3. Run the following SQL validation script to compare row counts, schemas, and data checksums between the legacy target table and the migrated target table.

```sql
-- SQL Assertion: Verify Row Count and Data Parity
-- Replace 'legacy_target_table' and 'migrated_target_table' with actual table names from d_legacy_ksh_dwh.sql

WITH legacy_metrics AS (
    SELECT 
        COUNT(*) as total_rows,
        SUM(DBMS_CRYPTO.HASH(UTL_RAW.CAST_TO_RAW(column_1 || column_2 || column_3), 2)) as data_checksum -- MD5 Hash
    FROM legacy_target_table
),
migrated_metrics AS (
    SELECT 
        COUNT(*) as total_rows,
        SUM(DBMS_CRYPTO.HASH(UTL_RAW.CAST_TO_RAW(column_1 || column_2 || column_3), 2)) as data_checksum
    FROM migrated_target_table
)
SELECT 
    l.total_rows as legacy_rows,
    m.total_rows as migrated_rows,
    l.data_checksum as legacy_hash,
    m.data_checksum as migrated_hash,
    CASE 
        WHEN l.total_rows = m.total_rows AND l.data_checksum = m.data_checksum THEN 'PASS'
        ELSE 'FAIL'
    END as parity_status
FROM legacy_metrics l
CROSS JOIN migrated_metrics m;
```

### Pass/Fail Criterion
* **Pass**: The `parity_status` query returns `PASS`, proving that row counts and data hashes are identical between the legacy and migrated runs.
* **Fail**: Row counts differ, or the data checksums do not match, indicating data corruption, truncation, or mapping errors during migration.

---

## Test Case 3: Error Handling & Non-Zero Exit Code Propagation

### Purpose
To verify that the migrated Airflow task correctly detects failures in the underlying SQL execution, propagates non-zero exit codes, and triggers the configured retry strategy.

### Setup
1. Temporarily rename or corrupt the SQL script `/vobs/dw_source/isxtst/sql/d_legacy_ksh_dwh.sql` in the execution environment to force a failure.
2. Alternatively, revoke database access privileges for the execution user (`DW.UNIX.ISXTST` equivalent) to trigger an ORA- error during `sqlplus` execution.

### Action
1. Trigger the Airflow DAG:
   ```bash
   airflow dags trigger dw_exttest_legacy_dwh
   ```
2. Monitor the task execution state and retry behavior.

```python
# test_error_propagation.py
import time
from airflow.models import DagRun
from airflow.utils.state import State

def test_task_failure_and_retry(dagbag):
    dag = dagbag.get_dag(dag_id="dw_exttest_legacy_dwh")
    
    # Trigger DAG run
    dag_run = dag.create_dagrun(
        state=State.RUNNING,
        execution_date=datetime.utcnow(),
        run_id="test_failure_propagation"
    )
    
    # Wait for execution to complete/fail
    timeout = 60
    start_time = time.time()
    while dag_run.state == State.RUNNING:
        time.sleep(2)
        dag_run.update_state()
        if time.time() - start_time > timeout:
            raise TimeoutError("DAG execution timed out")
            
    task_instance = dag_run.get_task_instance("dw_exttest_legacy_dwh_task")
    
    # Assertions
    assert task_instance.state == State.UP_FOR_RETRY, (
        f"Expected task state to be UP_FOR_RETRY, but got {task_instance.state}. "
        "Ensure non-zero exit codes from the shell script are not swallowed."
    )
```

### Pass/Fail Criterion
* **Pass**: The Airflow task fails on its first attempt due to the database/script error, propagates the failure (exit code `1`), and transitions to `UP_FOR_RETRY` (respecting the `retries: 1` configuration).
* **Fail**: The task completes with `SUCCESS` despite the underlying SQL/database failure (indicating exit codes are being swallowed), or it fails without scheduling a retry.

---

## Test Case 4: Environment Variable and Context Injection

### Purpose
To verify that the environment variables (`DWH_JOB_KENNUNG` and `HOME`) are correctly resolved and injected into the runtime context of the execution container/server.

### Setup
Configure the Airflow connection or execution environment to log environment variables during execution.

### Action
1. Trigger the Airflow DAG.
2. Inspect the task execution logs in Airflow.

```python
# test_env_injection.py
def test_logs_for_env_variables(caplog):
    # Simulate task execution and capture logs
    # Verify that the execution environment outputted the correct variables
    
    # Expected log outputs from the execution operator
    expected_kennung_log = "DWH_JOB_KENNUNG=EXTTEST_LEGACY_DWH"
    expected_home_log = "HOME=" # Should point to the resolved HOME_DIR variable
    
    # Assertions against the captured Airflow task execution logs
    assert any(expected_kennung_log in log.message for log in caplog.records), (
        "FAIL: DWH_JOB_KENNUNG was not exported to the execution environment."
    )
```

### Pass/Fail Criterion
* **Pass**: Airflow task logs confirm that `DWH_JOB_KENNUNG` is set to `'EXTTEST_LEGACY_DWH'` and `HOME` is correctly resolved to the target environment's home directory.
* **Fail**: The environment variables are missing, empty, or contain legacy pathing values (e.g., referencing retired includes like `DW.EXTTEST_HOLE_PFAD`).