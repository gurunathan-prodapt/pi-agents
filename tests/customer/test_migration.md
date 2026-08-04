# Migration Validation Test Suite: CUSTOMER.HISTORIZATION_LOAD

This document defines the migration-validation test suite to prove behavioral equivalence between the legacy Oracle/KornShell-based `CUSTOMER.HISTORIZATION_LOAD` job and the migrated Apache Airflow / BigQuery Python implementation.

---

## Test Suite Overview

The test suite is organized into six validation tests covering:
1. **SCD Type 2 Core Logic**: Idempotency, Expiration/Insertion on change, and New Customer Insertion.
2. **Temporal Snapshot Alignment**: Ensuring absolute microsecond alignment between expired and active records.
3. **Data Quality Guardrails**: Validating the soft-failure threshold logic (warning vs. hard failure).
4. **Error Propagation**: Ensuring database and script failures correctly bubble up to Airflow.
5. **End-to-End Orchestration**: Verifying parameter passing and environment variable handling.

---

## Test Case 1: SCD Type 2 - Idempotency (No Change)

### Purpose
Verify that running the historization load with staging data identical to the current active dimension records results in no changes to the target dimension table (no expirations, no new insertions).

### Setup
1. Populate the target table `ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT` with active records (`IS_CURRENT = 1`).
2. Populate the staging table `ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT` with identical attributes for the same customers on `RUN_DATE = '2023-10-25'`.

```sql
-- Clean tables
TRUNCATE TABLE ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT;
TRUNCATE TABLE ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT;

-- Insert baseline active records
INSERT INTO ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, IS_CURRENT, VALID_FROM, VALID_TO)
VALUES 
  (1001, 'GOLD', 'HIGH', 850, 1, TIMESTAMP('2023-10-18 10:00:00'), NULL),
  (1002, 'SILVER', 'MED', 550, 1, TIMESTAMP('2023-10-18 10:00:00'), NULL);

-- Insert identical staging records for the new run date
INSERT INTO ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, RUN_DATE)
VALUES 
  (1001, 'GOLD', 'HIGH', 850, DATE('2023-10-25')),
  (1002, 'SILVER', 'MED', 550, DATE('2023-10-25'));
```

### Action
Execute the migrated Python script `k_historization_load.py` with `RUN_DATE` set to `2023-10-25`.

```bash
export CRM_HOME="/opt/etl/customer"
export RUN_DATE="2023-10-25"
export GCP_PROJECT="test-gcp-project"
python3 /opt/etl/customer/customer/k_historization_load.py
```

### Pass/Fail Criterion
**Pass**: 
* The target table `DIM_CUSTOMER_SEGMENT` remains completely unchanged.
* No rows are updated or inserted.
* The quality check returns `0%` changed.

**Fail**: Any row is updated (`IS_CURRENT` flipped to 0) or a new row is inserted.

#### Verification SQL
```sql
SELECT 
  COUNT(*) as total_rows,
  COUNTIF(IS_CURRENT = 1) as active_rows,
  COUNTIF(VALID_TO IS NOT NULL) as expired_rows
FROM ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT;
-- EXPECTED: total_rows = 2, active_rows = 2, expired_rows = 0
```

---

## Test Case 2: SCD Type 2 - Attribute Change (Expiration & Insertion)

### Purpose
Verify that when a customer's `SEGMENT_CODE` or `SCORE_BAND` changes in staging, the active record in the dimension table is expired (`IS_CURRENT = 0`, `VALID_TO = current_time`) and a new active record is inserted (`IS_CURRENT = 1`, `VALID_FROM = current_time`).

### Setup
1. Populate the target table with active records.
2. Populate the staging table with a changed `SEGMENT_CODE` for Customer 1001 and a changed `SCORE_BAND` for Customer 1002.

```sql
TRUNCATE TABLE ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT;
TRUNCATE TABLE ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT;

-- Baseline
INSERT INTO ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, IS_CURRENT, VALID_FROM, VALID_TO)
VALUES 
  (1001, 'GOLD', 'HIGH', 850, 1, TIMESTAMP('2023-10-18 10:00:00'), NULL),
  (1002, 'SILVER', 'MED', 550, 1, TIMESTAMP('2023-10-18 10:00:00'), NULL);

-- Staging with changes
INSERT INTO ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, RUN_DATE)
VALUES 
  (1001, 'PLATINUM', 'HIGH', 900, DATE('2023-10-25')), -- Segment changed
  (1002, 'SILVER', 'LOW', 350, DATE('2023-10-25'));    -- Score band changed
```

### Action
Execute `k_historization_load.py` with `RUN_DATE = '2023-10-25'`.

### Pass/Fail Criterion
**Pass**:
* The original records for 1001 and 1002 are expired (`IS_CURRENT = 0`).
* New records for 1001 and 1002 are inserted with updated attributes and `IS_CURRENT = 1`.
* The `VALID_TO` timestamp of the expired record matches **exactly** the `VALID_FROM` timestamp of the new active record.

#### Verification SQL Assertions
```sql
-- Assertion 1: Verify row counts
SELECT 
  COUNT(*) as total_rows, -- Expected: 4
  COUNTIF(IS_CURRENT = 1) as active_rows, -- Expected: 2
  COUNTIF(IS_CURRENT = 0) as expired_rows -- Expected: 2
FROM ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT;

-- Assertion 2: Verify exact temporal alignment
WITH history_pairs AS (
  SELECT 
    CUSTOMER_ID,
    MAX(CASE WHEN IS_CURRENT = 0 THEN VALID_TO END) as expired_at,
    MAX(CASE WHEN IS_CURRENT = 1 THEN VALID_FROM END) as activated_at
  FROM ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT
  GROUP BY CUSTOMER_ID
)
SELECT 
  CUSTOMER_ID,
  expired_at,
  activated_at,
  (expired_at = activated_at) as is_perfectly_aligned
FROM history_pairs;
-- EXPECTED: is_perfectly_aligned must be TRUE for all rows.
```

---

## Test Case 3: SCD Type 2 - New Customer Insertion

### Purpose
Verify that a customer present in staging but completely missing from the target dimension is inserted as a new active record (`IS_CURRENT = 1`, `VALID_FROM = current_time`, `VALID_TO = NULL`).

### Setup
1. Populate the target table with existing customers.
2. Populate the staging table with existing customers plus a completely new customer (1003).

```sql
TRUNCATE TABLE ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT;
TRUNCATE TABLE ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT;

-- Baseline
INSERT INTO ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, IS_CURRENT, VALID_FROM, VALID_TO)
VALUES (1001, 'GOLD', 'HIGH', 850, 1, TIMESTAMP('2023-10-18 10:00:00'), NULL);

-- Staging containing new customer 1003
INSERT INTO ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, RUN_DATE)
VALUES 
  (1001, 'GOLD', 'HIGH', 850, DATE('2023-10-25')),
  (1003, 'BRONZE', 'LOW', 150, DATE('2023-10-25')); -- New Customer
```

### Action
Execute `k_historization_load.py` with `RUN_DATE = '2023-10-25'`.

### Pass/Fail Criterion
**Pass**:
* Customer 1001 remains unchanged.
* Customer 1003 is inserted with `IS_CURRENT = 1`, `VALID_FROM` equal to the execution timestamp, and `VALID_TO IS NULL`.

#### Verification SQL
```sql
SELECT 
  CUSTOMER_ID,
  SEGMENT_CODE,
  IS_CURRENT,
  VALID_TO IS NULL as valid_to_is_null
FROM ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT
WHERE CUSTOMER_ID = 1003;
-- EXPECTED: CUSTOMER_ID=1003, SEGMENT_CODE='BRONZE', IS_CURRENT=1, valid_to_is_null=TRUE
```

---

## Test Case 4: Data Quality Guardrail - Soft Failure Threshold

### Purpose
Verify that if the percentage of changed/re-versioned customers exceeds the `MAX_EXPECTED_CHANGE_PCT` (25%), the script logs a warning but exits with code `0` (soft failure/warning, not a hard pipeline failure).

### Setup
1. Populate the target table with 4 active records.
2. Populate the staging table with changes for 2 of those records (representing a 50% change rate, which is > 25%).

```sql
TRUNCATE TABLE ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT;
TRUNCATE TABLE ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT;

-- Baseline (4 customers)
INSERT INTO ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, IS_CURRENT, VALID_FROM, VALID_TO)
VALUES 
  (1001, 'GOLD', 'HIGH', 850, 1, TIMESTAMP('2023-10-18 10:00:00'), NULL),
  (1002, 'SILVER', 'MED', 550, 1, TIMESTAMP('2023-10-18 10:00:00'), NULL),
  (1003, 'BRONZE', 'LOW', 250, 1, TIMESTAMP('2023-10-18 10:00:00'), NULL),
  (1004, 'BRONZE', 'LOW', 200, 1, TIMESTAMP('2023-10-18 10:00:00'), NULL);

-- Staging (2 out of 4 changed = 50% change rate)
INSERT INTO ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT (CUSTOMER_ID, SEGMENT_CODE, SCORE_BAND, SCORE_VALUE, RUN_DATE)
VALUES 
  (1001, 'PLATINUM', 'HIGH', 950, DATE('2023-10-25')), -- Changed
  (1002, 'GOLD', 'HIGH', 750, DATE('2023-10-25')),     -- Changed
  (1003, 'BRONZE', 'LOW', 250, DATE('2023-10-25')),
  (1004, 'BRONZE', 'LOW', 200, DATE('2023-10-25'));
```

### Action
Execute `k_historization_load.py` and capture stdout and exit code.

```python
# Pytest execution snippet
import subprocess
import os

def test_quality_check_warning_threshold():
    env = os.environ.copy()
    env["RUN_DATE"] = "2023-10-25"
    env["CRM_HOME"] = "/opt/etl/customer"
    
    result = subprocess.run(
        ["python3", "/opt/etl/customer/customer/k_historization_load.py"],
        env=env,
        capture_output=True,
        text=True
    )
    
    # Assert soft failure (exit code 0)
    assert result.returncode == 0
    
    # Assert warning is logged in stdout
    assert "WARN: 50% of customers changed segment this week" in result.stdout
    assert "flagging for review, not failing the job" in result.stdout
```

### Pass/Fail Criterion
**Pass**: 
* Script exits with return code `0`.
* Standard output contains the warning message: `WARN: 50% of customers changed segment this week (expected <= 25%) - flagging for review, not failing the job`.

**Fail**: Script exits with a non-zero code, or fails to log the warning.

---

## Test Case 5: Error Propagation (Hard Failure)

### Purpose
Verify that if the core merge SQL execution fails (e.g., due to a missing table, syntax error, or permission issue in BigQuery), the script immediately logs the error and exits with code `1` to fail the Airflow task.

### Setup
Force a failure by temporarily renaming the staging table or pointing the script to a non-existent dataset.

```bash
# Set BQ_DATASET to a non-existent dataset to force a BigQuery error
export BQ_DATASET="NON_EXISTENT_DATASET"
```

### Action
Execute `k_historization_load.py` and capture the exit code.

### Pass/Fail Criterion
**Pass**:
* Script exits with return code `1`.
* Standard output contains: `ERROR: d_historization_load.sql failed with rc=1`.

**Fail**: Script exits with code `0` or fails to log the error.

#### Pytest Implementation
```python
def test_error_propagation_on_db_failure():
    env = os.environ.copy()
    env["RUN_DATE"] = "2023-10-25"
    env["GCP_PROJECT"] = "invalid-project-to-force-failure"
    
    result = subprocess.run(
        ["python3", "/opt/etl/customer/customer/k_historization_load.py"],
        env=env,
        capture_output=True,
        text=True
    )
    
    assert result.returncode == 1
    assert "ERROR: d_historization_load.sql failed with rc=1" in result.stdout
```

---

## Test Case 6: End-to-End DAG Integration Test

### Purpose
Verify that the Airflow DAG `customer_historization_load` correctly instantiates, resolves the logical execution date macro (`{{ ds }}`), and passes environment variables to the `BashOperator` task.

### Setup
Load the DAG into an Airflow testing context.

### Action
Execute the following pytest script to validate DAG structure and parameter rendering.

```python
import pytest
from airflow.models import DagBag, Variable
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType
import datetime

@pytest.fixture(autouse=True)
def set_airflow_variables(monkeypatch):
    # Mock Airflow Variables
    variables = {
        "GCP_PROJECT": "test-gcp-project",
        "GCP_REGION": "us-east1",
        "GCS_BUCKET": "test-bucket",
        "SCRIPTS_DIR": "/opt/etl/customer"
    }
    def mock_get(key, default_var=None):
        return variables.get(key, default_var)
    
    monkeypatch.setattr(Variable, "get", mock_get)

def test_dag_loads_with_no_errors():
    dag_bag = DagBag(dag_folder="/opt/etl/customer/customer", include_examples=False)
    dag = dag_bag.get_dag(dag_id="customer_historization_load")
    assert dag_bag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 1

def test_bash_operator_templates_render_correctly():
    dag_bag = DagBag(dag_folder="/opt/etl/customer/customer", include_examples=False)
    dag = dag_bag.get_dag(dag_id="customer_historization_load")
    task = dag.get_task("customer_historization_load")
    
    # Create a dummy DAG run to test template rendering
    execution_date = datetime.datetime(2023, 10, 25, tzinfo=datetime.timezone.utc)
    dag_run = dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=execution_date,
        run_type=DagRunType.MANUAL
    )
    
    ti = dag_run.get_task_instance(task_id="customer_historization_load")
    ti.task = task
    
    # Render templates
    ti.render_templates()
    
    # Assertions
    assert ti.task.bash_command == "python3 /opt/etl/customer/customer/r_historization_load.py"
    assert ti.task.env["RUN_DATE"] == "2023-10-25"
    assert ti.task.env["MAX_EXPECTED_CHANGE_PCT"] == "25"
    assert ti.task.env["GCP_PROJECT"] == "test-gcp-project"
    assert ti.task.env["GCP_REGION"] == "us-east1"
```

### Pass/Fail Criterion
**Pass**:
* The DAG imports without errors.
* The `BashOperator` correctly renders `RUN_DATE` as `2023-10-25` for an execution date of `2023-10-25`.
* All environment variables (`GCP_PROJECT`, `GCP_REGION`, `MAX_EXPECTED_CHANGE_PCT`) are correctly injected into the task context.

**Fail**: Import errors occur, or templated variables fail to render correctly.