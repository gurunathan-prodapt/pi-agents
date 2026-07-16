Here is the comprehensive migration-validation test suite for the `finance_month_end_workflow` pipeline. 

These tests are designed to be executed by a QA or CI/CD pipeline to guarantee that the migrated Airflow DAG and its underlying PySpark scripts behave identically to the legacy Automic (UC4) and Ab Initio/Spark implementations.

---

# SECTION 1 — UNIT & METADATA VALIDATION TESTS

## 1.1 Airflow DAG Integrity & Structure Validation
### Purpose
Verify that the migrated DAG file `finance_month_end_workflow.py` is syntactically correct, contains no import cycles, matches the expected task structure, and preserves the exact task dependencies defined in the legacy UC4 XML.

### Setup
- Install `pytest` and `apache-airflow` in the test environment.
- Place `finance_month_end_workflow.py` in the Airflow DAGs folder or add it to the python path.

### Action
Run the following `pytest` suite:

```python
import pytest
from airflow.models import DagBag, Variable
from airflow.utils.trigger_rule import TriggerRule

@pytest.fixture(scope="module", autouse=True)
def setup_airflow_variables():
    """Mock Airflow Variables required for DAG parsing."""
    Variable.set("finance_notify_email", "finance-etl@company.com")
    Variable.set("finance_force_close", "N")
    Variable.set("GCP_PROJECT", "gcp-finance-test")
    Variable.set("GCP_REGION", "europe-west1")
    Variable.set("DATAPROC_CLUSTER", "finance-spark-cluster")
    Variable.set("GCS_BUCKET", "finance-scripts-bucket")
    yield
    Variable.delete("finance_notify_email")
    Variable.delete("finance_force_close")
    Variable.delete("GCP_PROJECT")
    Variable.delete("GCP_REGION")
    Variable.delete("DATAPROC_CLUSTER")
    Variable.delete("GCS_BUCKET")

def test_dag_imports_and_loads_without_errors():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"
    
    dag = dag_bag.get_dag(dag_id="finance_month_end_workflow")
    assert dag is not None
    assert dag.dag_id == "finance_month_end_workflow"

def test_dag_structure_and_dependencies():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    dag = dag_bag.get_dag(dag_id="finance_month_end_workflow")
    
    expected_tasks = {
        "guard_last_business_day",
        "finance_pre_flight",
        "finance_stg_gl_extract_uk",
        "finance_stg_gl_extract_de",
        "finance_stg_gl_extract_fr",
        "finance_account_master_load",
        "finance_abinitio_gl_transform",
        "finance_abinitio_reconcile",
        "finance_spark_gl_aggregation",
        "finance_daily_gl_close",
        "publish_gcp_close_event",
        "trigger_retail_daily_workflow",
        "trigger_crm_weekly_workflow",
        "finance_period_close_notify"
    }
    
    actual_tasks = set(dag.task_ids)
    assert actual_tasks == expected_tasks, f"Task mismatch! Missing: {expected_tasks - actual_tasks}. Extra: {actual_tasks - expected_tasks}"

def test_reconciliation_failure_does_not_halt_dag():
    """
    Verify that the reconciliation task failure does not halt the DAG.
    This mirrors the legacy UC4: ON_FAILURE action="NOTIFY" then="CONTINUE".
    """
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    dag = dag_bag.get_dag(dag_id="finance_month_end_workflow")
    
    reconcile_task = dag.get_task("finance_abinitio_reconcile")
    daily_close_task = dag.get_task("finance_daily_gl_close")
    
    # Ensure daily close executes even if reconcile fails
    assert daily_close_task.trigger_rule == TriggerRule.ALL_DONE
    assert "finance_abinitio_reconcile" in reconcile_task.downstream_task_ids
```

### Pass/Fail Criterion
- **Pass**: The test suite executes with 100% success, proving that the DAG structure, task IDs, and trigger rules match the legacy specification.
- **Fail**: Any import errors are raised, tasks are missing, or the downstream trigger rules do not allow the DAG to continue on reconciliation failure.

---

## 1.2 Parameter & Date Template Verification
### Purpose
Verify that Airflow's Jinja templating engine resolves `PERIOD_DATE`, `PERIOD_NAME`, and `FISCAL_YEAR` to the exact string formats expected by the downstream PySpark scripts, matching the legacy UC4 system variables.

### Setup
- Initialize a mock Airflow task context for a specific execution date.

### Action
Run the following test using `pytest` to render and assert template outputs:

```python
from datetime import datetime
from airflow.models import DagBag, TaskInstance

def test_jinja_template_resolution():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    dag = dag_bag.get_dag(dag_id="finance_month_end_workflow")
    
    # Simulate execution on January 31st, 2025 at 20:00 UTC
    execution_date = datetime(2025, 1, 31, 20, 0, 0)
    
    # Retrieve tasks to test their templated fields
    extract_uk = dag.get_task("finance_stg_gl_extract_uk")
    transform = dag.get_task("finance_abinitio_gl_transform")
    aggregate = dag.get_task("finance_spark_gl_aggregation")
    
    # Create TaskInstances to resolve templates
    ti_uk = TaskInstance(task=extract_uk, execution_date=execution_date)
    ti_transform = TaskInstance(task=transform, execution_date=execution_date)
    ti_aggregate = TaskInstance(task=aggregate, execution_date=execution_date)
    
    # Mock the context
    context_uk = ti_uk.get_template_context()
    context_transform = ti_transform.get_template_context()
    context_aggregate = ti_aggregate.get_template_context()
    
    # Render fields
    rendered_uk_args = extract_uk.render_template(extract_uk.job["pyspark_job"]["args"], context_uk)
    rendered_transform_args = transform.render_template(transform.job["pyspark_job"]["args"], context_transform)
    rendered_aggregate_args = aggregate.render_template(aggregate.job["pyspark_job"]["args"], context_aggregate)
    
    # Assertions based on logical date 2025-01-31
    # PERIOD_DATE: Last day of previous month -> "2024-12-31"
    assert rendered_uk_args[0] == "2024-12-31"
    
    # PERIOD_NAME: Previous month formatted as MON-YYYY -> "DEC-2024"
    assert rendered_transform_args[1] == "DEC-2024"
    
    # FISCAL_YEAR: Current execution year -> "2025"
    assert rendered_aggregate_args[3] == "2025"
```

### Pass/Fail Criterion
- **Pass**: All rendered parameters match the expected date formats (`YYYY-MM-DD`, `MON-YYYY`, and `YYYY`) exactly.
- **Fail**: Any template resolves to an incorrect date or raises a rendering exception.

---

# SECTION 2 — FUNCTIONAL & BEHAVIOURAL EQUIVALENCE TESTS

## 2.1 Calendar Constraint Gate (`is_last_business_day`)
### Purpose
Verify that the `guard_last_business_day` task correctly allows execution *only* on the last business day (Monday through Friday) of the month, and short-circuits (skips) on all other days, unless `FORCE_CLOSE` is set to `"Y"`.

### Setup
- Prepare a list of test dates representing weekdays, weekends, and month-ends.

### Action
Run the following unit test against the `is_last_business_day` function:

```python
import pytest
from datetime import datetime
from unittest.mock import patch
from finance_month_end_workflow import is_last_business_day

@pytest.mark.parametrize(
    "test_date, force_close, expected_result",
    [
        # Last business day is a Friday (Jan 31, 2025)
        (datetime(2025, 1, 31), "N", True),
        # Mid-month weekday (Jan 15, 2025)
        (datetime(2025, 1, 15), "N", False),
        # Last day of month is Sunday, last business day is Friday 28th (Feb 2025)
        (datetime(2025, 2, 28), "N", True),
        (datetime(2025, 2, 27), "N", False),
        # Last day of month is Saturday, last business day is Friday 30th (May 2025)
        (datetime(2025, 5, 30), "N", True),
        (datetime(2025, 5, 31), "N", False),
        # Mid-month weekday with FORCE_CLOSE="Y" (Should bypass and return True)
        (datetime(2025, 1, 15), "Y", True),
    ]
)
def test_is_last_business_day_logic(test_date, force_close, expected_result):
    with patch("finance_month_end_workflow.FORCE_CLOSE", force_close):
        result = is_last_business_day(test_date)
        assert result is expected_result
```

### Pass/Fail Criterion
- **Pass**: The function returns `True` only on the calculated last business day of the month, or when `FORCE_CLOSE` is active.
- **Fail**: The function returns `True` on a non-business-end day or fails to bypass when `FORCE_CLOSE` is set to `"Y"`.

---

## 2.2 Extraction Parity (Oracle vs. PySpark BigQuery Extract)
### Purpose
Verify that the migrated PySpark extraction scripts (`run_gl_close_uk.py`, etc.) extract the exact same row counts and financial balances from the source database as the legacy `.ksh` scripts.

### Setup
- Set up a test database containing a static set of GL journal lines for period `DEC-2024`.
- Run the legacy `run_gl_close.ksh` script to generate a baseline CSV output.
- Run the migrated PySpark extraction job pointing to the same test database, writing to a temporary BigQuery staging table.

### Action
Execute a comparison query in BigQuery to verify data parity:

```sql
-- Assert zero differences in row counts and balance aggregations between Legacy and Migrated datasets
WITH legacy_summary AS (
  SELECT 
    'UK_ENTITY' AS entity,
    COUNT(*) AS total_rows,
    SUM(entered_dr) AS total_debit,
    SUM(entered_cr) AS total_credit
  FROM `gcp-finance-test.legacy_staging.gl_extract_uk_dec2024`
),
migrated_summary AS (
  SELECT 
    'UK_ENTITY' AS entity,
    COUNT(*) AS total_rows,
    SUM(entered_dr) AS total_debit,
    SUM(entered_cr) AS total_credit
  FROM `gcp-finance-test.finance_staging.stg_gl_extract_uk`
  WHERE period_name = 'DEC-2024'
)
SELECT 
  l.entity,
  (l.total_rows - m.total_rows) AS row_count_delta,
  (l.total_debit - m.total_debit) AS debit_delta,
  (l.total_credit - m.total_credit) AS credit_delta
FROM legacy_summary l
JOIN migrated_summary m ON l.entity = m.entity;
```

### Pass/Fail Criterion
- **Pass**: The query returns `0` for `row_count_delta`, `debit_delta`, and `credit_delta`.
- **Fail**: Any delta is non-zero, indicating data loss, duplication, or precision errors during extraction.

---

# SECTION 3 — TRANSFORMATION & RECONCILIATION VALIDATION

## 3.1 Ab Initio to PySpark Transformation Correctness (`gl_transform.py`)
### Purpose
Verify that the migrated PySpark transformation script (`gl_transform.py`) correctly replicates the business logic of the legacy Ab Initio graph (`gl_transform.xfr`), specifically handling:
1. Multi-entity joins (UK, DE, FR staging tables joined with the Account Master).
2. NULL handling for cost centers (defaulting to `'CC-UNMAPPED'`).
3. Type casting of transaction amounts to `NUMERIC(38, 9)`.

### Setup
- Populate staging tables with test records containing unmapped cost centers, null values, and varying decimal precisions.

### Action
Run a validation query against the output of the transformation task:

```sql
-- Test 1: Verify unmapped cost centers are defaulted correctly
ASSERT (
  SELECT COUNT(*) 
  FROM `gcp-finance-test.finance_warehouse.transformed_gl_transactions`
  WHERE cost_center IS NULL
) = 0 
MESSAGE "Error: NULL cost centers found in transformed output!";

-- Test 2: Verify unmapped cost centers are mapped to 'CC-UNMAPPED'
ASSERT (
  SELECT COUNT(*) 
  FROM `gcp-finance-test.finance_warehouse.transformed_gl_transactions` t
  LEFT JOIN `gcp-finance-test.finance_warehouse.dim_account_master` m 
    ON t.account_id = m.account_id
  WHERE m.account_id IS NULL AND t.cost_center != 'CC-UNMAPPED'
) = 0 
MESSAGE "Error: Unmapped accounts did not default to 'CC-UNMAPPED'!";

-- Test 3: Verify decimal precision preservation (No rounding errors)
ASSERT (
  SELECT SUM(CAST(amount AS BIGNUMERIC)) - SUM(amount)
  FROM `gcp-finance-test.finance_warehouse.transformed_gl_transactions`
) = 0
MESSAGE "Error: Precision loss detected during decimal casting!";
```

### Pass/Fail Criterion
- **Pass**: All assertions execute successfully without throwing errors.
- **Fail**: Any assertion fails, indicating incorrect transformation logic, unhandled NULLs, or precision loss.

---

## 3.2 Reconciliation Logic & Warning Handling (`gl_reconcile.py`)
### Purpose
Verify that the reconciliation script (`gl_reconcile.py`) correctly flags imbalances between the General Ledger and Sub-Ledgers, and that the Airflow DAG handles a reconciliation warning/failure without aborting the pipeline.

### Setup
- Inject an intentional $10,000 imbalance into the French sub-ledger staging table.
- Trigger the Airflow DAG run.

### Action
Monitor the execution status of the DAG run and inspect the BigQuery reconciliation audit table:

```python
import pytest
from airflow.models import DagBag, DagRun
from airflow.utils.state import DagRunState, TaskInstanceState

def test_reconciliation_warning_allows_dag_completion():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    dag = dag_bag.get_dag(dag_id="finance_month_end_workflow")
    
    # Trigger a manual DAG run
    dag_run = dag.create_dagrun(
        state=DagRunState.RUNNING,
        run_id="test_reconciliation_failure_run",
        conf={}
    )
    
    # Force the reconciliation task to fail to simulate an out-of-balance scenario
    reconcile_ti = dag_run.get_task_instance("finance_abinitio_reconcile")
    reconcile_ti.set_state(TaskInstanceState.FAILED)
    
    # Evaluate downstream task states
    daily_close_ti = dag_run.get_task_instance("finance_daily_gl_close")
    daily_close_ti.run(ignore_ti_state=True, ignore_upstream_states=False)
    
    # Assert that the daily close task ran successfully despite the reconciliation failure
    assert daily_close_ti.state == TaskInstanceState.SUCCESS
```

### Pass/Fail Criterion
- **Pass**: The `finance_daily_gl_close` task executes successfully even when `finance_abinitio_reconcile` fails.
- **Fail**: The DAG halts or skips downstream tasks when the reconciliation task fails.

---

# SECTION 4 — EXTERNAL INTEGRATIONS & NOTIFICATIONS

## 4.1 Pub/Sub Event Emission & Downstream DAG Triggering
### Purpose
Verify that the `publish_gcp_close_event` task publishes the correct metadata payload to Google Cloud Pub/Sub, and that the downstream DAGs (`retail_daily_workflow`, `crm_weekly_workflow`) are triggered successfully.

### Setup
- Create a subscription to the GCP Pub/Sub topic `finance-gl-close-complete` to capture test messages.

### Action
Execute the DAG up to the notification tasks, pull the message from the subscription, and verify the payload:

```python
from google.cloud import pubsub_v1
import json

def test_pubsub_payload_and_attributes():
    project_id = "gcp-finance-test"
    subscription_id = "finance-gl-close-complete-test-sub"
    
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    
    # Pull 1 message from the subscription
    response = subscriber.pull(request={"subscription": subscription_path, "max_messages": 1})
    
    assert len(response.received_messages) > 0, "No Pub/Sub message received!"
    
    received_message = response.received_messages[0]
    message_data = received_message.message.data.decode("utf-8")
    attributes = received_message.message.attributes
    
    # Assertions
    assert message_data == "GL_CLOSE_COMPLETE"
    assert "period_name" in attributes
    assert "fiscal_year" in attributes
    
    # Acknowledge message
    subscriber.acknowledge(
        request={"subscription": subscription_path, "ack_ids": [received_message.ack_id]}
    )
```

### Pass/Fail Criterion
- **Pass**: The Pub/Sub message is received with the exact payload `"GL_CLOSE_COMPLETE"` and contains the correct `period_name` and `fiscal_year` attributes.
- **Fail**: No message is published, or the attributes are missing/malformed.