Here is a comprehensive suite of migration-validation tests designed to prove that the migrated Airflow DAG (`retail_daily_workflow.py`) is behaviorally equivalent to the legacy UC4 workflow (`retail_daily_workflow.xml`).

---

# Migration Validation Test Suite: `retail_daily_workflow`

## Section 1: DAG Structure & Parameter Parity

### Test Case 1.1: DAG Metadata and Scheduling Parity
* **Purpose:** Verify that the migrated Airflow DAG retains the exact scheduling, timezone, and execution parameters defined in the legacy UC4 header.
* **Setup:** 
  * Parse the migrated DAG file `sales/retail_daily_workflow.py` within a test environment.
* **Action:** 
  * Execute a pytest suite to inspect the DAG object properties.
* **Pass/Fail Criterion:** 
  * **Pass:** The DAG ID is exactly `retail_daily_workflow`, the schedule interval is `'0 2 * * *'` (representing 02:00 daily), `catchup` is `False`, and `max_active_runs` is `1`.
  * **Fail:** Any of the metadata parameters do not match the legacy specification.

```python
# test_dag_metadata.py
import pytest
from airflow.models import DagBag

def test_dag_metadata_parity():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.get_dag("retail_daily_workflow")
    
    assert dag is not None, "DAG retail_daily_workflow not found."
    assert dag.schedule_interval == "0 2 * * *"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.default_args.get("owner") == "DW_TEAM"
    assert dag.default_args.get("email") == ["dw-alerts@company.com"]
```

### Test Case 1.2: Task Dependency and Topology Parity
* **Purpose:** Ensure that the execution graph (topology) of the Airflow DAG matches the legacy UC4 predecessor rules, including parallel splits, joins, and cross-domain sensors.
* **Setup:** Load the DAG object from the Airflow environment.
* **Action:** Programmatically inspect downstream and upstream task relationships.
* **Pass/Fail Criterion:**
  * **Pass:** 
    * `retail_pre_check` has downstream targets `['retail_stg_extract_north', 'retail_stg_extract_south']`.
    * `retail_product_master_load` has upstream dependencies `['retail_stg_extract_north', 'retail_stg_extract_south', 'wait_for_finance_gl_close']`.
    * `retail_data_quality_check` triggers both `retail_completion_notify_email` and `retail_completion_publish_event`.
  * **Fail:** Any dependency link is missing, or extra unauthorized links exist.

```python
# test_dag_dependencies.py
from airflow.models import DagBag

def test_dag_dependency_map():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.get_dag("retail_daily_workflow")
    
    # Verify pre-check triggers extracts
    pre_check = dag.get_task("retail_pre_check")
    assert "retail_stg_extract_north" in pre_check.downstream_task_ids
    assert "retail_stg_extract_south" in pre_check.downstream_task_ids
    
    # Verify product master load join condition
    prod_master = dag.get_task("retail_product_master_load")
    upstream_tasks = prod_master.upstream_task_ids
    assert "retail_stg_extract_north" in upstream_tasks
    assert "retail_stg_extract_south" in upstream_tasks
    assert "wait_for_finance_gl_close" in upstream_tasks
```

---

## Section 2: Transformation Correctness & Input/Output Parity

### Test Case 2.1: Date and Batch ID Macro Substitution Parity
* **Purpose:** Prove that the Airflow template substitutions for `LOAD_DATE` and `BATCH_ID` resolve to the exact same values as the legacy UC4 variables (`&$TODAY-1D` and `&$TODAY_YYYYMMDD`).
* **Setup:** 
  * Mock an execution date of `2024-10-27T02:00:00+01:00` (Europe/London).
* **Action:** 
  * Render the templates for the Oracle pre-check query and the Dataproc PySpark arguments.
* **Pass/Fail Criterion:**
  * **Pass:** 
    * The rendered date resolves to `2024-10-26` (representing `execution_date - 1 day`).
    * The batch ID resolves to `20241027`.
  * **Fail:** The rendered strings do not match the expected historical offsets.

```python
# test_macro_rendering.py
from datetime import datetime
from airflow.models import DagBag, TaskInstance

def test_macro_render_values():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.get_dag("retail_daily_workflow")
    task = dag.get_task("retail_pre_check")
    
    # Create a dummy execution date
    exec_date = datetime(2024, 10, 27, 2, 0, 0)
    ti = TaskInstance(task=task, execution_date=exec_date)
    context = ti.get_template_context()
    
    rendered_sql = task.render_template(task.sql, context)
    
    # Verify date subtraction logic (execution_date - 1 day)
    assert "TO_DATE('2024-10-26','YYYY-MM-DD')" in rendered_sql
```

### Test Case 2.2: Regional Extract Data Parity (North vs. South)
* **Purpose:** Prove that the migrated PySpark extraction scripts (`load_daily_sales.py`) produce identical staging outputs to the legacy KornShell scripts (`load_daily_sales.ksh`) when run against the same source database.
* **Setup:** 
  * Populate the source Oracle table `SOURCE_OPS.SALES_TXN` with a controlled set of 1,000 transactions (500 North, 500 South) for the target date.
* **Action:** 
  * Run the legacy script for both regions and save the output files.
  * Run the migrated PySpark jobs on Dataproc and save the output Parquet/Avro files.
  * Compare row counts, schema, and column values.
* **Pass/Fail Criterion:**
  * **Pass:** Both outputs contain exactly 500 rows per region, with identical schemas, matching transaction IDs, and identical precision for monetary values.
  * **Fail:** Row counts differ, schemas mismatch, or data values diverge.

```sql
-- Validation Query: Run against both legacy staging tables and migrated BigQuery/GCS external tables
SELECT 
  region,
  COUNT(*) as row_count,
  SUM(txn_amount) as total_sales,
  AVG(discount_applied) as avg_discount
FROM staging_sales_extract
WHERE txn_date = '2024-10-26'
GROUP BY region;
```

---

## Section 3: External-System Replacements & Integration

### Test Case 3.1: Oracle Pre-Check Connectivity and Query Parity
* **Purpose:** Verify that the `OracleOperator` task (`retail_pre_check`) successfully connects to the Oracle instance and executes the exact same query logic as the legacy inline SQL*Plus script.
* **Setup:** 
  * Configure the `oracle_dw_login` connection in Airflow to point to a test Oracle instance containing the `SOURCE_OPS.SALES_TXN` table.
* **Action:** 
  * Execute the `retail_pre_check` task.
* **Pass/Fail Criterion:**
  * **Pass:** The task executes successfully, returning the count of transactions for the target date without syntax or connection errors.
  * **Fail:** The task fails due to connection issues, incorrect schema references, or SQL syntax errors.

### Test Case 3.2: Cross-Domain Dependency Sensor (Finance GL Close)
* **Purpose:** Verify that the `ExternalTaskSensor` (`wait_for_finance_gl_close`) correctly blocks execution until the upstream finance DAG completes, and successfully proceeds once the dependency is met.
* **Setup:** 
  * Create a mock `finance_daily_workflow` DAG with a task named `finance_daily_gl_close`.
* **Action:** 
  * Trigger `retail_daily_workflow` while `finance_daily_gl_close` is in a `running` state. Verify the sensor pokes and waits.
  * Transition `finance_daily_gl_close` to `success`.
* **Pass/Fail Criterion:**
  * **Pass:** The sensor remains in a poking state while the upstream task is running, and immediately transitions to `success` once the upstream task succeeds.
  * **Fail:** The sensor fails, times out prematurely, or proceeds before the upstream task completes.

---

## Section 4: Data Quality, Error Handling & SLA Assertions

### Test Case 4.1: Data Quality Warning Handling (Soft Failures)
* **Purpose:** Prove that the migrated DAG respects the legacy "SUCCESS_OR_WARNING" behavior where data quality check failures do not block downstream notifications and triggers.
* **Setup:** 
  * Configure the PySpark data quality script (`retail_data_quality.py`) to exit with a warning status (non-zero exit code or warning flag written to metadata) to simulate a validation threshold breach.
* **Action:** 
  * Execute the `retail_data_quality_check` task.
* **Pass/Fail Criterion:**
  * **Pass:** The downstream tasks `retail_completion_notify_email` and `retail_completion_publish_event` execute successfully because their `trigger_rule` is set to `'all_done'`.
  * **Fail:** The downstream tasks are skipped or marked as failed due to the upstream warning.

```python
# test_trigger_rules.py
from airflow.models import DagBag

def test_dq_downstream_trigger_rules():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.get_dag("retail_daily_workflow")
    
    email_task = dag.get_task("retail_completion_notify_email")
    pubsub_task = dag.get_task("retail_completion_publish_event")
    
    # Ensure trigger rule is 'all_done' to match UC4 SUCCESS_OR_WARNING behavior
    assert email_task.trigger_rule == "all_done"
    assert pubsub_task.trigger_rule == "all_done"
```

### Test Case 4.2: Pub/Sub Event Notification Parity
* **Purpose:** Verify that the completion event published to Google Cloud Pub/Sub contains the exact metadata payload required by downstream workflows (such as `crm_weekly_workflow`), matching the legacy `uc4api publish_event` call.
* **Setup:** 
  * Subscribe a test pull subscription to the `retail-daily-complete-topic` topic.
* **Action:** 
  * Execute the `retail_completion_publish_event` task for execution date `2024-10-27`.
* **Pass/Fail Criterion:**
  * **Pass:** A message is published containing the attribute `date` with value `2024-10-26` and the message body `RETAIL_DAILY_COMPLETE`.
  * **Fail:** The message is missing, published to the wrong topic, or contains incorrect date attributes.

```python
# test_pubsub_payload.py
import pytest
from google.cloud import pubsub_v1

def test_pubsub_message_payload():
    project_id = "your-gcp-project-id"
    subscription_id = "test-retail-complete-sub"
    
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    
    # Pull messages from the test subscription
    response = subscriber.pull(request={"subscription": subscription_path, "max_messages": 1})
    
    assert len(response.received_messages) > 0
    message = response.received_messages[0].message
    
    assert message.data == b"RETAIL_DAILY_COMPLETE"
    assert message.attributes["date"] == "2024-10-26"
    
    # Acknowledge message to clean up queue
    ack_ids = [msg.ack_id for msg in response.received_messages]
    subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": ack_ids})
```