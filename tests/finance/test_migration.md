Here is the comprehensive migration validation test suite for the `FINANCE_DAILY_WORKFLOW` pipeline. 

These tests are designed to validate the Airflow DAG structure, parameter resolution, error-handling callbacks, and the behavioral equivalence of the migrated PySpark stubs against their legacy counterparts.

---

# SECTION 1 — AIRFLOW DAG STRUCTURE & METADATA TESTS

## Test Case 1.1: DAG Configuration and Scheduling Parity
### Purpose
Verify that the migrated Airflow DAG matches the legacy UC4 scheduling, timezone, and execution constraints exactly.

### Setup
* Airflow environment initialized with the migrated DAG file `finance_daily_workflow.py` loaded.
* Airflow Variables configured:
  ```json
  {
    "GCP_PROJECT": "test-gcp-project",
    "GCP_REGION": "europe-west1",
    "DATAPROC_CLUSTER_NAME": "test-dataproc-cluster",
    "GCS_BUCKET_NAME": "test-gcs-bucket",
    "finance_notify_email": "finance-etl@company.com"
  }
  ```

### Action
Execute a unit test using `pytest` to parse the DAG and assert its properties.

```python
# test_dag_metadata.py
import pytest
from airflow.models import DagBag

def test_dag_metadata_parity():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    dag = dag_bag.get_dag(dag_id="finance_daily_workflow")
    
    assert dag is not None, "DAG finance_daily_workflow failed to load or has syntax errors."
    assert dag.schedule_interval == "0 1 * * 1-5", "Schedule cron mismatch (Expected Mon-Fri at 01:00)."
    assert dag.timezone.name == "Europe/London", "Timezone mismatch (Expected Europe/London)."
    assert dag.catchup is False, "Catchup must be disabled to prevent retroactive runs."
    assert dag.max_active_runs == 1, "max_active_runs must be 1 to prevent concurrent day operations."
    assert dag.default_args.get('owner') == 'finance_etl', "Owner mismatch."
```

### Pass/Fail Criterion
* **Pass**: The DAG parses without import errors, and all metadata assertions (schedule, timezone, catchup, max active runs) pass.
* **Fail**: Any metadata assertion fails, or the DAG fails to parse due to syntax/import errors.

---

## Test Case 1.2: Task Dependency Topology Validation
### Purpose
Verify that the migrated task dependency tree matches the legacy UC4 execution sequence exactly.

### Setup
* Airflow environment initialized with the migrated DAG file loaded.

### Action
Execute a unit test to programmatically inspect the upstream and downstream relationships of each task.

```python
# test_dag_dependencies.py
from airflow.models import DagBag

def test_dag_dependency_map():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    dag = dag_bag.get_dag(dag_id="finance_daily_workflow")
    
    # Define expected task relationships
    dependencies = {
        "check_holiday_calendar": {
            "upstream": [],
            "downstream": ["finance_daily_pre_check"]
        },
        "finance_daily_pre_check": {
            "upstream": ["check_holiday_calendar"],
            "downstream": ["finance_daily_acct_load", "finance_daily_rate_extract"]
        },
        "finance_daily_acct_load": {
            "upstream": ["finance_daily_pre_check"],
            "downstream": ["finance_daily_gl_extract"]
        },
        "finance_daily_rate_extract": {
            "upstream": ["finance_daily_pre_check"],
            "downstream": ["finance_daily_gl_extract"]
        },
        "finance_daily_gl_extract": {
            "upstream": ["finance_daily_acct_load", "finance_daily_rate_extract"],
            "downstream": ["finance_daily_gl_close_log"]
        },
        "finance_daily_gl_close_log": {
            "upstream": ["finance_daily_gl_extract"],
            "downstream": ["finance_daily_gl_close_publish"]
        },
        "finance_daily_gl_close_publish": {
            "upstream": ["finance_daily_gl_close_log"],
            "downstream": []
        }
    }
    
    for task_id, deps in dependencies.items():
        task = dag.get_task(task_id)
        assert sorted([t.task_id for t in task.upstream_list]) == sorted(deps["upstream"]), f"Upstream mismatch for {task_id}"
        assert sorted([t.task_id for t in task.downstream_list]) == sorted(deps["downstream"]), f"Downstream mismatch for {task_id}"
```

### Pass/Fail Criterion
* **Pass**: The task dependency map matches the legacy sequence exactly.
* **Fail**: Any task has incorrect upstream or downstream dependencies.

---

# SECTION 2 — TRANSFORMATION & PARAMETER PARITY TESTS

## Test Case 2.1: Parameter and Variable Resolution
### Purpose
Verify that Airflow template variables resolve to the correct values, matching the legacy UC4 variables (`&PERIOD_DATE`, `&PERIOD_YEAR`, `&PERIOD_MONTH`).

### Setup
* Instantiate a mock `DagRun` with a logical execution date of `2024-11-15T01:00:00+00:00` (a Friday).

### Action
Render the templates for the Dataproc operators and assert the resolved arguments.

```python
# test_parameter_resolution.py
from datetime import datetime
from airflow.models import DagBag, DagRun, TaskInstance
from airflow.utils.types import DagRunType

def test_parameter_rendering():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    dag = dag_bag.get_dag(dag_id="finance_daily_workflow")
    
    # Create a mock execution run
    execution_date = datetime(2024, 11, 15, 1, 0, 0)
    dag_run = dag.create_dagrun(
        run_id="test_run_1",
        state="running",
        execution_date=execution_date,
        data_interval=(execution_date, execution_date),
        run_type=DagRunType.MANUAL
    )
    
    # 1. Test Rate Extract Parameter Resolution
    rate_task = dag.get_task("finance_daily_rate_extract")
    ti_rate = TaskInstance(task=rate_task, run_id=dag_run.run_id)
    ti_rate.render_templates()
    
    rendered_args = rate_task.job["pyspark_job"]["args"]
    assert rendered_args[1] == "2024-11-15", "PERIOD_DATE failed to resolve to YYYY-MM-DD"
    assert rendered_args[3] == "2024", "PERIOD_YEAR failed to resolve to YYYY"
    assert rendered_args[5] == "11", "PERIOD_MONTH failed to resolve to MM"
    
    # 2. Test Account Load Parameter Resolution
    acct_task = dag.get_task("finance_daily_acct_load")
    ti_acct = TaskInstance(task=acct_task, run_id=dag_run.run_id)
    ti_acct.render_templates()
    
    rendered_acct_args = acct_task.job["pyspark_job"]["args"]
    assert rendered_acct_args[1] == "2024-11-15", "PERIOD_DATE failed to resolve for Account Load"
```

### Pass/Fail Criterion
* **Pass**: All templated arguments render to the correct date, year, and month strings.
* **Fail**: Any rendered argument does not match the expected date format or value.

---

## Test Case 2.2: Holiday Calendar Short-Circuiting
### Purpose
Verify that the `check_holiday_calendar` task correctly skips downstream execution when the run date falls on a registered UK Public Holiday, and proceeds on regular business days.

### Setup
* Mock the execution context for a holiday (`2024-01-01`) and a non-holiday (`2024-11-15`).

### Action
Execute the `check_uk_holiday_calendar` Python callable with both contexts.

```python
# test_holiday_calendar.py
from datetime import datetime
from unittest.mock import MagicMock
from dags.finance_daily_workflow import check_uk_holiday_calendar

def test_holiday_calendar_logic():
    # Case A: Holiday (New Year's Day)
    mock_context_holiday = {
        'dag_run': MagicMock(logical_date=datetime(2024, 1, 1, 1, 0, 0))
    }
    assert check_uk_holiday_calendar(**mock_context_holiday) is False, "Should return False (skip) on UK Holiday"
    
    # Case B: Non-Holiday Business Day
    mock_context_business_day = {
        'dag_run': MagicMock(logical_date=datetime(2024, 11, 15, 1, 0, 0))
    }
    assert check_uk_holiday_calendar(**mock_context_business_day) is True, "Should return True (proceed) on Business Day"
```

### Pass/Fail Criterion
* **Pass**: The callable returns `False` for dates in the holiday list and `True` for regular business days.
* **Fail**: The callable returns `True` on a holiday or `False` on a business day.

---

# SECTION 3 — EXTERNAL SYSTEM REPLACEMENTS & INTEGRATION TESTS

## Test Case 3.1: Pub/Sub Event Publishing Parity
### Purpose
Verify that the `finance_daily_gl_close_publish` task publishes the exact event payload and attributes required by downstream cross-domain consumers (`RETAIL_DAILY_WORKFLOW` and `CRM_WEEKLY_WORKFLOW`).

### Setup
* Mock the Google Cloud Pub/Sub publish call.
* Instantiate a mock execution run for `2024-11-15`.

### Action
Render and execute the `PubSubPublishMessageOperator` task in a test harness, capturing the published message payload.

```python
# test_pubsub_payload.py
from datetime import datetime
from unittest.mock import MagicMock, patch
from airflow.models import DagBag, TaskInstance

@patch('airflow.providers.google.cloud.operators.pubsub.PubSubHook')
def test_pubsub_publish_payload(mock_pubsub_hook):
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    dag = dag_bag.get_dag(dag_id="finance_daily_workflow")
    task = dag.get_task("finance_daily_gl_close_publish")
    
    # Mock execution context
    execution_date = datetime(2024, 11, 15, 1, 0, 0)
    ti = TaskInstance(task=task, execution_date=execution_date)
    context = ti.get_template_context()
    ti.render_templates()
    
    # Execute task
    mock_hook_instance = mock_pubsub_hook.return_value
    task.execute(context=context)
    
    # Assert hook was called with correct arguments
    mock_hook_instance.publish.assert_called_once()
    call_args = mock_hook_instance.publish.call_args[1]
    
    published_messages = call_args['messages']
    assert len(published_messages) == 1
    
    message = published_messages[0]
    assert message['data'] == b"FINANCE_GL_CLOSE_COMPLETE", "Message data payload mismatch"
    assert message['attributes']['period_date'] == "2024-11-15", "Attribute period_date mismatch"
    assert 'audit_timestamp' in message['attributes'], "Missing audit_timestamp attribute"
```

### Pass/Fail Criterion
* **Pass**: The Pub/Sub message contains the exact byte payload `b"FINANCE_GL_CLOSE_COMPLETE"` and the correct `period_date` attribute.
* **Fail**: The payload or attributes are missing, malformed, or contain incorrect dates.

---

## Test Case 3.2: Output Log Literal Verification
### Purpose
Verify that the pipeline preserves the legacy output log literal exactly as written in the source system: `"[FINANCE_DAILY_GL_CLOSE] Period=" + PERIOD_DATE + " complete"`.

### Setup
* Mock the standard output stream (`sys.stdout`) and the logging framework.

### Action
Execute the `audit_log_gl_close` Python callable with a mock context for `2024-11-15`.

```python
# test_audit_log_literal.py
import io
import sys
import logging
from unittest.mock import MagicMock
from dags.finance_daily_workflow import audit_log_gl_close

def test_audit_log_literal_output():
    # Capture stdout and logs
    captured_output = io.StringIO()
    sys.stdout = captured_output
    
    mock_context = {
        'ds': '2024-11-15'
    }
    
    with pytest.LogCaptureFixture() as log_capture:
        audit_log_gl_close(**mock_context)
        
        # Restore stdout
        sys.stdout = sys.__stdout__
        
        expected_literal = "[FINANCE_DAILY_GL_CLOSE] Period=2024-11-15 complete"
        
        # Assert stdout output
        assert captured_output.getvalue().strip() == expected_literal, "Stdout literal mismatch"
```

### Pass/Fail Criterion
* **Pass**: The exact string `[FINANCE_DAILY_GL_CLOSE] Period=2024-11-15 complete` is printed to stdout.
* **Fail**: The printed string differs by even a single character, casing, or spacing.

---

# SECTION 4 — ERROR HANDLING & RETRY STRATEGY TESTS

## Test Case 4.1: Critical Failure Alerting (NOTIFY_AND_ABORT)
### Purpose
Verify that critical tasks (`finance_daily_pre_check`, `finance_daily_gl_extract`) trigger the `on_failure_alarm` callback upon failure, simulating the legacy `NOTIFY_AND_ABORT` action.

### Setup
* Mock the logging framework to capture error logs.
* Create a mock Airflow Context dictionary containing an exception.

### Action
Directly invoke the `on_failure_alarm` callback function with the mock context.

```python
# test_critical_alerts.py
import logging
from unittest.mock import MagicMock
from dags.finance_daily_workflow import on_failure_alarm

def test_on_failure_alarm_callback(caplog):
    mock_context = {
        'task_instance': MagicMock(task_id='finance_daily_pre_check'),
        'ds': '2024-11-15',
        'exception': Exception("Oracle Connection Timeout")
    }
    
    with caplog.at_level(logging.ERROR):
        on_failure_alarm(mock_context)
        
        # Assert critical alert details are logged
        assert any("SENDING CRITICAL ALERT to finance-etl@company.com and dw-alerts@company.com" in record.message for record in caplog.records)
        assert any("[CRITICAL] FINANCE_DAILY_WORKFLOW FAILED for 2024-11-15" in record.message for record in caplog.records)
        assert any("Oracle Connection Timeout" in record.message for record in caplog.records)
```

### Pass/Fail Criterion
* **Pass**: The callback logs the critical alert to the correct distribution lists (`finance-etl@company.com` and `dw-alerts@company.com`) and includes the exception details.
* **Fail**: The callback fails to execute, logs to the wrong recipients, or misses the exception context.

---

## Test Case 4.2: Non-Blocking Failure Alerting (NOTIFY then CONTINUE)
### Purpose
Verify that non-blocking tasks (`finance_daily_acct_load`, `finance_daily_rate_extract`) trigger the `on_failure_alarm_continue` callback upon failure, simulating the legacy warning-and-continue behavior.

### Setup
* Mock the logging framework to capture warning logs.
* Create a mock Airflow Context dictionary.

### Action
Directly invoke the `on_failure_alarm_continue` callback function with the mock context.

```python
# test_warning_alerts.py
import logging
from unittest.mock import MagicMock
from dags.finance_daily_workflow import on_failure_alarm_continue

def test_on_failure_alarm_continue_callback(caplog):
    mock_context = {
        'task_instance': MagicMock(task_id='finance_daily_rate_extract'),
        'ds': '2024-11-15',
        'exception': Exception("Rate Extract Query Failed")
    }
    
    with caplog.at_level(logging.WARNING):
        on_failure_alarm_continue(mock_context)
        
        # Assert warning alert details are logged
        assert any("SENDING WARNING ALERT to finance-etl@company.com" in record.message for record in caplog.records)
        assert any("[WARNING] FINANCE_DAILY_WORKFLOW non-blocking task failed on 2024-11-15" in record.message for record in caplog.records)
        assert any("Proceeding with downstream evaluation pipelines" in record.message for record in caplog.records)
```

### Pass/Fail Criterion
* **Pass**: The callback logs a warning alert to `finance-etl@company.com` and explicitly states that downstream execution will proceed.
* **Fail**: The callback logs an error instead of a warning, or targets the wrong email distribution list.