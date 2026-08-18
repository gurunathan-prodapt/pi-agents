# Migration Validation Test Suite: FINANCE.GL_AGGREGATE_AND_CLOSE

This document defines the migration-validation test suite for the `FINANCE.GL_AGGREGATE_AND_CLOSE` workflow. These tests verify behavioral equivalence between the legacy UC4/KornShell/Oracle environment and the migrated Apache Airflow/Python/BigQuery environment.

---

## Test Case 1: Airflow DAG Parameter Resolution & Jinja Templating

### Purpose
Verify that the Airflow DAG correctly resolves the dynamic environment variables `PERIOD_NAME` and `FISCAL_YEAR` using Jinja templates, matching the legacy UC4 variable evaluation logic.

### Setup
1. A test Airflow environment with the DAG `finance_gl_aggregate_and_close` loaded.
2. Airflow Variable `CURRENT_FISCAL_YEAR` set to `"2024"`.
3. Airflow Variable `finance_gl_aggregate_and_close_notify_email` set to `"test-finance@example.com"`.
4. A mock execution date set to `2024-03-15` (which should resolve the previous month to `FEB-2024`).

### Action
Execute a dry-run/render of the `gl_aggregate_and_close` task templates for the logical date `2024-03-15`.

### Pass/Fail Criterion
* **Pass**: The rendered bash command contains exactly:
  * `export PERIOD_NAME="FEB-2024"`
  * `export FISCAL_YEAR="2024"`
  * `export NOTIFY_EMAIL="test-finance@example.com"`
* **Fail**: Any of the variables are unresolved, resolve to incorrect values (e.g., current month instead of previous month), or raise rendering exceptions.

### Test Code (pytest)
```python
import pytest
from datetime import datetime
from airflow.models import DagBag, Variable
from airflow.utils.context import Context

@pytest.fixture(autouse=True)
def setup_airflow_variables(monkeypatch):
    # Mock Airflow Variables
    variables = {
        "CURRENT_FISCAL_YEAR": "2024",
        "finance_gl_aggregate_and_close_notify_email": "test-finance@example.com"
    }
    def mock_get(key, default_var=None):
        return variables.get(key, default_var)
    monkeypatch.setattr(Variable, "get", mock_get)

def test_dag_template_rendering():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.get_dag("finance_gl_aggregate_and_close")
    assert dag is not None
    
    task = dag.get_task("gl_aggregate_and_close")
    
    # Create a mock DagRun and TaskInstance context
    execution_date = datetime(2024, 3, 15)
    context = Context({
        "dag_run": dag.create_dagrun(
            state="running",
            execution_date=execution_date,
            run_id="test_run"
        ),
        "logical_date": execution_date,
        "params": {"home_dir": "/opt/airflow"},
        "var": {
            "value": {
                "CURRENT_FISCAL_YEAR": "2024",
                "finance_gl_aggregate_and_close_notify_email": "test-finance@example.com"
            }
        }
    })
    
    # Render the templates
    rendered_content = task.render_template(task.bash_command, context)
    
    assert 'export PERIOD_NAME="FEB-2024"' in rendered_content
    assert 'export FISCAL_YEAR="2024"' in rendered_content
    assert 'export NOTIFY_EMAIL="test-finance@example.com"' in rendered_content
    assert 'python /opt/airflow/finance/r_gl_aggregate_and_close.py' in rendered_content
```

---

## Test Case 2: Spark Execution Failure Isolation (Atomic Dependency)

### Purpose
Prove that if the Spark aggregation job fails, the script exits immediately with code `1` and **does not** write the close-audit record or send a success notification. This enforces the legacy safety constraint: *"the audit record must never be written on a failed aggregation run."*

### Setup
1. Mock the `subprocess.run` call for the Spark submission command to return a non-zero exit code (`rc=1`).
2. Mock the BigQuery client (or SQL*Plus command) to track if it gets executed.
3. Set environment variables: `PERIOD_NAME="JAN-2024"`, `FISCAL_YEAR="2024"`.

### Action
Run the `main()` function of `finance/r_gl_aggregate_and_close.py`.

### Pass/Fail Criterion
* **Pass**: The script exits with status code `1`, logs the error message, and the database execution block is **never** called.
* **Fail**: The script exits with code `0`, or attempts to execute the database audit step despite the Spark failure.

### Test Code (pytest)
```python
import sys
import pytest
from unittest.mock import patch, MagicMock, mock_open

@patch("subprocess.run")
@patch("google.cloud.bigquery.Client")
def test_spark_failure_prevents_audit(mock_bq_client, mock_subprocess):
    # Import the script dynamically
    import finance.r_gl_aggregate_and_close as script
    
    # Configure environment
    script.PERIOD_NAME = "JAN-2024"
    script.FISCAL_YEAR = "2024"
    script.GCP_PROJECT = "test-project"
    script.HAS_BIGQUERY = True
    
    # Mock Spark failure
    import subprocess
    mock_subprocess.side_effect = subprocess.CalledProcessError(returncode=1, cmd="spark-submit")
    
    # Run main and assert it exits with code 1
    with pytest.raises(SystemExit) as exc_info:
        script.main()
        
    assert exc_info.value.code == 1
    
    # Verify BigQuery client was never instantiated or queried
    mock_bq_client.assert_not_called()
```

---

## Test Case 3: BigQuery Transactional Integrity & Rollback Parity

### Purpose
Verify that the BigQuery script `d_gl_close_audit.sql` behaves as an atomic transaction. If either the `INSERT` into `GL_CLOSE_AUDIT` or the `UPDATE` to `GL_PERIOD_STATUS` fails, the entire transaction must roll back, leaving the database state unchanged.

### Setup
1. Create temporary test tables in BigQuery: `GL_CLOSE_AUDIT` and `GL_PERIOD_STATUS`.
2. Populate `GL_PERIOD_STATUS` with a record: `PERIOD_NAME = 'JAN-2024'`, `CLOSE_STATUS = 'OPEN'`.
3. To force a transaction failure, we will run a script where the `UPDATE` statement is intentionally broken (e.g., referencing a non-existent column or violating a type constraint), or we will simulate a query failure mid-transaction.

### Action
Execute the transactional SQL block in `d_gl_close_audit.sql` against BigQuery with parameters:
* `@period_name = 'JAN-2024'`
* `@fiscal_year = '2024'`

### Pass/Fail Criterion
* **Pass**: 
  * On a successful run: `GL_CLOSE_AUDIT` contains exactly 1 new row, and `GL_PERIOD_STATUS.CLOSE_STATUS` is updated to `'CLOSED'`.
  * On a forced failure run: No row is added to `GL_CLOSE_AUDIT`, and `GL_PERIOD_STATUS.CLOSE_STATUS` remains `'OPEN'` (complete rollback).
* **Fail**: A partial commit occurs (e.g., audit record is inserted, but status remains `'OPEN'`).

### Test Code (SQL Assertions)
```sql
-- =========================================================================
-- TEST SETUP
-- =========================================================================
CREATE OR REPLACE TABLE `ANALYTICS_SCHEMA.GL_CLOSE_AUDIT` (
    PERIOD_NAME STRING,
    FISCAL_YEAR STRING,
    CLOSED_BY STRING,
    CLOSED_AT TIMESTAMP
);

CREATE OR REPLACE TABLE `ANALYTICS_SCHEMA.GL_PERIOD_STATUS` (
    PERIOD_NAME STRING,
    CLOSE_STATUS STRING,
    CLOSED_AT TIMESTAMP
);

INSERT INTO `ANALYTICS_SCHEMA.GL_PERIOD_STATUS` (PERIOD_NAME, CLOSE_STATUS, CLOSED_AT)
VALUES ('JAN-2024', 'OPEN', NULL);

-- =========================================================================
-- ACTION: Run the transactional script (Simulating Success)
-- =========================================================================
DECLARE var_period_name STRING DEFAULT 'JAN-2024';
DECLARE var_fiscal_year STRING DEFAULT '2024';

BEGIN
  BEGIN TRANSACTION;

  INSERT INTO `ANALYTICS_SCHEMA.GL_CLOSE_AUDIT` (PERIOD_NAME, FISCAL_YEAR, CLOSED_BY, CLOSED_AT)
  VALUES (var_period_name, var_fiscal_year, SESSION_USER(), CURRENT_TIMESTAMP());

  UPDATE `ANALYTICS_SCHEMA.GL_PERIOD_STATUS`
  SET    CLOSE_STATUS = 'CLOSED',
         CLOSED_AT    = CURRENT_TIMESTAMP()
  WHERE  PERIOD_NAME  = var_period_name;

  COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
END;

-- ASSERT SUCCESS STATE
ASSERT (SELECT COUNT(1) FROM `ANALYTICS_SCHEMA.GL_CLOSE_AUDIT` WHERE PERIOD_NAME = 'JAN-2024') = 1 
  AS "Error: Audit record was not inserted!";
ASSERT (SELECT CLOSE_STATUS FROM `ANALYTICS_SCHEMA.GL_PERIOD_STATUS` WHERE PERIOD_NAME = 'JAN-2024') = 'CLOSED' 
  AS "Error: Period status was not updated to CLOSED!";

-- =========================================================================
-- ACTION: Run the transactional script (Simulating Failure / Rollback)
-- =========================================================================
-- Reset state
UPDATE `ANALYTICS_SCHEMA.GL_PERIOD_STATUS` SET CLOSE_STATUS = 'OPEN', CLOSED_AT = NULL WHERE PERIOD_NAME = 'JAN-2024';
DELETE FROM `ANALYTICS_SCHEMA.GL_CLOSE_AUDIT` WHERE TRUE;

BEGIN
  BEGIN TRANSACTION;

  INSERT INTO `ANALYTICS_SCHEMA.GL_CLOSE_AUDIT` (PERIOD_NAME, FISCAL_YEAR, CLOSED_BY, CLOSED_AT)
  VALUES (var_period_name, var_fiscal_year, SESSION_USER(), CURRENT_TIMESTAMP());

  -- Force failure by dividing by zero in the update statement
  UPDATE `ANALYTICS_SCHEMA.GL_PERIOD_STATUS`
  SET    CLOSE_STATUS = CAST(1/0 AS STRING),
         CLOSED_AT    = CURRENT_TIMESTAMP()
  WHERE  PERIOD_NAME  = var_period_name;

  COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
END;

-- ASSERT ROLLBACK STATE (Both tables must be clean/reset)
ASSERT (SELECT COUNT(1) FROM `ANALYTICS_SCHEMA.GL_CLOSE_AUDIT`) = 0 
  AS "Error: Transaction failed to roll back! Audit record exists.";
ASSERT (SELECT CLOSE_STATUS FROM `ANALYTICS_SCHEMA.GL_PERIOD_STATUS` WHERE PERIOD_NAME = 'JAN-2024') = 'OPEN' 
  AS "Error: Transaction failed to roll back! Status is not OPEN.";
```

---

## Test Case 4: User Identity and Schema Compatibility

### Purpose
Verify that the `CLOSED_BY` column in the target `GL_CLOSE_AUDIT` table is compatible with the longer string formats returned by BigQuery's `SESSION_USER()` (which returns email addresses like `sa-finance@prod-project.iam.gserviceaccount.com`), preventing string truncation errors that would have occurred if migrating directly to a legacy-width column.

### Setup
1. Target table `GL_CLOSE_AUDIT` deployed in BigQuery.
2. A test runner executing under a standard GCP Service Account identity.

### Action
1. Query the schema of `GL_CLOSE_AUDIT` to verify the data type of `CLOSED_BY`.
2. Execute the insert statement using `SESSION_USER()`.

### Pass/Fail Criterion
* **Pass**: The `CLOSED_BY` column is of type `STRING` (unbounded length in BigQuery), and the inserted value matches the executing service account's email address without error.
* **Fail**: The column is restricted to a short length (e.g., `STRING(30)` matching legacy Oracle `VARCHAR2(30)` limits) causing insertion failures when service account emails exceed the limit.

### Test Code (pytest / BigQuery SDK)
```python
from google.cloud import bigquery

def test_user_identity_schema_compatibility():
    client = bigquery.Client()
    
    # 1. Assert Schema Type
    table_ref = client.get_table("ANALYTICS_SCHEMA.GL_CLOSE_AUDIT")
    closed_by_field = next(field for field in table_ref.schema if field.name == "CLOSED_BY")
    
    assert closed_by_field.field_type == "STRING", "CLOSED_BY must be a STRING type"
    
    # 2. Assert successful insertion of long service account email
    test_query = """
        INSERT INTO `ANALYTICS_SCHEMA.GL_CLOSE_AUDIT` (PERIOD_NAME, FISCAL_YEAR, CLOSED_BY, CLOSED_AT)
        VALUES ('TEST-PER', '2024', SESSION_USER(), CURRENT_TIMESTAMP());
    """
    query_job = client.query(test_query)
    query_job.result()  # Should not raise any truncation or type errors
    
    # Clean up
    client.query("DELETE FROM `ANALYTICS_SCHEMA.GL_CLOSE_AUDIT` WHERE PERIOD_NAME = 'TEST-PER'").result()
```

---

## Test Case 5: Notification Dispatching Validation

### Purpose
Verify that the notification email is correctly formatted and dispatched via SMTP (or the system `mailx` fallback) upon successful completion of the aggregation and audit steps.

### Setup
1. Mock `smtplib.SMTP` to intercept outgoing emails.
2. Set environment variables: `PERIOD_NAME="DEC-2023"`, `FISCAL_YEAR="2023"`, `NOTIFY_EMAIL="finance-alerts@example.com"`.

### Action
Call the `send_notification` function in `finance/r_gl_aggregate_and_close.py`.

### Pass/Fail Criterion
* **Pass**: The SMTP mock captures an email sent to `"finance-alerts@example.com"` with the subject `"[FINANCE-OK] Month-End Close DEC-2023"` and a body containing the correct period, fiscal year, and completion timestamp.
* **Fail**: No email is sent, or the subject/body does not contain the required metadata.

### Test Code (pytest)
```python
import pytest
from unittest.mock import patch, MagicMock

@patch("smtplib.SMTP")
@patch("os.environ.get")
def test_notification_email_formatting(mock_env_get, mock_smtp):
    from finance.r_gl_aggregate_and_close import send_notification
    
    # Configure environment mocks
    env_vars = {
        "SMTP_HOST": "smtp.test.com",
        "SMTP_PORT": "25",
        "SMTP_FROM": "finance-etl@example.com"
    }
    mock_env_get.side_effect = lambda key, default=None: env_vars.get(key, default)
    
    # Setup SMTP mock instance
    mock_smtp_instance = MagicMock()
    mock_smtp.return_value.__enter__.return_value = mock_smtp_instance
    
    # Trigger notification
    send_notification(
        period_name="DEC-2023",
        fiscal_year="2023",
        notify_email="finance-alerts@example.com"
    )
    
    # Verify SMTP connection and message delivery
    mock_smtp.assert_called_once_with("smtp.test.com", 25)
    assert mock_smtp_instance.send_message.called
    
    # Inspect sent message
    sent_msg = mock_smtp_instance.send_message.call_args[0][0]
    assert sent_msg["To"] == "finance-alerts@example.com"
    assert sent_msg["Subject"] == "[FINANCE-OK] Month-End Close DEC-2023"
    
    body_content = sent_msg.get_content()
    assert "Month-end close complete for period: DEC-2023" in body_content
    assert "Fiscal year: 2023" in body_content
```