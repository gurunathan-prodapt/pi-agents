# Migration Validation Test Suite: `FINANCE.GL_AGGREGATE_AND_CLOSE`

This document defines the migration-validation tests to prove behavioral equivalence between the legacy UC4/Oracle/KornShell implementation and the migrated Apache Airflow/Google Cloud Dataproc/BigQuery implementation of the `FINANCE.GL_AGGREGATE_AND_CLOSE` job.

---

## Test Case 1: End-to-End Happy Path (Behavioral Equivalence)

### Purpose
Verify that when the Spark aggregation job succeeds, the close-audit record is written to BigQuery, the period status is updated to `CLOSED`, and the completion email is successfully dispatched.

### Setup
1. **BigQuery Environment**:
   * Ensure target tables exist in the test dataset (e.g., `test_analytics_schema`):
     * `gl_close_audit` (schema: `PERIOD_NAME` STRING, `FISCAL_YEAR` STRING, `CLOSED_BY` STRING, `CLOSED_AT` TIMESTAMP)
     * `gl_period_status` (schema: `PERIOD_NAME` STRING, `CLOSE_STATUS` STRING, `CLOSED_AT` TIMESTAMP)
   * Seed `gl_period_status` with:
     ```sql
     INSERT INTO `test_analytics_schema.gl_period_status` (PERIOD_NAME, CLOSE_STATUS, CLOSED_AT)
     VALUES ('Jan_2026', 'OPEN', NULL);
     ```
2. **Mocking**:
   * Mock the Google Cloud Dataproc API to return a successful job execution status (return code `0`).
   * Mock the SMTP server (`localhost:25`) or intercept the `mailx` command to capture outgoing emails.
3. **Environment Variables**:
   * Set `PERIOD_NAME="Jan_2026"`
   * Set `FISCAL_YEAR="2026"`
   * Set `GCP_PROJECT="test-gcp-project"`
   * Set `GCS_BUCKET="test-bucket"`
   * Set `DATAPROC_CLUSTER="test-cluster"`
   * Set `DATAPROC_REGION="us-central1"`
   * Set `FIN_HOME` to the directory containing the migrated SQL script.

### Action
Execute the migrated Python script:
```bash
python3 finance/r_gl_aggregate_and_close.py
```

### Pass/Fail Criteria
#### Pass
* The script exits with code `0`.
* A single row is inserted into `test_analytics_schema.gl_close_audit` matching:
  * `PERIOD_NAME` = `'Jan_2026'`
  * `FISCAL_YEAR` = `'2026'`
  * `CLOSED_BY` = Active service account email (e.g., `test-sa@test-gcp-project.iam.gserviceaccount.com`)
  * `CLOSED_AT` = Current UTC timestamp (within $\pm 10$ seconds of execution).
* The row in `test_analytics_schema.gl_period_status` for `PERIOD_NAME = 'Jan_2026'` is updated to:
  * `CLOSE_STATUS` = `'CLOSED'`
  * `CLOSED_AT` = Current UTC timestamp (within $\pm 10$ seconds of execution).
* An email is sent to `finance-etl@example.com` with the subject `[FINANCE-OK] Month-End Close Jan_2026` containing the correct period, fiscal year, and completion timestamp.

#### Fail
* The script exits with a non-zero code.
* No audit record is written, or the period status remains `'OPEN'`.
* No email is dispatched, or the email contains incorrect metadata.

---

## Test Case 2: Spark Failure Handling (Transactional Safety)

### Purpose
Verify that if the Spark aggregation job fails, the pipeline halts immediately, **no** close-audit record is written, the period status remains unchanged, and no success notification is sent.

### Setup
1. **BigQuery Environment**:
   * Seed `gl_period_status` with:
     ```sql
     INSERT INTO `test_analytics_schema.gl_period_status` (PERIOD_NAME, CLOSE_STATUS, CLOSED_AT)
     VALUES ('Jan_2026', 'OPEN', NULL);
     ```
   * Ensure `gl_close_audit` is empty for `'Jan_2026'`.
2. **Mocking**:
   * Mock the Google Cloud Dataproc API to return a failed job execution status (return code `1`).
   * Mock the SMTP server to verify that no emails are sent.
3. **Environment Variables**:
   * Set the same environment variables as in Test Case 1.

### Action
Execute the migrated Python script:
```bash
python3 finance/r_gl_aggregate_and_close.py
```

### Pass/Fail Criteria
#### Pass
* The script exits with code `1`.
* No rows are inserted into `test_analytics_schema.gl_close_audit` for `'Jan_2026'`.
* The row in `test_analytics_schema.gl_period_status` for `'Jan_2026'` remains:
  * `CLOSE_STATUS` = `'OPEN'`
  * `CLOSED_AT` = `NULL`
* No success email is sent to `finance-etl@example.com`.

#### Fail
* The script exits with code `0` or `2`.
* An audit record is written or the period status is updated despite the Spark failure.
* A success email is sent.

---

## Test Case 3: BigQuery Transactional Atomicity (Rollback on Error)

### Purpose
Verify that the BigQuery SQL script (`d_gl_close_audit.sql`) executes as an atomic transaction. If the status update fails (e.g., due to a database constraint or schema mismatch), the audit log insertion must be rolled back.

### Setup
1. **BigQuery Environment**:
   * To simulate a failure during the second statement (the `UPDATE`), we will temporarily rename the `gl_period_status` table or alter its schema to make the `UPDATE` statement fail (e.g., make `CLOSE_STATUS` a non-nullable integer in a mock table, or drop the table entirely).
   * Ensure `gl_close_audit` is empty.
2. **Query Parameters**:
   * `@period_name` = `'Jan_2026'`
   * `@fiscal_year` = `'2026'`

### Action
Execute the BigQuery SQL script using the Python BigQuery client:
```python
import pytest
from google.cloud import bigquery

def test_transactional_rollback():
    client = bigquery.Client()
    
    # Read SQL script
    with open("finance/d_gl_close_audit.sql", "r") as f:
        sql_content = f.read()
    
    # Replace production schema with test schema for safety
    sql_content = sql_content.replace("analytics_schema.", "test_analytics_schema.")
    
    # Force failure by dropping the status table before execution
    client.query("DROP TABLE IF EXISTS test_analytics_schema.gl_period_status").result()
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('period_name', 'STRING', 'Jan_2026'),
            bigquery.ScalarQueryParameter('fiscal_year', 'STRING', '2026')
        ]
    )
    
    # Execution must fail due to missing gl_period_status table
    with pytest.raises(Exception) as excinfo:
        client.query(sql_content, job_config=job_config).result()
    
    assert "Not found" in str(excinfo.value) or "gl_period_status" in str(excinfo.value)
    
    # Verify rollback: gl_close_audit must NOT contain the inserted row
    query = "SELECT COUNT(*) as cnt FROM `test_analytics_schema.gl_close_audit` WHERE PERIOD_NAME = 'Jan_2026'"
    result = client.query(query).to_dataframe()
    assert result['cnt'].values[0] == 0
```

### Pass/Fail Criteria
#### Pass
* The SQL execution raises a runtime exception.
* The transaction rolls back completely; no row is committed to `gl_close_audit`.

#### Fail
* The SQL execution succeeds despite the missing table/constraint.
* The transaction commits partially (e.g., the audit record is written but the status is not updated).

---

## Test Case 4: Airflow Parameter Rendering & DAG Structure

### Purpose
Verify that the Airflow DAG correctly calculates the dynamic parameters `PERIOD_NAME` (previous month in `MMM_YYYY` format) and `FISCAL_YEAR` (current calendar year) using Jinja templates, and passes them to the execution environment.

### Setup
* Install `pytest` and `apache-airflow`.
* Load the DAG file `finance/gl_aggregate_and_close_dag.py` into the test context.

### Action
Run a programmatic unit test to render the DAG's task templates for a specific execution date:

```python
from datetime import datetime
from airflow.models import DagBag, TaskInstance
from airflow.utils.state import State

def test_dag_parameter_rendering():
    dagbag = DagBag(dag_folder="finance", include_examples=False)
    dag = dagbag.get_dag(dag_id="finance_gl_aggregate_and_close")
    assert dag is not None
    
    task = dag.get_task("gl_aggregate_and_close")
    
    # Simulate execution on March 15, 2026
    execution_date = datetime(2026, 3, 15)
    ti = TaskInstance(task=task, execution_date=execution_date)
    
    # Render templates
    context = ti.get_template_context()
    ti.render_templates(context=context)
    
    # Assert rendered environment variables
    rendered_env = ti.task.env
    assert rendered_env["PERIOD_NAME"] == "Feb_2026"  # Previous month of March 2026
    assert rendered_env["FISCAL_YEAR"] == "2026"
    assert rendered_env["NOTIFY_EMAIL"] == "finance-etl@example.com"
```

### Pass/Fail Criteria
#### Pass
* The DAG loads without import errors.
* For execution date `2026-03-15`:
  * `PERIOD_NAME` renders exactly to `'Feb_2026'`.
  * `FISCAL_YEAR` renders exactly to `'2026'`.

#### Fail
* The DAG fails to load.
* Rendered parameters do not match the expected values (e.g., rendering the current month instead of the previous month, or using an incorrect date format).

---

## Test Case 5: User Mapping and Timestamp Precision

### Purpose
Verify that the migrated BigQuery SQL script correctly maps Oracle's `USER` to BigQuery's `SESSION_USER()` and Oracle's `SYSTIMESTAMP` to BigQuery's `CURRENT_TIMESTAMP()`, preserving audit trail integrity.

### Setup
1. **BigQuery Environment**:
   * Recreate the test tables:
     ```sql
     CREATE OR REPLACE TABLE `test_analytics_schema.gl_close_audit` (
         PERIOD_NAME STRING,
         FISCAL_YEAR STRING,
         CLOSED_BY STRING,
         CLOSED_AT TIMESTAMP
     );
     CREATE OR REPLACE TABLE `test_analytics_schema.gl_period_status` (
         PERIOD_NAME STRING,
         CLOSE_STATUS STRING,
         CLOSED_AT TIMESTAMP
     );
     INSERT INTO `test_analytics_schema.gl_period_status` (PERIOD_NAME, CLOSE_STATUS, CLOSED_AT)
     VALUES ('Jan_2026', 'OPEN', NULL);
     ```

### Action
Execute the SQL script using the active test runner's credentials:
```python
from google.cloud import bigquery

def test_user_and_timestamp_mapping():
    client = bigquery.Client()
    
    with open("finance/d_gl_close_audit.sql", "r") as f:
        sql_content = f.read().replace("analytics_schema.", "test_analytics_schema.")
        
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('period_name', 'STRING', 'Jan_2026'),
            bigquery.ScalarQueryParameter('fiscal_year', 'STRING', '2026')
        ]
    )
    
    # Run query
    client.query(sql_content, job_config=job_config).result()
    
    # Retrieve inserted audit record
    query = "SELECT * FROM `test_analytics_schema.gl_close_audit` WHERE PERIOD_NAME = 'Jan_2026'"
    df = client.query(query).to_dataframe()
    
    assert len(df) == 1
    row = df.iloc[0]
    
    # Assert CLOSED_BY is a valid email address (SESSION_USER() format)
    assert "@" in row["CLOSED_BY"]
    
    # Assert CLOSED_AT is populated and is a valid UTC timestamp
    assert row["CLOSED_AT"] is not None
    assert (datetime.now(tz=row["CLOSED_AT"].tzinfo) - row["CLOSED_AT"]).total_seconds() < 30
```

### Pass/Fail Criteria
#### Pass
* The `CLOSED_BY` column contains the email address of the executing service account (proving `SESSION_USER()` works as expected).
* The `CLOSED_AT` column contains a valid UTC timestamp matching the execution time.
* The schema of `CLOSED_BY` accommodates the full length of the email address without truncation.

#### Fail
* `CLOSED_BY` is null, hardcoded, or contains a legacy Oracle username (e.g., `USER`).
* `CLOSED_AT` is null or does not match the execution time.
* The query fails due to string truncation or type mismatch.