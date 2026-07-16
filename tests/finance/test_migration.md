As a Senior Data-Migration QA Engineer, I have designed a comprehensive test suite to validate the migration of the legacy Automic (UC4) workflow `FINANCE_MONTH_END_WORKFLOW` to the Google Cloud Composer (Airflow) DAG `finance_month_end_workflow`. 

These tests ensure **functional equivalence**, **data integrity**, **correct scheduling logic**, and **robust error handling** across the legacy and target environments.

---

## Section 1: Calendar & Scheduling Validation (Short-Circuit Guard)

### Test Case 1.1: Last Business Day Evaluation (Positive Case)
* **Purpose**: Verify that the `is_last_business_day` task evaluates to `True` and allows the workflow to proceed when executed on the actual last business day of the month.
* **Setup**: 
  * Set up a mock Airflow task context where `execution_date` is set to `2023-11-30` (Thursday, November 30, 2023—the last business day of November).
* **Action**: Execute the `check_last_business_day` python callable with the mocked context.
* **Pass/Fail Criterion**: The callable must return `True` without raising any exceptions.

### Test Case 1.2: Non-Last Business Day Evaluation (Negative Case)
* **Purpose**: Verify that the `is_last_business_day` task short-circuits (skips downstream tasks) when executed on a day that is not the last business day of the month.
* **Setup**: 
  * Set up a mock Airflow task context where `execution_date` is set to `2023-11-29` (Wednesday, November 29, 2023—not the last business day).
* **Action**: Execute the `check_last_business_day` python callable with the mocked context.
* **Pass/Fail Criterion**: The callable must raise an `AirflowSkipException` with the message `"Skipping: Today is not the last business day of the month."`

### Runnable Test Code (Pytest)
```python
import pytest
from datetime import datetime
from airflow.exceptions import AirflowSkipException

def test_check_last_business_day_positive():
    from finance_month_end_workflow import check_last_business_day
    # Nov 30, 2023 is a Thursday and the last business day of the month
    context = {"execution_date": datetime(2023, 11, 30)}
    assert check_last_business_day(**context) is True

def test_check_last_business_day_negative():
    from finance_month_end_workflow import check_last_business_day
    # Nov 29, 2023 is a Wednesday, but NOT the last business day (Nov 30 is)
    context = {"execution_date": datetime(2023, 11, 29)}
    with pytest.raises(AirflowSkipException) as exc_info:
        check_last_business_day(**context)
    assert "Skipping: Today is not the last business day" in str(exc_info.value)
```

---

## Section 2: Dynamic Parameter & Macro Translation Validation

### Test Case 2.1: Date Macro and String Formatting Parity
* **Purpose**: Ensure that the target Airflow Jinja templates resolve to the exact same values as the legacy UC4 variables (`&PERIOD_DATE`, `&PERIOD_NAME`, `&FISCAL_YEAR`) for any given execution date.
* **Setup**: 
  * Define a test matrix of execution dates (`data_interval_end`).
  * Render the Airflow task templates using the Airflow Jinja engine.
* **Action**: Compare the rendered strings against expected legacy outputs.
* **Pass/Fail Criterion**: Rendered parameters must match the legacy format specifications exactly:
  * `PERIOD_DATE` -> `YYYY-MM-DD` (Last day of the previous calendar month).
  * `PERIOD_NAME` -> `MON-YYYY` in uppercase (Previous calendar month).
  * `FISCAL_YEAR` -> `YYYY` (Year of the current execution interval).

### Runnable Test Code (Pytest / Airflow Template Validation)
```python
import pytest
from datetime import datetime
from jinja2 import Environment, DebugUndefined

@pytest.mark.parametrize(
    "execution_date, expected_period_date, expected_period_name, expected_fiscal_year",
    [
        # Execution in late Jan 2023 -> Targets Dec 2022
        (datetime(2023, 1, 31), "2022-12-31", "DEC-2022", "2023"),
        # Execution in late Mar 2023 -> Targets Feb 2023
        (datetime(2023, 3, 31), "2023-02-28", "FEB-2023", "2023"),
        # Leap year execution in late Mar 2024 -> Targets Feb 2024
        (datetime(2024, 3, 29), "2024-02-29", "FEB-2024", "2024"),
    ]
)
def test_macro_rendering_parity(execution_date, expected_period_date, expected_period_name, expected_fiscal_year):
    # Mocking Airflow context variables
    context = {
        "data_interval_end": execution_date,
        "macros": pytest.importorskip("airflow.templates.macros") # Accesses standard Airflow macros
    }
    
    env = Environment(undefined=DebugUndefined)
    
    # Templates from the DAG code
    period_date_template = "{{ (data_interval_end.replace(day=1) - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"
    period_name_template = "{{ (data_interval_end.replace(day=1) - macros.timedelta(days=1)).strftime('%b-%Y').upper() }}"
    fiscal_year_template = "{{ data_interval_end.year }}"
    
    rendered_date = env.from_string(period_date_template).render(context)
    rendered_name = env.from_string(period_name_template).render(context)
    rendered_year = env.from_string(fiscal_year_template).render(context)
    
    assert rendered_date == expected_period_date
    assert rendered_name == expected_period_name
    assert rendered_year == expected_fiscal_year
```

---

## Section 3: Pre-Flight Database Verification Validation

### Test Case 3.1: Pre-Flight SQL Execution and Connection Mapping
* **Purpose**: Verify that the `pre_flight` task correctly connects to the Oracle database using the migrated connection ID and executes the exact equivalent validation query.
* **Setup**:
  * Provision a mock Oracle database containing the `SOURCE_FIN.GL_JNL_LINES` table.
  * Populate the table with test records for period `OCT-2023`.
* **Action**: 
  * Execute the SQL statement rendered for `data_interval_end = datetime(2023, 11, 30)` (which targets `OCT-2023`).
* **Pass/Fail Criterion**: 
  * The connection must resolve to `oracle_finance_conn`.
  * The query must return the count of posted journal lines for the target period. If the count is 0, downstream tasks should not proceed (or fail if that is the business rule).

### Runnable Test Code (SQL Assertion)
```sql
-- Target Oracle Verification Script (to be run on the target Oracle instance)
-- Purpose: Verify that the query executed by the OracleOperator returns valid data.

SELECT COUNT(*) AS POSTED_COUNT 
FROM SOURCE_FIN.GL_JNL_LINES
WHERE PERIOD_NAME = 'OCT-2023' 
  AND STATUS = 'POSTED';

-- QA Assertion: Ensure that the schema structure matches expectations
DECLARE
    v_col_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_col_exists 
    FROM ALL_TAB_COLUMNS 
    WHERE OWNER = 'SOURCE_FIN' 
      AND TABLE_NAME = 'GL_JNL_LINES' 
      AND COLUMN_NAME IN ('PERIOD_NAME', 'STATUS');
      
    IF v_col_exists != 2 THEN
        raise_application_error(-20001, 'Schema validation failed: Required columns missing in GL_JNL_LINES.');
    END IF;
END;
/
```

---

## Section 4: Transformation Correctness & Data Parity

### Test Case 4.1: PySpark GL Aggregation Parity (Legacy Scala vs. Target PySpark)
* **Purpose**: Ensure that the migrated PySpark aggregation logic (`finance_etl_assembly.py`) produces identical analytical outputs to the legacy Scala Spark job (`finance-etl-assembly.jar`).
* **Setup**:
  * Prepare a static input dataset representing staging GL extracts (`STG_GL_EXTRACT_UK`, `STG_GL_EXTRACT_DE`, `STG_GL_EXTRACT_FR`) and the loaded account master dimension.
  * Run the legacy Scala Spark job on this input to generate a baseline output table `FACT_GL_AGGREGATION_LEGACY`.
* **Action**:
  * Run the migrated PySpark job on the same input to generate `FACT_GL_AGGREGATION_TARGET`.
  * Execute a full outer join comparison query between the two output tables.
* **Pass/Fail Criterion**: 
  * Row counts must match exactly.
  * Numeric columns (e.g., debit/credit balances, aggregated amounts) must match to 4 decimal places.
  * Null values must be handled identically across both outputs.

### Runnable Test Code (PySpark Parity Assertion)
```python
# PySpark QA Validation Script
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, abs as spark_abs

spark = SparkSession.builder.appName("GL_Aggregation_Parity_Validation").getOrCreate()

# Load outputs from both legacy and target runs
legacy_df = spark.read.table("finance_dw.fact_gl_aggregation_legacy")
target_df = spark.read.table("finance_dw.fact_gl_aggregation_target")

# 1. Row Count Assertion
legacy_count = legacy_df.count()
target_count = target_df.count()
assert legacy_count == target_count, f"Row count mismatch! Legacy: {legacy_count}, Target: {target_count}"

# 2. Schema Parity Assertion
assert legacy_df.schema == target_df.schema, "Schema structure mismatch between Legacy and Target tables!"

# 3. Data Value Parity Assertion (Full Outer Join on Primary Keys)
# Assuming PK is (period_name, account_id, cost_centre_id, entity_code)
join_cols = ["period_name", "account_id", "cost_centre_id", "entity_code"]
joined_df = legacy_df.alias("l").join(target_df.alias("t"), on=join_cols, how="full_outer")

# Calculate absolute differences on key financial metrics
mismatches = joined_df.filter(
    (spark_abs(col("l.total_debit") - col("t.total_debit")) > 0.0001) |
    (spark_abs(col("l.total_credit") - col("t.total_credit")) > 0.0001) |
    (col("l.total_debit").isNotNull() & col("t.total_debit").isNull()) |
    (col("l.total_debit").isNull() & col("t.total_debit").isNotNull())
)

mismatch_count = mismatches.count()
if mismatch_count > 0:
    print("Mismatched Records Sample:")
    mismatches.show(10)
    raise ValueError(f"Data parity validation failed! Found {mismatch_count} mismatched records.")
else:
    print("Data parity validation passed successfully!")
```

---

## Section 5: Error Handling, Retries, and Non-Blocking Branches

### Test Case 5.1: Non-Blocking Branch Behavior (Reconcile Failure)
* **Purpose**: Verify that if the `abinitio_reconcile` task fails, the workflow triggers a warning notification but does **not** block the execution of the downstream `daily_gl_close_audit` task. This mirrors the legacy UC4 `SUCCESS_OR_WARNING` condition.
* **Setup**:
  * Configure the Airflow environment to run the DAG.
  * Force a failure in the `abinitio_reconcile` task (e.g., by passing an invalid parameter or mocking its execution to return an error status).
  * Ensure the parallel task `spark_gl_aggregation` completes successfully.
* **Action**: Trigger the DAG run.
* **Pass/Fail Criterion**:
  * `abinitio_reconcile` must transition to a `FAILED` state.
  * `on_failure_alarm_warning` must be executed (verified via logs).
  * `daily_gl_close_audit` must execute successfully (due to its `all_done` trigger rule).
  * The DAG run must complete successfully overall.

### Test Case 5.2: Retry Policy and Terminal Failure Callback
* **Purpose**: Verify that regional extract tasks (`stg_gl_extract_uk`, etc.) retry exactly 3 times with a 60-second delay before executing the terminal failure callback.
* **Setup**:
  * Mock the Dataproc cluster to reject the PySpark job submission for `stg_gl_extract_uk`.
* **Action**: Execute the `stg_gl_extract_uk` task.
* **Pass/Fail Criterion**:
  * The task must attempt execution exactly 4 times (1 initial run + 3 retries).
  * The delay between attempts must be 60 seconds.
  * Upon exhausting all retries, `on_terminal_failure` (or `on_failure_alarm`) must execute and log the critical failure.

---

## Section 6: Downstream Lineage & Event Triggering

### Test Case 6.1: Cross-Domain Workflow Triggering
* **Purpose**: Verify that the completion of `daily_gl_close_audit` successfully triggers the downstream CRM and Retail workflows in a fire-and-forget manner.
* **Setup**:
  * Deploy mock DAGs for `crm_weekly_workflow` and `retail_daily_workflow` on Cloud Composer.
* **Action**: Execute the `daily_gl_close_audit` task and allow the DAG to proceed to the trigger tasks.
* **Pass/Fail Criterion**:
  * `trigger_crm_weekly_workflow` and `trigger_retail_daily_workflow` must execute successfully.
  * Active runs for both `crm_weekly_workflow` and `retail_daily_workflow` must be initiated in the Airflow metadata database.
  * The parent DAG must not wait for the downstream DAGs to complete (`wait_for_completion=False`).

### Runnable Test Code (Airflow Metadata Assertion)
```python
# Integration Test to verify downstream DAG triggers in Airflow Metadata DB
from airflow.models import DagRun
from airflow.utils.state import DagRunState
from airflow.utils.session import provide_session

@provide_session
def test_downstream_triggers(session=None):
    # Query the metadata database for DAG runs triggered today
    today = datetime.utcnow().date()
    
    crm_runs = session.query(DagRun).filter(
        DagRun.dag_id == "crm_weekly_workflow",
        DagRun.execution_date >= datetime.combine(today, datetime.min.time())
    ).all()
    
    retail_runs = session.query(DagRun).filter(
        DagRun.dag_id == "retail_daily_workflow",
        DagRun.execution_date >= datetime.combine(today, datetime.min.time())
    ).all()
    
    assert len(crm_runs) >= 1, "CRM Weekly Workflow was not triggered!"
    assert len(retail_runs) >= 1, "Retail Daily Workflow was not triggered!"
    
    print("Downstream workflow trigger validation passed!")
```