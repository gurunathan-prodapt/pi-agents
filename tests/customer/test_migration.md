# Migration Validation Test Suite: `customer/crm_weekly_workflow.xml`

This document provides a comprehensive suite of migration-validation tests to verify that the migrated Apache Airflow DAG (`crm_weekly_workflow.py`) and its associated PySpark/Python scripts behave identically to the legacy Automic (UC4) workflow. 

---

## Section 1: DAG Structure & Orchestration Parity Tests

### Test Case 1.1: DAG Structural Integrity & Dependency Validation
#### Purpose
Verify that the migrated Airflow DAG matches the legacy UC4 workflow structure, task dependencies, trigger rules, timeouts, and retry configurations.

#### Setup
*   A Python environment with `apache-airflow` (>= 2.5.0) and `pendulum` installed.
*   The migrated DAG file `crm_weekly_workflow.py` placed in the Airflow `dags/` directory.

#### Action
Run a programmatic unit test using `pytest` to parse the DAG and assert its structural properties against the legacy XML specification.

```python
# test_dag_structure.py
import pytest
from airflow.models import DagBag, Variable
from airflow.utils.trigger_rule import TriggerRule

@pytest.fixture(scope="module", autouse=True)
def setup_airflow_variables(monkeypatch):
    """Mock Airflow variables required for DAG parsing."""
    mock_vars = {
        "gcp_project_id": "test-gcp-project",
        "dataproc_cluster_name": "test-cluster",
        "dataproc_region": "europe-west1",
        "gcs_bucket_name": "test-bucket",
        "env": "PROD",
        "crm_notify_email": "crm-etl@company.com"
    }
    for key, val in mock_vars.items():
        Variable.set(key, val)

def test_dag_loads_with_no_errors():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"
    
    dag = dag_bag.get_dag(dag_id="crm_weekly_workflow")
    assert dag is not None
    assert dag.schedule_interval == "0 4 * * 0"
    assert dag.timezone.name == "Europe/London"

def test_dag_task_dependencies_and_properties():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    dag = dag_bag.get_dag(dag_id="crm_weekly_workflow")
    
    # Define expected upstream/downstream relationships
    expected_dependencies = {
        "wait_finance_event": {"downstream": ["wait_retail_event"], "upstream": []},
        "wait_retail_event": {
            "downstream": ["customer_extract_vip", "customer_extract_retail", "customer_extract_wholesale"],
            "upstream": ["wait_finance_event"]
        },
        "customer_extract_vip": {"downstream": ["abinitio_transform"], "upstream": ["wait_retail_event"]},
        "customer_extract_retail": {"downstream": ["abinitio_transform"], "upstream": ["wait_retail_event"]},
        "customer_extract_wholesale": {"downstream": ["abinitio_transform"], "upstream": ["wait_retail_event"]},
        "abinitio_transform": {"downstream": ["spark_segmentation", "python_lineage"], "upstream": ["customer_extract_vip", "customer_extract_retail", "customer_extract_wholesale"]},
        "spark_segmentation": {"downstream": ["completion_notify"], "upstream": ["abinitio_transform"]},
        "python_lineage": {"downstream": ["completion_notify"], "upstream": ["abinitio_transform"]},
        "completion_notify": {"downstream": [], "upstream": ["spark_segmentation", "python_lineage"]}
    }
    
    for task_id, deps in expected_dependencies.items():
        task = dag.get_task(task_id)
        assert sorted([t.task_id for t in task.downstream_list]) == sorted(deps["downstream"])
        assert sorted([t.task_id for t in task.upstream_list]) == sorted(deps["upstream"])

def test_task_retry_and_trigger_rules():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    dag = dag_bag.get_dag(dag_id="crm_weekly_workflow")
    
    # Extract tasks must have 3 retries and 120s delay
    extract_tasks = ["customer_extract_vip", "customer_extract_retail", "customer_extract_wholesale"]
    for task_id in extract_tasks:
        task = dag.get_task(task_id)
        assert task.retries == 3
        assert task.retry_delay.total_seconds() == 120
        assert task.trigger_rule == TriggerRule.ALL_DONE  # Soft-fail pass-through for wait_retail_event
        
    # Completion notify must run even if python_lineage fails
    completion_notify = dag.get_task("completion_notify")
    assert completion_notify.trigger_rule == TriggerRule.ALL_DONE
```

#### Pass/Fail Criterion
*   **Pass**: The test suite runs and all assertions pass (no import errors, exact dependency matches, correct retry parameters, and correct trigger rules).
*   **Fail**: Any assertion fails, indicating a structural deviation from the legacy UC4 specification.

---

### Test Case 1.2: Soft-Failure Pass-Through & Timeout Validation
#### Purpose
Verify that:
1. If `wait_retail_event` times out (120 minutes), the downstream extraction tasks still execute (simulating UC4 `ON_FAILURE then CONTINUE`).
2. If `python_lineage` fails, `completion_notify` still executes.
3. If `wait_finance_event` times out (240 minutes), the workflow halts (simulating UC4 `NOTIFY_AND_ABORT`).

#### Setup
*   An isolated Airflow execution environment (e.g., local `astro-cli` or Composer dev environment).
*   Mock GCS bucket configured.

#### Action
1.  **Scenario A (Retail Timeout)**: Do **not** write the retail event file to GCS. Trigger the DAG. Let `wait_retail_event` time out.
2.  **Scenario B (Lineage Failure)**: Force `python_lineage` to fail (e.g., by pointing its script URI to a non-existent file or raising an error). Trigger the DAG.
3.  **Scenario C (Finance Timeout)**: Do **not** write the finance event file to GCS. Trigger the DAG.

#### Pass/Fail Criterion
*   **Pass**:
    *   In **Scenario A**, `wait_retail_event` transitions to `FAILED` (or `SKIPPED` if soft-fail is configured), but `customer_extract_*` tasks transition to `RUNNING` and complete successfully.
    *   In **Scenario B**, `python_lineage` transitions to `FAILED`, but `completion_notify` transitions to `RUNNING` and successfully sends the email.
    *   In **Scenario C**, `wait_finance_event` times out and fails, and **no** downstream tasks are executed.
*   **Fail**: Any task executes out of order, or downstream tasks are blocked by soft-failure steps, or the workflow continues past a failed finance event.

---

## Section 2: Data Transformation & Parity Tests

Since the source scripts (`process_customer_data.ksh`, `crm_customer_scoring.mp`, `crm-assembly.jar`, and `crm_lineage_tracker.py`) were not resolved in the migration package, we define the exact functional specifications and verification tests for their PySpark replacements.

### Test Case 2.1: Customer Segment Extraction Parity (`process_customer_data.py`)
#### Purpose
Verify that the migrated PySpark script `process_customer_data.py` extracts the same customer records as the legacy Unix script `process_customer_data.ksh` for each segment (`VIP`, `RETAIL`, `WHOLESALE`).

#### Setup
*   **Input Table**: `DW_OWNER.STG_CUSTOMER_SALES` populated with a representative test dataset containing mixed segments, NULL values, and edge cases (e.g., negative sales, special characters in names).
*   **Legacy Output**: Run the legacy `.ksh` script on the legacy database and export the output to a CSV file (`legacy_extract_output.csv`).
*   **Target Output**: Run the migrated PySpark script on Dataproc using the same input dataset.

#### Action
Execute a PySpark comparison job to check for row-by-row and schema parity.

```python
# test_extraction_parity.py
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def test_extraction_parity():
    spark = SparkSession.builder.appName("TestExtractionParity").getOrCreate()
    
    # Load legacy output
    legacy_df = spark.read.csv("gs://test-bucket/gold_standard/legacy_extract_output.csv", header=True, inferSchema=True)
    
    # Load migrated output (generated by process_customer_data.py)
    migrated_df = spark.read.parquet("gs://test-bucket/migrated_output/customer_extract_vip/")
    
    # 1. Row Count Assertion
    assert legacy_df.count() == migrated_df.count(), f"Row count mismatch! Legacy: {legacy_df.count()}, Migrated: {migrated_df.count()}"
    
    # 2. Schema Parity Assertion
    assert legacy_df.schema.names == migrated_df.schema.names, "Schema column names mismatch!"
    
    # 3. Value Parity Assertion (Check MD5 hash of sorted rows to guarantee exact match)
    legacy_hash = legacy_df.orderBy("customer_id").select(F.md5(F.concat_ws("||", *legacy_df.columns)).alias("hash")).agg(F.collect_list("hash")).collect()[0][0]
    migrated_hash = migrated_df.orderBy("customer_id").select(F.md5(F.concat_ws("||", *migrated_df.columns)).alias("hash")).agg(F.collect_list("hash")).collect()[0][0]
    
    assert legacy_hash == migrated_hash, "Data value mismatch between legacy and migrated outputs!"
```

#### Pass/Fail Criterion
*   **Pass**: Row counts, schemas, and MD5 data hashes match exactly (100% parity).
*   **Fail**: Any mismatch in row count, schema, or data values.

---

### Test Case 2.2: Customer Scoring Transformation Parity (`crm_customer_scoring.py`)
#### Purpose
Verify that the PySpark translation of the Ab Initio graph `crm_customer_scoring.mp` correctly joins customer extracts with financial reconciliation data and aggregates scores without data loss or precision errors.

#### Setup
*   **Upstream Inputs**:
    *   `customer_extract_vip` output
    *   `customer_extract_retail` output
    *   `customer_extract_wholesale` output
    *   `FINANCE_SCHEMA.FACT_PERIOD_RECONCILIATION`
*   **Legacy Output**: The output table/file produced by the Ab Initio graph in the legacy environment.

#### Action
Run a PySpark test script to validate join logic, aggregation correctness, and NULL handling.

```python
# test_scoring_logic.py
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def test_scoring_null_handling_and_aggregations():
    spark = SparkSession.builder.appName("TestScoringLogic").getOrCreate()
    
    # Load the output of the migrated PySpark scoring script
    scored_df = spark.read.parquet("gs://test-bucket/migrated_output/crm_customer_scoring/")
    
    # Assertion 1: Ensure no active customer has a NULL score (Ab Initio default replacement check)
    null_scores_count = scored_df.filter(F.col("customer_score").isNull()).count()
    assert null_scores_count == 0, f"Found {null_scores_count} rows with NULL customer_score. Default values were not applied!"
    
    # Assertion 2: Verify aggregation logic (e.g., total_sales must equal sum of segment sales)
    # This ensures the PySpark join didn't duplicate rows (Cartesian product check)
    mismatched_totals = scored_df.filter(
        F.round(F.col("calculated_total_sales"), 2) != F.round(F.col("segment_sales_sum"), 2)
    ).count()
    assert mismatched_totals == 0, f"Found {mismatched_totals} rows with mismatched sales aggregations (join duplication issue)."
```

#### Pass/Fail Criterion
*   **Pass**: Zero NULL values in critical scoring columns, and all mathematical aggregations match legacy calculations exactly.
*   **Fail**: Presence of unexpected NULLs, or row duplication caused by incorrect join keys.

---

## Section 3: External System & Integration Tests

### Test Case 3.1: GCS Event Sensor Integration
#### Purpose
Verify that the `GCSObjectExistenceSensor` tasks correctly detect upstream pipeline completion markers and trigger downstream tasks.

#### Setup
*   Airflow DAG is active and paused.
*   The GCS bucket is empty of event markers.

#### Action
1.  Unpause and trigger the DAG.
2.  Verify that `wait_finance_event` enters a `sensing` state (reschedule mode).
3.  Write an empty file to `gs://{GCS_BUCKET}/events/FINANCE_GL_CLOSE_COMPLETE_2023-10-29` (assuming logical date is `2023-10-29`).
4.  Observe the sensor behavior.
5.  Write an empty file to `gs://{GCS_BUCKET}/events/RETAIL_DAILY_COMPLETE_2023-10-29`.
6.  Observe the sensor behavior.

#### Pass/Fail Criterion
*   **Pass**:
    *   `wait_finance_event` successfully detects the file within 5 minutes of creation and transitions to `SUCCESS`.
    *   `wait_retail_event` immediately begins sensing, detects its file, and transitions to `SUCCESS`.
*   **Fail**: Sensors fail to detect the files, time out, or proceed before the files are created.

---

### Test Case 3.2: Email Notification Integration
#### Purpose
Verify that the `completion_notify` task successfully sends an email matching the legacy `mailx` format and recipients.

#### Setup
*   Airflow SMTP configurations (or SendGrid integration) are active in the Cloud Composer environment.
*   The developer's test email is added to the `crm_notify_email` Airflow variable.

#### Action
1.  Trigger the `completion_notify` task directly from the Airflow UI.
2.  Check the inbox of the configured test email.

#### Pass/Fail Criterion
*   **Pass**: An email is received with:
    *   **Subject**: `[CRM-OK] Weekly CRM Load 2023-10-29` (matching the logical date).
    *   **Body**: `CRM_WEEKLY_WORKFLOW completed for RUN_DATE=2023-10-29`.
    *   **Recipients**: Sent to `crm-etl@company.com` and `dw-alerts@company.com` (or the test override).
*   **Fail**: No email is received, or the subject/body does not match the legacy literal format.

---

## Section 4: Data Quality & Schema Assertions

### Test Case 4.1: Target BigQuery/Hive Schema & Constraint Validation
#### Purpose
Verify that the final tables populated by `customer_segmentation.py` conform to the required enterprise schema, nullability constraints, and data types.

#### Setup
*   The final segmentation run has completed.
*   Access to the target database (BigQuery or Hive/Dataproc Metastore).

#### Action
Execute the following SQL validation script against the target database:

```sql
-- validation_assertions.sql
-- 1. Assert that critical columns do not contain NULLs (Entity Integrity)
SELECT 
  'NULL_CUSTOMER_ID_CHECK' AS test_name,
  COUNT(*) AS failure_count
FROM `target_project.crm_analytics.customer_segments`
WHERE customer_id IS NULL

UNION ALL

-- 2. Assert that the primary key is unique (Uniqueness Constraint)
SELECT 
  'DUPLICATE_CUSTOMER_CHECK' AS test_name,
  COUNT(*) - COUNT(DISTINCT customer_id) AS failure_count
FROM `target_project.crm_analytics.customer_segments`

UNION ALL

-- 3. Assert that segment codes are valid (Domain Constraint)
SELECT 
  'INVALID_SEGMENT_CODE_CHECK' AS test_name,
  COUNT(*) AS failure_count
FROM `target_project.crm_analytics.customer_segments`
WHERE segment_code NOT IN ('VIP', 'RETAIL', 'WHOLESALE')

UNION ALL

-- 4. Assert that run_date matches the execution date partition
SELECT 
  'PARTITION_DATE_CHECK' AS test_name,
  COUNT(*) AS failure_count
FROM `target_project.crm_analytics.customer_segments`
WHERE run_date != '2023-10-29';
```

#### Pass/Fail Criterion
*   **Pass**: The query returns `0` for all `failure_count` fields.
*   **Fail**: Any test returns a `failure_count` > 0, indicating data corruption, duplicate processing, or schema constraint violations.