# Migration Validation Test Suite: `ausd_bp_ta_bpr_optionen`

This document defines the comprehensive migration-validation test suite for the `ausd_bp_ta_bpr_optionen` data pipeline. These tests ensure behavioral equivalence, data integrity, schema compliance, and operational readiness of the migrated Google Cloud Platform (GCP) assets (BigQuery and Cloud Composer/Airflow) against the legacy Oracle/UC4 implementation.

---

## Test Case 1: End-to-End Output Parity (Reconciliation)

### Purpose
Verify that the migrated BigQuery pipeline produces the exact same output dataset as the legacy Oracle pipeline when provided with identical source data. This validates that no data is lost, corrupted, or transformed incorrectly during the migration.

### Setup
1. **Legacy Environment (Oracle)**:
   * Populate `sof$ta_bpr_instance` with a representative test dataset (e.g., 10,000 rows) containing standard cases, boundary values, and edge cases (e.g., very large IDs, negative IDs if permitted, and NULL values).
   * Clear and prepare the legacy target table `sof$ta_bpr_optionen`.
2. **Target Environment (BigQuery)**:
   * Populate `sof_ta_bpr_instance` with the exact same dataset.
   * Clear the target table `sof_ta_bpr_optionen`.

### Action
1. Execute the legacy Oracle SQL script `d_ausd_bp_ta_bpr_optionen.sql` in the legacy environment.
2. Execute the migrated BigQuery SQL script `d_ausd_bp_ta_bpr_optionen.sql` in the GCP environment.
3. Extract the results from both target tables and run a row-by-row comparison.

### Pass/Fail Criterion
* **Pass**: The row count of `sof_ta_bpr_optionen` matches `sof$ta_bpr_optionen` exactly. A full outer join on `cntrct_id` and `bpr_id` between the legacy and target tables yields zero mismatched rows.
* **Fail**: Any difference in row counts, or any row present in one target table but not the other.

### Test Code (Pytest)
```python
import os
import pytest
from google.cloud import bigquery
import cx_Oracle

def test_end_to_end_parity():
    # 1. Connect to Oracle (Legacy)
    oracle_conn = cx_Oracle.connect(
        user=os.environ["ORACLE_USER"],
        password=os.environ["ORACLE_PASSWORD"],
        dsn=os.environ["ORACLE_DSN"]
    )
    oracle_cursor = oracle_conn.cursor()
    
    # Fetch legacy results
    oracle_cursor.execute("SELECT cntrct_id, bpr_id FROM sof$ta_bpr_optionen ORDER BY cntrct_id, bpr_id")
    legacy_rows = oracle_cursor.fetchall()
    oracle_cursor.close()
    oracle_conn.close()

    # 2. Connect to BigQuery (Target)
    bq_client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])
    dataset = os.environ["BQ_DATASET"]
    
    bq_query = f"""
        SELECT cntrct_id, bpr_id 
        FROM `{bq_client.project}.{dataset}.sof_ta_bpr_optionen` 
        ORDER BY cntrct_id, bpr_id
    """
    bq_rows = [tuple(row.values()) for row in bq_client.query(bq_query).result()]

    # 3. Assert Equivalence
    assert len(legacy_rows) == len(bq_rows), f"Row count mismatch! Legacy: {len(legacy_rows)}, BQ: {len(bq_rows)}"
    
    # Detailed diff assertion
    diff = set(legacy_rows) ^ set(bq_rows)
    assert not diff, f"Data mismatch found in the following rows (Legacy vs BigQuery diff): {list(diff)[:10]}"
```

---

## Test Case 2: Idempotency and Truncation Validation

### Purpose
Verify that the target table `sof_ta_bpr_optionen` is completely truncated before insertion, ensuring that multiple executions of the pipeline do not result in duplicate records or orphaned data.

### Setup
1. Populate `sof_ta_bpr_instance` with 5 active records.
2. Pre-populate the target table `sof_ta_bpr_optionen` with 3 "stale" dummy records that do not exist in the source table.

### Action
1. Execute the BigQuery SQL script `d_ausd_bp_ta_bpr_optionen.sql`.
2. Query the target table `sof_ta_bpr_optionen`.

### Pass/Fail Criterion
* **Pass**: The 3 stale dummy records are completely removed. The target table contains exactly the 5 records matching the source table.
* **Fail**: Stale records remain in the target table, or the total row count exceeds 5.

### Test Code (SQL Assertion)
```sql
-- Step 1: Insert stale dummy data
INSERT INTO `{{ var.value.gcp_project_id }}.{{ var.value.bq_dataset }}.sof_ta_bpr_optionen` (cntrct_id, bpr_id)
VALUES ('DUMMY_CON_1', 'DUMMY_BPR_1'), ('DUMMY_CON_2', 'DUMMY_BPR_2');

-- Step 2: Run the migration script
-- (Execution of d_ausd_bp_ta_bpr_optionen.sql occurs here)

-- Step 3: Assert no dummy records remain
SELECT COUNT(1) AS stale_count 
FROM `{{ var.value.gcp_project_id }}.{{ var.value.bq_dataset }}.sof_ta_bpr_optionen`
WHERE cntrct_id LIKE 'DUMMY_CON%';

-- EXPECTED RESULT: stale_count = 0
```

---

## Test Case 3: Audit Variable (`v_datum`) Subquery Robustness

### Purpose
Verify that the `DECLARE/SET v_datum` logic executes successfully without runtime errors under various states of the `dwtk_meldungen` audit table, and correctly defaults to `'19000101'` when no matching metadata is found.

### Setup
Prepare three distinct test scenarios in the `dwtk_meldungen` table:
* **Scenario A**: Table is empty or has no rows where `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
* **Scenario B**: Table has multiple entries for `job_kennung = 'BERT_DROP_TEMP_TABLE'` with different `timecreated` timestamps.
* **Scenario C**: Table has an entry where `timecreated` is explicitly `NULL`.

### Action
Execute the variable declaration and assignment block in BigQuery for each scenario and capture the value of `v_datum`.

### Pass/Fail Criterion
* **Pass**: 
  * Scenario A sets `v_datum` to `'19000101'`.
  * Scenario B sets `v_datum` to the formatted string (`YYYYMMDD`) of the maximum `timecreated` timestamp.
  * Scenario C sets `v_datum` to `'19000101'`.
* **Fail**: Any runtime exception occurs, or the variable is assigned an incorrect value.

### Test Code (SQL Assertion)
```sql
-- Test Scenario A: No matching rows
DECLARE v_datum_a STRING;
SET v_datum_a = (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(m.timecreated))), '19000101')
  FROM (SELECT CAST(NULL AS TIMESTAMP) as timecreated, 'OTHER_JOB' as job_kennung) AS m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);
ASSERT v_datum_a = '19000101' AS 'Scenario A Failed: Expected 19000101';

-- Test Scenario B: Multiple rows (Verify MAX logic)
DECLARE v_datum_b STRING;
SET v_datum_b = (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(m.timecreated))), '19000101')
  FROM UNNEST([
    STRUCT(TIMESTAMP('2025-01-10 10:00:00') AS timecreated, 'BERT_DROP_TEMP_TABLE' AS job_kennung),
    STRUCT(TIMESTAMP('2025-01-15 14:30:00') AS timecreated, 'BERT_DROP_TEMP_TABLE' AS job_kennung),
    STRUCT(TIMESTAMP('2025-01-12 08:00:00') AS timecreated, 'BERT_DROP_TEMP_TABLE' AS job_kennung)
  ]) AS m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);
ASSERT v_datum_b = '20250115' AS 'Scenario B Failed: Expected 20250115';

-- Test Scenario C: NULL timestamp
DECLARE v_datum_c STRING;
SET v_datum_c = (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(m.timecreated))), '19000101')
  FROM (SELECT CAST(NULL AS TIMESTAMP) as timecreated, 'BERT_DROP_TEMP_TABLE' as job_kennung) AS m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);
ASSERT v_datum_c = '19000101' AS 'Scenario C Failed: Expected 19000101';
```

---

## Test Case 4: Schema and Data Type Integrity Assertions

### Purpose
Ensure that the target BigQuery table `sof_ta_bpr_optionen` matches the structural expectations of downstream consumers, including column names, data types, and nullability constraints.

### Setup
The target table `sof_ta_bpr_optionen` must be deployed in the target BigQuery dataset.

### Action
Query the BigQuery `INFORMATION_SCHEMA.COLUMNS` metadata view for the target table.

### Pass/Fail Criterion
* **Pass**: 
  * The table contains exactly two columns: `cntrct_id` and `bpr_id`.
  * The data types are compatible with the legacy Oracle schema (e.g., `STRING` or `INT64`/`NUMERIC` depending on the final schema definition).
  * No unexpected columns exist.
* **Fail**: Column names are missing, misspelled, or have incompatible data types.

### Test Code (Pytest)
```python
import os
import pytest
from google.cloud import bigquery

def test_schema_integrity():
    bq_client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])
    dataset = os.environ["BQ_DATASET"]
    table_name = "sof_ta_bpr_optionen"
    
    query = f"""
        SELECT column_name, data_type, is_nullable
        FROM `{bq_client.project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position
    """
    results = {row["column_name"]: row for row in bq_client.query(query).result()}
    
    # Assert expected columns exist
    assert "cntrct_id" in results, "Column 'cntrct_id' is missing from target table."
    assert "bpr_id" in results, "Column 'bpr_id' is missing from target table."
    assert len(results) == 2, f"Unexpected columns found in target table: {list(results.keys())}"
    
    # Assert data types (Assuming STRING/VARCHAR mapping based on design)
    assert results["cntrct_id"]["data_type"] in ["STRING", "INT64"], f"Unexpected type for cntrct_id: {results['cntrct_id']['data_type']}"
    assert results["bpr_id"]["data_type"] in ["STRING", "INT64"], f"Unexpected type for bpr_id: {results['bpr_id']['data_type']}"
```

---

## Test Case 5: Airflow DAG Orchestration and Variable Resolution

### Purpose
Validate that the Airflow DAG parses without syntax errors, correctly resolves dynamic environment variables (project ID, dataset, connection ID), and renders the SQL template with the correct paths.

### Setup
1. Place the DAG file `dw_bert_ausd_bp_ta_bpr_optionen.py` and the SQL file `d_ausd_bp_ta_bpr_optionen.sql` in the local Airflow testing environment.
2. Mock the Airflow Variables:
   * `gcp_project_id` = `test-gcp-project`
   * `bq_dataset` = `test_isbert_schema`

### Action
1. Run an Airflow DagBag import test to check for import errors.
2. Render the SQL template for the task `execute_bpr_optionen_update`.

### Pass/Fail Criterion
* **Pass**: 
  * The DAG is imported with zero errors.
  * The rendered SQL contains the resolved project and dataset variables (e.g., `test-gcp-project.test_isbert_schema.sof_ta_bpr_optionen`).
* **Fail**: Import errors are raised, or the SQL template contains unresolved Jinja placeholders (e.g., `{{ var.value... }}`).

### Test Code (Pytest)
```python
import pytest
from airflow.models import DagBag, Variable
from airflow.models.taskinstance import TaskInstance
from datetime import datetime

@pytest.fixture(autouse=True)
def setup_airflow_variables(monkeypatch):
    # Mock Airflow variables in the test environment
    Variable.set("gcp_project_id", "test-gcp-project")
    Variable.set("bq_dataset", "test_isbert_schema")
    Variable.set("bq_location", "EU")
    Variable.set("gcp_conn_id", "google_cloud_default")
    yield
    Variable.delete("gcp_project_id")
    Variable.delete("bq_dataset")
    Variable.delete("bq_location")
    Variable.delete("gcp_conn_id")

def test_dag_loads_with_no_errors():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    dag = dag_bag.get_dag(dag_id="dw_bert_ausd_bp_ta_bpr_optionen")
    
    assert dag_bag.import_errors == {}, f"DAG import errors: {dag_bag.import_errors}"
    assert dag is not None, "Failed to load DAG 'dw_bert_ausd_bp_ta_bpr_optionen'"

def test_sql_template_rendering():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    dag = dag_bag.get_dag(dag_id="dw_bert_ausd_bp_ta_bpr_optionen")
    task = dag.get_task("execute_bpr_optionen_update")
    
    # Create a dummy task instance to force template rendering
    ti = TaskInstance(task=task, execution_date=datetime(2025, 1, 1))
    rendered_sql = task.render_template(task.sql, ti.get_template_context())
    
    # Assert variables are resolved correctly in the SQL
    assert "test-gcp-project.test_isbert_schema.sof_ta_bpr_optionen" in rendered_sql
    assert "test-gcp-project.test_isbert_schema.sof_ta_bpr_instance" in rendered_sql
    assert "test-gcp-project.test_isbert_schema.dwtk_meldungen" in rendered_sql
    assert "{{" not in rendered_sql, "Unrendered Jinja placeholders remain in the SQL script."
```