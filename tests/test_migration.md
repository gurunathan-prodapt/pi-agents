# Migration Validation Test Suite: `ausd_bp_ta_bpr_evn`

This document defines the migration-validation tests to prove behavioral equivalence between the legacy Oracle-based job and the migrated BigQuery/Airflow-based job `ausd_bp_ta_bpr_evn`.

---

## Test Case 1: End-to-End Output Parity (Oracle vs. BigQuery)

### Purpose
Verify that running the migrated BigQuery SQL on the target platform with a given input dataset produces the exact same output (row-for-row, column-for-column) as the legacy Oracle SQL*Plus script running on the same input dataset.

### Setup
1. **Legacy Environment (Oracle)**:
   * Populate the source table `sof$ta_bpr_instance` with a controlled set of test records (at least 1,000 rows), containing a mix of matching EVN IDs, non-matching IDs, and edge cases.
   * Ensure the target table `sof$ta_bpr_evn` is empty.
2. **Target Environment (BigQuery)**:
   * Load the exact same test dataset into the BigQuery source table `sof_ta_bpr_instance`.
   * Ensure the target table `sof_ta_bpr_evn` is empty.

### Action
1. Execute the legacy Oracle SQL script:
   ```sql
   TRUNCATE TABLE sof$ta_bpr_evn;
   INSERT INTO sof$ta_bpr_evn (cntrct_id, bpr_id)
   SELECT bp.cntrct_id, bp.bpr_id
   FROM sof$ta_bpr_instance bp
   WHERE bp.bpr_id IN (32, 2506, 2839, 2840, 3055, 3056, 3821);
   COMMIT;
   ```
2. Execute the migrated BigQuery SQL script (`dags/sql/d_ausd_bp_ta_bpr_evn.sql`) using mocked Airflow variables:
   * `gcp_project`: `test-project`
   * `gcp_dataset`: `test_dataset`
   * `stichtag`: `20241027`
   * `wiederanlaufwert`: `0`

### Pass/Fail Criterion
* **Pass**: The row count in Oracle `sof$ta_bpr_evn` matches BigQuery `sof_ta_bpr_evn` exactly. A full outer join or MD5 checksum comparison of the sorted datasets shows zero differences.
* **Fail**: Any mismatch in row count, column values, or data types.

```python
# pytest test_output_parity.py
import pytest
from google.cloud import bigquery
import cx_Oracle

def test_oracle_bq_parity():
    # 1. Fetch Oracle Results
    oracle_conn = cx_Oracle.connect("user/pwd@host:port/service")
    oracle_cursor = oracle_conn.cursor()
    oracle_cursor.execute("SELECT cntrct_id, bpr_id FROM sof$ta_bpr_evn ORDER BY cntrct_id, bpr_id")
    oracle_rows = oracle_cursor.fetchall()
    oracle_conn.close()

    # 2. Fetch BigQuery Results
    bq_client = bigquery.Client()
    query = """
        SELECT cntrct_id, bpr_id 
        FROM `test-project.test_dataset.sof_ta_bpr_evn` 
        ORDER BY cntrct_id, bpr_id
    """
    bq_rows = [tuple(row.values()) for row in bq_client.query(query).result()]

    # 3. Assert Equivalence
    assert len(oracle_rows) == len(bq_rows), f"Row count mismatch: Oracle={len(oracle_rows)}, BQ={len(bq_rows)}"
    assert oracle_rows == bq_rows, "Data content mismatch between Oracle and BigQuery target tables!"
```

---

## Test Case 2: Transformation Correctness (Filter & Type Handling)

### Purpose
Verify that the filtering logic correctly includes only the specified EVN `bpr_id` values and handles boundary values, NULLs, and invalid IDs correctly.

### Setup
In the BigQuery source table `sof_ta_bpr_instance`, insert the following test cases:

| Row ID | cntrct_id | bpr_id | Description | Expected Action |
| :--- | :--- | :--- | :--- | :--- |
| 1 | 10001 | 32 | Standard-EVN (Valid) | **Include** |
| 2 | 10002 | 2506 | Komfort-EVN (Valid) | **Include** |
| 3 | 10003 | 2839 | Standard-EVN Separat (Valid) | **Include** |
| 4 | 10004 | 2840 | Komfort-EVN Separat (Valid) | **Include** |
| 5 | 10005 | 3055 | Komfort-Plus-EVN (Valid) | **Include** |
| 6 | 10006 | 3056 | Komfort-Plus-EVN Separat (Valid) | **Include** |
| 7 | 10007 | 3821 | Standard-Plus-EVN (Valid) | **Include** |
| 8 | 10008 | 9999 | Non-EVN ID (Invalid) | **Exclude** |
| 9 | 10009 | NULL | NULL ID (Invalid) | **Exclude** |
| 10 | NULL | 32 | NULL Contract ID (Valid ID) | **Include** (Verify NULL handling) |

### Action
Run the BigQuery SQL script.

### Pass/Fail Criterion
* **Pass**: Target table `sof_ta_bpr_evn` contains exactly 8 rows (Rows 1-7, and Row 10). Rows 8 and 9 are excluded.
* **Fail**: Any valid row is missing, or any invalid row (8, 9) is present in the target table.

```sql
-- SQL Assertion Test
WITH expected_results AS (
  SELECT 10001 AS cntrct_id, 32 AS bpr_id UNION ALL
  SELECT 10002, 2506 UNION ALL
  SELECT 10003, 2839 UNION ALL
  SELECT 10004, 2840 UNION ALL
  SELECT 10005, 3055 UNION ALL
  SELECT 10006, 3056 UNION ALL
  SELECT 10007, 3821 UNION ALL
  SELECT NULL, 32
),
actual_results AS (
  SELECT cntrct_id, bpr_id 
  FROM `test-project.test_dataset.sof_ta_bpr_evn`
),
mismatches AS (
  (SELECT * FROM expected_results EXCEPT DISTINCT SELECT * FROM actual_results)
  UNION ALL
  (SELECT * FROM actual_results EXCEPT DISTINCT SELECT * FROM expected_results)
)
SELECT COUNT(*) AS mismatch_count FROM mismatches;
-- ASSERT mismatch_count == 0;
```

---

## Test Case 3: Idempotency & Truncation (State Management)

### Purpose
Verify that the job is fully idempotent. Running the job multiple times on the same day or re-running a failed execution must not result in duplicate records or data accumulation.

### Setup
1. Populate BigQuery source table `sof_ta_bpr_instance` with 5 valid EVN records.
2. Ensure target table `sof_ta_bpr_evn` is empty.

### Action
1. Execute the BigQuery SQL script once. Record the row count and contents of `sof_ta_bpr_evn`.
2. Execute the BigQuery SQL script a second time with the exact same source data.
3. Modify the source data (delete 2 records, add 1 new record) and execute the BigQuery SQL script a third time.

### Pass/Fail Criterion
* **Pass**: 
  * After Step 1: Target table has exactly 5 rows.
  * After Step 2: Target table still has exactly 5 rows (no duplicates).
  * After Step 3: Target table has exactly 4 rows matching the updated source state.
* **Fail**: Target table accumulates rows across runs, indicating the `TRUNCATE TABLE` step failed to execute or was bypassed.

```python
# pytest test_idempotency.py
from google.cloud import bigquery

def test_idempotency():
    client = bigquery.Client()
    target_table = "test-project.test_dataset.sof_ta_bpr_evn"
    
    # Run 1
    # (Trigger pipeline execution here via Airflow API or direct SQL execution)
    
    # Check count after Run 1
    query_job = client.query(f"SELECT COUNT(*) as cnt FROM `{target_table}`")
    count_1 = list(query_job.result())[0].cnt
    assert count_1 == 5, f"Expected 5 rows, got {count_1}"
    
    # Run 2 (Simulate rerun)
    # (Trigger pipeline execution again)
    
    query_job = client.query(f"SELECT COUNT(*) as cnt FROM `{target_table}`")
    count_2 = list(query_job.result())[0].cnt
    assert count_2 == 5, f"Idempotency failed! Row count increased to {count_2} on rerun."
```

---

## Test Case 4: Airflow DAG Integration & Jinja Parameter Rendering

### Purpose
Verify that the Airflow DAG parses correctly, resolves environment variables dynamically, and renders the SQL template with the correct parameters (`stichtag`, `wiederanlaufwert`, `gcp_project`, `gcp_dataset`) without syntax errors.

### Setup
1. Set up a local or test Airflow environment (e.g., Cloud Composer local runner or MWAA utility).
2. Define Airflow Variables:
   * `gcp_project`: `prod-data-project`
   * `gcp_dataset`: `isbert_schema_prod`
3. Define DAG Run Configuration:
   * `stichtag`: `20241027`
   * `wiederanlaufwert`: `5000`

### Action
1. Parse the DAG file `dags/dw_bert_ausd_bp_ta_bpr_evn.py` programmatically to check for syntax errors.
2. Render the task `process_evn_basis_products` SQL template.

### Pass/Fail Criterion
* **Pass**: 
  * The DAG parses without errors.
  * The rendered SQL correctly replaces the template variables:
    * `{{ var.json.get("gcp_project", ...) }}` $\rightarrow$ `prod-data-project`
    * `{{ var.json.get("gcp_dataset", ...) }}` $\rightarrow$ `isbert_schema_prod`
    * `{{ dag_run.conf.get("stichtag", ...) }}` $\rightarrow$ `20241027`
    * `{{ dag_run.conf.get("wiederanlaufwert", ...) }}` $\rightarrow$ `5000`
* **Fail**: Jinja rendering fails, or variables resolve to fallback placeholders instead of the configured environment values.

```python
# pytest test_dag_rendering.py
from airflow.models import DagBag, Variable
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
import pytest

def test_dag_loads_with_no_errors():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    dag = dag_bag.get_dag(dag_id="dw_bert_ausd_bp_ta_bpr_evn")
    assert dag_bag.import_errors == {}
    assert dag is not None

def test_sql_template_rendering(monkeypatch):
    # Mock Airflow Variables
    Variable.set("gcp_project", "prod-data-project")
    Variable.set("gcp_dataset", "isbert_schema_prod")
    
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    dag = dag_bag.get_dag(dag_id="dw_bert_ausd_bp_ta_bpr_evn")
    task = dag.get_task("process_evn_basis_products")
    
    # Create a dummy DagRun with configuration parameters
    dag_run = dag.create_dagrun(
        state=DagRunState.RUNNING,
        run_id="test_run",
        run_type=DagRunType.MANUAL,
        conf={"stichtag": "20241027", "wiederanlaufwert": "5000"}
    )
    
    ti = dag_run.get_task_instance(task.task_id)
    ti.task = task
    
    # Render templates
    rendered_sql = task.render_template(task.sql, ti.get_template_context())
    
    # Assertions
    assert "prod-data-project.isbert_schema_prod.sof_ta_bpr_evn" in rendered_sql
    assert "prod-data-project.isbert_schema_prod.sof_ta_bpr_instance" in rendered_sql
    assert "DECLARE p_stichtag STRING DEFAULT '20241027';" in rendered_sql
    assert "DECLARE p_wiederanlaufwert INT64 DEFAULT CAST('5000' AS INT64);" in rendered_sql
```

---

## Test Case 5: Schema and Data Quality Assertions

### Purpose
Verify that the target table `sof_ta_bpr_evn` conforms to the expected BigQuery schema (column names, data types, and nullability constraints) and that no unexpected data truncation or type coercion occurs.

### Setup
Ensure the target table `sof_ta_bpr_evn` has been created in the target BigQuery dataset.

### Action
Query the `INFORMATION_SCHEMA.COLUMNS` view for the target table.

### Pass/Fail Criterion
* **Pass**: 
  * The table contains exactly two columns: `cntrct_id` and `bpr_id`.
  * The data types match the source system specifications (e.g., `INT64` or `STRING` as mapped during schema migration).
* **Fail**: Missing columns, unexpected columns, or mismatched data types.

```sql
-- BigQuery Schema Validation Query
SELECT 
  column_name, 
  data_type, 
  is_nullable
FROM 
  `prod-data-project.isbert_schema_prod.INFORMATION_SCHEMA.COLUMNS`
WHERE 
  table_name = 'sof_ta_bpr_evn'
ORDER BY 
  ordinal_position;

-- Expected Output:
-- +-------------+-----------+-------------+
-- | column_name | data_type | is_nullable |
-- +-------------+-----------+-------------+
-- | cntrct_id   | INT64     | YES         |  -- (Or STRING, depending on source DDL)
-- | bpr_id      | INT64     | YES         |  -- (Or STRING, depending on source DDL)
-- +-------------+-----------+-------------+
```