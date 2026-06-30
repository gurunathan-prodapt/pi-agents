# Migration Validation Test Suite: `ausd_bp_ta_cntrct_evn`

This document defines the comprehensive migration-validation test suite to prove behavioral equivalence between the legacy Oracle Data Warehouse job and the migrated Google Cloud Platform (BigQuery / Cloud Composer) implementation.

---

## Test Case 1: End-to-End Output Parity (Oracle vs. BigQuery)

### Purpose
To verify that given the exact same input data in the source table, the migrated BigQuery SQL script produces identical output to the legacy Oracle SQL script.

### Setup
1. **Oracle Environment**:
   * Seed the source table `sof$ta_bpr_evn` with a controlled set of test records representing all mapped `bpr_id` values, unmapped values, and multiple records per `cntrct_id`.
   * Ensure the target table `sof$ta_cntrct_evn` is empty.
2. **BigQuery Environment**:
   * Seed the target BigQuery source table `isbert_schema.sof_ta_bpr_evn` with the exact same dataset.
   * Ensure the target table `isbert_schema.sof_ta_cntrct_evn` is empty.

#### Seed Dataset
| cntrct_id | bpr_id | Description |
| :--- | :--- | :--- |
| 100001 | 32 | Mapped to 1 |
| 100001 | 2839 | Mapped to 10 (Total for 100001 = 11) |
| 100002 | 2506 | Mapped to 2 |
| 100002 | 2840 | Mapped to 20 (Total for 100002 = 22) |
| 100003 | 3055 | Mapped to 3 |
| 100003 | 3056 | Mapped to 30 (Total for 100003 = 33) |
| 100004 | 3821 | Mapped to 4 |
| 100004 | 9999 | Unmapped (Mapped to 0, Total for 100004 = 4) |
| 100005 | 9999 | Unmapped (Mapped to 0, Total for 100005 = 0) |

### Action
1. Execute the legacy Oracle SQL script `d_ausd_bp_ta_cntrct_evn.sql` in the Oracle test environment.
2. Execute the migrated BigQuery SQL script `d_ausd_bp_ta_cntrct_evn.sql` in the BigQuery test environment.
3. Extract the results from both target tables, sort them by `cntrct_id`, and compare.

### Pass/Fail Criterion
* **Pass**: The row count, schema, and values of all columns (`cntrct_id`, `evn`) match exactly between the Oracle target table and the BigQuery target table.
* **Fail**: Any discrepancy in row count, column values, or data types.

### Test Code (Pytest)
```python
import pytest
from google.cloud import bigquery
import cx_Oracle

def test_end_to_end_parity():
    # 1. Connect to Oracle and fetch results
    oracle_conn = cx_Oracle.connect("user/password@host:port/service")
    oracle_cursor = oracle_conn.cursor()
    oracle_cursor.execute("""
        SELECT cntrct_id, evn 
        FROM sof$ta_cntrct_evn 
        ORDER BY cntrct_id
    """)
    oracle_results = oracle_cursor.fetchall()
    oracle_cursor.close()
    oracle_conn.close()

    # 2. Connect to BigQuery and fetch results
    bq_client = bigquery.Client()
    bq_query = """
        SELECT cntrct_id, evn 
        FROM `your_gcp_project.isbert_schema.sof_ta_cntrct_evn` 
        ORDER BY cntrct_id
    """
    bq_results = [
        (row["cntrct_id"], row["evn"]) 
        for row in bq_client.query(bq_query).result()
    ]

    # 3. Assert Equivalence
    assert len(oracle_results) == len(bq_results), (
        f"Row count mismatch! Oracle: {len(oracle_results)}, BigQuery: {len(bq_results)}"
    )
    
    for o_row, bq_row in zip(oracle_results, bq_results):
        assert o_row == bq_row, f"Data mismatch! Oracle: {o_row}, BigQuery: {bq_row}"
```

---

## Test Case 2: Transformation Correctness & Edge Cases (Unit Test)

### Purpose
To validate that the BigQuery `CASE WHEN` statement correctly handles all mapped values, unmapped values, `NULL` values, and aggregations without relying on external database states.

### Setup
No physical tables are required. This test uses a BigQuery `WITH` clause (Common Table Expression) to inject mock data directly into the query execution.

### Action
Execute a test query containing the exact transformation logic against the mock dataset.

### Pass/Fail Criterion
* **Pass**: The query returns the exact expected aggregated `evn` values for all test cases, including `NULL` handling.
* **Fail**: Any returned `evn` value deviates from the expected mapping.

### Test Code (SQL Assertion)
```sql
WITH mock_source AS (
  -- Test Case 1: Standard mappings
  SELECT 101 AS cntrct_id, 32 AS bpr_id UNION ALL   -- Expected: 1
  SELECT 101 AS cntrct_id, 2839 AS bpr_id UNION ALL -- Expected: 10 (Sum: 11)
  
  -- Test Case 2: Standard mappings
  SELECT 102 AS cntrct_id, 2506 AS bpr_id UNION ALL -- Expected: 2
  SELECT 102 AS cntrct_id, 2840 AS bpr_id UNION ALL -- Expected: 20 (Sum: 22)
  
  -- Test Case 3: Standard mappings
  SELECT 103 AS cntrct_id, 3055 AS bpr_id UNION ALL -- Expected: 3
  SELECT 103 AS cntrct_id, 3056 AS bpr_id UNION ALL -- Expected: 30 (Sum: 33)
  
  -- Test Case 4: Standard mapping + Unmapped value
  SELECT 104 AS cntrct_id, 3821 AS bpr_id UNION ALL -- Expected: 4
  SELECT 104 AS cntrct_id, 9999 AS bpr_id UNION ALL -- Expected: 0 (Sum: 4)
  
  -- Test Case 5: Only unmapped values
  SELECT 105 AS cntrct_id, 8888 AS bpr_id UNION ALL -- Expected: 0 (Sum: 0)
  
  -- Test Case 6: NULL handling for bpr_id
  SELECT 106 AS cntrct_id, CAST(NULL AS INT64) AS bpr_id UNION ALL -- Expected: 0 (Sum: 0)
  
  -- Test Case 7: NULL handling for cntrct_id
  SELECT CAST(NULL AS INT64) AS cntrct_id, 32 AS bpr_id -- Expected: 1 (Sum: 1)
),
transformed_data AS (
  SELECT
    cntrct_id,
    SUM(
      CASE bpr_id
        WHEN 32   THEN 1
        WHEN 2839 THEN 10
        WHEN 2506 THEN 2
        WHEN 2840 THEN 20
        WHEN 3055 THEN 3
        WHEN 3056 THEN 30
        WHEN 3821 THEN 4
        ELSE 0
      END
    ) AS evn
  FROM
    mock_source
  GROUP BY
    cntrct_id
)
SELECT
  cntrct_id,
  evn,
  CASE 
    WHEN cntrct_id = 101 AND evn = 11 THEN 'PASS'
    WHEN cntrct_id = 102 AND evn = 22 THEN 'PASS'
    WHEN cntrct_id = 103 AND evn = 33 THEN 'PASS'
    WHEN cntrct_id = 104 AND evn = 4  THEN 'PASS'
    WHEN cntrct_id = 105 AND evn = 0  THEN 'PASS'
    WHEN cntrct_id = 106 AND evn = 0  THEN 'PASS'
    WHEN cntrct_id IS NULL AND evn = 1 THEN 'PASS'
    ELSE 'FAIL'
  END AS test_result
FROM
  transformed_data;
```

---

## Test Case 3: Idempotency & Truncate-and-Reload Behavior

### Purpose
To verify that the target table is successfully truncated before loading, ensuring that multiple runs of the job do not duplicate or append data.

### Setup
1. Seed the source table `isbert_schema.sof_ta_bpr_evn` with 5 records.
2. Populate the target table `isbert_schema.sof_ta_cntrct_evn` with 10 dummy records that do not exist in the source table.

### Action
1. Run the BigQuery SQL script `d_ausd_bp_ta_cntrct_evn.sql`.
2. Query the target table `isbert_schema.sof_ta_cntrct_evn`.

### Pass/Fail Criterion
* **Pass**: 
  * The 10 dummy records are completely removed.
  * The target table contains only the aggregated records derived from the current source table.
  * Running the script a second time yields the exact same row count and data.
* **Fail**: The dummy records persist, or the row count doubles on the second run.

### Test Code (Pytest)
```python
import pytest
from google.cloud import bigquery

def test_idempotency_and_truncate(bq_client):
    project = "your_gcp_project"
    dataset = "isbert_schema"
    target_table = f"{project}.{dataset}.sof_ta_cntrct_evn"
    source_table = f"{project}.{dataset}.sof_ta_bpr_evn"

    # 1. Insert dummy data into target table
    bq_client.query(f"""
        INSERT INTO `{target_table}` (cntrct_id, evn)
        VALUES (999999, 999), (888888, 888)
    """).result()

    # 2. Read the SQL script content
    with open("sql/d_ausd_bp_ta_cntrct_evn.sql", "r") as f:
        sql_script = f.read()

    # Resolve Jinja variables manually for testing
    sql_script = sql_script.replace(
        "{{ var.value.get('gcp_project_id', 'your_gcp_project') }}", project
    ).replace(
        "{{ var.value.get('gcp_dataset_name', 'isbert_schema') }}", dataset
    )

    # 3. Run the script the first time
    bq_client.query(sql_script).result()
    
    # Verify dummy data is gone
    dummy_check = bq_client.query(f"""
        SELECT COUNT(1) as cnt FROM `{target_table}` WHERE cntrct_id IN (999999, 888888)
    """).result()
    assert list(dummy_check)[0]["cnt"] == 0

    # Get row count after first run
    first_run_count = list(bq_client.query(f"SELECT COUNT(1) as cnt FROM `{target_table}`").result())[0]["cnt"]

    # 4. Run the script a second time
    bq_client.query(sql_script).result()
    second_run_count = list(bq_client.query(f"SELECT COUNT(1) as cnt FROM `{target_table}`").result())[0]["cnt"]

    # Verify row count is identical (idempotent)
    assert first_run_count == second_run_count, "Data was appended instead of truncated!"
```

---

## Test Case 4: Schema and Data Quality Assertions

### Purpose
To verify that the target table schema matches the expected BigQuery types and that data quality constraints (such as uniqueness of `cntrct_id`) are preserved.

### Setup
The target table `isbert_schema.sof_ta_cntrct_evn` must be fully populated.

### Action
1. Query `INFORMATION_SCHEMA.COLUMNS` to validate data types.
2. Query the target table to check for duplicate `cntrct_id` values.

### Pass/Fail Criterion
* **Pass**:
  * `cntrct_id` is of type `INT64`.
  * `evn` is of type `INT64`.
  * No duplicate `cntrct_id` values exist in the target table.
* **Fail**: Incorrect data types or duplicate `cntrct_id` values found.

### Test Code (SQL Assertions)
```sql
-- Assertion 1: Verify Schema Types
SELECT
  column_name,
  data_type,
  is_nullable
FROM
  `your_gcp_project.isbert_schema.INFORMATION_SCHEMA.COLUMNS`
WHERE
  table_name = 'sof_ta_cntrct_evn'
  AND column_name IN ('cntrct_id', 'evn');

-- Expected Output:
-- cntrct_id | INT64 | YES (or NO depending on DDL)
-- evn       | INT64 | YES

-- Assertion 2: Verify Uniqueness of Primary Key (cntrct_id)
SELECT
  cntrct_id,
  COUNT(1) AS occurrence_count
FROM
  `your_gcp_project.isbert_schema.sof_ta_cntrct_evn`
WHERE
  cntrct_id IS NOT NULL
GROUP BY
  cntrct_id
HAVING
  COUNT(1) > 1;

-- Expected Output: 0 rows returned.
```

---

## Test Case 5: Airflow DAG Integration & Variable Resolution

### Purpose
To verify that the Airflow DAG parses correctly without syntax errors, resolves all environment variables, and correctly references the SQL file.

### Setup
Place the DAG file `dags/dw_bert_ausd_bp_ta_cntrct_evn.py` and the SQL file `sql/d_ausd_bp_ta_cntrct_evn.sql` in the Airflow environment (or a local mock environment).

### Action
1. Run a programmatic DAG integrity test using `pytest`.
2. Mock the Airflow Variables and render the templated SQL task.

### Pass/Fail Criterion
* **Pass**:
  * The DAG is loaded with zero import errors.
  * The task `process_bert_basisprodukte_evn` successfully renders the SQL template with the correct project and dataset variables.
* **Fail**: Import errors are raised, or the SQL template fails to render.

### Test Code (Pytest)
```python
import pytest
from airflow.models import DagBag, Variable
from airflow.utils.state import State

def test_dag_integrity():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    assert len(dagbag.import_errors) == 0, f"DAG Import Errors: {dagbag.import_errors}"
    
    dag = dagbag.get_dag(dag_id="dw_bert_ausd_bp_ta_cntrct_evn")
    assert dag is not None
    assert len(dag.tasks) == 3
    
    # Verify task dependencies: start -> process_bert_basisprodukte_evn -> end
    start_task = dag.get_task("start")
    process_task = dag.get_task("process_bert_basisprodukte_evn")
    end_task = dag.get_task("end")
    
    assert process_task in start_task.downstream_list
    assert end_task in process_task.downstream_list

def test_sql_template_rendering(monkeypatch):
    # Mock Airflow Variables
    variables = {
        "gcp_project_id": "test-gcp-project",
        "gcp_dataset_name": "test_dataset"
    }
    
    def mock_get(key, default_var=None):
        return variables.get(key, default_var)
        
    monkeypatch.setattr(Variable, "get", mock_get)
    
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.get_dag(dag_id="dw_bert_ausd_bp_ta_cntrct_evn")
    task = dag.get_task("process_bert_basisprodukte_evn")
    
    # Mock the template rendering context
    from airflow.utils.context import Context
    context = Context()
    
    # Force rendering of templates
    rendered_sql = task.render_template(task.sql, context)
    
    # Assertions to ensure variables were correctly injected
    assert "test-gcp-project.test_dataset.sof_ta_cntrct_evn" in rendered_sql
    assert "test-gcp-project.test_dataset.sof_ta_bpr_evn" in rendered_sql
    assert "TRUNCATE TABLE" in rendered_sql
    assert "INSERT INTO" in rendered_sql
```