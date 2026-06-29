This document provides a comprehensive suite of migration-validation tests for the job `ausd_bp_ta_bpr_apn`. These tests are designed to prove that the migrated BigQuery SQL and Apache Airflow DAG are behaviorally equivalent to the legacy Oracle and UC4 implementation.

---

## Test Case 1: End-to-End Output Parity (A/B Validation)

### Purpose
To prove that given identical input datasets, the legacy Oracle SQL script and the migrated BigQuery SQL script produce identical output datasets.

### Setup
1. **Legacy Environment (Oracle)**:
   * Populate `isbert_schema.sof_ta_bpr_instance` and `isbert_schema.sof_ta_apn_carmen` with a representative set of production-like test data (at least 1,000 rows, including matching, non-matching, and duplicate keys).
   * Ensure the target table `sof$ta_bpr_apn` is empty.
2. **Target Environment (BigQuery)**:
   * Load the exact same dataset into the BigQuery tables `isbert_schema.sof_ta_bpr_instance` and `isbert_schema.sof_ta_apn_carmen`.
   * Ensure the target table `isbert_schema.sof_ta_bpr_apn` is empty.

### Action
1. Execute the legacy Oracle SQL script (`d_ausd_bp_ta_bpr_apn.sql`) in the Oracle environment.
2. Execute the migrated BigQuery SQL script (`sof_ta_bpr_apn_transform.sql`) in the BigQuery environment.
3. Extract the results from both target tables, sort them by `cntrct_id`, `bpr_id`, and `cntrct_id_ref`, and compare them.

### Pass/Fail Criterion
* **Pass**: The row count is identical, and every column value matches exactly between the Oracle target table and the BigQuery target table.
* **Fail**: There is a mismatch in row count, or any column value differs between the two environments.

### Validation Code (Python / PyTest)
```python
import pandas as pd
from google.cloud import bigquery
import cx_Oracle
import pytest

def test_output_parity():
    # 1. Fetch Oracle Results
    oracle_conn = cx_Oracle.connect("user/pwd@host:port/service")
    oracle_query = """
        SELECT cntrct_id, bpr_id, cntrct_id_ref, access_point_name 
        FROM isbert_schema.sof$ta_bpr_apn 
        ORDER BY cntrct_id, bpr_id, cntrct_id_ref
    """
    df_oracle = pd.read_sql(oracle_query, con=oracle_conn)
    oracle_conn.close()

    # 2. Fetch BigQuery Results
    bq_client = bigquery.Client()
    bq_query = """
        SELECT cntrct_id, bpr_id, cntrct_id_ref, access_point_name 
        FROM `isbert_schema.sof_ta_bpr_apn` 
        ORDER BY cntrct_id, bpr_id, cntrct_id_ref
    """
    df_bq = bq_client.query(bq_query).to_dataframe()

    # 3. Assert Equivalence
    assert len(df_oracle) == len(df_bq), f"Row count mismatch: Oracle ({len(df_oracle)}) vs BQ ({len(df_bq)})"
    pd.testing.assert_frame_equal(df_oracle, df_bq, check_dtype=False, obj="Oracle vs BigQuery Parity")
```

---

## Test Case 2: Transformation Correctness — `bpr_id` Whitelist Filter

### Purpose
To verify that only the specified basic product IDs (`bpr_id`) are processed and loaded into the target table, and all other IDs are filtered out.

### Setup
In the BigQuery test dataset, populate `isbert_schema.sof_ta_bpr_instance` with the following test cases:
* **Whitelisted IDs**: `2828`, `2829`, `2830`, `2831`, `2925`, `2926`, `2998`, `2999`, `3000` (one row each, with valid matching APN records).
* **Non-Whitelisted IDs**: `1000`, `2827`, `3001`, `NULL` (one row each, with valid matching APN records).

Populate `isbert_schema.sof_ta_apn_carmen` with matching contract references for all the above rows.

### Action
Run the BigQuery transformation script.

### Pass/Fail Criterion
* **Pass**: 
  * The target table contains exactly 9 rows.
  * Only the whitelisted `bpr_id` values are present in the target table.
  * No non-whitelisted or `NULL` `bpr_id` values exist in the target table.
* **Fail**: Any non-whitelisted `bpr_id` is found in the target table, or any whitelisted `bpr_id` is missing.

### Validation Code (SQL Assertions)
```sql
-- Assertion 1: Ensure no invalid bpr_ids exist in the target table
SELECT 
  COUNT(*) as invalid_rows_count
FROM `isbert_schema.sof_ta_bpr_apn`
WHERE bpr_id NOT IN (2828, 2829, 2830, 2831, 2925, 2926, 2998, 2999, 3000) 
   OR bpr_id IS NULL;

-- EXPECTED RESULT: 0
```

---

## Test Case 3: Transformation Correctness — Join Logic & Deduplication

### Purpose
To verify that:
1. The `INNER JOIN` correctly matches `bp.cntrct_id_ref = ap.cntrct_id`.
2. The `DISTINCT` keyword correctly deduplicates identical rows that may arise from duplicate source records.

### Setup
Populate BigQuery source tables with the following scenarios:
1. **Scenario A (Standard Match)**: 1 row in `bp` matches 1 row in `ap`. (Expected: 1 row in target)
2. **Scenario B (No Match in AP)**: 1 row in `bp` has a `cntrct_id_ref` that does not exist in `ap`. (Expected: 0 rows in target)
3. **Scenario C (No Match in BP)**: 1 row in `ap` has a `cntrct_id` that does not exist in `bp`. (Expected: 0 rows in target)
4. **Scenario D (Duplicates)**: 2 identical rows in `bp` matching 1 row in `ap`. (Expected: 1 row in target due to `DISTINCT`)

### Action
Run the BigQuery transformation script.

### Pass/Fail Criterion
* **Pass**: The target table contains only rows from Scenario A and Scenario D (exactly 1 row for each, total of 2 rows). Scenarios B and C are excluded, and Scenario D is successfully deduplicated.
* **Fail**: Unmatched rows are present, or duplicate rows exist in the target table.

### Validation Code (SQL Assertions)
```sql
-- Assertion 1: Verify duplicate rows are collapsed
SELECT 
  cntrct_id, bpr_id, cntrct_id_ref, access_point_name, COUNT(*) as occurrence_count
FROM `isbert_schema.sof_ta_bpr_apn`
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) > 1;

-- EXPECTED RESULT: 0 rows returned (No duplicates)
```

---

## Test Case 4: Transformation Correctness — NULL Handling

### Purpose
To verify how the query handles `NULL` values in join keys and payload fields.

### Setup
Populate BigQuery source tables with the following edge cases:
1. **Null Join Key in BP**: `bp.cntrct_id_ref` is `NULL`.
2. **Null Join Key in AP**: `ap.cntrct_id` is `NULL`.
3. **Null Payload in AP**: `ap.access_point_name` is `NULL` (but join keys match).

### Action
Run the BigQuery transformation script.

### Pass/Fail Criterion
* **Pass**:
  * Rows with `NULL` join keys are completely excluded from the target table.
  * Rows with a valid join but a `NULL` `access_point_name` are preserved in the target table with a `NULL` value in the `access_point_name` column.
* **Fail**: Rows with `NULL` join keys are loaded, or rows with `NULL` payload are incorrectly dropped.

### Validation Code (SQL Assertions)
```sql
-- Assertion 1: Ensure no NULL join keys made it to the target
SELECT COUNT(*) as null_join_failures
FROM `isbert_schema.sof_ta_bpr_apn`
WHERE cntrct_id_ref IS NULL;

-- EXPECTED RESULT: 0

-- Assertion 2: Ensure NULL payload is preserved if the join was valid
-- (Assuming we inserted 1 test row with a matching join but NULL access_point_name)
SELECT COUNT(*) as null_payload_success
FROM `isbert_schema.sof_ta_bpr_apn`
WHERE access_point_name IS NULL;

-- EXPECTED RESULT: 1
```

---

## Test Case 5: Idempotency and Truncate-and-Insert Behavior

### Purpose
To verify that the target table is completely cleared before reloading, ensuring that multiple runs of the job do not append duplicate data or leave stale records.

### Setup
1. Manually insert 5 dummy "stale" records into `isbert_schema.sof_ta_bpr_apn` (e.g., with `bpr_id = 99999`).
2. Ensure the source tables contain a clean set of 10 valid records.

### Action
Run the BigQuery transformation script.

### Pass/Fail Criterion
* **Pass**: 
  * The 5 dummy "stale" records are completely removed.
  * The target table contains exactly the 10 valid records from the source tables.
* **Fail**: Stale records remain in the table, or the table is empty, or data is appended instead of overwritten.

### Validation Code (SQL Assertions)
```sql
-- Assertion 1: Verify stale records are gone
SELECT COUNT(*) as stale_count 
FROM `isbert_schema.sof_ta_bpr_apn` 
WHERE bpr_id = 99999;

-- EXPECTED RESULT: 0

-- Assertion 2: Verify total row count matches expected source-driven count
SELECT COUNT(*) as total_count 
FROM `isbert_schema.sof_ta_bpr_apn`;

-- EXPECTED RESULT: 10
```

---

## Test Case 6: Airflow DAG Orchestration & Parameter Validation

### Purpose
To verify that the Airflow DAG is syntactically correct, handles environment variables correctly, and executes the BigQuery task successfully.

### Setup
1. Deploy the DAG `ausd_bp_ta_bpr_apn_dag` to a Composer/Airflow environment.
2. Ensure the Airflow Variable `gcp_project` is set to the test GCP project ID.

### Action
1. Run an Airflow DAG integrity test to check for import errors.
2. Perform a dry-run of the `transform_basisprodukte_apn` task to verify SQL compilation and variable interpolation.

### Pass/Fail Criterion
* **Pass**:
  * The DAG is parsed successfully without any `DAGImportError`.
  * The SQL query is rendered correctly, replacing `{{ var.value.gcp_project }}` with the actual project name.
  * The dry-run execution completes successfully.
* **Fail**: The DAG fails to load, or the SQL compilation fails due to syntax or variable interpolation errors.

### Validation Code (PyTest for Airflow Integrity)
```python
from airflow.models import DagBag

def test_dag_import_and_integrity():
    dag_bag = DagBag(dag_folder="src/dags", include_examples=False)
    
    # Assert no import errors
    assert len(dag_bag.import_errors) == 0, f"DAG Import Errors: {dag_bag.import_errors}"
    
    # Assert DAG exists
    dag = dag_bag.get_dag(dag_id="ausd_bp_ta_bpr_apn_dag")
    assert dag is not None, "DAG 'ausd_bp_ta_bpr_apn_dag' not found"
    
    # Assert task structure
    tasks = dag.tasks
    task_ids = [t.task_id for t in tasks]
    assert "start_pipeline" in task_ids
    assert "transform_basisprodukte_apn" in task_ids
    assert "end_pipeline" in task_ids
    
    # Verify task dependencies
    transform_task = dag.get_task("transform_basisprodukte_apn")
    assert "start_pipeline" in [t.task_id for t in transform_task.upstream_list]
    assert "end_pipeline" in [t.task_id for t in transform_task.downstream_list]
```

---

## Test Case 7: Schema and Data Quality Assertions

### Purpose
To verify that the target table structure matches the design specification and that data types are correctly enforced.

### Setup
The target table `isbert_schema.sof_ta_bpr_apn` must be created using the DDL script.

### Action
Query the BigQuery `INFORMATION_SCHEMA.COLUMNS` view to validate the schema.

### Pass/Fail Criterion
* **Pass**: The table contains exactly the 4 columns specified in the design, with correct data types:
  * `cntrct_id`: `INTEGER` (INT64)
  * `bpr_id`: `INTEGER` (INT64)
  * `cntrct_id_ref`: `INTEGER` (INT64)
  * `access_point_name`: `STRING`
* **Fail**: Any column is missing, has an incorrect data type, or unexpected columns exist.

### Validation Code (SQL Assertions)
```sql
-- Assertion: Verify column names and data types
SELECT 
  column_name, 
  data_type
FROM `isbert_schema.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'sof_ta_bpr_apn'
ORDER BY ordinal_position;

/* EXPECTED RESULT:
+-------------------+-----------+
| column_name       | data_type |
+-------------------+-----------+
| cntrct_id         | INT64     |
| bpr_id            | INT64     |
| cntrct_id_ref     | INT64     |
| access_point_name | STRING    |
+-------------------+-----------+
*/
```