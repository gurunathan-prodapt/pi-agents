Here is a comprehensive suite of migration-validation tests designed to verify the behavioral equivalence, transformation correctness, and operational readiness of the migrated `BERT_V_TA_DISC_ZUSGF` job.

---

# Migration Validation Test Suite: BERT_V_TA_DISC_ZUSGF

This test suite is designed to be executed in a QA/Dry-Run environment on Google Cloud Platform (GCP). It uses `pytest` alongside the Google Cloud BigQuery SDK to run automated assertions, and standard SQL validation queries for manual or automated pipeline checks.

---

## Test Case 1: End-to-End Output Parity (Golden Dataset)

### Purpose
To prove that a representative set of inputs processed through the migrated BigQuery SQL script produces the exact same output as the legacy Oracle PL/SQL pipeline.

### Setup
1. Create a temporary source table `tmp_test_sof_ta_discount` in the test dataset.
2. Populate it with a "Golden Dataset" containing standard contract discount scenarios.
3. Create an empty target table `tmp_test_sof_ta_disc_zusgf` using the target schema.

```sql
-- Setup Source Data
CREATE OR REPLACE TABLE `your_project.test_dataset.tmp_test_sof_ta_discount` AS
SELECT 'C001' AS cntrct_id, 1 AS cntrct_obj_version, 'V1' AS disc_vector_ty, 'Mitarbeiterrabatt' AS rabatt, 10.0 AS rabatthoehe UNION ALL
SELECT 'C001', 1, 'V1', 'Neukundenbonus', 5.0 UNION ALL
SELECT 'C002', 1, 'V2', 'Treuerabatt', 15.5 UNION ALL
-- Contract with single discount
SELECT 'C003', 2, 'V1', 'Sondernachlass', 20.0;
```

### Action
Execute the migrated SQL logic, targeting the temporary tables instead of production tables.

```python
import pytest
from google.cloud import bigquery

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client()

def test_e2e_output_parity(bq_client):
    # Define the query using the migrated logic pointing to test tables
    query = """
    CREATE OR REPLACE TABLE `your_project.test_dataset.tmp_test_sof_ta_disc_zusgf` AS
    WITH dzg AS (
      SELECT DISTINCT cntrct_id, cntrct_obj_version, disc_vector_ty
      FROM `your_project.test_dataset.tmp_test_sof_ta_discount`
    ),
    con AS (
      SELECT
        cntrct_id,
        cntrct_obj_version,
        SUBSTR(
          STRING_AGG(single_rabatt, ', ' ORDER BY single_rabatt), 
          1, 
          500
        ) AS rabatt_alle
      FROM (
        SELECT DISTINCT
          cntrct_id,
          cntrct_obj_version,
          CONCAT(rabatt, ' (', CAST(rabatthoehe AS STRING), '%)') AS single_rabatt
        FROM `your_project.test_dataset.tmp_test_sof_ta_discount`
        WHERE rabatt IS NOT NULL
      )
      GROUP BY cntrct_id, cntrct_obj_version
    )
    SELECT
      dzg.cntrct_id,
      dzg.cntrct_obj_version,
      dzg.disc_vector_ty,
      con.rabatt_alle
    FROM dzg
    LEFT JOIN con
      ON dzg.cntrct_id = con.cntrct_id
     AND dzg.cntrct_obj_version = con.cntrct_obj_version;
    """
    
    # Run the query
    query_job = bq_client.query(query)
    query_job.result()  # Wait for completion

    # Fetch results to assert
    assert_query = """
    SELECT cntrct_id, cntrct_obj_version, disc_vector_ty, rabatt_alle 
    FROM `your_project.test_dataset.tmp_test_sof_ta_disc_zusgf`
    ORDER BY cntrct_id, cntrct_obj_version;
    """
    results = list(bq_client.query(assert_query).result())
    
    # Expected Map
    expected = {
        ('C001', 1, 'V1'): "Mitarbeiterrabatt (10.0%), Neukundenbonus (5.0%)", # Alphabetical ordering check
        ('C002', 1, 'V2'): "Treuerabatt (15.5%)",
        ('C003', 2, 'V1'): "Sondernachlass (20.0%)"
    }
    
    assert len(results) == 3
    for row in results:
        key = (row.cntrct_id, row.cntrct_obj_version, row.disc_vector_ty)
        assert key in expected
        assert row.rabatt_alle == expected[key]
```

### Pass/Fail Criterion
*   **Pass**: The target table is populated with exactly 3 rows, matching the expected concatenated strings and alphabetical order.
*   **Fail**: Row counts do not match, strings are concatenated in the wrong order, or formatting differs from `Rabattname (Wert%)`.

---

## Test Case 2: Transformation Correctness — Alphabetical Ordering

### Purpose
To verify that multiple discounts for a single contract version are concatenated in strict alphabetical order of the *final concatenated string* (matching the legacy Oracle `ORDER BY rabatt_alle` behavior).

### Setup
Insert discounts where alphabetical ordering of the raw `rabatt` name differs from the alphabetical ordering of the formatted string, or where names are similar but percentages differ.

```sql
CREATE OR REPLACE TABLE `your_project.test_dataset.tmp_test_ordering` AS
SELECT 'C999' AS cntrct_id, 1 AS cntrct_obj_version, 'V1' AS disc_vector_ty, 'B_Discount' AS rabatt, 20.0 AS rabatthoehe UNION ALL
SELECT 'C999', 1, 'V1', 'A_Discount', 10.0 UNION ALL
SELECT 'C999', 1, 'V1', 'A_Discount', 5.0;
```

### Action
Run the aggregation query on this dataset.

```python
def test_alphabetical_ordering(bq_client):
    query = """
    SELECT 
      SUBSTR(
        STRING_AGG(single_rabatt, ', ' ORDER BY single_rabatt), 
        1, 
        500
      ) AS rabatt_alle
    FROM (
      SELECT DISTINCT
        CONCAT(rabatt, ' (', CAST(rabatthoehe AS STRING), '%)') AS single_rabatt
      FROM `your_project.test_dataset.tmp_test_ordering`
      WHERE rabatt IS NOT NULL
    );
    """
    result = list(bq_client.query(query).result())[0]
    
    # "A_Discount (10.0%)" vs "A_Discount (5.0%)" -> "1" comes before "5"
    # "B_Discount (20.0%)" comes last.
    expected_string = "A_Discount (10.0%), A_Discount (5.0%), B_Discount (20.0%)"
    
    assert result.rabatt_alle == expected_string
```

### Pass/Fail Criterion
*   **Pass**: The output string is ordered exactly as: `A_Discount (10.0%), A_Discount (5.0%), B_Discount (20.0%)`.
*   **Fail**: Any other ordering is returned (e.g., ordering by raw `rabatt` first, which might make the order of the two `A_Discount` rows non-deterministic).

---

## Test Case 3: Transformation Correctness — NULL Handling & Left Join Preservation

### Purpose
To verify that:
1. Rows with `rabatt IS NULL` are excluded from the string concatenation.
2. Contracts that have *only* NULL discounts (or no discounts at all) are still preserved in the target table with a `NULL` value for `rabatt_alle` (due to the `LEFT JOIN` with the distinct contract list `dzg`).

### Setup
Populate test data with:
*   `C004`: Has one valid discount and one NULL discount.
*   `C005`: Has only a NULL discount.
*   `C006`: Has no entries in the discount subquery but exists in the base contract list (simulated by having a row with `rabatt IS NULL`).

```sql
CREATE OR REPLACE TABLE `your_project.test_dataset.tmp_test_nulls` AS
-- C004: Mixed
SELECT 'C004' AS cntrct_id, 1 AS cntrct_obj_version, 'V1' AS disc_vector_ty, 'Valid Discount' AS rabatt, 10.0 AS rabatthoehe UNION ALL
SELECT 'C004', 1, 'V1', CAST(NULL AS STRING), 5.0 UNION ALL
-- C005: Only NULL discount
SELECT 'C005', 1, 'V1', CAST(NULL AS STRING), CAST(NULL AS FLOAT64);
```

### Action
Run the migration query against the NULL test dataset.

```python
def test_null_handling(bq_client):
    query = """
    WITH dzg AS (
      SELECT DISTINCT cntrct_id, cntrct_obj_version, disc_vector_ty
      FROM `your_project.test_dataset.tmp_test_nulls`
    ),
    con AS (
      SELECT
        cntrct_id,
        cntrct_obj_version,
        SUBSTR(
          STRING_AGG(single_rabatt, ', ' ORDER BY single_rabatt), 
          1, 
          500
        ) AS rabatt_alle
      FROM (
        SELECT DISTINCT
          cntrct_id,
          cntrct_obj_version,
          CONCAT(rabatt, ' (', CAST(rabatthoehe AS STRING), '%)') AS single_rabatt
        FROM `your_project.test_dataset.tmp_test_nulls`
        WHERE rabatt IS NOT NULL
      )
      GROUP BY cntrct_id, cntrct_obj_version
    )
    SELECT
      dzg.cntrct_id,
      dzg.cntrct_obj_version,
      dzg.disc_vector_ty,
      con.rabatt_alle
    FROM dzg
    LEFT JOIN con
      ON dzg.cntrct_id = con.cntrct_id
     AND dzg.cntrct_obj_version = con.cntrct_obj_version;
    """
    results = { (r.cntrct_id, r.cntrct_obj_version): r.rabatt_alle for r in bq_client.query(query).result() }
    
    # Assertions
    assert results[('C004', 1)] == "Valid Discount (10.0%)"  # NULL discount ignored, valid one kept
    assert results[('C005', 1)] is None                      # Preserved in target, but rabatt_alle is NULL
```

### Pass/Fail Criterion
*   **Pass**: `C004` contains only the non-null discount. `C005` is present in the output dataset with a `NULL` value for `rabatt_alle`.
*   **Fail**: `C005` is missing from the output (indicating an `INNER JOIN` bug), or `C004` contains stringified nulls like `"null (5.0%)"`.

---

## Test Case 4: Edge-Case Validation — 500 Character Truncation

### Purpose
To verify the behavior of the 500-character truncation limit. As called out in the design document, BigQuery applies a hard slice at character 500 (`SUBSTR(..., 1, 500)`), which may result in a trailing, partially-cut item. This test asserts that the BigQuery implementation behaves exactly as designed.

### Setup
Generate a contract with 25 distinct discounts, each of length 25 characters, ensuring the total concatenated length exceeds 500 characters.

```sql
CREATE OR REPLACE TABLE `your_project.test_dataset.tmp_test_truncation` AS
SELECT 
  'C_LIMIT' AS cntrct_id, 
  1 AS cntrct_obj_version, 
  'V1' AS disc_vector_ty,
  FORMAT('Discount_Name_Exceeding_Limit_With_Long_Index_Value_%02d', val) AS rabatt,
  10.0 AS rabatthoehe
FROM UNNEST(GENERATE_ARRAY(1, 25)) AS val;
```

### Action
Run the aggregation query and measure the length and content of the output.

```python
def test_character_truncation_limit(bq_client):
    query = """
    WITH dzg AS (
      SELECT DISTINCT cntrct_id, cntrct_obj_version, disc_vector_ty
      FROM `your_project.test_dataset.tmp_test_truncation`
    ),
    con AS (
      SELECT
        cntrct_id,
        cntrct_obj_version,
        SUBSTR(
          STRING_AGG(single_rabatt, ', ' ORDER BY single_rabatt), 
          1, 
          500
        ) AS rabatt_alle
      FROM (
        SELECT DISTINCT
          cntrct_id,
          cntrct_obj_version,
          CONCAT(rabatt, ' (', CAST(rabatthoehe AS STRING), '%)') AS single_rabatt
        FROM `your_project.test_dataset.tmp_test_truncation`
        WHERE rabatt IS NOT NULL
      )
      GROUP BY cntrct_id, cntrct_obj_version
    )
    SELECT con.rabatt_alle
    FROM dzg
    LEFT JOIN con
      ON dzg.cntrct_id = con.cntrct_id
     AND dzg.cntrct_obj_version = con.cntrct_obj_version;
    """
    result = list(bq_client.query(query).result())[0]
    rabatt_alle = result.rabatt_alle
    
    assert len(rabatt_alle) == 500
    # Verify that the string is sliced mid-element at character 500
    assert rabatt_alle.endswith("Discount_Name_Exceeding_Limit_With_Long_Index_Value_08 (10.0%), Discount_Name_Exceeding_Limit_With_Long_Index_Value_09 (10.0%), Discount_Name_Exceeding_Limit_With_Long_Index_Value_10 (10.0%), Discount_Name_Exceeding_Limit_With_Long_Index_Value_11 (10.0%), Discount_Name_Exceeding_Limit_With_Long_Index_Value_12 (10.0%), Discount_Name_Exceeding_Limit_With_Long_Index_Value_13 (10.0%), Discount_Name_Exceeding_Limit_With_Long_Index_Value_14 (10.0%), Discount_Name_Exceeding_Limit_With_Long_Index_Value_15 (10.")
```

### Pass/Fail Criterion
*   **Pass**: The returned string is exactly 500 characters long and ends with a partial slice of the final element as specified in the design mitigation.
*   **Fail**: The string length exceeds 500 characters, or the query fails due to string overflow.

---

## Test Case 5: Data-Quality & Schema Assertions

### Purpose
To verify that the target table `sof_ta_disc_zusgf` is created with the correct column names, data types, and constraints, and that the row count matches the source's distinct contract keys.

### Setup
Ensure the production or dry-run migration script has executed and populated `your_project.bert_dataset.sof_ta_disc_zusgf`.

### Action
Run metadata and row-count validation queries.

```python
def test_schema_and_row_counts(bq_client):
    dataset_id = "bert_dataset"
    table_id = "sof_ta_disc_zusgf"
    
    # 1. Schema Validation
    table_ref = bq_client.dataset(dataset_id).table(table_id)
    table = bq_client.get_table(table_ref)
    
    schema_dict = {field.name: field.field_type for field in table.schema}
    
    expected_schema = {
        "cntrct_id": "STRING",
        "cntrct_obj_version": "INTEGER", # Or INT64
        "disc_vector_ty": "STRING",
        "rabatt_alle": "STRING"
    }
    
    for col, expected_type in expected_schema.items():
        assert col in schema_dict
        # BigQuery API returns 'INTEGER' for INT64
        if expected_type == "INTEGER":
            assert schema_dict[col] in ["INTEGER", "INT64"]
        else:
            assert schema_dict[col] == expected_type

    # 2. Row Count Reconciliation Assertion
    # The target table must have exactly the same number of rows as the distinct combinations of 
    # (cntrct_id, cntrct_obj_version, disc_vector_ty) in the source table.
    reconciliation_query = """
    WITH src AS (
      SELECT COUNT(DISTINCT CONCAT(cntrct_id, '_', CAST(cntrct_obj_version AS STRING), '_', disc_vector_ty)) AS src_count
      FROM `your_project.bert_dataset.sof_ta_discount`
    ),
    tgt AS (
      SELECT COUNT(*) AS tgt_count
      FROM `your_project.bert_dataset.sof_ta_disc_zusgf`
    )
    SELECT src_count, tgt_count, (src_count = tgt_count) AS is_equal
    FROM src, tgt;
    """
    recon_result = list(bq_client.query(reconciliation_query).result())[0]
    assert recon_result.is_equal is True, f"Row count mismatch! Source: {recon_result.src_count}, Target: {recon_result.tgt_count}"
```

### Pass/Fail Criterion
*   **Pass**: Target table schema matches the expected column names and types, and the target row count is exactly equal to the distinct source contract keys.
*   **Fail**: Schema mismatch detected, or target row count deviates from the source distinct key count.

---

## Test Case 6: Airflow Orchestration & DAG Compilation Validation

### Purpose
To verify that the Airflow DAG `bert_v_ta_disc_zusgf` compiles without syntax errors, has the correct task structure, and references the correct SQL file path.

### Setup
Place the DAG file `bert_v_ta_disc_zusgf_dag.py` and the SQL script in the local Airflow testing environment or mock the Airflow environment.

### Action
Run a Python test using the Airflow `DagBag` utility.

```python
from airflow.models import DagBag

def test_dag_compilation_and_structure():
    dagbag = DagBag(dag_folder="gcs_dag_bucket/dags", include_examples=False)
    
    # 1. Check for import errors
    assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"
    
    # 2. Verify DAG existence
    dag_id = "bert_v_ta_disc_zusgf"
    dag = dagbag.get_dag(dag_id)
    assert dag is not None, f"DAG {dag_id} not found in DagBag"
    
    # 3. Verify Task Structure
    expected_tasks = ["start", "load_sof_ta_disc_zusgf", "end"]
    actual_tasks = [task.task_id for task in dag.tasks]
    assert set(expected_tasks) == set(actual_tasks)
    
    # 4. Verify Task Dependencies
    start_task = dag.get_task("start")
    load_task = dag.get_task("load_sof_ta_disc_zusgf")
    end_task = dag.get_task("end")
    
    assert load_task in start_task.downstream_list
    assert end_task in load_task.downstream_list
```

### Pass/Fail Criterion
*   **Pass**: The DAG compiles with zero import errors, contains all three expected tasks, and maintains the linear execution order: `start` -> `load_sof_ta_disc_zusgf` -> `end`.
*   **Fail**: Import errors are raised (e.g., missing dependencies, syntax errors), tasks are missing, or dependencies are incorrectly configured.