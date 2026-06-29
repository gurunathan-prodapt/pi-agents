# Migration Validation Test Suite: `ausd_bp_ta_bpr_bcp`

This document defines the migration-validation test suite for the BigQuery/Airflow job `ausd_bp_ta_bpr_bcp`. These tests are designed to prove behavioral equivalence between the legacy Oracle/KornShell implementation and the new Google Cloud Platform (GCP) implementation.

---

## Test Suite Overview

The test suite is organized into the following validation categories:
1. **Output Parity & Transformation Correctness**: Verifies filtering, deduplication, and mapping logic.
2. **Metadata Lookup Resilience**: Validates the behavior of the `v_datum` lookup query under various source states.
3. **Idempotency & Truncate Validation**: Ensures that target tables are cleared correctly and that multiple runs do not duplicate data.
4. **Null & Edge-Case Handling**: Asserts correct behavior when encountering missing or malformed data.
5. **Schema & Data Quality Assertions**: Validates structural integrity and column types in BigQuery.
6. **Airflow DAG Orchestration Validation**: Confirms the DAG parses and executes tasks in the correct sequence.

---

## Test Environment Setup (Pytest Fixture)

The following Python helper fixture can be used across the test cases to initialize and clean up BigQuery test tables.

```python
import os
import pytest
from google.cloud import bigquery

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client()

@pytest.fixture(scope="module")
def test_env(bq_client):
    # Retrieve configuration from environment variables
    project_id = os.getenv("GCP_PROJECT_ID", "your-gcp-project")
    dataset_id = os.getenv("GCP_DATASET_ID", "isbert_schema")
    
    # Define table paths
    src_instance = f"{project_id}.{dataset_id}.sof_ta_bpr_instance"
    src_meldungen = f"{project_id}.{dataset_id}.dwtk_meldungen"
    tgt_bcp = f"{project_id}.{dataset_id}.sof_ta_bpr_bcp"
    
    env = {
        "project_id": project_id,
        "dataset_id": dataset_id,
        "src_instance": src_instance,
        "src_meldungen": src_meldungen,
        "tgt_bcp": tgt_bcp
    }
    
    # Ensure target table exists (schema setup)
    # In a real CI/CD pipeline, DDL scripts should run before tests.
    yield env
    
    # Optional: Clean up test tables after run
    bq_client.query(f"TRUNCATE TABLE `{tgt_bcp}`").result()
```

---

## Section 1: Output Parity & Transformation Correctness

### Test Case 1.1: Happy Path Filtering and Deduplication
* **Purpose**: Verify that the transformation correctly filters records where `bpr_id = '3142'` and deduplicates identical records using the `DISTINCT` operator.
* **Setup**:
  1. Truncate `sof_ta_bpr_instance` and `sof_ta_bpr_bcp`.
  2. Insert mock records into `sof_ta_bpr_instance` containing:
     * Duplicate records for `bpr_id = '3142'`.
     * Records with other `bpr_id` values (e.g., `'9999'`).
* **Action**: Execute the SQL script `d_ausd_bp_ta_bpr_bcp.sql`.
* **Pass/Fail Criterion**: 
  * Only records with `bpr_id = '3142'` are migrated.
  * Duplicate records are collapsed into a single row.
  * The row count and values match the expected output exactly.

```python
def test_happy_path_filtering_and_deduplication(bq_client, test_env):
    # 1. Setup Mock Data
    setup_queries = [
        f"TRUNCATE TABLE `{test_env['src_instance']}`",
        f"TRUNCATE TABLE `{test_env['tgt_bcp']}`",
        f"""
        INSERT INTO `{test_env['src_instance']}` (cntrct_id, bpr_id, cntrct_id_ref)
        VALUES 
          ('CON_001', '3142', 'REF_001'), -- Valid
          ('CON_001', '3142', 'REF_001'), -- Duplicate (should be removed)
          ('CON_002', '3142', 'REF_002'), -- Valid
          ('CON_003', '9999', 'REF_003'), -- Invalid bpr_id (should be filtered out)
          ('CON_004', '3142', NULL)       -- Valid with NULL ref
        """
    ]
    for q in setup_queries:
        bq_client.query(q).result()

    # 2. Action: Run the migration SQL
    sql_path = "src/sql/d_ausd_bp_ta_bpr_bcp.sql"
    with open(sql_path, "r") as f:
        sql_script = f.read()
        
    # Replace hardcoded dataset references with test environment variables if necessary
    sql_script = sql_script.replace("`isbert_schema.", f"`{test_env['project_id']}.{test_env['dataset_id']}.")
    
    bq_client.query(sql_script).result()

    # 3. Assertions
    results_query = f"SELECT cntrct_id, bpr_id, cntrct_id_ref FROM `{test_env['tgt_bcp']}` ORDER BY cntrct_id"
    results = list(bq_client.query(results_query).result())
    
    assert len(results) == 3, f"Expected 3 rows, got {len(results)}"
    
    # Row 1 Assertion
    assert results[0]["cntrct_id"] == "CON_001"
    assert results[0]["bpr_id"] == "3142"
    assert results[0]["cntrct_id_ref"] == "REF_001"
    
    # Row 2 Assertion
    assert results[1]["cntrct_id"] == "CON_002"
    assert results[1]["bpr_id"] == "3142"
    assert results[1]["cntrct_id_ref"] == "REF_002"

    # Row 3 Assertion (NULL handling check)
    assert results[2]["cntrct_id"] == "CON_004"
    assert results[2]["bpr_id"] == "3142"
    assert results[2]["cntrct_id_ref"] is None
```

---

## Section 2: Metadata Lookup Resilience

### Test Case 2.1: Metadata Date Resolution (`dwtk_meldungen`)
* **Purpose**: Verify that the query to resolve `v_datum` executes successfully and handles empty states or multiple records without failing the transaction.
* **Setup**:
  * **Scenario A**: `dwtk_meldungen` is completely empty.
  * **Scenario B**: `dwtk_meldungen` contains multiple records for `BERT_DROP_TEMP_TABLE`.
* **Action**: Execute the metadata declaration block of the SQL script.
* **Pass/Fail Criterion**:
  * Scenario A must fall back to `'19000101'`.
  * Scenario B must resolve to the formatted string of the maximum `timecreated` timestamp.

```python
def test_metadata_date_resolution(bq_client, test_env):
    # Scenario A: Empty Table
    bq_client.query(f"TRUNCATE TABLE `{test_env['src_meldungen']}`").result()
    
    test_query_a = f"""
        DECLARE v_datum STRING;
        SET v_datum = (
          SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')
          FROM `{test_env['src_meldungen']}` m
          WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
        );
        SELECT v_datum;
    """
    result_a = list(bq_client.query(test_query_a).result())
    assert result_a[0][0] == "19000101", "Expected default date '19000101' when table is empty"

    # Scenario B: Multiple Records
    bq_client.query(f"""
        INSERT INTO `{test_env['src_meldungen']}` (job_kennung, timecreated)
        VALUES 
          ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2026-01-10 10:00:00')),
          ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2026-01-15 15:30:00')),
          ('OTHER_JOB', TIMESTAMP('2026-01-20 12:00:00'))
    """).result()
    
    test_query_b = f"""
        DECLARE v_datum STRING;
        SET v_datum = (
          SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')
          FROM `{test_env['src_meldungen']}` m
          WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
        );
        SELECT v_datum;
    """
    result_b = list(bq_client.query(test_query_b).result())
    assert result_b[0][0] == "20260115", "Expected max date '20260115' matching the latest job execution"
```

---

## Section 3: Idempotency & Truncate Validation

### Test Case 3.1: Target Table Truncation and Idempotency
* **Purpose**: Ensure that the target table `sof_ta_bpr_bcp` is completely cleared before new records are inserted, preventing data accumulation across multiple runs.
* **Setup**:
  1. Populate the target table `sof_ta_bpr_bcp` with stale records.
  2. Populate the source table `sof_ta_bpr_instance` with a new set of records.
* **Action**: Execute the SQL script twice in succession.
* **Pass/Fail Criterion**:
  * After the first execution, all stale records are gone, and only the new records exist.
  * After the second execution, the row count and data remain identical to the first execution (proving idempotency).

```python
def test_idempotency_and_truncate(bq_client, test_env):
    # 1. Populate Target with Stale Data
    bq_client.query(f"TRUNCATE TABLE `{test_env['tgt_bcp']}`").result()
    bq_client.query(f"""
        INSERT INTO `{test_env['tgt_bcp']}` (cntrct_id, bpr_id, cntrct_id_ref)
        VALUES ('STALE_01', '3142', 'REF_STALE')
    """).result()

    # 2. Populate Source with New Data
    bq_client.query(f"TRUNCATE TABLE `{test_env['src_instance']}`").result()
    bq_client.query(f"""
        INSERT INTO `{test_env['src_instance']}` (cntrct_id, bpr_id, cntrct_id_ref)
        VALUES ('NEW_01', '3142', 'REF_NEW')
    """).result()

    # 3. Action: Run the script first time
    sql_path = "src/sql/d_ausd_bp_ta_bpr_bcp.sql"
    with open(sql_path, "r") as f:
        sql_script = f.read().replace("`isbert_schema.", f"`{test_env['project_id']}.{test_env['dataset_id']}.")
    
    bq_client.query(sql_script).result()

    # Assert stale data is gone and only new data exists
    res1 = list(bq_client.query(f"SELECT * FROM `{test_env['tgt_bcp']}`").result())
    assert len(res1) == 1
    assert res1[0]["cntrct_id"] == "NEW_01"

    # 4. Action: Run the script second time
    bq_client.query(sql_script).result()

    # Assert data remains identical (Idempotent)
    res2 = list(bq_client.query(f"SELECT * FROM `{test_env['tgt_bcp']}`").result())
    assert len(res2) == 1
    assert res2[0]["cntrct_id"] == "NEW_01"
```

---

## Section 4: Null & Edge-Case Handling

### Test Case 4.1: Null Handling in Key Fields
* **Purpose**: Verify that the query handles NULL values in `cntrct_id`, `bpr_id`, and `cntrct_id_ref` gracefully without throwing runtime errors.
* **Setup**:
  * Insert records into `sof_ta_bpr_instance` where:
    * `cntrct_id` is NULL.
    * `bpr_id` is NULL.
    * `cntrct_id_ref` is NULL.
* **Action**: Execute the SQL script.
* **Pass/Fail Criterion**:
  * The script executes successfully.
  * Rows with NULL `bpr_id` are filtered out (since they cannot equal `'3142'`).
  * Rows with NULL `cntrct_id` or `cntrct_id_ref` but valid `bpr_id = '3142'` are successfully inserted.

```python
def test_null_handling_in_source(bq_client, test_env):
    # 1. Setup Source Data with NULLs
    bq_client.query(f"TRUNCATE TABLE `{test_env['src_instance']}`").result()
    bq_client.query(f"TRUNCATE TABLE `{test_env['tgt_bcp']}`").result()
    bq_client.query(f"""
        INSERT INTO `{test_env['src_instance']}` (cntrct_id, bpr_id, cntrct_id_ref)
        VALUES 
          (NULL, '3142', 'REF_01'),       -- NULL Contract ID (Valid BPR)
          ('CON_02', '3142', NULL),       -- NULL Reference ID (Valid BPR)
          ('CON_03', NULL, 'REF_03')      -- NULL BPR ID (Should be filtered out)
    """).result()

    # 2. Action
    sql_path = "src/sql/d_ausd_bp_ta_bpr_bcp.sql"
    with open(sql_path, "r") as f:
        sql_script = f.read().replace("`isbert_schema.", f"`{test_env['project_id']}.{test_env['dataset_id']}.")
    bq_client.query(sql_script).result()

    # 3. Assertions
    results = list(bq_client.query(f"SELECT cntrct_id, bpr_id, cntrct_id_ref FROM `{test_env['tgt_bcp']}`").result())
    
    assert len(results) == 2, "Expected exactly 2 rows to be migrated"
    
    # Verify that the row with NULL bpr_id was excluded
    for row in results:
        assert row["bpr_id"] == "3142"
        if row["cntrct_id"] is None:
            assert row["cntrct_id_ref"] == "REF_01"
        if row["cntrct_id_ref"] is None:
            assert row["cntrct_id"] == "CON_02"
```

---

## Section 5: Schema & Data Quality Assertions

### Test Case 5.1: Target Table Schema Validation
* **Purpose**: Verify that the target table `sof_ta_bpr_bcp` matches the expected schema structure (column names, data types, and nullability) in BigQuery.
* **Setup**: None (reads BigQuery metadata).
* **Action**: Query `INFORMATION_SCHEMA.COLUMNS` for the target table.
* **Pass/Fail Criterion**:
  * The table must contain exactly three columns: `CNTRCT_ID`, `BPR_ID`, and `CNTRCT_ID_REF`.
  * Data types must match the expected types (e.g., `STRING`).

```python
def test_target_schema_structure(bq_client, test_env):
    query = f"""
        SELECT column_name, data_type, is_nullable
        FROM `{test_env['project_id']}.{test_env['dataset_id']}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'sof_ta_bpr_bcp'
    """
    columns = {row["column_name"].upper(): row for row in bq_client.query(query).result()}
    
    # Assert expected columns exist
    assert "CNTRCT_ID" in columns, "Missing column CNTRCT_ID"
    assert "BPR_ID" in columns, "Missing column BPR_ID"
    assert "CNTRCT_ID_REF" in columns, "Missing column CNTRCT_ID_REF"
    
    # Assert data types (Assuming STRING based on legacy character mappings)
    assert columns["CNTRCT_ID"]["data_type"] == "STRING"
    assert columns["BPR_ID"]["data_type"] == "STRING"
    assert columns["CNTRCT_ID_REF"]["data_type"] == "STRING"
```

---

## Section 6: Airflow DAG Orchestration Validation

### Test Case 6.1: DAG Compilation and Structure Test
* **Purpose**: Verify that the Airflow DAG `dw_bert_ausd_bp_ta_bpr_bcp` compiles without syntax or import errors and contains the correct task structure.
* **Setup**: Add the DAG file path to the Python path.
* **Action**: Load the DAG using the Airflow Bag.
* **Pass/Fail Criterion**:
  * No import errors are raised.
  * The DAG contains the task `execute_bpr_bcp_processing`.
  * The task uses the `BigQueryExecuteQueryOperator`.

```python
from airflow.models import DagBag

def test_dag_imports_and_structure():
    dag_bag = DagBag(dag_folder="src/dags", include_examples=False)
    
    # Assert no import errors
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"
    
    # Assert DAG exists
    dag_id = "dw_bert_ausd_bp_ta_bpr_bcp"
    dag = dag_bag.get_dag(dag_id)
    assert dag is not None, f"DAG {dag_id} not found"
    
    # Assert Task exists and is configured correctly
    task_id = "execute_bpr_bcp_processing"
    assert dag.has_task(task_id), f"Task {task_id} not found in DAG"
    
    task = dag.get_task(task_id)
    from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
    assert isinstance(task, BigQueryExecuteQueryOperator), "Task is not a BigQueryExecuteQueryOperator"
```