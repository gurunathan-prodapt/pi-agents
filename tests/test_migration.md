# Migration Validation Test Suite: `ausd_bp_ta_bpr_opt_text`

This document defines the migration-validation test suite to verify that the migrated Apache Airflow and Google Cloud BigQuery assets for the job `ausd_bp_ta_bpr_opt_text` are behaviorally equivalent to the legacy UC4/KornShell/Oracle implementation.

---

## Test Suite Overview

The validation strategy is divided into five core test cases:
1. **End-to-End Output Parity**: Verifies that identical inputs in Oracle and BigQuery yield identical outputs.
2. **Transformation & Join Correctness**: Validates the inner join logic, handling of missing keys, and NULL values.
3. **Parameter Parsing & Metadata Logging**: Validates that Airflow runtime parameters are correctly parsed and logged to `PoolBasisprodukt`.
4. **Idempotency & Restartability**: Proves that the `TRUNCATE` step prevents duplicate data on job retries.
5. **Schema & Data Quality Assertions**: Enforces structural integrity and data-type constraints in BigQuery.

---

## Section 1: End-to-End Output Parity

### Purpose
To prove that the BigQuery transformation produces the exact same output as the legacy Oracle job when provided with identical source datasets.

### Setup
1. **Legacy Environment (Oracle)**:
   * Populate `sof$ta_bpr_optionen` and `sof$ta_bpr_beschr` with a controlled test dataset of 1,000 rows (including edge cases: long text descriptions, special characters, and German umlauts).
   * Clear the target table: `TRUNCATE TABLE sof$ta_bpr_opt_text;`
2. **Target Environment (BigQuery)**:
   * Populate `isbert_schema.sof_ta_bpr_optionen` and `isbert_schema.sof_ta_bpr_beschr` with the exact same 1,000 rows.
   * Clear the target table: `TRUNCATE TABLE isbert_schema.sof_ta_bpr_opt_text;`

### Action
1. Execute the legacy Oracle SQL script `d_ausd_bp_ta_bpr_opt_text.sql` via SQL*Plus.
2. Execute the migrated BigQuery SQL script `gcp/bigquery/sql/d_ausd_bp_ta_bpr_opt_text.sql` using the BigQuery client.
3. Extract the contents of both target tables, sorting them by `cntrct_id` and `bpr_id`.
4. Compute and compare MD5 checksums of the concatenated columns for both outputs.

### Pass/Fail Criterion
* **Pass**: The row counts match exactly, and the MD5 checksum of the sorted output from Oracle matches the MD5 checksum of the sorted output from BigQuery.
* **Fail**: Any discrepancy in row count, column values, or character encoding (e.g., corrupted German umlauts like ä, ö, ü, ß).

### Test Code (Pytest)
```python
import hashlib
import pytest
from google.cloud import bigquery
import cx_Oracle

def get_oracle_data(conn_str):
    query = """
        SELECT cntrct_id, bpr_id, pds_description 
        FROM sof$ta_bpr_opt_text 
        ORDER BY cntrct_id, bpr_id
    """
    with cx_Oracle.connect(conn_str) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

def get_bigquery_data(client, project_id, dataset_id):
    query = f"""
        SELECT cntrct_id, bpr_id, pds_description 
        FROM `{project_id}.{dataset_id}.sof_ta_bpr_opt_text` 
        ORDER BY cntrct_id, bpr_id
    """
    query_job = client.query(query)
    return [tuple(row.values()) for row in query_job.result()]

def generate_hash(data_rows):
    hasher = hashlib.md5()
    for row in data_rows:
        # Normalize None/Null to empty string and encode to UTF-8
        normalized_row = tuple("" if val is None else str(val) for val in row)
        hasher.update(str(normalized_row).encode('utf-8'))
    return hasher.hexdigest()

def test_end_to_end_parity():
    # Configurations
    oracle_conn_str = "user/password@host:port/service"
    bq_client = bigquery.Client()
    gcp_project = "prj-dwh-prod-1234"
    bq_dataset = "isbert_schema"
    
    # Fetch data
    oracle_rows = get_oracle_data(oracle_conn_str)
    bq_rows = get_bigquery_data(bq_client, gcp_project, bq_dataset)
    
    # Assertions
    assert len(oracle_rows) == len(bq_rows), f"Row count mismatch! Oracle: {len(oracle_rows)}, BQ: {len(bq_rows)}"
    
    oracle_hash = generate_hash(oracle_rows)
    bq_hash = generate_hash(bq_rows)
    
    assert oracle_hash == bq_hash, "Data content mismatch detected between Oracle and BigQuery target tables!"
```

---

## Section 2: Transformation & Join Correctness

### Purpose
To verify that the inner join on `bpr_id` behaves correctly under edge-case scenarios, including unmatched keys, duplicate keys, and NULL values.

### Setup
Populate the BigQuery source tables with the following specific test patterns:
* **Pattern A (Standard Match)**: `bpr_id = 'BPR001'` exists in both tables.
* **Pattern B (Orphan Option)**: `bpr_id = 'BPR002'` exists in `sof_ta_bpr_optionen` but NOT in `sof_ta_bpr_beschr`.
* **Pattern C (Orphan Description)**: `bpr_id = 'BPR003'` exists in `sof_ta_bpr_beschr` but NOT in `sof_ta_bpr_optionen`.
* **Pattern D (NULL Key)**: A row in `sof_ta_bpr_optionen` has `bpr_id = NULL`.
* **Pattern E (Duplicate Descriptions)**: `bpr_id = 'BPR005'` has two different description records in `sof_ta_bpr_beschr`.

### Action
1. Execute the BigQuery SQL script `gcp/bigquery/sql/d_ausd_bp_ta_bpr_opt_text.sql`.
2. Query the target table `sof_ta_bpr_opt_text` to verify which patterns were loaded.

### Pass/Fail Criterion
* **Pass**:
  * Pattern A is successfully joined and loaded.
  * Pattern B is excluded (inner join constraint).
  * Pattern C is excluded (inner join constraint).
  * Pattern D is excluded (NULL keys cannot join).
  * Pattern E results in a Cartesian product (two rows in target for the single option contract), matching standard SQL inner join behavior.
* **Fail**: Any orphan or NULL key records are found in the target table, or duplicate mappings are lost.

### Test Code (SQL Assertions)
```sql
-- Assertions to be executed in BigQuery after running the transformation
SELECT
  -- 1. Verify Pattern A is present
  ASSERT(
    (SELECT COUNT(1) FROM `isbert_schema.sof_ta_bpr_opt_text` WHERE bpr_id = 'BPR001') = 1,
    'Error: Standard match Pattern A missing!'
  ),
  
  -- 2. Verify Pattern B (Orphan Option) is excluded
  ASSERT(
    (SELECT COUNT(1) FROM `isbert_schema.sof_ta_bpr_opt_text` WHERE bpr_id = 'BPR002') = 0,
    'Error: Orphan option Pattern B was not excluded!'
  ),
  
  -- 3. Verify Pattern C (Orphan Description) is excluded
  ASSERT(
    (SELECT COUNT(1) FROM `isbert_schema.sof_ta_bpr_opt_text` WHERE bpr_id = 'BPR003') = 0,
    'Error: Orphan description Pattern C was not excluded!'
  ),
  
  -- 4. Verify Pattern D (NULL Key) is excluded
  ASSERT(
    (SELECT COUNT(1) FROM `isbert_schema.sof_ta_bpr_opt_text` WHERE bpr_id IS NULL) = 0,
    'Error: NULL bpr_id records found in target table!'
  ),
  
  -- 5. Verify Pattern E (Duplicate Descriptions) created exactly 2 rows
  ASSERT(
    (SELECT COUNT(1) FROM `isbert_schema.sof_ta_bpr_opt_text` WHERE bpr_id = 'BPR005') = 2,
    'Error: Duplicate descriptions Pattern E did not generate 2 target rows!'
  );
```

---

## Section 3: Parameter Parsing & Metadata Logging

### Purpose
To verify that the Airflow DAG correctly parses runtime parameters (`stichtag`, `job_kennung`, `eintrags_nr`, `wiederanlauf_wert`) from the DAG run configuration, applies default values when parameters are missing, and writes accurate execution logs to `PoolBasisprodukt`.

### Setup
1. Clear the metadata log table: `TRUNCATE TABLE isbert_schema.PoolBasisprodukt;`
2. Ensure `sof_ta_bpr_opt_text` contains a known number of records (e.g., exactly 150 rows).

### Action
1. Trigger the Airflow DAG `bereitstellung_basisprodukte_bert` with the following custom configuration JSON:
   ```json
   {
     "stichtag": "24122024",
     "job_kennung": "QA_TEST_RUN_12",
     "eintrags_nr": "456",
     "wiederanlauf_wert": "9999"
   }
   ```
2. Trigger the Airflow DAG a second time with an **empty** configuration JSON `{}` to test default fallback logic.

### Pass/Fail Criterion
* **Pass**:
  * **Run 1 (Custom Params)**:
    * `job_kennung` is logged as `'QA_TEST_RUN_12'`.
    * `eintrags_nr` is logged as `'456'`.
    * `stichtag` is parsed as `DATE '2024-12-24'`.
    * `wiederanlauf_wert` is logged as `9999`.
    * `source_record_count` is logged as `150`.
    * `status` is `'A'` and `note` is `'Initialbefuellung'`.
  * **Run 2 (Default Params)**:
    * `job_kennung` defaults to `'ausd_bp_ta_bpr_opt_text'`.
    * `eintrags_nr` defaults to `'0'`.
    * `stichtag` defaults to the current system date (`CURRENT_DATE()`).
    * `wiederanlauf_wert` defaults to `0`.
* **Fail**: Any parameter is parsed incorrectly, dates fail to parse, or the DAG run fails with a Python type error.

### Test Code (SQL Assertions)
```sql
-- Assertions for Run 1 (Custom Parameters)
SELECT
  ASSERT(
    (SELECT COUNT(1) FROM `isbert_schema.PoolBasisprodukt` 
     WHERE job_kennung = 'QA_TEST_RUN_12' 
       AND eintrags_nr = '456' 
       AND stichtag = '2024-12-24' 
       AND wiederanlauf_wert = 9999
       AND source_record_count = 150
       AND status = 'A'
       AND note = 'Initialbefuellung') = 1,
    'Error: Custom parameters were not logged correctly in PoolBasisprodukt!'
  );

-- Assertions for Run 2 (Default Parameters)
SELECT
  ASSERT(
    (SELECT COUNT(1) FROM `isbert_schema.PoolBasisprodukt` 
     WHERE job_kennung = 'ausd_bp_ta_bpr_opt_text' 
       AND eintrags_nr = '0' 
       AND stichtag = CURRENT_DATE() 
       AND wiederanlauf_wert = 0
       AND status = 'A') = 1,
    'Error: Default fallback parameters were not logged correctly in PoolBasisprodukt!'
  );
```

---

## Section 4: Idempotency & Restartability

### Purpose
To prove that the job is fully idempotent and safe to restart. Multiple consecutive executions must not duplicate data in the target table `sof_ta_bpr_opt_text`.

### Setup
1. Populate source tables `sof_ta_bpr_optionen` and `sof_ta_bpr_beschr` with 100 matching records.
2. Insert 50 dummy/stale records directly into `sof_ta_bpr_opt_text` to simulate a dirty state from a previous failed run.

### Action
1. Execute the BigQuery SQL script `gcp/bigquery/sql/d_ausd_bp_ta_bpr_opt_text.sql` (Run 1).
2. Record the row count of `sof_ta_bpr_opt_text`.
3. Immediately execute the BigQuery SQL script again (Run 2).
4. Record the row count of `sof_ta_bpr_opt_text` again.

### Pass/Fail Criterion
* **Pass**:
  * After Run 1, the 50 stale records are completely removed, and exactly 100 records exist.
  * After Run 2, the table still contains exactly 100 records (no duplication).
* **Fail**: Stale records persist after execution, or row counts double on the second run.

### Test Code (Pytest)
```python
def test_idempotency(bq_client):
    project_id = "prj-dwh-prod-1234"
    dataset_id = "isbert_schema"
    target_table = f"`{project_id}.{dataset_id}.sof_ta_bpr_opt_text`"
    
    # 1. Setup: Insert stale records
    setup_query = f"""
        INSERT INTO {target_table} (cntrct_id, bpr_id, pds_description)
        VALUES ('STALE_01', 'STALE_BPR', 'Stale Description')
    """
    bq_client.query(setup_query).result()
    
    # Run 1
    with open("gcp/bigquery/sql/d_ausd_bp_ta_bpr_opt_text.sql", "r") as f:
        sql_script = f.read()
    bq_client.query(sql_script).result()
    
    # Verify stale records are gone and only fresh joins exist
    count_job_1 = bq_client.query(f"SELECT COUNT(1) FROM {target_table}")
    count_1 = list(count_job_1.result())[0][0]
    
    # Run 2 (Simulating immediate restart)
    bq_client.query(sql_script).result()
    count_job_2 = bq_client.query(f"SELECT COUNT(1) FROM {target_table}")
    count_2 = list(count_job_2.result())[0][0]
    
    assert count_1 > 0, "Target table is empty after execution!"
    assert count_1 == count_2, f"Job is not idempotent! Run 1 count: {count_1}, Run 2 count: {count_2}"
    
    # Verify stale record is not present
    stale_check = bq_client.query(f"SELECT COUNT(1) FROM {target_table} WHERE cntrct_id = 'STALE_01'")
    stale_count = list(stale_check.result())[0][0]
    assert stale_count == 0, "Stale records were not truncated during execution!"
```

---

## Section 5: Schema & Data Quality Assertions

### Purpose
To verify that the target tables in BigQuery conform to the expected schema definitions, column types, and nullability constraints.

### Setup
Ensure target tables `sof_ta_bpr_opt_text` and `PoolBasisprodukt` have been created in the target BigQuery dataset.

### Action
Query the BigQuery `INFORMATION_SCHEMA.COLUMNS` view for both tables and assert metadata properties.

### Pass/Fail Criterion
* **Pass**:
  * Column names and data types match the target specification exactly.
  * Special characters (like `$`) are successfully replaced with underscores (`_`).
  * `stichtag` is defined as `DATE`.
  * `wiederanlauf_wert` and `source_record_count` are defined as `INT64`.
* **Fail**: Any column mismatch, incorrect data type, or unexpected nullability setting.

### Test Code (Pytest)
```python
def test_schema_assertions(bq_client):
    project_id = "prj-dwh-prod-1234"
    dataset_id = "isbert_schema"
    
    # Expected schema for sof_ta_bpr_opt_text
    expected_opt_text_schema = {
        "cntrct_id": "STRING",
        "bpr_id": "STRING",
        "pds_description": "STRING"
    }
    
    # Expected schema for PoolBasisprodukt
    expected_pool_schema = {
        "job_kennung": "STRING",
        "eintrags_nr": "STRING",
        "stichtag": "DATE",
        "wiederanlauf_wert": "INT64",
        "created_at": "TIMESTAMP",
        "source_record_count": "INT64",
        "status": "STRING",
        "note": "STRING"
    }
    
    # Query actual schema for sof_ta_bpr_opt_text
    query_opt = f"""
        SELECT column_name, data_type 
        FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'sof_ta_bpr_opt_text'
    """
    actual_opt_schema = {row["column_name"]: row["data_type"] for row in bq_client.query(query_opt).result()}
    
    # Query actual schema for PoolBasisprodukt
    query_pool = f"""
        SELECT column_name, data_type 
        FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'PoolBasisprodukt'
    """
    actual_pool_schema = {row["column_name"]: row["data_type"] for row in bq_client.query(query_pool).result()}
    
    # Assertions
    assert actual_opt_schema == expected_opt_text_schema, f"Schema mismatch for sof_ta_bpr_opt_text! Expected: {expected_opt_text_schema}, Got: {actual_opt_schema}"
    assert actual_pool_schema == expected_pool_schema, f"Schema mismatch for PoolBasisprodukt! Expected: {expected_pool_schema}, Got: {actual_pool_schema}"
```