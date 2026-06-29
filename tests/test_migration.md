# Migration Validation Test Suite: `ausd_bp_ta_apn_carmen`

This document defines the migration-validation test suite to verify that the migrated Google Cloud BigQuery and Cloud Composer (Airflow) implementation of the `ausd_bp_ta_apn_carmen` job is behaviorally equivalent to the legacy Oracle/UC4 implementation.

---

## Test Case 1: End-to-End Output Parity (Golden Dataset Test)

### Purpose
To prove that given identical source data in both the legacy Oracle environment and the migrated BigQuery environment, the target tables (`sof$ta_apn_carmen` in Oracle and `sof.ta_apn_carmen` in BigQuery) produce identical outputs.

### Setup
1. **Legacy Environment (Oracle)**:
   * Populate the source tables with a controlled "golden" dataset containing 100 test cases (covering standard joins, boundary dates, and nulls).
   * Populate `isbert_schema.dwtk_meldungen` with a record where `job_kennung = 'BERT_DROP_TEMP_TABLE'` and `timecreated = TO_DATE('2023-10-15 08:00:00', 'YYYY-MM-DD HH24:MI:SS')`.
2. **Target Environment (BigQuery)**:
   * Replicate the exact same golden dataset into the BigQuery staging tables:
     * `your-gcp-project.src_carmen.pds_ta_pdp_context_assoc`
     * `your-gcp-project.src_carmen.pds_ta_pdp_context`
     * `your-gcp-project.src_carmen.pds_ta_access_point`
     * `your-gcp-project.isbert_schema.dwtk_meldungen`

### Action
1. Execute the legacy Oracle job `d_ausd_bp_ta_apn_carmen.sql` via SQL*Plus.
2. Execute the migrated BigQuery SQL script (or trigger the Airflow DAG task `run_ta_apn_carmen_transformation`).
3. Extract the contents of both target tables to a local environment for comparison.

### Pass/Fail Criterion
The test passes if the row count and the MD5 hash of the sorted datasets are identical.

```python
# pytest / python validation script
import pandas as pd
import hashlib
from google.cloud import bigquery
import oracledb  # Modern Oracle driver

def test_e2e_output_parity():
    # 1. Fetch from Oracle Target
    conn = oracledb.connect(user="user", password="pwd", dsn="oracle_host:1521/service")
    oracle_query = "SELECT CNTRCT_ID, ACCESS_POINT_NAME FROM sof$ta_apn_carmen ORDER BY CNTRCT_ID, ACCESS_POINT_NAME"
    df_oracle = pd.read_sql(oracle_query, con=conn)
    conn.close()

    # 2. Fetch from BigQuery Target
    bq_client = bigquery.Client(project="your-gcp-project")
    bq_query = """
        SELECT CNTRCT_ID, ACCESS_POINT_NAME 
        FROM `your-gcp-project.sof.ta_apn_carmen` 
        ORDER BY CNTRCT_ID, ACCESS_POINT_NAME
    """
    df_bq = bq_client.query(bq_query).to_dataframe()

    # 3. Assert Row Count
    assert len(df_oracle) == len(df_bq), f"Row count mismatch: Oracle ({len(df_oracle)}) vs BigQuery ({len(df_bq)})"

    # 4. Assert Content Equivalence
    # Convert columns to identical types to prevent false mismatches
    df_oracle['CNTRCT_ID'] = df_oracle['CNTRCT_ID'].astype('Int64')
    df_bq['CNTRCT_ID'] = df_bq['CNTRCT_ID'].astype('Int64')
    df_oracle['ACCESS_POINT_NAME'] = df_oracle['ACCESS_POINT_NAME'].astype(str)
    df_bq['ACCESS_POINT_NAME'] = df_bq['ACCESS_POINT_NAME'].astype(str)

    pd.testing.assert_frame_equal(df_oracle, df_bq, check_dtype=True, obj="Target Tables Comparison")
```

---

## Test Case 2: Transformation Correctness (Temporal Filtering & Edge Cases)

### Purpose
To validate that the complex point-in-time temporal logic (using `insert_at`, `modified_at`, `valid_from`, and `valid_to` against `v_datum`) correctly filters records.

### Setup
Insert test records into BigQuery staging tables with a reference date (`v_datum`) of `2023-10-15` (derived from `timecreated = '2023-10-15 12:00:00'`).

| Scenario ID | Description | `insert_at` | `modified_at` | `valid_from` | `valid_to` | `is_production` | `cntrct_id` | Expected Outcome |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **TC2-01** | Standard Active Record | `2023-10-01` | `NULL` | `2023-10-01` | `NULL` | `1` | `1001` | **Included** |
| **TC2-02** | Modified in Future | `2023-10-01` | `2023-10-20` | `2023-10-01` | `NULL` | `1` | `1002` | **Included** |
| **TC2-03** | Modified in Past | `2023-10-01` | `2023-10-10` | `2023-10-01` | `NULL` | `1` | `1003` | **Excluded** (Modified before `v_datum`) |
| **TC2-04** | Valid in Future | `2023-10-01` | `NULL` | `2023-10-20` | `NULL` | `1` | `1004` | **Excluded** (`valid_from` > `v_datum`) |
| **TC2-05** | Expired in Past | `2023-10-01` | `NULL` | `2023-10-01` | `2023-10-14` | `1` | `1005` | **Excluded** (`valid_to` <= `v_datum`) |
| **TC2-06** | Non-Production | `2023-10-01` | `NULL` | `2023-10-01` | `NULL` | `0` | `1006` | **Excluded** (`is_production` != 1) |
| **TC2-07** | Null Contract ID | `2023-10-01` | `NULL` | `2023-10-01` | `NULL` | `1` | `NULL` | **Excluded** (`cntrct_id IS NOT NULL`) |

### Action
1. Populate the BigQuery tables with the scenarios above.
2. Run the BigQuery SQL script.
3. Query the target table `sof.ta_apn_carmen`.

### Pass/Fail Criterion
The target table must contain exactly **2 rows** (Contract IDs `1001` and `1002`). All other contract IDs must be excluded.

```sql
-- SQL Assertion Script
DECLARE actual_count INT64;
DECLARE expected_count INT64 SET 2;

-- Check target table contents
SELECT COUNT(1) INTO actual_count 
FROM `your-gcp-project.sof.ta_apn_carmen`
WHERE CNTRCT_ID IN (1001, 1002);

-- Assert correct records are present and incorrect ones are absent
ASSERT actual_count = expected_count 
  AS "ERROR: Temporal filtering failed. Expected 2 records, found " || CAST(actual_count AS STRING);

ASSERT (SELECT COUNT(1) FROM `your-gcp-project.sof.ta_apn_carmen` WHERE CNTRCT_ID IN (1003, 1004, 1005, 1006)) = 0
  AS "ERROR: Excluded temporal records leaked into target table!";
```

---

## Test Case 3: Metadata & Fallback Handling (`v_datum`)

### Purpose
To verify that the reference date (`v_datum`) is correctly extracted from `isbert_schema.dwtk_meldungen` and that the fallback logic works correctly when no metadata record exists.

### Setup
* **Scenario A (Standard)**: `dwtk_meldungen` contains multiple records for `BERT_DROP_TEMP_TABLE`.
* **Scenario B (Fallback)**: `dwtk_meldungen` contains no records for `BERT_DROP_TEMP_TABLE`.

### Action
1. **For Scenario A**:
   * Insert two records into `dwtk_meldungen`:
     * `job_kennung = 'BERT_DROP_TEMP_TABLE'`, `timecreated = '2023-10-10 00:00:00'`
     * `job_kennung = 'BERT_DROP_TEMP_TABLE'`, `timecreated = '2023-10-15 00:00:00'`
   * Execute the `v_datum` extraction query.
2. **For Scenario B**:
   * Truncate/Clear `dwtk_meldungen` of any `BERT_DROP_TEMP_TABLE` records.
   * Execute the `v_datum` extraction query.

### Pass/Fail Criterion
* **Scenario A**: `v_datum` must resolve to `'20231015'` (the maximum date).
* **Scenario B**: `v_datum` must resolve to `'19000101'` (the fallback default).

```sql
-- Scenario A Test
DECLARE v_datum_a STRING;
SET v_datum_a = (
  WITH mock_dwtk AS (
    SELECT 'BERT_DROP_TEMP_TABLE' AS job_kennung, TIMESTAMP('2023-10-10 00:00:00') AS timecreated
    UNION ALL
    SELECT 'BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-10-15 00:00:00')
  )
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(timecreated))), '19000101')
  FROM mock_dwtk
  WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
);
ASSERT v_datum_a = '20231015' AS "Scenario A Failed: Expected '20231015', got " || v_datum_a;

-- Scenario B Test
DECLARE v_datum_b STRING;
SET v_datum_b = (
  WITH mock_dwtk AS (
    SELECT CAST(NULL AS STRING) AS job_kennung, CAST(NULL AS TIMESTAMP) AS timecreated
  )
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(timecreated))), '19000101')
  FROM mock_dwtk
  WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
);
ASSERT v_datum_b = '19000101' AS "Scenario B Failed: Expected '19000101', got " || v_datum_b;
```

---

## Test Case 4: Target Truncation & Idempotency

### Purpose
To verify that the target table `sof.ta_apn_carmen` is successfully truncated before every run, ensuring that the job is fully idempotent and does not append duplicate records on retries.

### Setup
1. Insert 5 dummy rows into `sof.ta_apn_carmen`.
2. Set up the source tables with data that will produce exactly 3 rows when processed.

### Action
1. Execute the BigQuery SQL script.
2. Query the target table `sof.ta_apn_carmen`.

### Pass/Fail Criterion
The target table must contain exactly **3 rows** (the 5 pre-existing dummy rows must be completely removed by the `TRUNCATE` step).

```python
def test_idempotency_and_truncate(bq_client):
    target_table_id = "your-gcp-project.sof.ta_apn_carmen"
    
    # 1. Seed target table with dummy data
    dummy_data = [
        {"CNTRCT_ID": 99991, "ACCESS_POINT_NAME": "dummy.apn.1"},
        {"CNTRCT_ID": 99992, "ACCESS_POINT_NAME": "dummy.apn.2"}
    ]
    bq_client.insert_rows_json(target_table_id, dummy_data)
    
    # Verify dummy data is there
    pre_count = bq_client.query(f"SELECT COUNT(1) FROM `{target_table_id}`").to_dataframe().iloc[0, 0]
    assert pre_count >= 2
    
    # 2. Run the migration SQL script
    # (Assuming the script is stored locally or executed via client)
    with open("d_ausd_bp_ta_apn_carmen.sql", "r") as f:
        sql_script = f.read()
        
    query_job = bq_client.query(sql_script)
    query_job.result() # Wait for execution
    
    # 3. Assert that dummy data is gone and only new data exists
    post_df = bq_client.query(f"SELECT * FROM `{target_table_id}` WHERE CNTRCT_ID IN (99991, 99992)").to_dataframe()
    assert len(post_df) == 0, "Truncate failed! Pre-existing dummy records still exist in the target table."
```

---

## Test Case 5: Schema & Data Quality Assertions

### Purpose
To verify that the target table schema matches the design specifications and that no invalid data types or null values violate constraints.

### Setup
The target table `sof.ta_apn_carmen` must be deployed in the target environment.

### Action
Query the BigQuery `INFORMATION_SCHEMA.COLUMNS` view to validate the schema structure.

### Pass/Fail Criterion
The schema must strictly match:
* `CNTRCT_ID` -> `INT64`
* `ACCESS_POINT_NAME` -> `STRING`

Additionally, there must be no records in the target table where `CNTRCT_ID` is `NULL`.

```sql
-- Schema Validation Query
SELECT 
  column_name, 
  data_type,
  is_nullable
FROM `your-gcp-project.sof.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'ta_apn_carmen';

-- Assertions to run post-execution
ASSERT (
  SELECT COUNT(1) 
  FROM `your-gcp-project.sof.ta_apn_carmen` 
  WHERE CNTRCT_ID IS NULL
) = 0 AS "DATA QUALITY ERROR: CNTRCT_ID contains NULL values!";
```