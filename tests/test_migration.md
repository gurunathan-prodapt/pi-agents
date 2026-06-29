This document provides a comprehensive suite of migration-validation tests for the BigQuery job `ausd_bp_ta_bpr_apn`. These tests are designed to prove that the migrated GCP pipeline is behaviorally equivalent to the legacy Oracle/UC4 implementation.

---

## Test Case 1: End-to-End Output Parity (Reconciliation)

### Purpose
To prove that running the migrated BigQuery pipeline with a snapshot of legacy source data produces the exact same output dataset as the legacy Oracle execution, down to row counts, column values, and sorting.

### Setup
1. Extract a static snapshot of the legacy Oracle source tables: `sof$ta_bpr_instance` and `sof$ta_apn_carmen`.
2. Run the legacy Oracle SQL script `d_ausd_bp_ta_bpr_apn.sql` on these source tables to populate the legacy target table `sof$ta_bpr_apn`. Export this target dataset to a CSV file (`oracle_expected_output.csv`).
3. Load the same source snapshots into the BigQuery staging tables: `dw_bert.sof_ta_bpr_instance` and `dw_bert.sof_ta_apn_carmen`.
4. Ensure the target table `dw_bert.sof_ta_bpr_apn` is empty before execution.

### Action
Execute the BigQuery DML script `d_ausd_bp_ta_bpr_apn.sql` via a test runner or Airflow task, then execute a Python validation script to compare the BigQuery target table contents with the exported Oracle target dataset.

```python
# test_output_parity.py
import pandas as pd
from google.cloud import bigquery
import pytest

def test_reconcile_oracle_vs_bigquery():
    # 1. Load expected Oracle output
    oracle_df = pd.read_csv("oracle_expected_output.csv")
    # Normalize column names to uppercase for comparison
    oracle_df.columns = [col.upper() for col in oracle_df.columns]
    oracle_df = oracle_df.sort_values(by=["CNTRCT_ID", "BPR_ID", "CNTRCT_ID_REF"]).reset_index(drop=True)

    # 2. Fetch actual BigQuery output
    client = bigquery.Client()
    query = """
        SELECT CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, ACCESS_POINT_NAME 
        FROM `gcp-enterprise-dwh.dw_bert.sof_ta_bpr_apn`
        ORDER BY CNTRCT_ID, BPR_ID, CNTRCT_ID_REF
    """
    bq_df = client.query(query).to_dataframe()
    bq_df.columns = [col.upper() for col in bq_df.columns]

    # 3. Assertions
    # Check Row Counts
    assert len(oracle_df) == len(bq_df), f"Row count mismatch! Oracle: {len(oracle_df)}, BigQuery: {len(bq_df)}"
    
    # Check Data Equivalence (handling potential nulls gracefully)
    pd.testing.assert_frame_equal(
        oracle_df, 
        bq_df, 
        check_dtype=False, 
        check_exact=False, 
        atol=1e-5
    )
```

### Pass/Fail Criterion
*   **Pass**: The BigQuery target table contains the exact same number of rows as the Oracle target table, and `pandas.testing.assert_frame_equal` passes with zero mismatches.
*   **Fail**: Row counts differ, or any column value differs between the two datasets.

---

## Test Case 2: Transformation Correctness — BPR ID Filtering

### Purpose
To verify that the BigQuery DML correctly filters the dataset based on the specified list of valid `bpr_id` values: `(2828, 2829, 2830, 2831, 2925, 2926, 2998, 2999, 3000)`. Records with any other `bpr_id` must be excluded.

### Setup
1. Truncate the BigQuery source tables.
2. Insert mock records into `dw_bert.sof_ta_bpr_instance` containing both valid and invalid `bpr_id` values.
3. Insert corresponding matching records into `dw_bert.sof_ta_apn_carmen`.

```sql
-- Mocking source data
INSERT INTO `gcp-enterprise-dwh.dw_bert.sof_ta_bpr_instance` (cntrct_id, bpr_id, cntrct_id_ref) VALUES
  (1001, 2828, 9001), -- Valid (VPN)
  (1002, 2999, 9002), -- Valid (Blackberry 10% discount)
  (1003, 9999, 9003), -- Invalid BPR ID
  (1004, 2827, 9004); -- Invalid BPR ID (boundary value)

INSERT INTO `gcp-enterprise-dwh.dw_bert.sof_ta_apn_carmen` (cntrct_id, access_point_name) VALUES
  (9001, 'vpn.partner.de'),
  (9002, 'blackberry.net'),
  (9003, 'invalid.apn'),
  (9004, 'boundary.apn');
```

### Action
1. Run the BigQuery DML script `d_ausd_bp_ta_bpr_apn.sql`.
2. Execute the following validation query:

```sql
-- Validation Query
SELECT BPR_ID, COUNT(1) as row_count
FROM `gcp-enterprise-dwh.dw_bert.sof_ta_bpr_apn`
GROUP BY BPR_ID;
```

### Pass/Fail Criterion
*   **Pass**: The target table contains exactly 2 rows (for contracts `1001` and `1002`). No rows with `bpr_id` `9999` or `2827` exist in the target table.
*   **Fail**: Any record with an invalid `bpr_id` is found in the target table, or valid records are missing.

---

## Test Case 3: Transformation Correctness — Join Logic, Deduplication, and NULL Handling

### Purpose
To verify that:
1. The `INNER JOIN` on `bp.cntrct_id_ref = ap.cntrct_id` behaves correctly (unmatched records are dropped).
2. `NULL` values in join keys do not cause errors and are correctly excluded.
3. The `DISTINCT` keyword successfully deduplicates identical source rows.

### Setup
1. Truncate the BigQuery source tables.
2. Insert mock records representing unmatched keys, duplicate rows, and `NULL` values:

```sql
-- Mocking source data
INSERT INTO `gcp-enterprise-dwh.dw_bert.sof_ta_bpr_instance` (cntrct_id, bpr_id, cntrct_id_ref) VALUES
  (2001, 2828, 8001), -- Valid match
  (2001, 2828, 8001), -- Duplicate row in source
  (2002, 2828, NULL), -- NULL join key
  (2003, 2828, 8003); -- Unmatched join key (no matching record in apn_carmen)

INSERT INTO `gcp-enterprise-dwh.dw_bert.sof_ta_apn_carmen` (cntrct_id, access_point_name) VALUES
  (8001, 'vpn.partner.de'),
  (8001, 'vpn.partner.de'), -- Duplicate row in source
  (NULL, 'null.apn'),        -- NULL join key
  (8004, 'unmatched.apn');   -- Unmatched join key
```

### Action
1. Run the BigQuery DML script `d_ausd_bp_ta_bpr_apn.sql`.
2. Execute the following validation query:

```sql
-- Validation Query
SELECT CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, ACCESS_POINT_NAME, COUNT(*) as occurrence_count
FROM `gcp-enterprise-dwh.dw_bert.sof_ta_bpr_apn`
GROUP BY CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, ACCESS_POINT_NAME;
```

### Pass/Fail Criterion
*   **Pass**: 
    *   The target table contains exactly 1 row (for contract `2001`).
    *   The duplicate record is deduplicated (`occurrence_count` is exactly 1).
    *   The records with `NULL` join keys and unmatched keys (`2002`, `2003`) are excluded.
*   **Fail**: Duplicate rows exist in the target table, or unmatched/NULL keys are incorrectly joined and loaded.

---

## Test Case 4: Data Quality, Schema, and Clustering Assertions

### Purpose
To verify that the target table `dw_bert.sof_ta_bpr_apn` strictly adheres to the defined schema, data types, and clustering configurations specified in the migration design.

### Setup
Ensure the target table `dw_bert.sof_ta_bpr_apn` has been created using the DDL script `ddl/sof_ta_bpr_apn.sql`.

### Action
Execute a Python script using `pytest` to inspect BigQuery metadata:

```python
# test_schema_metadata.py
from google.cloud import bigquery

def test_target_table_metadata():
    client = bigquery.Client()
    table_ref = client.get_table("gcp-enterprise-dwh.dw_bert.sof_ta_bpr_apn")
    
    # 1. Verify Schema Data Types
    expected_schema = {
        "CNTRCT_ID": "INTEGER",
        "BPR_ID": "INTEGER",
        "CNTRCT_ID_REF": "INTEGER",
        "ACCESS_POINT_NAME": "STRING"
    }
    
    actual_schema = {field.name: field.field_type for field in table_ref.schema}
    assert actual_schema == expected_schema, f"Schema mismatch! Expected: {expected_schema}, Got: {actual_schema}"
    
    # 2. Verify Clustering Configuration
    expected_clustering = ["bpr_id", "cntrct_id"]
    actual_clustering = table_ref.clustering_fields
    assert actual_clustering == expected_clustering, f"Clustering mismatch! Expected: {expected_clustering}, Got: {actual_clustering}"
```

### Pass/Fail Criterion
*   **Pass**: The table schema matches the expected types exactly, and the clustering fields are configured as `['bpr_id', 'cntrct_id']`.
*   **Fail**: Any column type is incorrect, or clustering is missing or misconfigured.

---

## Test Case 5: Orchestration, Idempotency, and Truncate-Insert Behavior

### Purpose
To verify that the Airflow DAG orchestrates the pipeline correctly, and that the BigQuery DML is fully idempotent (i.e., running the job multiple times sequentially does not duplicate data because of the `TRUNCATE` step).

### Setup
1. Populate the BigQuery source tables with a standard test dataset (e.g., 100 valid matching records).
2. Ensure the Airflow DAG `dag_bert_ausd_bp_ta_bpr_apn` is deployed to Cloud Composer.

### Action
1. Trigger the Airflow DAG manually or via CLI.
2. Record the row count of the target table `dw_bert.sof_ta_bpr_apn` after the first run.
3. Trigger the Airflow DAG a second time with the same source data.
4. Record the row count of the target table after the second run.

```python
# test_idempotency.py
from google.cloud import bigquery

def test_job_idempotency():
    client = bigquery.Client()
    
    def get_row_count():
        query = "SELECT COUNT(1) as cnt FROM `gcp-enterprise-dwh.dw_bert.sof_ta_bpr_apn`"
        result = client.query(query).to_dataframe()
        return result["cnt"].values[0]
        
    # Run 1 check
    count_run_1 = get_row_count()
    assert count_run_1 > 0, "First run did not insert any data."
    
    # Run 2 check (simulated after second DAG run)
    count_run_2 = get_row_count()
    assert count_run_1 == count_run_2, f"Idempotency failed! Run 1 count: {count_run_1}, Run 2 count: {count_run_2}"
```

### Pass/Fail Criterion
*   **Pass**: The row count after the second execution is exactly equal to the row count after the first execution, proving that the `TRUNCATE TABLE` step successfully cleared the target before inserting.
*   **Fail**: The row count doubles or increases after the second run, indicating that the target table was not truncated.