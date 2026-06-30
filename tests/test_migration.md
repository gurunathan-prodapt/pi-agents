# Migration Validation Test Suite: `ausd_bp_ta_bpr_evn`

This document defines the migration-validation test suite to prove behavioral equivalence between the legacy Oracle-based job `DW.BERT_AUSD_BP_TA_BPR_EVN` and the migrated Google Cloud Platform (GCP) BigQuery/Cloud Composer implementation.

---

## Test Case 1: End-to-End Output Parity (Golden Dataset Test)

### Purpose
To verify that the migrated BigQuery pipeline produces the exact same output dataset as the legacy Oracle database when executed against identical source data.

### Setup
1. **Legacy Environment (Oracle)**:
   * Populate the legacy source table `sof$ta_bpr_instance` with a controlled "Golden Dataset" containing 1,000 rows (including matching EVN IDs, non-matching IDs, boundary values, and NULLs).
   * Execute the legacy SQL script `d_ausd_bp_ta_bpr_evn.sql`.
   * Export the resulting legacy target table `sof$ta_bpr_evn` to a CSV file (`oracle_golden_output.csv`).

2. **Target Environment (BigQuery)**:
   * Replicate the exact same 1,000-row "Golden Dataset" into the BigQuery source table `gcp-bert-prd.bert_dataset.sof_ta_bpr_instance`.
   * Ensure the target table `gcp-bert-prd.bert_dataset.sof_ta_bpr_evn` is initialized.

### Action
Execute a Python test script that triggers the BigQuery SQL transformation, extracts the target table data, and performs a row-by-row, column-by-column comparison against the legacy export.

```python
import pandas as pd
import pytest
from google.cloud import bigquery

PROJECT_ID = "gcp-bert-prd"
DATASET_ID = "bert_dataset"
TARGET_TABLE = "sof_ta_bpr_evn"

def test_output_parity():
    # 1. Initialize BigQuery Client
    client = bigquery.Client(project=PROJECT_ID)
    
    # 2. Execute the target SQL logic
    sql_path = "gcs/queries/d_ausd_bp_ta_bpr_evn.sql"
    with open(sql_path, "r") as f:
        sql_query = f.read()
        
    query_job = client.query(sql_query)
    query_job.result()  # Wait for query to complete
    
    # 3. Fetch results from BigQuery target table
    bq_query = f"SELECT cntrct_id, bpr_id FROM `{PROJECT_ID}.{DATASET_ID}.{TARGET_TABLE}` ORDER BY cntrct_id, bpr_id"
    bq_df = client.query(bq_query).to_dataframe()
    
    # 4. Load Legacy Golden Output
    oracle_df = pd.read_csv("tests/golden_data/oracle_golden_output.csv")
    oracle_df = oracle_df.sort_values(by=["cntrct_id", "bpr_id"]).reset_index(drop=True)
    bq_df = bq_df.sort_values(by=["cntrct_id", "bpr_id"]).reset_index(drop=True)
    
    # 5. Assert exact equivalence
    pd.testing.assert_frame_equal(
        oracle_df[["cntrct_id", "bpr_id"]].astype(float), 
        bq_df[["cntrct_id", "bpr_id"]].astype(float), 
        check_dtype=False,
        obj="Oracle vs BigQuery Golden Dataset Comparison"
    )
```

### Pass/Fail Criterion
* **Pass**: The BigQuery target table contains the exact same rows (keys and values) as the legacy Oracle target table, with zero mismatches, omissions, or extra records.
* **Fail**: Any difference in row count, column values, or sorting order between the two datasets.

---

## Test Case 2: Transformation Correctness (Filter Logic & ID Inclusion/Exclusion)

### Purpose
To verify that the SQL filter logic correctly includes only the specified EVN basis product IDs (`32, 2506, 2839, 2840, 3055, 3056, 3821`) and strictly excludes all other IDs.

### Setup
1. Truncate the BigQuery source table `sof_ta_bpr_instance`.
2. Insert a test dataset containing:
   * **Valid IDs (Inclusion)**: `32`, `2506`, `2839`, `2840`, `3055`, `3056`, `3821`
   * **Invalid IDs (Exclusion)**: Boundary values (`31`, `33`, `2505`, `2507`), negative values (`-32`), and unrelated IDs (`9999`, `12345`).

### Action
Execute the BigQuery SQL script and query the target table to verify that only the valid IDs exist.

```sql
-- Step 1: Prepare Test Data
TRUNCATE TABLE `gcp-bert-prd.bert_dataset.sof_ta_bpr_instance`;

INSERT INTO `gcp-bert-prd.bert_dataset.sof_ta_bpr_instance` (cntrct_id, bpr_id)
VALUES
  (101, 32),    -- Standard-EVN (Include)
  (102, 2506),  -- Komfort-EVN (Include)
  (103, 2839),  -- Standard-EVN Separat (Include)
  (104, 2840),  -- Komfort-EVN Separat (Include)
  (105, 3055),  -- Komfort-Plus-EVN (Include)
  (106, 3056),  -- Komfort-Plus-EVN Separat (Include)
  (107, 3821),  -- Standard-Plus-EVN (Include)
  (201, 31),    -- Boundary Exclude
  (202, 33),    -- Boundary Exclude
  (203, 2505),  -- Boundary Exclude
  (204, 2507),  -- Boundary Exclude
  (205, 9999),  -- Random Exclude
  (206, -32);   -- Negative Exclude

-- Step 2: Run Migrated Query
-- (Executes gcs/queries/d_ausd_bp_ta_bpr_evn.sql)

-- Step 3: Assertions
-- Assertion A: Total count in target must be exactly 7
SELECT ASSERT_ROWS_MODIFIED(7) AS assertion_a
FROM (
  SELECT * FROM `gcp-bert-prd.bert_dataset.sof_ta_bpr_evn`
);

-- Assertion B: No excluded IDs must exist in target
SELECT ASSERT(COUNT(*) = 0, 'Found excluded bpr_id in target table!') AS assertion_b
FROM `gcp-bert-prd.bert_dataset.sof_ta_bpr_evn`
WHERE bpr_id NOT IN (32, 2506, 2839, 2840, 3055, 3056, 3821);
```

### Pass/Fail Criterion
* **Pass**: The target table contains exactly 7 rows, matching contract IDs `101` through `107`. No records with contract IDs starting with `2` exist in the target table.
* **Fail**: Any excluded ID is found in the target table, or any valid ID is missing.

---

## Test Case 3: NULL Handling and Schema Integrity

### Purpose
To verify that the target table schema matches the design specifications and that the pipeline handles NULL values in source columns without throwing runtime exceptions or corrupting data.

### Setup
1. Truncate the BigQuery source table `sof_ta_bpr_instance`.
2. Insert test rows containing NULL values:
   * Row 1: `cntrct_id` is NULL, `bpr_id` is a valid EVN ID (`32`).
   * Row 2: `cntrct_id` is valid (`108`), `bpr_id` is NULL.
   * Row 3: Both `cntrct_id` and `bpr_id` are NULL.

### Action
Execute the BigQuery SQL script and run schema and data-quality assertions.

```python
import pytest
from google.cloud import bigquery

PROJECT_ID = "gcp-bert-prd"
DATASET_ID = "bert_dataset"

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client(project=PROJECT_ID)

def test_schema_and_null_handling(bq_client):
    # 1. Insert NULL test cases into source
    setup_sql = f"""
    TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.sof_ta_bpr_instance`;
    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sof_ta_bpr_instance` (cntrct_id, bpr_id)
    VALUES
      (NULL, 32),
      (108, NULL),
      (NULL, NULL);
    """
    bq_client.query(setup_sql).result()

    # 2. Run the migration SQL
    with open("gcs/queries/d_ausd_bp_ta_bpr_evn.sql", "r") as f:
        bq_client.query(f.read()).result()

    # 3. Assert Schema Types
    table_ref = bq_client.get_table(f"{PROJECT_ID}.{DATASET_ID}.sof_ta_bpr_evn")
    schema_dict = {field.name: field.field_type for field in table_ref.schema}
    
    assert schema_dict["cntrct_id"] == "INTEGER", "cntrct_id must be INT64/INTEGER"
    assert schema_dict["bpr_id"] == "INTEGER", "bpr_id must be INT64/INTEGER"

    # 4. Assert NULL Handling Behavior
    # - Row with NULL cntrct_id and valid bpr_id (32) MUST be preserved.
    # - Rows with NULL bpr_id MUST be filtered out.
    query = f"SELECT cntrct_id, bpr_id FROM `{PROJECT_ID}.{DATASET_ID}.sof_ta_bpr_evn`"
    results = list(bq_client.query(query).result())
    
    assert len(results) == 1, f"Expected exactly 1 row, got {len(results)}"
    assert results[0]["cntrct_id"] is None, "Expected cntrct_id to be NULL"
    assert results[0]["bpr_id"] == 32, "Expected bpr_id to be 32"
```

### Pass/Fail Criterion
* **Pass**: 
  * Target table schema columns `cntrct_id` and `bpr_id` are both typed as `INTEGER` (INT64).
  * The row with `cntrct_id = NULL` and `bpr_id = 32` is successfully migrated.
  * Rows with `bpr_id = NULL` are correctly excluded.
* **Fail**: The query fails to execute, column types are incorrect, or NULL values are handled incorrectly (e.g., NULL `bpr_id` is imported).

---

## Test Case 4: Idempotency & Truncate Behavior

### Purpose
To verify that the target table is completely truncated before insertion, ensuring that multiple consecutive runs of the job do not duplicate data or leave orphaned records. This replaces the legacy Oracle truncate wrapper `DWPA_UTIL_SKRIPT.runstatement`.

### Setup
1. Populate the BigQuery source table `sof_ta_bpr_instance` with 3 valid records.
2. Populate the BigQuery target table `sof_ta_bpr_evn` with 5 dummy/stale records that do not exist in the source table.

### Action
Execute the BigQuery SQL script and verify that the 5 stale records are deleted and only the 3 valid records from the source table exist in the target table.

```sql
-- Step 1: Setup Source Data
TRUNCATE TABLE `gcp-bert-prd.bert_dataset.sof_ta_bpr_instance`;
INSERT INTO `gcp-bert-prd.bert_dataset.sof_ta_bpr_instance` (cntrct_id, bpr_id)
VALUES (1, 32), (2, 32), (3, 32);

-- Step 2: Setup Target Table with Stale Data
TRUNCATE TABLE `gcp-bert-prd.bert_dataset.sof_ta_bpr_evn`;
INSERT INTO `gcp-bert-prd.bert_dataset.sof_ta_bpr_evn` (cntrct_id, bpr_id)
VALUES (999, 32), (998, 32), (997, 32), (996, 32), (995, 32);

-- Step 3: Run the Migrated Query
-- (Executes gcs/queries/d_ausd_bp_ta_bpr_evn.sql)

-- Step 4: Assertions
-- Assertion A: Stale records (995-999) must be completely gone
SELECT ASSERT(COUNT(*) = 0, 'Stale records were not truncated!') AS assertion_a
FROM `gcp-bert-prd.bert_dataset.sof_ta_bpr_evn`
WHERE cntrct_id >= 995;

-- Assertion B: Target table must contain exactly 3 rows
SELECT ASSERT(COUNT(*) = 3, 'Target table does not contain exactly 3 rows!') AS assertion_b
FROM `gcp-bert-prd.bert_dataset.sof_ta_bpr_evn`;
```

### Pass/Fail Criterion
* **Pass**: The target table contains exactly the 3 records from the source table. All 5 pre-existing stale records are completely removed.
* **Fail**: Stale records remain in the target table, or the final row count is not exactly 3.

---

## Test Case 5: Airflow DAG Parameter Parsing and Dynamic SQL Generation

### Purpose
To verify that the Airflow DAG's Python helper functions (`prepare_evn_sql`, `validate_runtime_params`) correctly parse, validate, and dynamically generate SQL based on custom parameters passed via `dag_run.conf`.

### Setup
An Airflow execution context is mocked using `pytest` to pass custom parameters:
* `project_id`: `"gcp-bert-dev"`
* `dataset`: `"bert_test_dataset"`
* `source_table`: `"custom_instance"`
* `target_table`: `"custom_evn"`
* `bpr_ids`: `"32, 2506"` (passed as a comma-separated string)

### Action
Execute the Python unit tests against the DAG code to verify parameter parsing and SQL generation.

```python
import pytest
from dags.dw_bert_ausd_bp_ta_bpr_evn_dag import prepare_evn_sql, validate_runtime_params

def test_validate_runtime_params_valid():
    # Mock context with valid parameters
    context = {
        "dag_run": type("MockDagRun", (), {
            "conf": {
                "stichtag": "20260421",
                "wiederanlauf_wert": "1005"
            }
        })()
    }
    result = validate_runtime_params(**context)
    assert result["stichtag"] == "20260421"
    assert result["wiederanlauf_wert"] == 1005

def test_validate_runtime_params_invalid_date():
    # Mock context with invalid date format
    context = {
        "dag_run": type("MockDagRun", (), {
            "conf": {
                "stichtag": "21-04-2026",
                "wiederanlauf_wert": 0
            }
        })()
    }
    with pytest.raises(ValueError, match="stichtag must be in YYYYMMDD format"):
        validate_runtime_params(**context)

def test_dynamic_sql_generation():
    # Mock context with custom tables and filtered IDs
    context = {
        "dag_run": type("MockDagRun", (), {
            "conf": {
                "project_id": "gcp-bert-dev",
                "dataset": "bert_test_dataset",
                "source_table": "custom_instance",
                "target_table": "custom_evn",
                "bpr_ids": "32, 2506"
            }
        })()
    }
    
    generated_sql = prepare_evn_sql(**context)
    
    # Assertions on generated SQL string
    assert "TRUNCATE TABLE `gcp-bert-dev.bert_test_dataset.custom_evn`" in generated_sql
    assert "FROM `gcp-bert-dev.bert_test_dataset.custom_instance` AS bp" in generated_sql
    assert "bp.bpr_id IN (\n        32,\n        2506\n    )" in generated_sql
```

### Pass/Fail Criterion
* **Pass**: 
  * `validate_runtime_params` successfully parses valid dates and integers, and raises a `ValueError` for invalid formats.
  * `prepare_evn_sql` dynamically generates a syntactically correct BigQuery SQL statement containing the custom project, dataset, table names, and filtered IDs.
* **Fail**: Parameter parsing fails on valid inputs, fails to raise errors on invalid inputs, or the generated SQL contains incorrect table references or malformed filter clauses.