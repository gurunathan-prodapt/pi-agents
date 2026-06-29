This document provides a comprehensive suite of migration-validation test cases for the BigQuery ELT pipeline `ausd_bp_ta_ibcp_ccid`. These tests are designed to prove behavioral equivalence between the legacy Oracle/KSH implementation and the new Google Cloud Composer/BigQuery architecture.

---

## Test Case 1: Extraction & Load Parity (Oracle vs. BigQuery Staging)

### Purpose
Verify that the data extracted from the legacy Oracle database (`GL_CODE_COMBINATIONS` and `IBCP_STAGE_TXN`) matches the data loaded into the BigQuery staging tables (`stg_oracle_ccid` and `stg_ibcp_txns`) exactly, ensuring no data loss, truncation, or corruption occurs during the JDBC-to-GCS-to-BigQuery hop.

### Setup
1. Seed the source Oracle database with a known set of test records (including edge cases like special characters, maximum length strings, and boundary dates).
2. Clear the target BigQuery staging tables: `prj-ausd-stage-gcp.bq_stage_ta.stg_oracle_ccid` and `prj-ausd-stage-gcp.bq_stage_ta.stg_ibcp_txns`.

### Action
Execute the Airflow tasks responsible for extraction and staging load:
1. `extract_ccids_to_gcs` followed by `load_ccids_to_staging`.
2. `extract_txns_to_gcs` followed by `load_txns_to_staging`.

Run the following Python validation script using `pytest` to compare the source Oracle data directly with the BigQuery staging tables.

```python
import pytest
import os
from google.cloud import bigquery
import sqlalchemy as sa

# Database connection strings (configured via environment variables in CI/CD)
ORACLE_URI = os.getenv("ORACLE_TEST_CONN_URI", "oracle+cx_oracle://user:pass@host:port/service")
BQ_PROJECT = "prj-ausd-stage-gcp"
BQ_DATASET = "bq_stage_ta"

@pytest.fixture(scope="module")
def oracle_engine():
    return sa.create_engine(ORACLE_URI)

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client(project=BQ_PROJECT)

def test_ccid_extraction_parity(oracle_engine, bq_client):
    # 1. Query Oracle Source
    oracle_query = """
        SELECT 
          CAST(ccid AS VARCHAR2(100)) AS code_combination_id,
          segment1, segment2, segment3, segment4, segment5,
          summary_flag, enabled_flag,
          TO_CHAR(start_date_active, 'YYYY-MM-DD') AS start_date_active,
          TO_CHAR(end_date_active, 'YYYY-MM-DD') AS end_date_active
        FROM GL_CODE_COMBINATIONS
        WHERE segment1 IN ('AU', '080')
    """
    with oracle_engine.connect() as conn:
        oracle_rows = sorted([dict(r) for r in conn.execute(sa.text(oracle_query)).mappings()])

    # 2. Query BigQuery Staging
    bq_query = f"""
        SELECT 
          code_combination_id,
          segment1, segment2, segment3, segment4, segment5,
          summary_flag, enabled_flag,
          start_date_active, end_date_active
        FROM `{BQ_PROJECT}.{BQ_DATASET}.stg_oracle_ccid`
    """
    query_job = bq_client.query(bq_query)
    bq_rows = sorted([dict(r) for r in query_job.result()])

    # 3. Assert Equivalence
    assert len(oracle_rows) == len(bq_rows), f"Row count mismatch! Oracle: {len(oracle_rows)}, BQ: {len(bq_rows)}"
    
    for o_row, b_row in zip(oracle_rows, bq_rows):
        assert o_row["code_combination_id"] == b_row["code_combination_id"]
        assert o_row["segment1"] == b_row["segment1"]
        assert o_row["segment2"] == b_row["segment2"]
        assert o_row["segment3"] == b_row["segment3"]
        assert o_row["segment4"] == b_row["segment4"]
        assert o_row["segment5"] == b_row["segment5"]
        assert o_row["summary_flag"] == b_row["summary_flag"]
        assert o_row["enabled_flag"] == b_row["enabled_flag"]
        assert o_row["start_date_active"] == b_row["start_date_active"]
        assert o_row["end_date_active"] == b_row["end_date_active"]
```

### Pass/Fail Criterion
*   **Pass**: The row counts match exactly, and every column value in BigQuery staging is identical to its corresponding value in the Oracle source database.
*   **Fail**: Any row count discrepancy or value mismatch (including date formatting differences or string truncations).

---

## Test Case 2: Segment Concatenation & NULL Handling

### Purpose
Verify that the transformation logic correctly concatenates the five individual segments into the `full_gl_account_string` and properly handles NULL values in optional segments (`segment4` and `segment5`) by applying the specified defaults (`'0000'` and `'000'`).

### Setup
Truncate the staging table `stg_oracle_ccid` and insert the following controlled test cases:

| code_combination_id | segment1 | segment2 | segment3 | segment4 | segment5 | summary_flag | enabled_flag |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `TC2_01` | `AU` | `100` | `610000` | `1234` | `999` | `N` | `Y` |
| `TC2_02` | `080` | `200` | `620000` | NULL | `888` | `N` | `Y` |
| `TC2_03` | `AU` | `300` | `630000` | `5678` | NULL | `N` | `Y` |
| `TC2_04` | `080` | `400` | `640000` | NULL | NULL | `N` | `Y` |

Clear the target dimension table `prj-ausd-core-gcp.finance_ta.dim_ccid_ibcp`.

### Action
1. Execute the `MERGE` statement defined in `src/sql/merge_ccid_ibcp.sql` (Step 1).
2. Run the following validation query in BigQuery:

```sql
SELECT 
  code_combination_id,
  full_gl_account_string,
  sub_account_code,
  intercompany_partner_code
FROM `prj-ausd-core-gcp.finance_ta.dim_ccid_ibcp`
WHERE code_combination_id LIKE 'TC2_%'
ORDER BY code_combination_id;
```

### Pass/Fail Criterion
The output must match the expected values exactly:

| code_combination_id | Expected full_gl_account_string | Expected sub_account_code | Expected intercompany_partner_code |
| :--- | :--- | :--- | :--- |
| `TC2_01` | `AU-100-610000-1234-999` | `1234` | `999` |
| `TC2_02` | `080-200-620000-0000-888` | NULL | `888` |
| `TC2_03` | `AU-300-630000-5678-000` | `5678` | NULL |
| `TC2_04` | `080-400-640000-0000-000` | NULL | NULL |

*   **Pass**: All concatenated strings match the expected format, and NULL values in segments are correctly defaulted in the concatenated string while preserving the original NULLs in the individual target columns.
*   **Fail**: Any deviation in the concatenated string format or failure to apply defaults.

---

## Test Case 3: Transformation Correctness - Filter Logic (Australian Entities)

### Purpose
Verify that the transformation logic strictly filters for Australian entities (`'AU'` and `'080'`) and discards records belonging to other entities (e.g., `'US'`, `'NZ'`, `'100'`).

### Setup
Truncate staging tables and insert a mix of Australian and non-Australian records:

**`stg_oracle_ccid`**:
*   `ccid_1`: `segment1 = 'AU'` (Should pass)
*   `ccid_2`: `segment1 = '080'` (Should pass)
*   `ccid_3`: `segment1 = 'US'` (Should be filtered out)
*   `ccid_4`: `segment1 = 'NZ'` (Should be filtered out)

**`stg_ibcp_txns`**:
*   `txn_1`: `entity_code = 'AU'` (Should pass)
*   `txn_2`: `entity_code = '080'` (Should pass)
*   `txn_3`: `entity_code = '100'` (Should be filtered out)

Clear target tables `dim_ccid_ibcp` and `fact_ibcp_ledger`.

### Action
1. Execute the transformation scripts (Step 1 and Step 2 of `merge_ccid_ibcp.sql`).
2. Run the following validation queries:

```sql
-- Assertion 1: No non-AU records in dim_ccid_ibcp
SELECT COUNT(*) AS invalid_dim_count 
FROM `prj-ausd-core-gcp.finance_ta.dim_ccid_ibcp`
WHERE entity_code NOT IN ('AU', '080');

-- Assertion 2: No non-AU records in fact_ibcp_ledger
SELECT COUNT(*) AS invalid_fact_count 
FROM `prj-ausd-core-gcp.finance_ta.fact_ibcp_ledger`
WHERE entity_code NOT IN ('AU', '080');
```

### Pass/Fail Criterion
*   **Pass**: Both queries return a count of `0`.
*   **Fail**: Any record with an entity code other than `'AU'` or `'080'` is found in the target core tables.

---

## Test Case 4: MERGE Behavioral Equivalence (Idempotency & Upsert)

### Purpose
Verify that the `MERGE` statement behaves idempotently and correctly handles inserts, updates, and key preservation without generating duplicate records.

### Setup
1. Seed the target table `dim_ccid_ibcp` with an initial record:
   *   `code_combination_id = 'TC4_01'`
   *   `entity_code = 'AU'`
   *   `cost_center_code = '100'`
   *   `enabled_flag = 'Y'`
   *   `dw_last_update_ts = TIMESTAMP('2025-01-01 00:00:00')`

2. Populate the staging table `stg_oracle_ccid` with two records:
   *   **Record 1 (Update)**: `code_combination_id = 'TC4_01'`, `segment1 = 'AU'`, `segment2 = '100'`, `enabled_flag = 'N'` (Status changed to disabled).
   *   **Record 2 (Insert)**: `code_combination_id = 'TC4_02'`, `segment1 = '080'`, `segment2 = '200'`, `enabled_flag = 'Y'`.

### Action
1. Execute the `MERGE` statement (Step 1 of `merge_ccid_ibcp.sql`).
2. Run the following validation query:

```sql
SELECT 
  code_combination_id,
  cost_center_code,
  enabled_flag,
  dw_last_update_ts > TIMESTAMP('2025-01-01 00:00:00') AS is_updated
FROM `prj-ausd-core-gcp.finance_ta.dim_ccid_ibcp`
WHERE code_combination_id IN ('TC4_01', 'TC4_02')
ORDER BY code_combination_id;
```

### Pass/Fail Criterion
The output must match the following:

| code_combination_id | cost_center_code | enabled_flag | is_updated |
| :--- | :--- | :--- | :--- |
| `TC4_01` | `100` | `N` | `true` |
| `TC4_02` | `200` | `Y` | `true` |

*   **Pass**: `TC4_01` is updated in place (no duplicate row created, `enabled_flag` changes to `'N'`), and `TC4_02` is inserted as a new record.
*   **Fail**: Duplicate keys are generated, updates fail to apply, or timestamps are not updated.

---

## Test Case 5: Data Quality & Schema Assertions

### Purpose
Assert that the target tables conform to strict data quality rules, including non-nullability of primary keys, correct data type casting (especially dates and numeric amounts), and valid flag values.

### Setup
Run the full ELT pipeline with a standard production-like dataset.

### Action
Execute the following test suite using `pytest` and the Google Cloud BigQuery client:

```python
import pytest
from google.cloud import bigquery

PROJECT_ID = "prj-ausd-core-gcp"
DATASET = "finance_ta"
client = bigquery.Client(project=PROJECT_ID)

def test_dim_ccid_null_constraints():
    """Assert that primary keys in dim_ccid_ibcp are never NULL."""
    query = f"""
        SELECT COUNT(*) AS null_keys 
        FROM `{PROJECT_ID}.{DATASET}.dim_ccid_ibcp` 
        WHERE code_combination_id IS NULL
    """
    result = list(client.query(query).result())[0]
    assert result.null_keys == 0, "Found NULL values in code_combination_id!"

def test_fact_ledger_null_constraints():
    """Assert that primary keys and foreign keys in fact_ibcp_ledger are never NULL."""
    query = f"""
        SELECT 
          COUNTIF(txn_id IS NULL) AS null_txns,
          COUNTIF(code_combination_id IS NULL) AS null_ccids
        FROM `{PROJECT_ID}.{DATASET}.fact_ibcp_ledger`
    """
    result = list(client.query(query).result())[0]
    assert result.null_txns == 0, "Found NULL values in txn_id!"
    assert result.null_ccids == 0, "Found NULL values in code_combination_id within fact table!"

def test_flag_values():
    """Assert that flags are strictly restricted to 'Y' or 'N'."""
    query = f"""
        SELECT COUNT(*) AS invalid_flags 
        FROM `{PROJECT_ID}.{DATASET}.dim_ccid_ibcp` 
        WHERE summary_flag NOT IN ('Y', 'N') OR enabled_flag NOT IN ('Y', 'N')
    """
    result = list(client.query(query).result())[0]
    assert result.invalid_flags == 0, "Flags contain values other than 'Y' or 'N'!"

def test_date_validity():
    """Assert that start_date is always less than or equal to end_date (when end_date is populated)."""
    query = f"""
        SELECT COUNT(*) AS invalid_dates 
        FROM `{PROJECT_ID}.{DATASET}.dim_ccid_ibcp` 
        WHERE start_date > end_date
    """
    result = list(client.query(query).result())[0]
    assert result.invalid_dates == 0, "Found records where start_date is after end_date!"
```

### Pass/Fail Criterion
*   **Pass**: All tests execute successfully with zero failures.
*   **Fail**: Any assertion fails (e.g., NULL keys found, invalid flags, or logical date errors).

---

## Test Case 6: Reconciliation Task Validation

### Purpose
Verify that the Airflow Python reconciliation task (`reconcile_and_verify`) correctly identifies discrepancies between staging and core tables, raising an `AirflowException` when data leaks or omissions occur, and passing silently when data is fully reconciled.

### Setup
1. Clear staging and core tables.
2. Insert 10 records into `stg_oracle_ccid` (all with `segment1 = 'AU'`).
3. Insert 10 matching records into `dim_ccid_ibcp`.

### Action
Execute the reconciliation logic under two scenarios:

#### Scenario A: Perfect Reconciliation
Run the reconciliation audit function.

```python
from airflow.exceptions import AirflowException
import pytest
# Import the reconciliation function from the DAG file
from src.dags.ausd_bp_ta_ibcp_ccid_dag import run_reconciliation_audit

def test_reconciliation_passes_when_equal():
    # This should run without raising any exceptions
    try:
        run_reconciliation_audit()
    except AirflowException as e:
        pytest.fail(f"Reconciliation failed unexpectedly: {e}")
```

#### Scenario B: Reconciliation Failure (Data Discrepancy)
Manually insert an extra record into `stg_oracle_ccid` (creating a row count mismatch) and run the audit.

```python
def test_reconciliation_fails_on_mismatch(bq_client):
    # Insert a mismatching record into staging
    mismatch_query = """
        INSERT INTO `prj-ausd-stage-gcp.bq_stage_ta.stg_oracle_ccid` 
        (code_combination_id, segment1, segment2, segment3, summary_flag, enabled_flag)
        VALUES ('MISMATCH_999', 'AU', '999', '999999', 'N', 'Y')
    """
    bq_client.query(mismatch_query).result()

    # Assert that the reconciliation task raises AirflowException
    with pytest.raises(AirflowException) as excinfo:
        run_reconciliation_audit()
    
    assert "Reconciliation error detected" in str(excinfo.value)

    # Clean up mismatch record
    cleanup_query = """
        DELETE FROM `prj-ausd-stage-gcp.bq_stage_ta.stg_oracle_ccid` 
        WHERE code_combination_id = 'MISMATCH_999'
    """
    bq_client.query(cleanup_query).result()
```

### Pass/Fail Criterion
*   **Pass**: Scenario A completes silently with no errors, and Scenario B successfully raises an `AirflowException` detailing the mismatch.
*   **Fail**: Scenario A raises an exception, or Scenario B passes silently despite the data discrepancy.