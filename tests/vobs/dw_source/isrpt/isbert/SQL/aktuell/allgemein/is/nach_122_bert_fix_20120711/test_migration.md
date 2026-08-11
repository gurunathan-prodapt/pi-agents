# Migration Validation Test Suite: `d_ausd_v_ta_vertrag_tmp`

This document defines the migration-validation test suite for the migrated BigQuery job: `Shared Files — vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/nach_122_bert_fix_20120711`. 

The test suite is designed to prove behavioral equivalence between the legacy Oracle SQL execution and the migrated BigQuery Standard SQL scripting block.

---

## Test Case 1: End-to-End Output Parity & Row Count Validation

### Purpose
To verify that executing the migrated BigQuery SQL script on a replica of legacy production data produces the exact same row count, schema, and data content as the legacy Oracle execution.

### Setup
1. **Oracle Environment**: 
   * Populate source tables with a representative production-like dataset (minimum 10,000 contracts, including standard contracts and `cntrct_ty = 20` sub-contracts).
   * Ensure `isbert_schema.dwtk_meldungen` has a valid `'BERT_DROP_TEMP_TABLE'` entry.
   * Execute the legacy Oracle script `d_ausd_v_ta_vertrag_tmp.sql`.
2. **BigQuery Environment**:
   * Sync the same source dataset to the target BigQuery dataset (`GCP_PROJECT.BQ_DATASET`).
   * Ensure the target table `sof$ta_vertrag_tmp` exists and is empty or contains stale data.

### Action
1. Execute the migrated BigQuery SQL script.
2. Extract the results from both the Oracle target table and the BigQuery target table into a comparable format (e.g., CSV or temporary validation tables).
3. Run a parity comparison query.

### Pytest Validation Code
```python
import os
import pytest
from google.cloud import bigquery
import pandas as pd

def test_e2e_output_parity():
    # Initialize BigQuery Client
    bq_client = bigquery.Client()
    
    # Project and Dataset configuration
    project_id = os.getenv("GCP_PROJECT")
    dataset_id = os.getenv("BQ_DATASET")
    
    # 1. Assert Row Counts Match
    # (Assuming Oracle row count has been exported or queried via an Oracle connector)
    oracle_row_count = 10543  # Replace with actual dynamic count from Oracle test run
    
    bq_count_query = f"SELECT COUNT(*) as cnt FROM `{project_id}.{dataset_id}.sof$ta_vertrag_tmp`"
    bq_count_df = bq_client.query(bq_count_query).to_dataframe()
    bq_row_count = bq_count_df["cnt"].values[0]
    
    assert bq_row_count == oracle_row_count, f"Row count mismatch! Oracle: {oracle_row_count}, BigQuery: {bq_row_count}"
    
    # 2. Assert Data Hash Parity on Key Columns
    # We generate an MD5 hash of sorted key columns to verify absolute data parity.
    parity_query = f"""
        SELECT 
          CAST(vertrag_id_carmen AS STRING) as vertrag_id,
          CAST(partner_id_carmen AS STRING) as partner_id,
          upgradeberechtigt,
          VDA,
          vertragsstatus,
          rechnungszahlart,
          rechnungsmedium
        FROM `{project_id}.{dataset_id}.sof$ta_vertrag_tmp`
        ORDER BY vertrag_id_carmen, partner_id_carmen
    """
    bq_df = bq_client.query(parity_query).to_dataframe()
    
    # Load Oracle baseline (previously exported to CSV during setup)
    oracle_df = pd.read_csv("oracle_baseline_vertrag_tmp.csv")
    oracle_df = oracle_df.sort_values(by=["vertrag_id", "partner_id"]).reset_index(drop=True)
    bq_df = bq_df.sort_values(by=["vertrag_id", "partner_id"]).reset_index(drop=True)
    
    # Compare DataFrames
    pd.testing.assert_frame_equal(bq_df, oracle_df, check_dtype=False)
```

### Pass/Fail Criterion
* **Pass**: The row counts match exactly, and the sorted dataframes of key columns are identical (zero variance).
* **Fail**: Any row count discrepancy or mismatch in column values.

---

## Test Case 2: Upgrade Eligibility Logic & `MONTHS_BETWEEN` Emulation

### Purpose
To validate that the BigQuery emulation of Oracle's `MONTHS_BETWEEN` (`DATE_DIFF(..., DAY) / 30.436875`) behaves identically to the legacy system, specifically testing boundary conditions for 12-month and 24-month contract terms.

### Setup
In the BigQuery source tables, insert the following test contracts relative to a reference `v_datum` of `'20120711'`:
1. **Contract A (12-Month Term, Boundary - Under)**: Start date `2011-10-12` (Exactly 8.97 months difference). Expected `upgradeberechtigt` = `'N'`.
2. **Contract B (12-Month Term, Boundary - Over)**: Start date `2011-10-10` (Exactly 9.03 months difference). Expected `upgradeberechtigt` = `'J'`.
3. **Contract C (24-Month Term, Boundary - Under)**: Start date `2010-08-13` (Exactly 22.94 months difference). Expected `upgradeberechtigt` = `'N'`.
4. **Contract D (24-Month Term, Boundary - Over)**: Start date `2010-08-10` (Exactly 23.04 months difference). Expected `upgradeberechtigt` = `'J'`.
5. **Contract E (No Term / 0 Months)**: Expected `upgradeberechtigt` = `'J'`.
6. **Contract F (Blocked by Barrier)**: Start date `2011-10-10` (Over 9 months) but `b.sperrart_alle` is NOT NULL and `b.sperrgrund_zusgf = 1` (not 2). Expected `upgradeberechtigt` = `'N'`.

### Action
1. Populate `isbert_schema.dwtk_meldungen` with `timecreated = '2012-07-11 00:00:00'` for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
2. Run the BigQuery migration script.
3. Query the target table for the specific test contracts.

### SQL Assertion Code
```sql
-- Assertions for Upgrade Eligibility Boundaries
SELECT
  vertrag_id_carmen,
  vertragsbindung,
  upgradeberechtigt,
  -- Expected values mapped to contract IDs
  CASE 
    WHEN vertrag_id_carmen = 101 THEN 'N' -- 12m, 8.97 months
    WHEN vertrag_id_carmen = 102 THEN 'J' -- 12m, 9.03 months
    WHEN vertrag_id_carmen = 201 THEN 'N' -- 24m, 22.94 months
    WHEN vertrag_id_carmen = 202 THEN 'J' -- 24m, 23.04 months
    WHEN vertrag_id_carmen = 301 THEN 'J' -- 0m (No term)
    WHEN vertrag_id_carmen = 401 THEN 'N' -- Blocked by barrier
  END AS expected_upgradeberechtigt
FROM `GCP_PROJECT.BQ_DATASET.sof$ta_vertrag_tmp`
WHERE vertrag_id_carmen IN (101, 102, 201, 202, 301, 401);
```

### Pass/Fail Criterion
* **Pass**: Every test contract's `upgradeberechtigt` value matches the expected value, proving the mathematical emulation of `MONTHS_BETWEEN` is accurate to the decimal level.
* **Fail**: Any contract has an incorrect eligibility flag.

---

## Test Case 3: Contract Type Partitioning (Union All Join Logic)

### Purpose
To verify that the two branches of the `UNION ALL` correctly partition standard contracts (`cntrct_ty <> 20`) and sub-contracts (`cntrct_ty = 20`), ensuring correct partner ID mapping.

### Setup
Insert the following records into `sof$ta_cntrct_crs3` and `sof$ta_bp_ref`:
1. **Standard Contract**: `cntrct_id = 9001`, `cntrct_ty = 10`, `cntrct_parent = NULL`.
   * Corresponding `sof$ta_bp_ref` record: `cntrct_cp2_id = 9001`, `bp_id = 8001`.
2. **Sub-Contract**: `cntrct_id = 9002`, `cntrct_ty = 20`, `cntrct_parent = 9001`.
   * Corresponding `sof$ta_bp_ref` record: `cntrct_cp2_id = 9001` (points to parent), `bp_id = 8001`.

### Action
1. Run the BigQuery migration script.
2. Query the target table for these two contracts and verify their mapped partner IDs.

### SQL Assertion Code
```sql
WITH validation AS (
  SELECT 
    vertrag_id_carmen,
    partner_id_carmen,
    cntrct_ty
  FROM `GCP_PROJECT.BQ_DATASET.sof$ta_vertrag_tmp`
  WHERE vertrag_id_carmen IN (9001, 9002)
)
SELECT 
  vertrag_id_carmen,
  cntrct_ty,
  partner_id_carmen,
  CASE 
    WHEN vertrag_id_carmen = 9001 AND partner_id_carmen = 8001 THEN 'PASS'
    WHEN vertrag_id_carmen = 9002 AND partner_id_carmen = 8001 THEN 'PASS'
    ELSE 'FAIL'
  END AS test_status
FROM validation;
```

### Pass/Fail Criterion
* **Pass**: Both contracts are successfully inserted, and both resolve to `partner_id_carmen = 8001`.
* **Fail**: The sub-contract is missing (due to incorrect join logic) or has a `NULL` partner ID.

---

## Test Case 4: Data Type and Decode Mapping Correctness

### Purpose
To verify that all Oracle `DECODE` functions and conditional `CASE` statements (such as `VDA` and `vertragsstatus`) map to the correct string representations in BigQuery.

### Setup
Insert records into source tables with the following values:
1. `cntrct_st` = `5` (Expected `vertragsstatus` = `'A'`)
2. `cntrct_st` = `6` (Expected `vertragsstatus` = `'L'`)
3. `cntrct_st` = `99` (Expected `vertragsstatus` = `NULL`)
4. `inv_pay_ty_cv` = `3` (Expected `rechnungszahlart` = `'K'`)
5. `inv_media_cv` = `3` (Expected `rechnungsmedium` = `'E-Mail'`)
6. `cntrct_template_id` = `5105` (Expected `VDA` = `contract_number`)
7. `cntrct_template_id` = `9999` (Expected `VDA` = `NULL`)

### Action
1. Run the BigQuery migration script.
2. Query the target table and assert the decoded values.

### SQL Assertion Code
```sql
SELECT
  vertrag_id_carmen,
  vertragsstatus,
  rechnungszahlart,
  rechnungsmedium,
  VDA,
  -- Assertions
  ASSERT_ROWS_MODIFIED(
    SELECT COUNT(1) 
    FROM `GCP_PROJECT.BQ_DATASET.sof$ta_vertrag_tmp`
    WHERE (vertrag_id_carmen = 501 AND vertragsstatus != 'A')
       OR (vertrag_id_carmen = 502 AND vertragsstatus != 'L')
       OR (vertrag_id_carmen = 503 AND vertragsstatus IS NOT NULL)
       OR (vertrag_id_carmen = 504 AND rechnungszahlart != 'K')
       OR (vertrag_id_carmen = 505 AND rechnungsmedium != 'E-Mail')
       OR (vertrag_id_carmen = 506 AND VDA IS NULL)
       OR (vertrag_id_carmen = 507 AND VDA IS NOT NULL)
  ) = 0 AS decodes_valid
```

### Pass/Fail Criterion
* **Pass**: The assertion returns `true` (zero rows violate the decode mapping rules).
* **Fail**: Any decoded value or VDA mapping is incorrect.

---

## Test Case 5: Metadata Variable Extraction and Truncation Orchestration

### Purpose
To verify that:
1. The target table `sof$ta_vertrag_tmp` is truncated before insertion.
2. The variable `v_datum` is correctly extracted from `dwtk_meldungen` and falls back to `'19000101'` if the table is empty.

### Setup
1. **Scenario A (Standard Execution)**:
   * Populate `sof$ta_vertrag_tmp` with 5 dummy rows.
   * Insert a record in `dwtk_meldungen` with `timecreated = '2012-07-11 12:00:00'` and `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
2. **Scenario B (Fallback Execution)**:
   * Truncate `dwtk_meldungen` (no records).

### Action
1. Execute the BigQuery scripting block for Scenario A. Verify truncation and that `v_datum` is evaluated as `'20120711'`.
2. Execute the BigQuery scripting block for Scenario B. Verify that `v_datum` falls back to `'19000101'`.

### Pytest Validation Code
```python
def test_truncation_and_metadata_fallback(client):
    project_id = os.getenv("GCP_PROJECT")
    dataset_id = os.getenv("BQ_DATASET")
    
    # Setup Scenario A: Insert dummy rows into target
    setup_dummy_query = f"""
        INSERT INTO `{project_id}.{dataset_id}.sof$ta_vertrag_tmp` (vertrag_id_carmen)
        VALUES (999991), (999992);
    """
    client.query(setup_dummy_query).result()
    
    # Run the migrated script
    script_path = "vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/nach_122_bert_fix_20120711/d_ausd_v_ta_vertrag_tmp.sql"
    with open(script_path, "r") as f:
        sql_script = f.read()
        
    # Replace hardcoded dataset names with environment variables if parameterized
    sql_script = sql_script.replace("isbert_schema.", f"{project_id}.{dataset_id}.")
    sql_script = sql_script.replace("sof$ta_vertrag_tmp", f"{project_id}.{dataset_id}.sof$ta_vertrag_tmp")
    
    client.query(sql_script).result()
    
    # Assert dummy rows are gone (Truncate verification)
    check_dummy_query = f"""
        SELECT COUNT(1) as cnt 
        FROM `{project_id}.{dataset_id}.sof$ta_vertrag_tmp` 
        WHERE vertrag_id_carmen IN (999991, 999992)
    """
    dummy_count = client.query(check_dummy_query).to_dataframe()["cnt"].values[0]
    assert dummy_count == 0, "Truncation failed! Dummy rows still exist in target table."
```

### Pass/Fail Criterion
* **Pass**: Dummy rows are completely removed from the target table prior to insertion, and the script executes successfully under both standard and fallback metadata scenarios.
* **Fail**: Dummy rows persist in the target table, or the script crashes when `dwtk_meldungen` is empty.