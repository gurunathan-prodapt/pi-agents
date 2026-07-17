Here is a comprehensive suite of migration-validation tests designed for the migrated weekly customer address reconciliation job (`DW.DWH_KUNDE_ABGL_WOECHENTLICH_JP`). 

These tests are structured to prove behavioral equivalence between the legacy Oracle/KornShell environment and the target Google Cloud Composer (Airflow) + BigQuery environment.

---

# Test Suite: `DW.DWH_KUNDE_ABGL_WOECHENTLICH` Validation

## 1. Output Parity & Functional Equivalence

### Test Case 1.1: End-to-End Address Reconciliation Output Parity
* **Purpose**: Prove that given the same input datasets (Customer Master and Reference), the migrated BigQuery SQL logic produces the exact same mismatch records as the legacy Oracle SQL script.
* **Setup**:
  1. Populate a test Oracle schema with a set of 10 customer records in `DWH_KERN.T_KUNDE` and corresponding reference records in `STAMMDATEN.T_KUNDE_REFERENZ`.
     * Include 3 matching records.
     * Include 3 records with mismatched `PLZ` (Postal Code).
     * Include 2 records with mismatched `ORT` (City).
     * Include 2 records with mismatched `STRASSE` (Street).
     * Include 1 record where `AKTUALISIERT_AM` is greater than the test `p_Stichtag` (should be excluded).
  2. Load the exact same dataset into the target BigQuery tables: `project.DWH_KERN.T_KUNDE` and `project.STAMMDATEN.T_KUNDE_REFERENZ`.
* **Action**:
  1. Execute the legacy Oracle SQL script using SQL*Plus with `p_Stichtag = '20241007'`. Export the output to a CSV file (`legacy_output.csv`).
  2. Execute the migrated BigQuery SQL query (from `dw_dwh_kunde_abgl_woechentlich_sql.py`) with `@p_Stichtag = '20241007'`. Export the results to a CSV file (`target_output.csv`).
* **Pass/Fail Criterion**: The contents of `legacy_output.csv` and `target_output.csv` must match exactly (100% row-by-row parity, ignoring order if sorted differently, though both specify `ORDER BY KUNDE`).

```python
# pytest code for output parity validation
import pandas as pd

def test_sql_output_parity():
    legacy_df = pd.read_csv("legacy_output.csv").sort_values(by="KUNDE").reset_index(drop=True)
    target_df = pd.read_csv("target_output.csv").sort_values(by="KUNDE").reset_index(drop=True)
    
    # Assert identical shapes and column values
    pd.testing.assert_frame_equal(legacy_df, target_df, check_dtype=False)
```

---

## 2. Transformation Correctness & Edge Cases

### Test Case 2.1: NULL Handling and Coalesce Equivalence
* **Purpose**: Verify that `NULL` values in address fields (`PLZ`, `ORT`, `STRASSE`) do not cause false positives or false negatives during comparison. The legacy Oracle script uses `nvl(field, 'x')` while BigQuery uses `COALESCE(field, 'x')`.
* **Setup**:
  * Populate test tables in BigQuery with the following edge cases:
    * Case A: Master has `PLZ = NULL`, Reference has `PLZ = NULL` (Should be treated as **Equal** -> No mismatch).
    * Case B: Master has `PLZ = NULL`, Reference has `PLZ = '12345'` (Should be treated as **Mismatched** -> Flagged).
    * Case C: Master has `PLZ = 'x'`, Reference has `PLZ = NULL` (Should be treated as **Mismatched** -> Flagged).
* **Action**: Run the BigQuery reconciliation query.
* **Pass/Fail Criterion**: 
  * Case A must **not** appear in the output.
  * Case B and Case C **must** appear in the output.

```sql
-- SQL Assertion to verify NULL handling
WITH test_results AS (
  SELECT
    k.KUNDE,
    (COALESCE(k.PLZ, 'x') != COALESCE(r.PLZ, 'x')) AS plz_mismatch
  FROM (
    SELECT 'A' AS KUNDE, CAST(NULL AS STRING) AS PLZ, '2024-10-07' AS AKTUALISIERT_AM UNION ALL
    SELECT 'B', CAST(NULL AS STRING), '2024-10-07' UNION ALL
    SELECT 'C', 'x', '2024-10-07'
  ) k
  JOIN (
    SELECT 'A' AS KUNDE, CAST(NULL AS STRING) AS PLZ UNION ALL
    SELECT 'B', '12345', CAST(NULL AS STRING) UNION ALL
    SELECT 'C', CAST(NULL AS STRING), CAST(NULL AS STRING)
  ) r ON k.KUNDE = r.KUNDE
)
SELECT 
  KUNDE, 
  plz_mismatch 
FROM test_results;

-- EXPECTED OUTPUT:
-- KUNDE | plz_mismatch
-- A     | false
-- B     | true
-- C     | true
```

### Test Case 2.2: Date Filter Boundary Conditions (`AKTUALISIERT_AM`)
* **Purpose**: Prove that the date filter `k.AKTUALISIERT_AM <= PARSE_DATE('%Y%m%d', @p_Stichtag)` behaves identically to Oracle's `to_date` comparison.
* **Setup**:
  * Set `@p_Stichtag = '20241007'`.
  * Insert records into `T_KUNDE` with `AKTUALISIERT_AM` values of:
    * Record 1: `2024-10-06` (Before Stichtag -> Should be processed).
    * Record 2: `2024-10-07` (On Stichtag -> Should be processed).
    * Record 3: `2024-10-08` (After Stichtag -> Should be excluded).
* **Action**: Execute the reconciliation query.
* **Pass/Fail Criterion**: Record 1 and Record 2 are evaluated for mismatches; Record 3 is completely ignored.

---

## 3. External-System Replacements & Orchestration

### Test Case 3.1: Airflow Variable Resolution and Fallbacks
* **Purpose**: Ensure that the custom variable retrieval function `get_gcp_variable` correctly fetches Airflow variables and throws appropriate errors when required variables are missing.
* **Setup**: Initialize a clean Airflow metadata database.
* **Action**:
  1. Define `GCP_PROJECT` in Airflow Variables and call `get_gcp_variable("GCP_PROJECT")`.
  2. Call `get_gcp_variable("NON_EXISTENT_VAR")` without a default value.
  3. Call `get_gcp_variable("NON_EXISTENT_VAR", default="fallback_value")`.
* **Pass/Fail Criterion**:
  1. Step 1 returns the correct project ID.
  2. Step 2 raises a `KeyError`.
  3. Step 3 returns `"fallback_value"`.

```python
import pytest
from airflow.models import Variable
from dw_dwh_kunde.bin.r_abgl_kunde_woech import get_gcp_variable

def test_get_gcp_variable(cleanup_vars):
    # Setup
    Variable.set("GCP_PROJECT", "test-gcp-project")
    
    # Test 1: Successful retrieval
    assert get_gcp_variable("GCP_PROJECT") == "test-gcp-project"
    
    # Test 2: Missing variable raises KeyError
    with pytest.raises(KeyError) as excinfo:
        get_gcp_variable("MISSING_VARIABLE")
    assert "Missing required Airflow Variable" in str(excinfo.value)
    
    # Test 3: Fallback value
    assert get_gcp_variable("MISSING_VARIABLE", default="fallback") == "fallback"
```

### Test Case 3.2: Verbatim Logging Verification
* **Purpose**: Prove that the Airflow task logs capture the exact German logging strings required by legacy downstream monitoring systems.
* **Setup**: Configure a mock logger and run the `pre_execution_logging` and `post_execution_logging` tasks.
* **Action**:
  1. Trigger `pre_execution_logging` with `lauf_woche = '20241007'`.
  2. Trigger `post_execution_logging` with `lauf_woche = '20241007'` (mocking BigQuery hook to return 42 deviations).
* **Pass/Fail Criterion**:
  * The log output must contain the exact strings:
    * `Kundenadressabgleich fuer Lauf 20241007 angestossen`
    * `Starte Adressabgleich Kundenstammdaten...`
    * `Anzahl gefundener Abweichungen: 42`
    * `Adressabgleich Kundenstammdaten ohne erkennbare Fehler beendet`

```python
def test_verbatim_logging(caplog):
    import logging
    from dw_dwh_kunde.bin.r_abgl_kunde_woech import pre_execution_logging
    
    context = {"templates_dict": {"lauf_woche": "20241007"}}
    
    with caplog.at_level(logging.INFO):
        pre_execution_logging(**context)
        
    assert "Kundenadressabgleich fuer Lauf 20241007 angestossen" in caplog.text
    assert "Starte Adressabgleich Kundenstammdaten..." in caplog.text
```

---

## 4. Data-Quality, Schema, & Row-Count Assertions

### Test Case 4.1: Target Table Schema Validation
* **Purpose**: Assert that the target table `T_ABGL_KUNDE_ERR` is created with correct data types, partitioning, and clustering to prevent performance degradation.
* **Setup**: Deploy the DDL schema to BigQuery.
* **Action**: Query the BigQuery `INFORMATION_SCHEMA.COLUMNS` and `INFORMATION_SCHEMA.TABLES` for `T_ABGL_KUNDE_ERR`.
* **Pass/Fail Criterion**:
  * Column `STICHTAG` must be of type `DATE`.
  * Column `KUNDEN_ID` must be of type `STRING` and `NOT NULL`.
  * The table must be partitioned by `STICHTAG`.
  * The table must be clustered by `KUNDEN_ID`.

```sql
-- SQL Assertion for Schema Validation
SELECT 
  column_name, 
  data_type, 
  is_nullable
FROM 
  `project.reporting_dataset.INFORMATION_SCHEMA.COLUMNS`
WHERE 
  table_name = 'T_ABGL_KUNDE_ERR'
  AND column_name IN ('STICHTAG', 'KUNDEN_ID');

-- Assert Partitioning and Clustering
SELECT 
  is_partitioning_supported, 
  clustering_fields
FROM 
  `project.reporting_dataset.INFORMATION_SCHEMA.TABLES`
WHERE 
  table_name = 'T_ABGL_KUNDE_ERR';
```

### Test Case 4.2: Row-Count Integrity Check
* **Purpose**: Ensure that the number of rows inserted into `T_ABGL_KUNDE_ERR` matches the count of discrepancies calculated during the run.
* **Setup**: Run the reconciliation pipeline for a specific `ds` (e.g., `2024-10-07`).
* **Action**:
  1. Query the count of rows in `T_ABGL_KUNDE_ERR` where `STICHTAG = '2024-10-07'`.
  2. Compare this count with the value logged in `task_log_count` (retrieved via XCom).
* **Pass/Fail Criterion**: The physical row count in the table must exactly equal the logged discrepancy count.