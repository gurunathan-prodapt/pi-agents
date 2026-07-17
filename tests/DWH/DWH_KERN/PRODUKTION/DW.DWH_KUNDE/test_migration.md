Here is the comprehensive test suite designed to validate the migration of the weekly customer address reconciliation job `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS` from its legacy Oracle/UC4/KSH environment to Google Cloud Composer (Airflow) and BigQuery.

---

# Migration Validation Test Suite
**Target Job:** `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS`  
**Migration Pattern:** UC4 + KSH + Oracle SQL $\rightarrow$ Cloud Composer (Airflow) + BigQuery SQL

---

## 1. Output Parity Tests

### Test Case 1.1: End-to-End Output Parity (Oracle vs. BigQuery)
* **Purpose:** Prove that given identical inputs in the staging/source tables, the migrated BigQuery SQL query produces the exact same output rows (discrepancies) as the legacy Oracle SQL query.
* **Setup:**
  1. Populate an Oracle test environment with a defined set of records in `DWH_KERN.T_KUNDE` and `STAMMDATEN.T_KUNDE_REFERENZ`.
  2. Populate the BigQuery target tables `{GCP_PROJECT}.{BQ_DATASET_DWH}.T_KUNDE` and `{GCP_PROJECT}.{BQ_DATASET_STAMM}.T_KUNDE_REFERENZ` with the exact same records.
  3. Include:
     * 5 matching records (no discrepancies).
     * 1 record with a mismatched Postal Code (`PLZ`).
     * 1 record with a mismatched City (`ORT`).
     * 1 record with a mismatched Street (`STRASSE`).
     * 1 record updated after the `stichtag` (should be excluded).
* **Action:**
  1. Execute the legacy Oracle SQL script `d_abgl_kunde_woech.sql` with `p_Stichtag = '20260301'`.
  2. Execute the migrated BigQuery SQL script `d_abgl_kunde_woech.sql` with `@stichtag = '20260301'`.
  3. Compare the output datasets on keys: `KUNDE`, `MARKER`, `PLZ`, `REF_PLZ`, `ORT`, `REF_ORT`, `STRASSE`, `REF_STRASSE`.
* **Pass/Fail Criterion:** The row count, column values, and ordering (`ORDER BY KUNDE`) must match exactly ($100\%$ parity).

```python
# pytest test_output_parity.py
import pytest
import pandas as pd
from google.cloud import bigquery
import cx_Oracle

def test_oracle_bq_parity():
    stichtag = "20260301"
    
    # 1. Fetch Oracle Results
    oracle_conn = cx_Oracle.connect("dwh_kern/pwd@DWHP1")
    oracle_query = """
        SELECT 'ABWEICHUNG' as MARKER, k.KUNDE, k.PLZ, r.PLZ as REF_PLZ, k.ORT, r.ORT as REF_ORT, k.STRASSE, r.STRASSE as REF_STRASSE
        FROM DWH_KERN.T_KUNDE k
        JOIN STAMMDATEN.T_KUNDE_REFERENZ r ON r.KUNDE = k.KUNDE
        WHERE k.AKTUALISIERT_AM <= TO_DATE(:stichtag, 'YYYYMMDD')
          AND (NVL(k.PLZ,'x') != NVL(r.PLZ,'x') OR NVL(k.ORT,'x') != NVL(r.ORT,'x') OR NVL(k.STRASSE,'x') != NVL(r.STRASSE,'x'))
        ORDER BY k.KUNDE
    """
    oracle_df = pd.read_sql(oracle_query, con=oracle_conn, params={"stichtag": stichtag})
    oracle_conn.close()

    # 2. Fetch BigQuery Results
    bq_client = bigquery.Client()
    bq_query = f"""
        SELECT 'ABWEICHUNG' as MARKER, k.KUNDE, k.PLZ, r.PLZ as REF_PLZ, k.ORT, r.ORT as REF_ORT, k.STRASSE, r.STRASSE as REF_STRASSE
        FROM `{bq_client.project}.DWH_KERN.T_KUNDE` k
        JOIN `{bq_client.project}.STAMMDATEN.T_KUNDE_REFERENZ` r ON r.KUNDE = k.KUNDE
        WHERE k.AKTUALISIERT_AM <= PARSE_DATE('%Y%m%d', '{stichtag}')
          AND (IFNULL(k.PLZ,'x') != IFNULL(r.PLZ,'x') OR IFNULL(k.ORT,'x') != IFNULL(r.ORT,'x') OR IFNULL(k.STRASSE,'x') != IFNULL(r.STRASSE,'x'))
        ORDER BY k.KUNDE
    """
    bq_df = bq_client.query(bq_query).to_dataframe()

    # 3. Assert Equivalence
    pd.testing.assert_frame_equal(oracle_df, bq_df, check_dtype=False)
```

---

## 2. Transformation Correctness & Edge Cases

### Test Case 2.1: NULL Handling (Oracle `NVL` vs. BigQuery `IFNULL`)
* **Purpose:** Verify that fields containing `NULL` values are handled identically to the legacy Oracle `NVL(col, 'x')` logic, preventing false positives or false negatives during comparison.
* **Setup:** Insert test records into BigQuery tables where:
  * Case A: `k.PLZ` is `NULL` and `r.PLZ` is `NULL` (Should be treated as equal $\rightarrow$ No discrepancy).
  * Case B: `k.PLZ` is `NULL` and `r.PLZ` is `'12345'` (Should be treated as unequal $\rightarrow$ Discrepancy).
  * Case C: `k.PLZ` is `'12345'` and `r.PLZ` is `NULL` (Should be treated as unequal $\rightarrow$ Discrepancy).
* **Action:** Run the BigQuery reconciliation query.
* **Pass/Fail Criterion:** 
  * Case A must **not** appear in the output.
  * Case B and Case C must appear in the output with `MARKER = 'ABWEICHUNG'`.

```sql
-- SQL Assertion Test
WITH test_data AS (
  SELECT 
    'Case A' as test_id, 
    STRUCT(1 as KUNDE, CAST(NULL AS STRING) as PLZ, 'Berlin' as ORT, 'Strasse' as STRASSE, DATE '2026-01-01' as AKTUALISIERT_AM) as k,
    STRUCT(1 as KUNDE, CAST(NULL AS STRING) as PLZ, 'Berlin' as ORT, 'Strasse' as STRASSE) as r
  UNION ALL
  SELECT 
    'Case B', 
    STRUCT(2, CAST(NULL AS STRING), 'Berlin', 'Strasse', DATE '2026-01-01'),
    STRUCT(2, '12345', 'Berlin', 'Strasse')
  UNION ALL
  SELECT 
    'Case C', 
    STRUCT(3, '12345', 'Berlin', 'Strasse', DATE '2026-01-01'),
    STRUCT(3, CAST(NULL AS STRING), 'Berlin', 'Strasse')
)
SELECT 
  test_id,
  (IFNULL(k.PLZ, 'x') != IFNULL(r.PLZ, 'x')) AS is_discrepancy
FROM test_data;

-- ASSERTION: 
-- Case A -> is_discrepancy = FALSE
-- Case B -> is_discrepancy = TRUE
-- Case C -> is_discrepancy = TRUE
```

### Test Case 2.2: Date Filter Boundary (`AKTUALISIERT_AM` <= `stichtag`)
* **Purpose:** Ensure that only records updated on or before the `stichtag` are evaluated.
* **Setup:** Insert three records into `T_KUNDE` with `stichtag = '20260301'`:
  1. Record 1: `AKTUALISIERT_AM = '2026-02-28'` (Before `stichtag` $\rightarrow$ Should be evaluated).
  2. Record 2: `AKTUALISIERT_AM = '2026-03-01'` (On `stichtag` $\rightarrow$ Should be evaluated).
  3. Record 3: `AKTUALISIERT_AM = '2026-03-02'` (After `stichtag` $\rightarrow$ Should be excluded).
  * All three records have address discrepancies against `T_KUNDE_REFERENZ`.
* **Action:** Execute the BigQuery reconciliation query with `stichtag = '20260301'`.
* **Pass/Fail Criterion:** Only Record 1 and Record 2 are returned in the output. Record 3 is completely ignored.

---

## 3. External-System Replacements & Wrapper Logic

### Test Case 3.1: Verbatim Logging and Discrepancy Counting (`grep -c` replacement)
* **Purpose:** Prove that the Python wrapper `r_abgl_kunde_woech.py` correctly parses the BigQuery result set, counts discrepancies, and prints the exact German log messages and warnings.
* **Setup:** Mock the BigQuery client to return:
  * Run A: 0 discrepancy rows.
  * Run B: 3 discrepancy rows.
* **Action:** Execute `run_reconciliation_workflow()` for both runs and capture `stdout` and `stderr`.
* **Pass/Fail Criterion:**
  * **Run A (0 Discrepancies):**
    * `stdout` must contain: `"Kundenadressabgleich fuer Lauf <date> angestossen"`
    * `stdout` must contain: `"Starte Adressabgleich Kundenstammdaten fuer Stichtag <date>"`
    * `stdout` must contain: `"Anzahl gefundener Abweichungen: 0"`
    * `stdout` must contain: `"Adressabgleich Kundenstammdaten ohne erkennbare Fehler beendet"`
    * `stderr` must be empty.
  * **Run B (3 Discrepancies):**
    * `stdout` must contain: `"Anzahl gefundener Abweichungen: 3"`
    * `stderr` must contain: `"[W] <timestamp> 3 Abweichungen im Kundenadressabgleich gefunden, siehe Protokoll"`

```python
# pytest test_wrapper_logging.py
import sys
import io
from unittest.mock import MagicMock, patch
from r_abgl_kunde_woech import run_reconciliation_workflow

@patch('r_abgl_kunde_woech.bigquery.Client')
def test_wrapper_logging_no_discrepancies(mock_bq_client):
    # Mock BQ to return 0 rows
    mock_client_instance = mock_bq_client.return_value
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = []
    mock_client_instance.query.return_value = mock_query_job

    captured_stdout = io.StringIO()
    sys.stdout = captured_stdout

    run_reconciliation_workflow(
        project_id="test-project",
        dataset_dwh="DWH_KERN",
        dataset_stamm="STAMMDATEN",
        stichtag="20260301"
    )
    
    sys.stdout = sys.__stdout__
    output = captured_stdout.getvalue()

    assert "Kundenadressabgleich fuer Lauf 20260301 angestossen" in output
    assert "Starte Adressabgleich Kundenstammdaten fuer Stichtag 20260301" in output
    assert "Anzahl gefundener Abweichungen: 0" in output
    assert "Adressabgleich Kundenstammdaten ohne erkennbare Fehler beendet" in output

@patch('r_abgl_kunde_woech.bigquery.Client')
def test_wrapper_warnings_on_discrepancies(mock_bq_client):
    # Mock BQ to return 2 rows with 'ABWEICHUNG'
    mock_client_instance = mock_bq_client.return_value
    mock_query_job = MagicMock()
    
    mock_row_1 = MagicMock()
    mock_row_1.MARKER = 'ABWEICHUNG'
    mock_row_1.KUNDE = 1001
    mock_row_1.PLZ = '12345'
    mock_row_1.REF_PLZ = '54321'
    
    mock_query_job.result.return_value = [mock_row_1, mock_row_1]
    mock_client_instance.query.return_value = mock_query_job

    captured_stderr = io.StringIO()
    sys.stderr = captured_stderr

    run_reconciliation_workflow(
        project_id="test-project",
        dataset_dwh="DWH_KERN",
        dataset_stamm="STAMMDATEN",
        stichtag="20260301"
    )
    
    sys.stderr = sys.__stderr__
    err_output = captured_stderr.getvalue()

    assert "[W]" in err_output
    assert "2 Abweichungen im Kundenadressabgleich gefunden" in err_output
```

---

## 4. Data Quality & Schema Assertions

### Test Case 4.1: Schema and Type Constraints
* **Purpose:** Ensure that the target BigQuery tables conform to the expected schema types to prevent runtime casting failures (e.g., `PARSE_DATE` errors).
* **Setup:** None (inspects active BigQuery metadata).
* **Action:** Query the BigQuery `INFORMATION_SCHEMA.COLUMNS` for the source tables.
* **Pass/Fail Criterion:**
  * `T_KUNDE.AKTUALISIERT_AM` must be of type `DATE` or `TIMESTAMP`.
  * `T_KUNDE.KUNDE` and `T_KUNDE_REFERENZ.KUNDE` must have matching data types (e.g., both `INT64` or both `STRING`) to prevent join degradation.

```sql
-- Schema validation assertion
SELECT 
  column_name, 
  data_type 
FROM 
  `DWH_KERN.INFORMATION_SCHEMA.COLUMNS`
WHERE 
  table_name = 'T_KUNDE' 
  AND column_name IN ('KUNDE', 'AKTUALISIERT_AM');

-- EXPECTED RESULT:
-- KUNDE: INT64 (or STRING)
-- AKTUALISIERT_AM: DATE (or TIMESTAMP)
```

### Test Case 4.2: Dynamic Date Parameter Fallback (7 Days Ago)
* **Purpose:** Verify that if no `stichtag` parameter is passed to the execution wrapper, it correctly defaults to the date from 7 days ago.
* **Setup:** Calculate the expected default date dynamically in Python: `(today - 7 days).strftime('%Y%m%d')`.
* **Action:** Call `resolve_stichtag(None)` from the python module.
* **Pass/Fail Criterion:** The returned string must match the calculated date exactly.

```python
# pytest test_date_fallback.py
import datetime
from r_abgl_kunde_woech import resolve_stichtag

def test_resolve_stichtag_fallback():
    expected_default = (datetime.date.today() - datetime.timedelta(days=7)).strftime('%Y%m%d')
    assert resolve_stichtag(None) == expected_default
    assert resolve_stichtag("20260520") == "20260520"
```