Here is a comprehensive suite of migration-validation tests designed for a Senior QA Engineer to validate the migration of the `DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JS` job from its legacy UC4/Ab Initio/Oracle state to Cloud Composer/Dataproc Serverless/BigQuery.

---

# Test Suite Overview: Migration Validation

These tests are structured to prove behavioral equivalence, data integrity, and operational compliance between the legacy and migrated systems.

```
                                  [TEST SUITE STRUCTURE]
                                             │
      ┌──────────────────────┬───────────────┴──────────────┬──────────────────────┐
      ▼                      ▼                              ▼                      ▼
[1. Output Parity]   [2. Transformations]       [3. External Systems]     [4. Orchestration]
  - End-to-End         - Null Handling            - BQ Target Writes        - Log Verbatim
  - Schema Parity      - Rounding & Decimals      - DLQ GCS Exports         - Pre/Post Checks
                       - Join & Split Logic
```

---

## Section 1: Output Parity & Schema Validation

### Test Case 1.1: End-to-End Output Parity (Golden Dataset Run)
#### Purpose
Verify that running the migrated PySpark job with a controlled "golden" input dataset produces identical outputs to the legacy Ab Initio graph run.

#### Setup
1. **Legacy Environment:**
   * Populate Oracle tables `STG_UMSATZ_TRANSAKTIONEN`, `DIM_KONZERNGESELLSCHAFT`, and `STG_TARIFGRUPPEN_MAPPING` with a deterministic test dataset representing month `202601` and company `ALL`.
   * Run the legacy script `r_umsatz_konsolidierung_monatlich.ksh -m 202601 -k ALL`.
   * Extract the resulting rows from Oracle table `FACT_UMSATZ_KONZERN_MONAT` to a CSV file (`legacy_output.csv`).
2. **Migrated Environment:**
   * Load the exact same test dataset into the corresponding BigQuery tables: `STG_UMSATZ_TRANSAKTIONEN`, `DIM_KONZERNGESELLSCHAFT`, and `STG_TARIFGRUPPEN_MAPPING`.
   * Run the PySpark job `umsatz_konsolidierung.py` on Dataproc Serverless with arguments `202601 ALL`.

#### Action
Execute a validation script to compare the BigQuery target table `FACT_UMSATZ_KONZERN_MONAT` against the legacy CSV output.

```python
# pytest test_parity.py
import pandas as pd
from google.cloud import bigquery

def test_e2e_output_parity():
    # Retrieve BigQuery results
    client = bigquery.Client()
    query = """
        SELECT konzerngesellschaft, verarbeitungsmonat, tarifgruppen_code, waehrung, 
               umsatz_summe_cent, storno_summe_cent, anzahl_buchungen
        FROM `DWH_TARGET.FACT_UMSATZ_KONZERN_MONAT`
        WHERE verarbeitungsmonat = '202601'
        ORDER BY konzerngesellschaft, tarifgruppen_code, waehrung
    """
    df_migrated = client.query(query).to_dataframe()
    
    # Load legacy golden dataset
    df_legacy = pd.read_csv("legacy_output.csv")
    df_legacy = df_legacy.sort_values(by=["konzerngesellschaft", "tarifgruppen_code", "waehrung"]).reset_index(drop=True)
    df_migrated = df_migrated.sort_values(by=["konzerngesellschaft", "tarifgruppen_code", "waehrung"]).reset_index(drop=True)
    
    # Assert structural and value equivalence
    pd.testing.assert_frame_equal(df_migrated, df_legacy, check_dtype=False)
```

#### Pass/Fail Criterion
* **Pass:** The BigQuery target table contains the exact same rows, column values, and row counts as the legacy Oracle target table.
* **Fail:** Any variance in numeric sums, counts, or string formatting is detected.

---

### Test Case 1.2: Target Schema and Type Mapping Validation
#### Purpose
Verify that the target BigQuery table `FACT_UMSATZ_KONZERN_MONAT` matches the structural expectations of downstream systems and aligns with the legacy Oracle schema.

#### Setup
Deploy the target BigQuery table `FACT_UMSATZ_KONZERN_MONAT` using the production DDL.

#### Action
Query the BigQuery Information Schema to assert column names, data types, and nullability.

```sql
-- SQL Assertion
SELECT 
  column_name, 
  data_type, 
  is_nullable
FROM 
  `DWH_TARGET.INFORMATION_SCHEMA.COLUMNS`
WHERE 
  table_name = 'FACT_UMSATZ_KONZERN_MONAT'
ORDER BY 
  ordinal_position;
```

#### Pass/Fail Criterion
* **Pass:** The schema matches the following specification exactly:
  | Column Name | Data Type | Is Nullable |
  | :--- | :--- | :--- |
  | `konzerngesellschaft` | `STRING` | `YES` |
  | `verarbeitungsmonat` | `STRING` | `YES` |
  | `tarifgruppen_code` | `STRING` | `YES` |
  | `waehrung` | `STRING` | `YES` |
  | `umsatz_summe_cent` | `INT64` | `YES` |
  | `storno_summe_cent` | `INT64` | `YES` |
  | `anzahl_buchungen` | `INT64` | `YES` |
* **Fail:** Any column is missing, has an incorrect data type (e.g., float instead of integer for cent values), or has mismatched nullability.

---

## Section 2: Transformation Correctness

### Test Case 2.1: Rounding and Decimal-to-Cent Conversion
#### Purpose
Verify that the PySpark transformation correctly rounds decimal amounts to cent integers (`round(umsatz_betrag * 100.0, 0)`) without floating-point precision loss.

#### Setup
Insert test records into `STG_UMSATZ_TRANSAKTIONEN` with edge-case decimal values:
* Record 1: `umsatz_betrag = 10.004` (Should round to `1000` cents)
* Record 2: `umsatz_betrag = 10.005` (Should round to `1001` cents)
* Record 3: `umsatz_betrag = 10.006` (Should round to `1001` cents)
* Record 4: `umsatz_betrag = -10.005` (Should round to `-1001` cents)

#### Action
Run the PySpark job and query the target BigQuery table.

```sql
-- SQL Assertion
SELECT 
  umsatz_summe_cent 
FROM 
  `DWH_TARGET.FACT_UMSATZ_KONZERN_MONAT`
WHERE 
  verarbeitungsmonat = '202602' 
  AND tarifgruppen_code = 'TEST_ROUND';
```

#### Pass/Fail Criterion
* **Pass:** The calculated cent values match the mathematical rounding expectations exactly (e.g., `10.005` becomes `1001` cents).
* **Fail:** Floating-point representation errors occur (e.g., `10.005` becomes `1000` or `1002` due to truncation or precision loss).

---

### Test Case 2.2: Null Handling and Default Fallbacks
#### Purpose
Verify that null values in source fields are handled gracefully according to the business rules (e.g., `waehrung` defaults to `'EUR'`).

#### Setup
Insert a record into `STG_UMSATZ_TRANSAKTIONEN` where `waehrung` is `NULL`.

#### Action
Run the PySpark job and query the target table to verify the currency fallback.

```sql
-- SQL Assertion
SELECT 
  waehrung, 
  COUNT(1) as cnt
FROM 
  `DWH_TARGET.FACT_UMSATZ_KONZERN_MONAT`
WHERE 
  verarbeitungsmonat = '202603'
GROUP BY 
  waehrung;
```

#### Pass/Fail Criterion
* **Pass:** The target table contains `'EUR'` for records that had a null currency in the staging table.
* **Fail:** The target table contains `NULL` values in the `waehrung` column, or the job crashes with a `NullPointerException`.

---

### Test Case 2.3: Join Logic and Split Routing (Regular vs. Storno)
#### Purpose
Verify that transactions are correctly classified and routed based on `buchungsart` (where `'STORNO'` and `'GUTSCHRIFT'` map to Stornos, and everything else maps to Regular bookings).

#### Setup
Insert four staging records:
1. `buchungsart = 'REGULAER'`, `umsatz_betrag = 100.00`
2. `buchungsart = 'NEU'`, `umsatz_betrag = 50.00`
3. `buchungsart = 'STORNO'`, `umsatz_betrag = 30.00`
4. `buchungsart = 'GUTSCHRIFT'`, `umsatz_betrag = 20.00`

#### Action
Run the PySpark job and query the aggregated sums.

```sql
-- SQL Assertion
SELECT 
  umsatz_summe_cent, 
  storno_summe_cent, 
  anzahl_buchungen
FROM 
  `DWH_TARGET.FACT_UMSATZ_KONZERN_MONAT`
WHERE 
  verarbeitungsmonat = '202604' 
  AND tarifgruppen_code = 'TEST_SPLIT';
```

#### Pass/Fail Criterion
* **Pass:** 
  * `umsatz_summe_cent` equals `15000` (100.00 + 50.00).
  * `storno_summe_cent` equals `5000` (30.00 + 20.00).
  * `anzahl_buchungen` equals `2` (only regular bookings are counted).
* **Fail:** Stornos are aggregated into regular sums, or booking counts include storno records.

---

## Section 3: External-System Replacements

### Test Case 3.1: Dead Letter Queue (DLQ) GCS Export for Unmatched Dimensions
#### Purpose
Verify that records with unmatched `konzerngesellschaft` identifiers are filtered out of the main pipeline and written to the GCS error directory.

#### Setup
Insert a staging record with an invalid company code: `konzerngesellschaft = 'INVALID_CO'`. Ensure this code does not exist in `DIM_KONZERNGESELLSCHAFT`.

#### Action
Run the PySpark job and check for the existence and content of the error file in GCS.

```bash
# Bash Assertion
GCS_BUCKET=$(airflow variables get GCS_BUCKET)
ERROR_FILE="gs://${GCS_BUCKET}/opt/dwh/errors/umsatz/umsatz_unmatched_ALL_202605.dat"

# Check if file exists in GCS
gsutil ls "${ERROR_FILE}"
if [ $? -eq 0 ]; then
    echo "DLQ File exists. Checking content..."
    gsutil cat "${ERROR_FILE}" | grep "INVALID_CO"
else
    echo "DLQ File does not exist!"
    exit 1
fi
```

#### Pass/Fail Criterion
* **Pass:** The unmatched record is excluded from the BigQuery target table and is successfully written to the GCS error file with a pipe (`|`) delimiter.
* **Fail:** The unmatched record is either imported into the target table, silently dropped without logging, or the GCS export fails.

---

## Section 4: Orchestration & Quality Assertions

### Test Case 4.1: Verbatim Logging Compliance
#### Purpose
Verify that the Airflow DAG and PySpark logs preserve the legacy German log statements character-for-character.

#### Setup
Trigger the Airflow DAG `dw_dwh_umsatz_konsolidierung_monatlich_js` for logical date `2026-06-01`.

#### Action
Retrieve and scan the task logs for the exact legacy strings.

```python
# pytest test_logging.py
def test_verbatim_logging_compliance():
    # Simulate log extraction from Airflow task run
    # In production, this reads from Cloud Logging or Airflow task log files
    log_content = get_airflow_task_logs(
        dag_id="dw_dwh_umsatz_konsolidierung_monatlich_js",
        task_id="log_legacy_start",
        execution_date="2026-06-01"
    )
    
    expected_start_msg = "Umsatzkonsolidierung fuer Monat 202606, Konzerngesellschaft ALL angestossen"
    assert expected_start_msg in log_content, f"Log missing verbatim start message: '{expected_start_msg}'"
```

#### Pass/Fail Criterion
* **Pass:** The exact string `"Umsatzkonsolidierung fuer Monat 202606, Konzerngesellschaft ALL angestossen"` is printed in the logs.
* **Fail:** The log message is missing, translated, or contains typographical differences.

---

### Test Case 4.2: Pre-Validation Guard (Period Existence Check)
#### Purpose
Verify that the DAG fails gracefully before running the PySpark job if the processing period is not registered in `DIM_PERIODE`.

#### Setup
Ensure that no record exists in `DIM_PERIODE` for `VERARBEITUNGSMONAT = '202607'` and `KONZERNGESELLSCHAFT = 'ALL'`.

#### Action
Trigger the Airflow DAG for logical date `2026-07-01`.

#### Pass/Fail Criterion
* **Pass:** The task `validate_period` fails, halting the DAG execution and preventing the `umsatz_konsolidierung` PySpark task from running.
* **Fail:** The DAG bypasses the validation and attempts to run the PySpark job on empty or unregistered period data.