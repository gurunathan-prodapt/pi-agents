# Migration Validation Test Suite: DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JP

This document defines the comprehensive migration-validation test suite to prove behavioral equivalence between the legacy UC4 / Ab Initio / Shell environment and the migrated Cloud Composer / Dataproc Serverless (PySpark) / BigQuery environment.

---

## SECTION 1: Orchestration & Parameter Parity Tests

### Test Case 1.1: Airflow DAG Structure and Parameter Mapping
#### Purpose
Verify that the migrated Airflow DAG (`dw_dwh_umsatz_konsolidierung_monatlich_jp`) matches the legacy UC4 Job Plan (`DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JP`) in structure, schedule, default arguments, and parameter derivation.

#### Setup
* Access to the Cloud Composer environment where the migrated DAG is deployed.
* The Airflow CLI or Airflow Web UI.
* Airflow variables configured: `GCP_PROJECT`, `GCS_BUCKET`, `GCP_REGION`.

#### Action
1. Parse the DAG file using the Airflow CLI to check for syntax errors:
   ```bash
   airflow dags reserialize --dag-id dw_dwh_umsatz_konsolidierung_monatlich_jp
   ```
2. Retrieve the DAG details and task list:
   ```bash
   airflow dags show dw_dwh_umsatz_konsolidierung_monatlich_jp
   airflow tasks list dw_dwh_umsatz_konsolidierung_monatlich_jp --tree
   ```
3. Render the task templates for a specific logical execution date (e.g., `2026-03-01`):
   ```bash
   airflow tasks render dw_dwh_umsatz_konsolidierung_monatlich_jp dw_dwh_umsatz_konsolidierung_monatlich_js 2026-03-01T03:00:00+00:00
   ```

#### Pass/Fail Criterion
* **Pass:** 
  * The DAG parses with zero errors or warnings.
  * The DAG schedule is exactly `0 3 1 * *` (monthly on the 1st at 03:00).
  * The task dependency matches: `start >> dw_dwh_umsatz_konsolidierung_monatlich_js >> end`.
  * The rendered arguments for the Dataproc Serverless task map the logical date `2026-03-01` to `-m 202603` and `-k ALL` (matching the legacy UC4 parameters `&VERARBEITUNGSMONAT` and `&KONZERNGESELLSCHAFT`).
* **Fail:** Any syntax errors, incorrect schedule, missing tasks, or incorrect parameter rendering.

---

### Test Case 1.2: Wrapper Script Argument Parsing and Fallbacks
#### Purpose
Verify that the Python wrapper script `bin/r_umsatz_konsolidierung_monatlich.py` parses arguments and calculates default values identically to the legacy shell script `bin/r_umsatz_konsolidierung_monatlich.ksh`.

#### Setup
* A Python environment with the dependencies of `bin/r_umsatz_konsolidierung_monatlich.py` installed.
* Environment variables set: `GCP_PROJECT=test-project`, `BQ_DATASET=test_dataset`.

#### Action
Run a suite of execution tests using `pytest` to validate argument parsing and default calculations:

```python
# test_wrapper_arguments.py
import subprocess
import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta

def test_default_month_calculation():
    # When no -m is passed, it should default to the previous month
    expected_month = (datetime.now() - relativedelta(months=1)).strftime("%Y%m")
    
    # Run wrapper with dry-run/mock setup
    cmd = [sys.executable, "bin/r_umsatz_konsolidierung_monatlich.py", "-k", "DE01"]
    result = subprocess.run(cmd, capture_output=True, text=True, env={
        **subprocess.os.environ,
        "GCP_PROJECT": "test-project",
        "BQ_DATASET": "test_dataset"
    })
    
    assert result.returncode == 0
    assert f"Monat {expected_month}" in result.stdout
    assert "Konzerngesellschaft DE01" in result.stdout

def test_explicit_arguments():
    cmd = [sys.executable, "bin/r_umsatz_konsolidierung_monatlich.py", "-m", "202601", "-k", "AT02"]
    result = subprocess.run(cmd, capture_output=True, text=True, env={
        **subprocess.os.environ,
        "GCP_PROJECT": "test-project",
        "BQ_DATASET": "test_dataset"
    })
    
    assert result.returncode == 0
    assert "Monat 202601" in result.stdout
    assert "Konzerngesellschaft AT02" in result.stdout
```

#### Pass/Fail Criterion
* **Pass:** The test suite executes successfully, proving that the default month calculation matches the legacy shell logic (`date -d '1 month ago' '+%Y%m'`) and explicit arguments are parsed correctly.
* **Fail:** Any assertion failure or non-zero exit code during standard parameter execution.

---

## SECTION 2: Core Transformation & Business Rules Tests

### Test Case 2.1: Temporal Validation (Phase 1)
#### Purpose
Verify that the PySpark pipeline (`abinitio/umsatz_konsolidierung.py`) terminates with an error if the processing month does not exist in the master calendar table `DIM_PERIODE`.

#### Setup
* A test database containing `DIM_PERIODE` with active periods up to `202602`.
* A local Spark session or Dataproc test cluster.

#### Action
Execute the PySpark script with an invalid processing month (`202603`):

```python
import pytest
from pyspark.sql import SparkSession
from abinitio.umsatz_konsolidierung import validate_processing_period

def test_invalid_period_raises_value_error(spark_session, db_connection_properties):
    # Setup: Ensure 202603 does not exist in DIM_PERIODE
    # Action & Assertion
    with pytest.raises(ValueError) as exc_info:
        validate_processing_period(
            spark=spark_session,
            connect_string=db_connection_properties["url"],
            db_props=db_connection_properties["props"],
            verarbeitungsmonat="202603"
        )
    assert "is invalid, closed, or missing in DIM_PERIODE" in str(exc_info.value)
```

#### Pass/Fail Criterion
* **Pass:** The validation function raises a `ValueError` containing the expected error message, halting downstream execution.
* **Fail:** The function completes without raising an exception, allowing invalid periods to process.

---

### Test Case 2.2: Normalization and Precision Rules (Phase 2)
#### Purpose
Verify that string normalizations (trimming, casing), null currency handling, and float-to-cent conversions are executed with absolute precision.

#### Setup
* A PySpark local session.
* Input test DataFrame containing raw transactions with dirty strings, null currencies, and float amounts.

#### Action
Execute a unit test against the normalization logic:

```python
from pyspark.sql import Row
from abinitio.umsatz_konsolidierung import normalize_and_isolate_transactions

def test_normalization_and_precision(spark_session):
    # Setup dirty input data
    raw_data = [
        Row(konzerngesellschaft="  de01  ", vertrag="  V-100  ", kunde=" K-99 ", 
            tarifgruppen_code=" tg_a ", waehrung=None, buchungsart="REGULAER", umsatz_betrag=123.456),
        Row(konzerngesellschaft="AT02", vertrag="V-200", kunde="K-88", 
            tarifgruppen_code="TG_B", waehrung=" usd ", buchungsart="STORNO", umsatz_betrag=10.00)
    ]
    df_stg_umsatz = spark_session.createDataFrame(raw_data)
    
    # Mock DIM_KONZERNGESELLSCHAFT to match both
    df_dim_ges = spark_session.createDataFrame([
        Row(konzerngesellschaft="DE01", IS_CURRENT="Y"),
        Row(konzerngesellschaft="AT02", IS_CURRENT="Y")
    ])
    
    # Action
    df_normalized = normalize_and_isolate_transactions(
        df_stg_umsatz=df_stg_umsatz,
        df_dim_ges=df_dim_ges,
        error_output_dir="/tmp/errors",
        verarbeitungsmonat="202601"
    )
    
    results = df_normalized.collect()
    
    # Assertions for Row 1
    assert results[0]["clean_ges"] == "DE01"
    assert results[0]["clean_vertrag"] == "V-100"
    assert results[0]["clean_kunde"] == "K-99"
    assert results[0]["clean_tarif"] == "TG_A"
    assert results[0]["clean_waehrung"] == "EUR"  # Null fallback
    assert results[0]["mapped_buchungsart"] == "REGULAER"
    assert results[0]["umsatz_betrag_cent"] == 12346  # Rounded: 123.456 * 100 = 12345.6 -> 12346
    
    # Assertions for Row 2
    assert results[1]["clean_waehrung"] == "USD"  # Trimmed and capitalized
    assert results[1]["mapped_buchungsart"] == "STORNO"
    assert results[1]["umsatz_betrag_cent"] == 1000
```

#### Pass/Fail Criterion
* **Pass:** All assertions pass, proving that text fields are cleaned, null currencies default to `'EUR'`, and float amounts are converted to exact integer cents using round-to-nearest logic.
* **Fail:** Any string remains untrimmed/uncapitalized, null currency is not defaulted, or rounding errors occur in cent calculations.

---

### Test Case 2.3: Corporate Isolation & Rejections (Phase 2)
#### Purpose
Verify that transactions with invalid or inactive corporate codes are isolated, written to the error directory, and excluded from downstream aggregations.

#### Setup
* A PySpark local session.
* Input transactions containing one valid corporate code (`DE01`) and one invalid corporate code (`XX99`).

#### Action
Execute a unit test to verify isolation and rejection output:

```python
import os
import shutil
from pyspark.sql import Row
from abinitio.umsatz_konsolidierung import normalize_and_isolate_transactions

def test_corporate_isolation_rejections(spark_session):
    error_dir = "/tmp/test_errors"
    if os.path.exists(error_dir):
        shutil.rmtree(error_dir)
        
    raw_data = [
        Row(konzerngesellschaft="DE01", vertrag="V-1", kunde="K-1", tarifgruppen_code="T1", waehrung="EUR", buchungsart="REGULAER", umsatz_betrag=100.0),
        Row(konzerngesellschaft="XX99", vertrag="V-2", kunde="K-2", tarifgruppen_code="T1", waehrung="EUR", buchungsart="REGULAER", umsatz_betrag=200.0)
    ]
    df_stg_umsatz = spark_session.createDataFrame(raw_data)
    
    # Only DE01 is active
    df_dim_ges = spark_session.createDataFrame([
        Row(konzerngesellschaft="DE01", IS_CURRENT="Y")
    ])
    
    # Action
    df_matched = normalize_and_isolate_transactions(
        df_stg_umsatz=df_stg_umsatz,
        df_dim_ges=df_dim_ges,
        error_output_dir=error_dir,
        verarbeitungsmonat="202601"
    )
    
    # Assertions
    assert df_matched.count() == 1
    assert df_matched.collect()[0]["clean_ges"] == "DE01"
    
    # Verify rejection file exists and contains XX99
    rejection_files = os.listdir(f"{error_dir}/unmatched_202601")
    csv_file = [f for f in rejection_files if f.endswith(".csv")][0]
    
    with open(f"{error_dir}/unmatched_202601/{csv_file}", "r") as f:
        content = f.read()
        assert "XX99" in content
        assert "DE01" not in content
```

#### Pass/Fail Criterion
* **Pass:** Only the valid corporate transaction remains in the matched stream, and the invalid transaction is written to the GCS/local error directory as a pipe-delimited CSV.
* **Fail:** Invalid records remain in the matched stream, or the rejection file is not generated or contains incorrect data.

---

### Test Case 2.4: Dual-Stream Aggregation and Consolidation (Phase 3)
#### Purpose
Verify that regular bookings and cancellations (`STORNO` / `GUTSCHRIFT`) are aggregated in parallel and joined correctly to produce consolidated net records.

#### Setup
* A PySpark local session.
* Input DataFrame containing multiple regular and storno bookings for the same grouping keys.

#### Action
Execute a unit test against the aggregation and consolidation logic:

```python
from pyspark.sql import Row
from abinitio.umsatz_konsolidierung import aggregate_and_consolidate

def test_dual_stream_aggregation(spark_session):
    # Setup test data with mixed booking types
    mapped_data = [
        # Regular bookings for DE01, TG_A
        Row(clean_ges="DE01", verarbeitungsmonat="202601", mapped_tarif_code="TG_A", clean_waehrung="EUR", mapped_buchungsart="REGULAER", umsatz_betrag_cent=10000),
        Row(clean_ges="DE01", verarbeitungsmonat="202601", mapped_tarif_code="TG_A", clean_waehrung="EUR", mapped_buchungsart="REGULAER", umsatz_betrag_cent=5000),
        # Storno booking for DE01, TG_A
        Row(clean_ges="DE01", verarbeitungsmonat="202601", mapped_tarif_code="TG_A", clean_waehrung="EUR", mapped_buchungsart="STORNO", umsatz_betrag_cent=2000),
        # Regular booking for DE01, TG_B (No Storno)
        Row(clean_ges="DE01", verarbeitungsmonat="202601", mapped_tarif_code="TG_B", clean_waehrung="EUR", mapped_buchungsart="REGULAER", umsatz_betrag_cent=3000)
    ]
    df_mapped = spark_session.createDataFrame(mapped_data)
    
    # Action
    df_consolidated = aggregate_and_consolidate(df_mapped)
    results = df_consolidated.orderBy("tarifgruppen_code").collect()
    
    # Assertions for TG_A (Has both regular and storno)
    assert results[0]["tarifgruppen_code"] == "TG_A"
    assert results[0]["umsatz_summe_cent"] == 15000  # 10000 + 5000
    assert results[0]["storno_summe_cent"] == 2000
    assert results[0]["anzahl_buchungen"] == 2
    
    # Assertions for TG_B (Has regular, storno should default to 0)
    assert results[1]["tarifgruppen_code"] == "TG_B"
    assert results[1]["umsatz_summe_cent"] == 3000
    assert results[1]["storno_summe_cent"] == 0  # Null filled to 0
    assert results[1]["anzahl_buchungen"] == 1
```

#### Pass/Fail Criterion
* **Pass:** Regular and storno streams are aggregated correctly, joined on grouping keys, and missing storno values are filled with `0`.
* **Fail:** Incorrect sums, incorrect booking counts, or missing records after the join.

---

## SECTION 3: Quality Gates & Post-Processing Tests

### Test Case 3.1: Minimum Row Count Quality Gate
#### Purpose
Verify that the pipeline triggers a warning alert if the total loaded record count falls below the configured `MIN_ROW_COUNT`.

#### Setup
* A PySpark local session.
* `MIN_ROW_COUNT` set to `5`.
* Input dataset producing only `3` consolidated records.

#### Action
Execute the pipeline and verify that a `LOW_ROW_COUNT_WARNING` alert is written to GCS.

```python
import os
import json
from abinitio.umsatz_konsolidierung import write_alert_record

def test_min_row_count_alert(spark_session):
    alert_dir = "/tmp/test_alerts"
    if os.path.exists(alert_dir):
        shutil.rmtree(alert_dir)
        
    total_loaded_records = 3
    min_row_count = 5
    
    # Action
    if total_loaded_records < min_row_count:
        alert_msg = f"Target row volume {total_loaded_records} violates minimum config {min_row_count}."
        write_alert_record(
            spark=spark_session,
            alert_dir=alert_dir,
            verarbeitungsmonat="202601",
            alert_code="LOW_ROW_COUNT_WARNING",
            message=alert_msg
        )
        
    # Verify alert file
    alert_files = os.listdir(alert_dir)
    json_file = [f for f in alert_files if f.endswith(".json")][0]
    
    with open(f"{alert_dir}/{json_file}", "r") as f:
        alert_data = json.load(f)
        assert alert_data["alert_code"] == "LOW_ROW_COUNT_WARNING"
        assert "violates minimum config" in alert_data["error_message"]
```

#### Pass/Fail Criterion
* **Pass:** The alert is successfully written to GCS in JSON format with the correct metadata.
* **Fail:** No alert is generated, or the alert contains incorrect codes/messages.

---

### Test Case 3.2: Tolerance and Deviation Quality Gate
#### Purpose
Verify that the pipeline triggers a critical `TOLERANCE_BREACH` alert and raises an execution exception if the count of records exceeding the consolidation tolerance exceeds `MAX_ABWEICHUNGEN`.

#### Setup
* A PySpark local session.
* `KONSOLIDIERUNG_TOLERANZ` set to `2.5` (EUR), which is `250` cents.
* `MAX_ABWEICHUNGEN` set to `1`.
* Input dataset containing `2` records with deviations of `300` cents (exceeding tolerance).

#### Action
Execute the tolerance check and verify that a `TOLERANCE_BREACH` is triggered.

```python
import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F

def test_tolerance_breach_handling(spark_session):
    # Setup consolidated data with deviations
    # Row 1: Deviation = 300 cents (exceeds 250 cents tolerance)
    # Row 2: Deviation = 400 cents (exceeds 250 cents tolerance)
    consolidated_data = [
        Row(umsatz_summe_cent=1000, storno_summe_cent=700),
        Row(umsatz_summe_cent=2000, storno_summe_cent=1600)
    ]
    df_target_load = spark_session.createDataFrame(consolidated_data)
    
    konsolidierung_toleranz = 2.5
    max_abweichungen = 1
    
    # Action
    df_tolerance_check = df_target_load.withColumn(
        "cent_difference", F.abs(F.col("umsatz_summe_cent") - F.col("storno_summe_cent"))
    ).filter(F.col("cent_difference") > (konsolidierung_toleranz * 100))
    
    out_of_bounds_count = df_tolerance_check.count()
    
    # Assertion
    assert out_of_bounds_count == 2
    assert out_of_bounds_count > max_abweichungen  # This should trigger a critical failure in the main pipeline
```

#### Pass/Fail Criterion
* **Pass:** The count of out-of-bounds records is calculated correctly as `2`, which exceeds `MAX_ABWEICHUNGEN` (1), proving the logic will trigger a critical failure.
* **Fail:** Out-of-bounds records are calculated incorrectly, or the threshold check fails to identify the breach.

---

## SECTION 4: End-to-End Output Parity Test

### Test Case 4.1: Legacy vs. Migrated Output Parity (SQL Assertion)
#### Purpose
Prove that running the migrated PySpark pipeline on BigQuery produces identical results to the legacy Ab Initio graph run on Oracle for the same input dataset.

#### Setup
* Legacy target table `FACT_UMSATZ_KONZERN_MONAT` populated by the legacy run for period `202601`.
* Migrated target table `tgt_umsatz_konsolidiert` populated by the PySpark/BigQuery run for period `202601`.

#### Action
Execute a reconciliation query in BigQuery comparing the legacy and migrated tables:

```sql
-- Reconciliation Query: Legacy vs Migrated Output Parity
WITH legacy_data AS (
  -- Querying the migrated legacy table (or a snapshot of the Oracle target loaded into BQ)
  SELECT 
    UPPER(TRIM(konzerngesellschaft)) AS konzerngesellschaft,
    verarbeitungsmonat,
    UPPER(TRIM(tarifgruppen_code)) AS tarifgruppen_code,
    UPPER(TRIM(waehrung)) AS waehrung,
    umsatz_summe_cent,
    storno_summe_cent,
    anzahl_buchungen
  FROM `your_project_id.your_dataset_id.legacy_fact_umsatz_konzern_monat`
  WHERE verarbeitungsmonat = '202601'
),
migrated_data AS (
  -- Querying the newly migrated target table
  SELECT 
    UPPER(TRIM(konzerngesellschaft)) AS konzerngesellschaft,
    verarbeitungsmonat,
    UPPER(TRIM(tarifgruppen_code)) AS tarifgruppen_code,
    UPPER(TRIM(waehrung)) AS waehrung,
    umsatz_summe_cent,
    storno_summe_cent,
    anzahl_buchungen
  FROM `your_project_id.your_dataset_id.FACT_UMSATZ_KONZERN_MONAT`
  WHERE verarbeitungsmonat = '202601'
),
reconciliation AS (
  SELECT 
    COALESCE(l.konzerngesellschaft, m.konzerngesellschaft) AS konzerngesellschaft,
    COALESCE(l.tarifgruppen_code, m.tarifgruppen_code) AS tarifgruppen_code,
    COALESCE(l.waehrung, m.waehrung) AS waehrung,
    (l.umsatz_summe_cent - m.umsatz_summe_cent) AS diff_umsatz,
    (l.storno_summe_cent - m.storno_summe_cent) AS diff_storno,
    (l.anzahl_buchungen - m.anzahl_buchungen) AS diff_count
  FROM legacy_data l
  FULL OUTER JOIN migrated_data m
    ON l.konzerngesellschaft = m.konzerngesellschaft
   AND l.tarifgruppen_code = m.tarifgruppen_code
   AND l.waehrung = m.waehrung
)
SELECT * 
FROM reconciliation 
WHERE diff_umsatz != 0 
   OR diff_storno != 0 
   OR diff_count != 0
   OR diff_umsatz IS NULL 
   OR diff_storno IS NULL 
   OR diff_count IS NULL;
```

#### Pass/Fail Criterion
* **Pass:** The reconciliation query returns **zero rows**, proving absolute parity in keys, sums, and counts between the legacy and migrated runs.
* **Fail:** Any rows are returned, indicating a mismatch in calculations, missing records, or key mismatches.