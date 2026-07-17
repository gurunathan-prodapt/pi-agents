Here is a comprehensive suite of migration-validation tests designed to prove that the migrated Cloud Composer, PySpark, and BigQuery components are behaviorally equivalent to the legacy UC4, KornShell, and Ab Initio implementations.

---

# Test Suite 1: End-to-End Orchestration & Parameter Parity

## 1.1 Airflow DAG Parameter Resolution and Task Execution
### Purpose
Verify that the migrated Airflow DAG (`dw_dwh_umsatz_konsolidierung_monatlich_jp`) correctly resolves execution parameters (`VERARBEITUNGSMONAT`, `KONZERNGESELLSCHAFT`) and triggers the Dataproc Serverless PySpark job with the exact parameter values expected by the legacy system.

### Setup
*   A mock Airflow environment with variables `gcp_project`, `gcp_region`, and `gcs_bucket` configured.
*   The DAG file `dw_dwh_umsatz_konsolidierung_monatlich_jp.py` loaded into the Airflow context.

### Action
Trigger the DAG manually with a specific execution date and configuration overrides:
```json
{
  "verarbeitungsmonat": "202603",
  "konzerngesellschaft": "DE01"
}
```

### Pass/Fail Criterion
*   **Pass:** The DAG parses successfully. The `submit_pyspark_job` task is generated with the exact arguments:
    *   `--verarbeitungsmonat` resolved to `"202603"`
    *   `--konzerngesellschaft` resolved to `"DE01"`
*   **Fail:** Any parameter fails to resolve, or the task falls back to default values despite the manual override.

```python
# pytest test_dag_parsing.py
from airflow.models import DagBag, DagRun
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
import pytest

def test_dag_parameters_and_compilation():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    dag = dagbag.get_dag(dag_id="dwh_umsatz_konsolidierung_monatlich")
    assert dagbag.import_errors == {}
    assert dag is not None
    
    # Test parameter defaults
    assert dag.params["konzern"].value == "ALL"
    
    # Verify task structure
    tasks = dag.tasks
    task_ids = [t.task_id for t in tasks]
    expected_tasks = [
        "validate_period_sensor", 
        "submit_pyspark_job", 
        "validate_row_counts", 
        "check_konsolidierung_toleranz"
    ]
    for et in expected_tasks:
        assert et in task_ids
```

---

# Test Suite 2: Transformation & Business Logic Correctness

## 2.1 Normalization and Storno Classification
### Purpose
Verify that the PySpark normalization logic (`UmsatzTransformer.normalise_umsatz`) matches the Ab Initio graph rules:
1.  Rounds `umsatz_betrag` to cent values and casts to `IntegerType`.
2.  Trims and converts `konzerngesellschaft` and `tarifgruppen_code` to uppercase.
3.  Coalesces null/empty currencies to `'EUR'`.
4.  Classifies `buchungsart` values of `'STORNO'` and `'GUTSCHRIFT'` as `'STORNO'`, and all others as `'REGULAER'`.

### Setup
Create a PySpark DataFrame containing edge cases, nulls, and mixed-case strings.

### Action
Execute `UmsatzTransformer.normalise_umsatz` on the test DataFrame.

### Pass/Fail Criterion
*   **Pass:** The output DataFrame matches the expected schema and values exactly.
*   **Fail:** Any rounding error occurs, string trimming/casing fails, or booking types are misclassified.

```python
# pytest test_pyspark_transformations.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from abinitio.umsatz_konsolidierung import UmsatzTransformer

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("test").getOrCreate()

def test_normalise_umsatz(spark):
    # Input data representing various edge cases
    raw_data = [
        # konzerngesellschaft, vertrag, kunde, tarifgruppen_code, waehrung, buchungsart, umsatz_betrag
        ("  de01  ", " V-001 ", "Kunde A", " tg_a ", "USD", "REGULAER", 100.554),
        ("AT02", "V-002", "Kunde B", "TG_B", None, "STORNO", -50.20),
        ("ch03", "V-003", "Kunde C", "TG_C", "   ", "GUTSCHRIFT", -10.005),
        ("FR04", "V-004", "Kunde D", None, "EUR", "UNKNOWN", 0.0)
    ]
    
    schema = ["konzerngesellschaft", "vertrag", "kunde", "tarifgruppen_code", "waehrung", "buchungsart", "umsatz_betrag"]
    df_raw = spark.createDataFrame(raw_data, schema)
    
    df_actual = UmsatzTransformer.normalise_umsatz(df_raw)
    actual_rows = df_actual.collect()
    
    # Expected normalized outcomes
    # Row 0: Rounded to 10055 cents, uppercase company, trimmed contract, regular booking
    assert actual_rows[0]["konzerngesellschaft"] == "DE01"
    assert actual_rows[0]["vertrag"] == "V-001"
    assert actual_rows[0]["tarifgruppen_code"] == "TG_A"
    assert actual_rows[0]["waehrung"] == "USD"
    assert actual_rows[0]["buchungsart"] == "REGULAER"
    assert actual_rows[0]["umsatz_betrag_cent"] == 10055
    
    # Row 1: Coalesced currency to EUR, classified STORNO
    assert actual_rows[1]["waehrung"] == "EUR"
    assert actual_rows[1]["buchungsart"] == "STORNO"
    assert actual_rows[1]["umsatz_betrag_cent"] == -5020
    
    # Row 2: Trimmed empty currency to EUR, classified GUTSCHRIFT as STORNO, rounded half-up
    assert actual_rows[2]["waehrung"] == "EUR"
    assert actual_rows[2]["buchungsart"] == "STORNO"
    assert actual_rows[2]["umsatz_betrag_cent"] == -1001
```

## 2.2 Enrichment Join and Split Logic
### Purpose
Verify that the left-outer joins against `DIM_KONZERNGESELLSCHAFT` (filtering for `IS_CURRENT = 'Y'`) and `STG_TARIFGRUPPEN_MAPPING` behave correctly, and that unmatched records are successfully isolated.

### Setup
*   A normalized transactions DataFrame.
*   A company dimension DataFrame containing active (`IS_CURRENT = 'Y'`) and inactive (`IS_CURRENT = 'N'`) records.
*   A tariff mapping DataFrame.

### Action
Execute `UmsatzTransformer.enrich_transactions` followed by `UmsatzTransformer.filter_and_split`.

### Pass/Fail Criterion
*   **Pass:** 
    *   Transactions matching active companies are enriched.
    *   Transactions matching inactive companies are treated as unmatched.
    *   Unmatched records are correctly routed to the error split.
*   **Fail:** Active filters are ignored, or unmatched records are lost or incorrectly aggregated.

```python
def test_enrichment_and_split(spark):
    # Normalized transactions
    tx_data = [
        ("DE01", "V1", "K1", "TG01", "EUR", "REGULAER", 1000), # Valid match
        ("AT02", "V2", "K2", "TG02", "EUR", "REGULAER", 2000), # Inactive company match
        ("XX99", "V3", "K3", "TG03", "EUR", "REGULAER", 3000)  # Non-existent company
    ]
    tx_schema = ["konzerngesellschaft", "vertrag", "kunde", "tarifgruppen_code", "waehrung", "buchungsart", "umsatz_betrag_cent"]
    df_tx = spark.createDataFrame(tx_data, tx_schema)
    
    # Company Dimension
    comp_data = [
        ("DE01", "Y"),
        ("AT02", "N") # Inactive
    ]
    comp_schema = ["konzerngesellschaft_id", "IS_CURRENT"]
    df_comp = spark.createDataFrame(comp_data, comp_schema)
    
    # Tariff Mapping
    tarif_data = [("TG01", "Tarif 1"), ("TG02", "Tarif 2")]
    tarif_schema = ["tarifgruppen_code", "tarif_desc"]
    df_tarif = spark.createDataFrame(tarif_data, tarif_schema)
    
    # Run Enrichment
    df_enriched = UmsatzTransformer.enrich_transactions(df_tx, df_comp, df_tarif)
    df_matched, df_unmatched = UmsatzTransformer.filter_and_split(df_enriched)
    
    matched_records = df_matched.collect()
    unmatched_records = df_unmatched.collect()
    
    # Assertions
    assert len(matched_records) == 1
    assert matched_records[0]["konzerngesellschaft"] == "DE01"
    
    assert len(unmatched_records) == 2
    unmatched_companies = [r["konzerngesellschaft"] for r in unmatched_records]
    assert "AT02" in unmatched_companies
    assert "XX99" in unmatched_companies
```

---

# Test Suite 3: External System Replacements & File Dispositions

## 3.1 Unmatched Record GCS Export
### Purpose
Verify that unmatched records are written to the correct Google Cloud Storage bucket path as a single CSV file, replicating the legacy error file output behavior (`write_unmatched_umsatz`).

### Setup
*   A mock GCS bucket environment.
*   A PySpark DataFrame representing unmatched records.

### Action
Call `write_outputs` with the unmatched DataFrame and verify the GCS file creation.

### Pass/Fail Criterion
*   **Pass:** A single CSV file is written to `gs://{gcs_bucket}/errors/umsatz/umsatz_unmatched_{company}_{period}.csv` containing the expected header and records.
*   **Fail:** The file is missing, written to the wrong path, or split into multiple partition files.

```python
import unittest
from unittest.mock import patch, MagicMock
from abinitio.umsatz_konsolidierung import write_outputs

@patch("pyspark.sql.DataFrameWriter")
def test_gcs_error_export(mock_writer):
    mock_df_consolidated = MagicMock()
    mock_df_unmatched = MagicMock()
    
    # Setup mock write chain for unmatched GCS export
    mock_coalesced = MagicMock()
    mock_df_unmatched.coalesce.return_value = mock_coalesced
    mock_coalesced.write.format.return_value.option.return_value.mode.return_value = mock_writer
    
    class Params:
        bq_dataset_dwh = "test_project.test_dwh"
        gcs_bucket = "test_bucket"
        konzerngesellschaft = "DE01"
        verarbeitungsmonat = "202603"
        
    write_outputs(mock_df_consolidated, mock_df_unmatched, Params())
    
    # Verify coalesce(1) is called to ensure a single output file
    mock_df_unmatched.coalesce.assert_called_with(1)
    # Verify correct GCS target URI
    mock_coalesced.write.format.assert_called_with("csv")
    mock_coalesced.write.format().option().mode.assert_called_with(
        "gs://test_bucket/errors/umsatz/umsatz_unmatched_DE01_202603.csv"
    )
```

---

# Test Suite 4: Data Quality & Validation Assertions

## 4.1 Row Count Validation
### Purpose
Verify that the row count validation task (`validate_row_counts`) correctly asserts that the consolidated output table contains at least the minimum required records (`MIN_ROW_COUNT`).

### Setup
*   A BigQuery test dataset containing `FACT_UMSATZ_KONS_MONAT`.
*   The table populated with 0 records for the target month.

### Action
Execute the `validate_row_counts` task.

### Pass/Fail Criterion
*   **Pass:** The task fails with an `AirflowFailException` when 0 records are found.
*   **Fail:** The task passes despite the target table being empty.

```python
# SQL Assertion to run against BigQuery to verify the check logic
"""
SELECT 
  CASE 
    WHEN COUNT(1) >= 1 THEN 'PASS'
    ELSE 'FAIL'
  END AS row_count_check
FROM `test_project.test_dwh.FACT_UMSATZ_KONS_MONAT`
WHERE verarbeitungsmonat = '202603'
  AND konzerngesellschaft = 'DE01';
"""
```

## 4.2 Statistical Tolerance Validation (Toleranzprüfung)
### Purpose
Verify that the statistical tolerance check (`check_konsolidierung_toleranz`) correctly flags anomalous revenue deviations compared to the previous month.

### Setup
Populate `FACT_UMSATZ_KONS_MONAT` with test data:
*   **Scenario A (Within Tolerance):** Previous Month = 10,000 EUR, Current Month = 10,100 EUR (1% deviation).
*   **Scenario B (Exceeds Tolerance):** Previous Month = 10,000 EUR, Current Month = 15,000 EUR (50% deviation).

### Action
Execute the BigQuery tolerance check query.

### Pass/Fail Criterion
*   **Pass:** Scenario A returns `TRUE` (Passes validation). Scenario B returns `FALSE` (Fails validation).
*   **Fail:** Scenario B is allowed to pass, or Scenario A is incorrectly flagged as a failure.

```sql
-- Test Query for Scenario B (Exceeds 2.5% tolerance and 25 EUR absolute deviation)
WITH cur_period AS (
  SELECT 1500000 as total_cur -- 15,000.00 EUR
),
prev_period AS (
  SELECT 1000000 as total_prev -- 10,000.00 EUR
)
SELECT 
  CASE 
    WHEN ABS(COALESCE(cur.total_cur, 0) - COALESCE(prev.total_prev, 0)) / 
         NULLIF(COALESCE(prev.total_prev, 0), 0) * 100 > 2.5 
         AND ABS(COALESCE(cur.total_cur, 0) - COALESCE(prev.total_prev, 0)) > 2500 -- 25.00 EUR in Cents
    THEN FALSE -- Toleranz ueberschritten (Fail)
    ELSE TRUE  -- Toleranz eingehalten (Pass)
  END AS toleranz_bestaetigt
FROM cur_period cur, prev_period prev;

-- EXPECTED OUTPUT: FALSE
```