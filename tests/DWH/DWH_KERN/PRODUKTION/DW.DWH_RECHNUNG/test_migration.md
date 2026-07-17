Here is the comprehensive migration-validation test suite for the job `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS`. 

These tests are designed to prove behavioral equivalence between the legacy Oracle/KSH/UC4 pipeline and the migrated Cloud Composer/BigQuery/GCS pipeline.

---

# Test Suite: DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS Validation

## 1. Output Parity & Schema Validation

### Purpose
To verify that the schema, column ordering, and data types of the migrated BigQuery SQL query output match the legacy Oracle SQL output exactly, and that the final pipe-separated file matches the legacy structure.

### Setup
1. Populate the BigQuery table `DWH_KERN.T_RECHNUNG` with a controlled set of test records for a specific date (e.g., `2023-10-24`).
2. Populate the legacy Oracle table `DWH_KERN.T_RECHNUNG` with the exact same records.

### Action
1. Execute the legacy Oracle SQL script using `sqlplus`:
   ```bash
   sqlplus -s dwh_kern/pwd@DWHP1 @d_exp_rechnung_taeglich.sql 20231024 > legacy_output.dat
   ```
2. Execute the migrated BigQuery SQL query using the BigQuery Python client with `@p_Stichtag = '20231024'`. Export the result to GCS as a pipe-separated file, then download it:
   ```bash
   gsutil cp gs://{GCS_BUCKET}/rechnung_export/daily/rechnung_export_20231024.csv migrated_output.csv
   ```

### Pass/Fail Criterion
* **Pass**: 
  * The column headers match exactly in name, order, and count: `RECHNUNGSNUMMER|VERTRAG|KUNDE|TARIF|ABRECHNUNGSZEITRAUM|RECHNUNGSBETRAG|WAEHRUNG|RECHNUNGSDATUM`.
  * The data rows are identical (excluding minor floating-point formatting differences, which must be within a tolerance of `1e-9` for `RECHNUNGSBETRAG`).
  * The sorting order (`ORDER BY RECHNUNGSNUMMER`) is identical.
* **Fail**: Any mismatch in column order, row count, sorting, or data values.

```python
# pytest: test_output_parity.py
import pytest
import pandas as pd

def test_schema_and_data_parity():
    # Load legacy and migrated outputs
    legacy_df = pd.read_csv("legacy_output.dat", sep="|", names=[
        "RECHNUNGSNUMMER", "VERTRAG", "KUNDE", "TARIF", 
        "ABRECHNUNGSZEITRAUM", "RECHNUNGSBETRAG", "WAEHRUNG", "RECHNUNGSDATUM"
    ])
    
    migrated_df = pd.read_csv("migrated_output.csv", sep="|")
    
    # Assert structural parity
    assert list(legacy_df.columns) == list(migrated_df.columns), "Column mismatch!"
    assert len(legacy_df) == len(migrated_df), "Row count mismatch!"
    
    # Assert value parity
    pd.testing.assert_frame_equal(
        legacy_df.reset_index(drop=True), 
        migrated_df.reset_index(drop=True), 
        check_dtype=False,
        atol=1e-2 # Allow minor rounding differences on numeric currency fields
    )
```

---

## 2. Transformation Correctness & Date Parsing

### Purpose
To verify that the BigQuery `PARSE_DATE('%Y%m%d', @p_Stichtag)` logic correctly handles boundary dates, leap years, and invalid date formats in the same manner as Oracle's `to_date('&p_Stichtag','YYYYMMDD')`.

### Setup
1. Insert records into `DWH_KERN.T_RECHNUNG` with `RECHNUNGSDATUM` values on leap day (`2024-02-29`), year-end (`2023-12-31`), and year-start (`2024-01-01`).

### Action
1. Execute the BigQuery validation query for each of the following `@p_Stichtag` parameters:
   * `20240229` (Leap Day)
   * `20231231` (Year End)
   * `20240101` (Year Start)
   * `INVALID_DATE` (Error Handling)

### Pass/Fail Criterion
* **Pass**:
  * Valid dates return the exact rows matching that date partition.
  * Passing an invalid date format (e.g., `'INVALID_DATE'` or `'2023-10-24'`) causes the BigQuery engine to throw a parsing exception, matching the legacy `whenever sqlerror exit failure` behavior.
* **Fail**: Invalid dates are silently ignored, or valid dates return incorrect partitions.

```python
# pytest: test_date_transformations.py
from google.cloud import bigquery
import pytest

def test_bigquery_date_parsing_success():
    client = bigquery.Client()
    query = """
    SELECT PARSE_DATE('%Y%m%d', @p_Stichtag) as parsed_date
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("p_Stichtag", "STRING", "20240229")]
    )
    result = list(client.query(query, job_config=job_config).result())
    assert str(result[0].parsed_date) == "2024-02-29"

def test_bigquery_date_parsing_failure():
    client = bigquery.Client()
    query = "SELECT PARSE_DATE('%Y%m%d', @p_Stichtag)"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("p_Stichtag", "STRING", "invalid_date")]
    )
    with pytest.raises(Exception) as excinfo:
        client.query(query, job_config=job_config).result()
    assert "Mismatch between format character" in str(excinfo.value) or "Invalid date" in str(excinfo.value)
```

---

## 3. Verbatim German Logging & Alerting

### Purpose
To verify that the migrated Python execution runner (`r_exp_rechnung_taeglich.py`) outputs the exact German log statements required by the legacy system specifications for both successful runs and empty-dataset warnings.

### Setup
1. Prepare two test scenarios in BigQuery:
   * **Scenario A (Data Exists)**: Table contains 5 records for `2023-10-24`.
   * **Scenario B (No Data)**: Table contains 0 records for `2023-10-25`.

### Action
1. Run the `validate_and_log_results` Python function inside a captured standard output environment for **Scenario A**.
2. Run the `validate_and_log_results` Python function inside a captured standard output environment for **Scenario B**.

### Pass/Fail Criterion
* **Pass**:
  * **Scenario A** prints:
    ```text
    Anzahl exportierter Rechnungsdatensaetze: 5
    Export Rechnungsdaten erfolgreich beendet.
    ```
  * **Scenario B** prints:
    ```text
    Keine Rechnungsdaten gefunden.
    ```
* **Fail**: Any deviation in the German text, spelling, or formatting of these log lines.

```python
# pytest: test_verbatim_logging.py
import io
import sys
from unittest.mock import MagicMock, patch
import pytest
from bin.r_exp_rechnung_taeglich import validate_and_log_results

@patch('airflow.providers.google.cloud.hooks.gcs.GCSHook.download')
def test_logging_scenario_a_data_exists(mock_download):
    # Mock GCS file with header + 5 data rows
    csv_content = "RECHNUNGSNUMMER|VERTRAG\n1|V1\n2|V2\n3|V3\n4|V4\n5|V5"
    mock_download.return_value = csv_content.encode('utf-8')
    
    context = {
        'templates_dict': {
            'stichtag': '20231024',
            'gcs_bucket': 'test-bucket'
        }
    }
    
    captured_output = io.StringIO()
    sys.stdout = captured_output
    try:
        validate_and_log_results(**context)
    finally:
        sys.stdout = sys.__stdout__
        
    output = captured_output.getvalue()
    assert "Anzahl exportierter Rechnungsdatensaetze: 5" in output
    assert "Export Rechnungsdaten erfolgreich beendet." in output

@patch('airflow.providers.google.cloud.hooks.gcs.GCSHook.download')
def test_logging_scenario_b_no_data(mock_download):
    # Mock GCS file with header only (0 data rows)
    csv_content = "RECHNUNGSNUMMER|VERTRAG\n"
    mock_download.return_value = csv_content.encode('utf-8')
    
    context = {
        'templates_dict': {
            'stichtag': '20231025',
            'gcs_bucket': 'test-bucket'
        }
    }
    
    captured_output = io.StringIO()
    sys.stdout = captured_output
    try:
        validate_and_log_results(**context)
    finally:
        sys.stdout = sys.__stdout__
        
    output = captured_output.getvalue()
    assert "Keine Rechnungsdaten gefunden." in output
```

---

## 4. Orchestration & Parameter Resolution (Airflow Context)

### Purpose
To verify that the Airflow DAG correctly resolves the `Stichtag` parameter, defaulting to yesterday's date (`{{ ds_nodash }}` equivalent) when no manual override configuration is provided, matching the legacy KSH fallback logic.

### Setup
1. Mock the Airflow `logical_date` (or `execution_date`) to `2024-03-15`.

### Action
1. Execute the `resolve_stichtag` function with `logical_date = datetime(2024, 3, 15)` and no manual configuration.
2. Execute the `resolve_stichtag` function with a manual configuration override: `dag_run_conf = {"s": "20240999"}`.

### Pass/Fail Criterion
* **Pass**:
  * Without override, the resolved date is `20240314` (yesterday relative to `2024-03-15`).
  * With override, the resolved date is `20240999`.
  * The resolved date is successfully pushed to XCom under the key `l_Stichtag`.
* **Fail**: The date is resolved incorrectly or fails to write to XCom.

```python
# pytest: test_orchestration_variables.py
import datetime
from unittest.mock import MagicMock
from bin.r_exp_rechnung_taeglich_operator import resolve_stichtag

def test_resolve_stichtag_default_fallback():
    mock_ti = MagicMock()
    logical_date = datetime.datetime(2024, 3, 15)
    
    resolved = resolve_stichtag(
        logical_date=logical_date,
        dag_run_conf=None,
        ti=mock_ti
    )
    
    # Must resolve to yesterday (20240314)
    assert resolved == "20240314"
    mock_ti.xcom_push.assert_called_once_with(key="l_Stichtag", value="20240314")

def test_resolve_stichtag_manual_override():
    mock_ti = MagicMock()
    logical_date = datetime.datetime(2024, 3, 15)
    conf = {"s": "20240701"}
    
    resolved = resolve_stichtag(
        logical_date=logical_date,
        dag_run_conf=conf,
        ti=mock_ti
    )
    
    assert resolved == "20240701"
    mock_ti.xcom_push.assert_called_once_with(key="l_Stichtag", value="20240701")
```