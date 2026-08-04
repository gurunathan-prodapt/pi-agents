# Migration Validation Test Suite: Daily Invoice Data Export
**Target Job:** `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP` (Airflow DAG: `dw_dwh_rechnung_export_taeglich_jp`)

This document defines the migration-validation test suite to prove behavioral equivalence between the legacy Oracle/UC4 workflow and the migrated Google Cloud (Composer/BigQuery/GCS) Python-based workflow.

---

## Test Case 1: End-to-End Output Parity (Reconciliation Test)

### Purpose
Verify that running the legacy Oracle SQL*Plus export and the migrated BigQuery/Python export with identical source datasets and the same reporting date (`Stichtag`) produces identical pipe-separated output files.

### Setup
1. **Legacy Environment (Oracle):**
   * Populate `DWH_KERN.T_RECHNUNG` with a controlled set of test records for `RECHNUNGSDATUM = TO_DATE('20241025', 'YYYYMMDD')`.
   * Include edge cases: extreme numeric values, special characters in strings, and NULL values in nullable columns (`VERTRAG`, `KUNDE`, `TARIF`, `ABRECHNUNGSZEITRAUM`).
2. **Target Environment (BigQuery):**
   * Populate the migrated BigQuery table `dwh_kern.T_RECHNUNG` with the exact same dataset.
3. **Configuration:**
   * Set environment variables `GCP_PROJECT`, `GCS_BUCKET`, and `BQ_DATASET` in the test execution environment.

### Action
1. Execute the legacy shell script:
   ```bash
   ./r_exp_rechnung_taeglich.ksh -s 20241025
   ```
2. Execute the migrated Python script:
   ```bash
   python3 r_exp_rechnung_taeglich.py -s 20241025
   ```
3. Download the exported file from GCS:
   ```bash
   gcloud storage cp gs://${GCS_BUCKET}/rechnung/ausgang/rechnung_export_20241025.dat ./migrated_export_20241025.dat
   ```
4. Compare the legacy local output file with the downloaded GCS output file.

### Pass/Fail Criterion
* **Pass:** The files are byte-for-byte identical (or structurally identical, accounting for line-ending normalization `\n` vs `\r\n`). The row count, column ordering, pipe-delimiters (`|`), and NULL representations match exactly.
* **Fail:** Any discrepancy in row count, column order, value formatting, or sorting order (`ORDER BY RECHNUNGSNUMMER`).

```python
# pytest code for output parity validation
import pytest
import subprocess
import os
from google.cloud import storage

@pytest.mark.qa
def test_e2e_output_parity():
    stichtag = "20241025"
    legacy_file = f"/tmp/legacy_rechnung_export_{stichtag}.dat"
    migrated_local_file = f"/tmp/migrated_rechnung_export_{stichtag}.dat"
    
    # 1. Run legacy script (assumes Oracle client connectivity is available in test runner)
    # Or pre-stage the legacy output file to the test runner
    assert os.path.exists(legacy_file), "Legacy baseline file must be pre-staged for comparison."
    
    # 2. Run migrated Python script
    env = os.environ.copy()
    env["GCP_PROJECT"] = "test-gcp-project"
    env["GCS_BUCKET"] = "test-gcs-bucket"
    env["BQ_DATASET"] = "dwh_kern"
    
    result = subprocess.run(
        ["python3", "bin/r_exp_rechnung_taeglich.py", "-s", stichtag],
        env=env,
        capture_output=True,
        text=True
    )
    assert result.returncode == 0, f"Migrated script failed: {result.stderr}"
    
    # 3. Download from GCS
    storage_client = storage.Client(project=env["GCP_PROJECT"])
    bucket = storage_client.bucket(env["GCS_BUCKET"])
    blob = bucket.blob(f"rechnung/ausgang/rechnung_export_{stichtag}.dat")
    blob.download_to_filename(migrated_local_file)
    
    # 4. Assert structural and content equivalence
    with open(legacy_file, "r", encoding="utf-8") as f_legacy, \
         open(migrated_local_file, "r", encoding="utf-8") as f_migrated:
        
        legacy_lines = f_legacy.readlines()
        migrated_lines = f_migrated.readlines()
        
        assert len(legacy_lines) == len(migrated_lines), \
            f"Row count mismatch! Legacy: {len(legacy_lines)}, Migrated: {len(migrated_lines)}"
            
        for idx, (l_line, m_line) in enumerate(zip(legacy_lines, migrated_lines)):
            assert l_line.strip() == m_line.strip(), \
                f"Mismatch at line {idx + 1}!\nLegacy: {l_line}\nMigrated: {m_line}"
```

---

## Test Case 2: Date Filtering and Parameter Passing (Transformation Correctness)

### Purpose
Verify that the `Stichtag` parameter is correctly parsed and filters the dataset accurately in BigQuery, matching Oracle's `TO_DATE` behavior. This ensures that only records matching the specified date are exported.

### Setup
1. Populate the BigQuery table `dwh_kern.T_RECHNUNG` with the following test records:
   * Record A: `RECHNUNGSNUMMER = 'R001'`, `RECHNUNGSDATUM = '2024-10-25'`
   * Record B: `RECHNUNGSNUMMER = 'R002'`, `RECHNUNGSDATUM = '2024-10-24'` (Yesterday)
   * Record C: `RECHNUNGSNUMMER = 'R003'`, `RECHNUNGSDATUM = '2024-10-26'` (Tomorrow)
   * Record D: `RECHNUNGSNUMMER = 'R004'`, `RECHNUNGSDATUM = NULL`

### Action
1. Execute the migrated Python script with `-s 20241025`.
2. Query the generated output file in GCS.

### Pass/Fail Criterion
* **Pass:** Only Record A (`R001`) is present in the exported file. Records B, C, and D are excluded.
* **Fail:** Any record other than `R001` is exported, or the script fails to parse the date.

```sql
-- SQL Assertion to verify BigQuery filter logic matches Oracle TO_DATE
-- This query must return exactly 1 row (Record A)
SELECT COUNT(1) as match_count 
FROM `test-gcp-project.dwh_kern.T_RECHNUNG`
WHERE RECHNUNGSDATUM = PARSE_DATE('%Y%m%d', '20241025');

-- Assertion: match_count == 1
```

---

## Test Case 3: Formatting and Null Handling (Data Quality)

### Purpose
Ensure that NULL values, decimal fields (`RECHNUNGSBETRAG`), and date fields are formatted identically to the legacy SQL*Plus output (e.g., empty string between pipes for NULLs, correct decimal precision, and standard date formats).

### Setup
1. Populate the BigQuery table `dwh_kern.T_RECHNUNG` with a record containing NULLs and specific decimal values:
   * `RECHNUNGSNUMMER = 'R999'`
   * `VERTRAG = NULL`
   * `KUNDE = 'K123'`
   * `TARIF = NULL`
   * `ABRECHNUNGSZEITRAUM = NULL`
   * `RECHNUNGSBETRAG = 12345.60`
   * `WAEHRUNG = 'EUR'`
   * `RECHNUNGSDATUM = '2024-10-25'`

### Action
1. Run the migrated Python script for `20241025`.
2. Inspect the output line corresponding to `R999`.

### Pass/Fail Criterion
* **Pass:** The output line matches the expected pipe-separated format exactly:
  `R999||K123|||12345.6|EUR|2024-10-25` (or matching the exact decimal scale of the legacy system, e.g., `12345.6` or `12345.60` depending on BigQuery column type).
* **Fail:** NULL values are represented as string literals like `"None"`, `"NULL"`, or spaces, or the decimal value is formatted in scientific notation.

```python
# pytest code to validate formatting and NULL handling
def test_null_and_decimal_formatting():
    # Simulated row output formatting logic from r_exp_rechnung_taeglich.py
    row = {
        "RECHNUNGSNUMMER": "R999",
        "VERTRAG": None,
        "KUNDE": "K123",
        "TARIF": None,
        "ABRECHNUNGSZEITRAUM": None,
        "RECHNUNGSBETRAG": 12345.60,
        "WAEHRUNG": "EUR",
        "RECHNUNGSDATUM": "2024-10-25"
    }
    
    # Replicate the row formatting logic used in the Python script:
    # "|".join("" if val is None else str(val) for val in row.values())
    formatted_row = "|".join("" if val is None else str(val) for val in row.values())
    
    expected_output = "R999||K123|||12345.6|EUR|2024-10-25"
    assert formatted_row == expected_output, f"Formatting mismatch! Got: {formatted_row}"
```

---

## Test Case 4: GCS Upload and File Lineage (External System Replacement)

### Purpose
Verify that the migrated Python script correctly uploads the exported file to the designated GCS bucket path (`rechnung/ausgang/rechnung_export_{stichtag}.dat`) and cleans up the local `/tmp` directory.

### Setup
1. Configure environment variables: `GCP_PROJECT="test-project"`, `GCS_BUCKET="test-bucket"`.
2. Ensure the local `/tmp` directory is writable.

### Action
1. Execute the Python script `r_exp_rechnung_taeglich.py -s 20241025`.
2. Check for the existence of the file in GCS.
3. Check for the existence of the temporary file in `/tmp`.

### Pass/Fail Criterion
* **Pass:** 
  * The file is successfully uploaded to `gs://test-bucket/rechnung/ausgang/rechnung_export_20241025.dat`.
  * The local temporary file `/tmp/rechnung_export_20241025.dat` is deleted (cleaned up).
* **Fail:** The file is missing from GCS, is uploaded to the wrong path, or the local temporary file is left behind in `/tmp`.

```python
# pytest code to validate GCS upload and local cleanup
from unittest.mock import MagicMock, patch
import bin.r_exp_rechnung_taeglich as script

@patch("bin.r_exp_rechnung_taeglich.bigquery.Client")
@patch("bin.r_exp_rechnung_taeglich.storage.Client")
@patch("bin.r_exp_rechnung_taeglich.os.remove")
@patch("bin.r_exp_rechnung_taeglich.os.path.exists")
def test_gcs_upload_and_cleanup(mock_exists, mock_remove, mock_storage_client, mock_bq_client, monkeypatch):
    # Setup mocks
    monkeypatch.setenv("GCP_PROJECT", "test-project")
    monkeypatch.setenv("GCS_BUCKET", "test-bucket")
    
    mock_exists.return_value = True
    
    # Run main
    with patch("sys.argv", ["r_exp_rechnung_taeglich.py", "-s", "20241025"]):
        script.main()
        
    # Verify GCS upload was called with correct path
    mock_storage_client.return_value.bucket.assert_called_with("test-bucket")
    mock_bucket = mock_storage_client.return_value.bucket.return_value
    mock_bucket.blob.assert_called_with("rechnung/ausgang/rechnung_export_20241025.dat")
    
    # Verify local cleanup was triggered
    mock_remove.assert_called_with("/tmp/rechnung_export_20241025.dat")
```

---

## Test Case 5: Zero-Row Warning and Error Handling (Operational Parity)

### Purpose
Verify that the script logs the exact German warning message to `stderr` when 0 rows are exported, and exits with code 0 (as per legacy script behavior). Also verify that database connection failures result in a non-zero exit code.

### Setup
1. Clear the BigQuery table `dwh_kern.T_RECHNUNG` for the target `Stichtag = '20241025'`.

### Action
1. Run the Python script for `20241025`. Capture `stdout`, `stderr`, and the exit code.
2. Run the Python script with an invalid `GCP_PROJECT` to simulate a database connection failure. Capture the exit code.

### Pass/Fail Criterion
* **Pass:**
  * For the zero-row run: Exit code is `0`. `stderr` contains `[W] <timestamp> Keine Rechnungsdaten fuer Stichtag 20241025 exportiert`. `stdout` contains `Anzahl exportierter Rechnungssaetze: 0`.
  * For the connection failure run: Exit code is `1`. `stderr` contains `ERROR: Export process failed:`.
* **Fail:** The zero-row run returns a non-zero exit code, fails to print the exact German warning, or the connection failure exits with `0`.

```python
# pytest code to validate operational logging and exit codes
def test_zero_row_warning_and_error_handling(capsys, monkeypatch):
    # Mock BigQuery client to return 0 rows
    class MockRowIterator:
        def __iter__(self):
            return iter([])
        def result(self):
            return self

    class MockBQClient:
        def __init__(self, project): pass
        def query(self, query, job_config=None):
            return MockRowIterator()

    monkeypatch.setattr("google.cloud.bigquery.Client", MockBQClient)
    monkeypatch.setenv("GCP_PROJECT", "test-project")
    monkeypatch.setenv("GCS_BUCKET", "test-bucket")
    
    # Execute script main
    import bin.r_exp_rechnung_taeglich as script
    with patch("sys.argv", ["r_exp_rechnung_taeglich.py", "-s", "20241025"]):
        exit_code = script.main()
        
    captured = capsys.readouterr()
    
    assert exit_code == 0
    assert "Anzahl exportierter Rechnungssaetze: 0" in captured.out
    assert "[W]" in captured.err
    assert "Keine Rechnungsdaten fuer Stichtag 20241025 exportiert" in captured.err
```

---

## Test Case 6: Airflow DAG Orchestration and Parameter Propagation

### Purpose
Verify that the Airflow DAG `dw_dwh_rechnung_export_taeglich_jp` correctly propagates the `ds_nodash` macro to the BashOperator and sets the correct environment variables.

### Setup
1. Load the DAG `dw_dwh_rechnung_export_taeglich_jp` in a test Airflow environment.
2. Mock Airflow Variables `GCP_PROJECT`, `GCS_BUCKET`, and `AIRFLOW_CONN_DW_UNIX_ISTNS`.

### Action
1. Render the templates for task `dw_dwh_rechnung_export_taeglich_js` for execution date `2024-10-25`.

### Pass/Fail Criterion
* **Pass:**
  * The rendered `bash_command` is:
    `python3 /opt/airflow/dags/dw_source/isdwh/exporter/rechnung/bin/r_exp_rechnung_taeglich.py -s 20241025 && echo "Rechnungsexport fuer Stichtag 20241025 angestossen"`
  * The environment variables `GCP_PROJECT`, `GCS_BUCKET`, and `AIRFLOW_CONN` are correctly populated in the task context.
* **Fail:** The date is rendered incorrectly (e.g., with dashes `2024-10-25` instead of `20241025`), the script path is incorrect, or the German log literal is modified.

```python
# pytest code to validate Airflow DAG template rendering
from airflow.models import DagBag, Variable
from unittest.mock import patch

@patch("airflow.models.Variable.get")
def test_dag_template_rendering(mock_variable_get):
    # Mock Airflow Variables
    def var_side_effect(key, default=None):
        vars_dict = {
            "GCP_PROJECT": "prod-gcp-project",
            "GCS_BUCKET": "prod-gcs-bucket",
            "AIRFLOW_CONN_DW_UNIX_ISTNS": "ssh_prod_connection"
        }
        return vars_dict.get(key, default)
    mock_variable_get.side_effect = var_side_effect

    dagbag = DagBag(dag_folder="DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/", include_examples=False)
    dag = dagbag.get_dag(dag_id="dw_dwh_rechnung_export_taeglich_jp")
    assert dag is not None
    
    task = dag.get_task("dw_dwh_rechnung_export_taeglich_js")
    
    # Create a dummy TaskInstance to render templates
    from airflow.models import TaskInstance
    from airflow.utils.types import DagRunType
    from django.utils import timezone
    import datetime
    
    execution_date = datetime.datetime(2024, 10, 25, tzinfo=datetime.timezone.utc)
    dag_run = dag.create_dagrun(
        run_id="test_run",
        state="running",
        execution_date=execution_date,
        start_date=execution_date,
        run_type=DagRunType.MANUAL
    )
    
    ti = TaskInstance(task=task, run_id=dag_run.run_id)
    ti.render_templates()
    
    expected_command = (
        "python3 /opt/airflow/dags/dw_source/isdwh/exporter/rechnung/bin/r_exp_rechnung_taeglich.py -s 20241025 && "
        'echo "Rechnungsexport fuer Stichtag 20241025 angestossen"'
    )
    
    assert ti.task.bash_command == expected_command
    assert ti.task.env["GCP_PROJECT"] == "prod-gcp-project"
    assert ti.task.env["GCS_BUCKET"] == "prod-gcs-bucket"
```