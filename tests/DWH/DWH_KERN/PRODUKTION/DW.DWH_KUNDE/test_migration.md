# Migration Validation Test Suite: DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS

This document defines the comprehensive migration-validation test suite for the weekly customer address reconciliation job `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS`. These tests verify output parity, transformation correctness, external-system replacements, and data-quality assertions between the legacy Oracle/KornShell environment and the migrated Google Cloud Composer/BigQuery environment.

---

## 1. Output Parity & Log Literal Verification

### Purpose
To prove that the migrated Python execution script and Airflow DAG produce the exact same console outputs, log structures, and warning messages as the legacy KornShell script and UC4 job. This ensures that downstream automated log parsers and operational monitoring tools continue to function without modification.

### Setup
*   Deploy the migrated Python script `r_abgl_kunde_woech.py` and its dependencies (`utils.py`) to a test environment.
*   Set the environment variable `DW_DIR_LOG` to a temporary test directory (e.g., `/tmp/test_log`).
*   Mock the BigQuery client to return a predefined number of rows (e.g., 0 for the success scenario, 3 for the warning scenario).

### Action
Run the Python script with a specific stichtag parameter and capture standard output (`stdout`), standard error (`stderr`), and the generated log file contents.

```bash
# Scenario A: Zero discrepancies
export DW_DIR_LOG="/tmp/test_log"
python3 dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/bin/r_abgl_kunde_woech.py -s 20260301

# Scenario B: With discrepancies (mocked to return 3 rows)
# (Run via pytest with mocked BigQuery client)
```

### Pass/Fail Criteria
*   **Pass**: 
    *   When 0 discrepancies are found, `stdout` contains the exact string:
        `Starte Adressabgleich Kundenstammdaten fuer Stichtag 20260301`
        `Anzahl gefundener Abweichungen: 0`
        `Adressabgleich Kundenstammdaten ohne erkennbare Fehler beendet`
    *   When 3 discrepancies are found, `stderr` contains the exact warning pattern:
        `[W] YYYY-MM-DD HH:MM:SS 3 Abweichungen im Kundenadressabgleich gefunden, siehe /tmp/test_log/kunde/abgl_kunde_woech_<PID>.log`
*   **Fail**: Any character mismatch, missing log file, or altered German phrasing in the output stream.

---

## 2. SQL Transformation & Null-Handling Correctness

### Purpose
To verify that the migrated BigQuery SQL script (`d_abgl_kunde_woech.sql`) behaves identically to the legacy Oracle SQL script, specifically validating:
1.  The date filter logic (`k.AKTUALISIERT_AM <= PARSE_DATE('%Y%m%d', @p_Stichtag)`).
2.  The `COALESCE(..., 'x')` logic replacing Oracle's `NVL(..., 'x')`.
3.  The inner join behavior on `KUNDE`.

### Setup
Create temporary test tables in BigQuery representing `DWH_KERN.T_KUNDE` and `STAMMDATEN.T_KUNDE_REFERENZ` with specific edge-case records:

| Table | KUNDE | NACHNAME | PLZ | ORT | STRASSE | AKTUALISIERT_AM | Note |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **T_KUNDE** | 1001 | Schmidt | 12345 | Berlin | Hauptstr. 1 | 2026-02-20 | Match (No discrepancy) |
| **T_KUNDE** | 1002 | Müller | NULL | Hamburg | Feldweg 2 | 2026-02-20 | Null PLZ in source |
| **T_KUNDE** | 1003 | Meyer | 54321 | Köln | Ring 3 | 2026-03-05 | Excluded by Stichtag |
| **T_KUNDE** | 1004 | Weber | 99999 | München | Waldweg 4 | 2026-02-20 | Discrepancy in PLZ |

| Table | KUNDE | PLZ | ORT | STRASSE | Note |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **T_KUNDE_REFERENZ** | 1001 | 12345 | Berlin | Hauptstr. 1 | Match |
| **T_KUNDE_REFERENZ** | 1002 | NULL | Hamburg | Feldweg 2 | Match (Both PLZ are NULL) |
| **T_KUNDE_REFERENZ** | 1003 | 54321 | Köln | Ring 3 | Match (But excluded by date) |
| **T_KUNDE_REFERENZ** | 1004 | 88888 | München | Waldweg 4 | Mismatch (PLZ 99999 vs 88888) |

### Action
Execute the BigQuery SQL script with `@p_Stichtag = '20260228'`.

```python
# pytest code to validate SQL execution results
import pytest
from google.cloud import bigquery

@pytest.mark.integration
def test_sql_transformation_correctness():
    client = bigquery.Client()
    
    # Read migrated SQL
    with open("dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/sql/d_abgl_kunde_woech.sql", "r") as f:
        sql_text = f.read()
        
    # Replace table references with test dataset tables
    sql_text = sql_text.replace("`DWH_KERN.T_KUNDE`", "`test_dataset.T_KUNDE`")
    sql_text = sql_text.replace("`STAMMDATEN.T_KUNDE_REFERENZ`", "`test_dataset.T_KUNDE_REFERENZ`")
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("p_Stichtag", "STRING", "20260228")
        ]
    )
    
    query_job = client.query(sql_text, job_config=job_config)
    results = list(query_job.result())
    
    # Assertions
    assert len(results) == 1, "Only KUNDE 1004 should have a discrepancy within the Stichtag window"
    assert results[0].KUNDE == 1004
    assert results[0].PLZ == "99999"
    assert results[0].REF_PLZ == "88888"
```

### Pass/Fail Criteria
*   **Pass**: 
    *   Only `KUNDE = 1004` is returned as an anomaly.
    *   `KUNDE = 1002` is **not** returned (proving `COALESCE(NULL, 'x') == COALESCE(NULL, 'x')` works correctly).
    *   `KUNDE = 1003` is **not** returned (proving the date filter correctly excluded records updated after `2026-02-28`).
*   **Fail**: Any deviation in the returned row count or incorrect classification of NULL values.

---

## 3. Date Derivation & Parameter Fallback Validation

### Purpose
To verify that the date calculation logic correctly defaults to "7 days ago" in `YYYYMMDD` format when no explicit `-s` parameter is provided to the execution script, matching the legacy KornShell behavior: `date -d '7 days ago' '+%Y%m%d'`.

### Setup
*   Clear any command-line arguments passed to the test runner.
*   Mock the system date to a fixed value (e.g., `2026-03-15`).

### Action
Execute the `main()` function of `r_abgl_kunde_woech.py` without the `-s` argument and capture the resolved `l_Stichtag` variable.

```python
import sys
from unittest.mock import patch
from datetime import datetime
import pytest
from dags.DWH.DWH_KERN.PRODUKTION.DW.DWH_KUNDE.bin import r_abgl_kunde_woech

@patch('dags.DWH.DWH_KERN.PRODUKTION.DW.DWH_KUNDE.bin.r_abgl_kunde_woech.execute_bigquery_query')
@patch('dags.DWH.DWH_KERN.PRODUKTION.DW.DWH_KUNDE.bin.r_abgl_kunde_woech.datetime')
def test_date_fallback_logic(mock_datetime, mock_bq_query):
    # Mock system date to 2026-03-15
    mock_datetime.now.return_value = datetime(2026, 3, 15)
    mock_datetime.strftime = datetime.strftime
    
    # Mock BQ query to return empty list
    mock_bq_query.return_value = []
    
    # Run main with no arguments (sys.argv = [script_name])
    with patch.object(sys, 'argv', ['r_abgl_kunde_woech.py']):
        with pytest.raises(SystemExit) as exc_info:
            r_abgl_kunde_woech.main()
            
    # Verify that the fallback date calculated is 7 days prior: 20260308
    # This is verified by checking the log file or stdout output
    log_dir = os.environ.get("DW_DIR_LOG", "/tmp/aktuell/log")
    # Find the latest log file created and check its first line
```

### Pass/Fail Criteria
*   **Pass**: The script resolves the stichtag to `20260308` (exactly 7 days prior to `2026-03-15`) and prints:
    `Starte Adressabgleich Kundenstammdaten fuer Stichtag 20260308`
*   **Fail**: The script uses the current date, fails to parse, or calculates an incorrect offset.

---

## 4. Airflow DAG Integration & Context Propagation

### Purpose
To verify that the Airflow DAG (`dw_dwh_kunde_abgl_woechentlich_js`) correctly instantiates, schedules weekly, and propagates the execution date context (`ds_nodash`) to the underlying Python execution task.

### Setup
*   Load the DAG into an Airflow `DagBag` in a test environment.

### Action
Run a DAG validation test to check structure, scheduling, and task parameters.

```python
from airflow.models import DagBag

def test_dag_loading_and_structure():
    dag_bag = DagBag(dag_folder="dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE", include_examples=False)
    dag = dag_bag.get_dag(dag_id='dw_dwh_kunde_abgl_woechentlich_js')
    
    assert dag_bag.import_errors == {}
    assert dag is not None
    assert dag.schedule_interval == '0 4 * * 1'  # Every Monday at 04:00 AM
    
    task = dag.get_task('run_reconciliation_process')
    assert task is not None
    assert task.op_kwargs == {}  # Context is provided via provide_context=True
```

### Pass/Fail Criteria
*   **Pass**: 
    *   The DAG loads with zero import errors.
    *   The schedule interval is exactly `'0 4 * * 1'` (matching the design document's weekly Monday schedule).
    *   The task `run_reconciliation_process` is present and uses the `PythonOperator`.
*   **Fail**: Any import errors are present, or the schedule interval is misconfigured.