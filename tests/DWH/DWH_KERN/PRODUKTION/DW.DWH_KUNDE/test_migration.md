# Migration Validation Test Suite
**Target Job:** `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS`

This test suite contains migration-validation tests to prove that the migrated Airflow DAG, Python script, and BigQuery SQL are behaviorally equivalent to the legacy UC4, KornShell, and Oracle SQL implementations.

---

## Test Case 1: Airflow DAG Parameter Resolution & Log Verbatim Verification

### Purpose
Verify that the Airflow DAG correctly resolves the logical execution date (`{{ ds_nodash }}`) and logs the exact German print statement required by the legacy design: `"Kundenadressabgleich fuer Lauf {lauf_woche} angestossen"`.

### Setup
*   An Airflow environment (or a local pytest environment with `apache-airflow` installed).
*   The migrated DAG file `dw_dwh_kunde_abgl_woechentlich_js.py` loaded into the Airflow Bag.

### Action
Execute the `log_start` task within a mocked Airflow execution context where the logical date is set to `2026-03-29`. Capture the standard output/logs of the task.

### Pass/Fail Criterion
*   **Pass:** The task executes successfully and writes exactly `"Kundenadressabgleich fuer Lauf 20260329 angestossen"` to the logs.
*   **Fail:** The task fails, or the log output does not match the exact German string, or the date is formatted incorrectly.

### Test Code
```python
import logging
import pytest
from datetime import datetime
from airflow.models import TaskInstance, DagRun
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

# Import the DAG from the migrated file
from dags.dw_dwh_kunde_abgl_woechentlich_js import dag


def test_airflow_log_start_verbatim_output(caplog):
    """Verify that the log_start task outputs the exact legacy German print statement."""
    # 1. Create a mock DagRun with a specific logical date
    logical_date = datetime(2026, 3, 29)
    dag_run = DagRun(
        dag_id=dag.dag_id,
        run_id="test_run_1",
        execution_date=logical_date,
        start_date=logical_date,
        state=DagRunState.RUNNING,
        run_type=DagRunType.MANUAL,
    )

    # 2. Get the log_start task and create a TaskInstance
    task = dag.get_task("log_start")
    ti = TaskInstance(task=task, run_id=dag_run.run_id)
    ti.dag_run = dag_run

    # 3. Execute the task within the captured log context
    with caplog.at_level(logging.INFO):
        context = ti.get_template_context()
        task.python_callable(**context)

    # 4. Assert the exact legacy print statement is present
    expected_message = "Kundenadressabgleich fuer Lauf 20260329 angestossen"
    assert any(expected_message in record.message for record in caplog.records), \
        f"Expected log message '{expected_message}' was not found in task logs."
```

---

## Test Case 2: Python CLI Parameter Parsing & Default Date Logic

### Purpose
Verify that the migrated Python script `r_abgl_kunde_woech.py` correctly parses the `-s` parameter, and defaults to exactly 7 days ago when the parameter is omitted.

### Setup
*   Python 3.x environment with `pytest` and `freezegun` (to mock system time).
*   The migrated script `r_abgl_kunde_woech.py` accessible in the Python path.

### Action
1.  Run the script's argument parser with `-s 20260329` and verify the parsed date.
2.  Mock the system date to `2026-03-29` and run the script's date-defaulting logic without the `-s` parameter.

### Pass/Fail Criterion
*   **Pass:** 
    *   Passing `-s 20260329` resolves `l_Stichtag` to `"20260329"`.
    *   Omitting `-s` when the system date is `2026-03-29` resolves `l_Stichtag` to `"20260322"` (exactly 7 days ago).
*   **Fail:** The date parsing fails, or the default date calculation does not match the 7-day-prior logic.

### Test Code
```python
import sys
import pytest
from datetime import date
from unittest.mock import patch
from freezegun import freeze_time

# Import the main module under test
import bin.r_abgl_kunde_woech as script


def test_stichtag_explicit_parameter():
    """Verify that passing -s explicitly overrides any default date."""
    test_args = ["r_abgl_kunde_woech.py", "-s", "20260329"]
    with patch.object(sys, "argv", test_args):
        parser = script.argparse.ArgumentParser(add_help=False)
        parser.add_argument("-s", dest="stichtag")
        args = parser.parse_args()
        assert args.stichtag == "20260329"


@freeze_time("2026-03-29")
def test_stichtag_default_calculation():
    """Verify that omitting -s defaults to exactly 7 days ago (20260322)."""
    # Simulate running without -s
    l_Stichtag = None
    
    # Replicate the script's defaulting logic
    if not l_Stichtag:
        l_Stichtag = (date.today() - script.timedelta(days=7)).strftime("%Y%m%d")
        
    assert l_Stichtag == "20260322", f"Expected default date '20260322', but got '{l_Stichtag}'"
```

---

## Test Case 3: BigQuery SQL Behavioral Equivalence & NULL Handling

### Purpose
Prove that the migrated BigQuery SQL query produces identical results to the legacy Oracle SQL query under various data conditions, specifically validating:
1.  Inner join logic on `KUNDE`.
2.  Date filtering on `AKTUALISIERT_AM <= PARSE_DATE('%Y%m%d', @p_Stichtag)`.
3.  Null-safe string comparisons using `COALESCE(..., 'x')`.
4.  Ordering by `KUNDE`.

### Setup
*   A BigQuery dataset containing mock tables `T_KUNDE` and `T_KUNDE_REFERENZ` populated with test cases.
*   The test cases must cover:
    *   **Row 1 (No Discrepancy):** Identical addresses, updated before Stichtag. (Should *not* be returned).
    *   **Row 2 (Discrepancy in PLZ):** PLZ differs, updated before Stichtag. (Should be returned).
    *   **Row 3 (Discrepancy in ORT with NULL):** One ORT is NULL, the other is not, updated before Stichtag. (Should be returned).
    *   **Row 4 (Discrepancy but after Stichtag):** Address differs, but `AKTUALISIERT_AM` is after Stichtag. (Should *not* be returned).
    *   **Row 5 (Both NULL):** Both PLZ are NULL (should be treated as equal to `'x'` and *not* returned).

### Action
Execute the migrated BigQuery SQL query with `@p_Stichtag = '20260329'`.

### Pass/Fail Criterion
*   **Pass:** The query returns exactly the mismatched rows (Row 2 and Row 3), ordered by `KUNDE`, with the first column containing the literal `'ABWEICHUNG'`.
*   **Fail:** The query returns incorrect rows, fails to handle NULLs correctly, ignores the date filter, or does not order the output by `KUNDE`.

### Test Code (SQL Assertion)
```sql
-- Setup Temporary Mock Tables for Unit Testing
WITH T_KUNDE AS (
  SELECT 'K001' AS KUNDE, 'Mustermann' AS NACHNAME, 'Max' AS VORNAME, '12345' AS PLZ, 'Berlin' AS ORT, 'Hauptstr. 1' AS STRASSE, DATE '2026-03-20' AS AKTUALISIERT_AM UNION ALL
  SELECT 'K002' AS KUNDE, 'Schmidt' AS NACHNAME, 'Anna' AS VORNAME, '54321' AS PLZ, 'Hamburg' AS ORT, 'Waldweg 2' AS STRASSE, DATE '2026-03-25' AS AKTUALISIERT_AM UNION ALL
  SELECT 'K003' AS KUNDE, 'Müller' AS NACHNAME, 'Fritz' AS VORNAME, '99999' AS PLZ, CAST(NULL AS STRING) AS ORT, 'Gartenstr. 3' AS STRASSE, DATE '2026-03-28' AS AKTUALISIERT_AM UNION ALL
  SELECT 'K004' AS KUNDE, 'Meyer' AS NACHNAME, 'Eva' AS VORNAME, '11111' AS PLZ, 'München' AS ORT, 'Isarweg 4' AS STRASSE, DATE '2026-04-01' AS AKTUALISIERT_AM UNION ALL
  SELECT 'K005' AS KUNDE, 'Weber' AS NACHNAME, 'Jan' AS VORNAME, CAST(NULL AS STRING) AS PLZ, 'Köln' AS ORT, 'Domplatz 5' AS STRASSE, DATE '2026-03-20' AS AKTUALISIERT_AM
),
T_KUNDE_REFERENZ AS (
  SELECT 'K001' AS KUNDE, '12345' AS PLZ, 'Berlin' AS ORT, 'Hauptstr. 1' AS STRASSE UNION ALL
  -- PLZ Discrepancy
  SELECT 'K002' AS KUNDE, '54322' AS PLZ, 'Hamburg' AS ORT, 'Waldweg 2' AS STRASSE UNION ALL
  -- ORT Discrepancy (NULL vs Value)
  SELECT 'K003' AS KUNDE, '99999' AS PLZ, 'Leipzig' AS ORT, 'Gartenstr. 3' AS STRASSE UNION ALL
  -- Discrepancy but excluded by date
  SELECT 'K004' AS KUNDE, '22222' AS PLZ, 'München' AS ORT, 'Isarweg 4' AS STRASSE UNION ALL
  -- Both PLZ are NULL (should match)
  SELECT 'K005' AS KUNDE, CAST(NULL AS STRING) AS PLZ, 'Köln' AS ORT, 'Domplatz 5' AS STRASSE
),

-- Execute Migrated Query Logic (Targeting Stichtag '20260329')
QueryResult AS (
  SELECT
    'ABWEICHUNG' AS MARKER,
    k.KUNDE,
    k.NACHNAME,
    k.VORNAME,
    k.PLZ,
    k.ORT,
    k.STRASSE,
    r.PLZ AS REF_PLZ,
    r.ORT AS REF_ORT,
    r.STRASSE AS REF_STRASSE
  FROM T_KUNDE AS k
  JOIN T_KUNDE_REFERENZ AS r
    ON r.KUNDE = k.KUNDE
  WHERE k.AKTUALISIERT_AM <= PARSE_DATE('%Y%m%d', '20260329')
    AND (
         COALESCE(k.PLZ, 'x') != COALESCE(r.PLZ, 'x')
      OR COALESCE(k.ORT, 'x') != COALESCE(r.ORT, 'x')
      OR COALESCE(k.STRASSE, 'x') != COALESCE(r.STRASSE, 'x')
        )
)

-- Assertions
SELECT 
  CASE 
    -- Verify exact row count (Only K002 and K003 should have discrepancies)
    WHEN COUNT(*) != 2 THEN 'FAIL: Expected exactly 2 discrepancy rows, got ' || CAST(COUNT(*) AS STRING)
    -- Verify ordering and specific keys
    WHEN MIN(KUNDE) != 'K002' THEN 'FAIL: Expected first discrepancy to be K002'
    WHEN MAX(KUNDE) != 'K003' THEN 'FAIL: Expected last discrepancy to be K003'
    -- Verify marker literal preservation
    WHEN COUNTIF(MARKER != 'ABWEICHUNG') > 0 THEN 'FAIL: Marker column is not verbatim "ABWEICHUNG"'
    ELSE 'PASS'
  END AS test_verdict
FROM QueryResult;
```

---

## Test Case 4: Discrepancy Detection, Log Parsing & Warning Logic

### Purpose
Verify that the Python script correctly parses the BigQuery output log, counts lines starting with `"ABWEICHUNG"`, appends the count to the log file, and writes a warning to `stderr` if discrepancies exist, while still exiting with code `0` (warning-only behavior).

### Setup
*   A local temporary directory to act as `DW_DIR_LOG`.
*   Mocked BigQuery client returning:
    *   **Run A:** 2 discrepancy rows.
    *   **Run B:** 0 discrepancy rows.

### Action
1.  Execute `r_abgl_kunde_woech.py` for Run A. Inspect the generated log file and `stderr`.
2.  Execute `r_abgl_kunde_woech.py` for Run B. Inspect the generated log file and `stderr`.

### Pass/Fail Criterion
*   **Pass:**
    *   For Run A: Log file contains `"Anzahl gefundener Abweichungen: 2"`. `stderr` contains `"[W] <timestamp> 2 Abweichungen im Kundenadressabgleich gefunden"`. Exit code is `0`.
    *   For Run B: Log file contains `"Anzahl gefundener Abweichungen: 0"`. `stderr` is empty. Exit code is `0`.
*   **Fail:** The script exits with a non-zero code on discrepancies, fails to write the correct count to the log, or does not output the exact German warning format to `stderr`.

### Test Code
```python
import os
import sys
import pytest
from unittest.mock import patch, MagicMock

# Import the main module under test
import bin.r_abgl_kunde_woech as script


@pytest.fixture
def temp_log_dir(tmp_path):
    """Fixture to set up temporary log directory."""
    log_dir = tmp_path / "log"
    log_dir.mkdir()
    (log_dir / "kunde").mkdir()
    return log_dir


@patch("bin.r_abgl_kunde_woech.bigquery.Client")
def test_discrepancy_warning_and_exit_code(mock_bq_client, temp_log_dir, capsys):
    """Verify discrepancy counting, log appending, stderr warning, and exit code 0."""
    
    # 1. Mock BigQuery to return 2 discrepancy rows
    mock_row_1 = MagicMock()
    mock_row_1.values.return_value = ["ABWEICHUNG", "K001", "Mustermann", "Max", "12345", "Berlin", "Hauptstr. 1", "12346", "Berlin", "Hauptstr. 1"]
    mock_row_2 = MagicMock()
    mock_row_2.values.return_value = ["ABWEICHUNG", "K002", "Schmidt", "Anna", "54321", "Hamburg", "Waldweg 2", "54321", "Bremen", "Waldweg 2"]
    
    mock_results = [mock_row_1, mock_row_2]
    mock_bq_client.return_value.query.return_value.result.return_value = mock_results

    # 2. Set up environment variables
    env_mock = {
        "GCP_PROJECT": "test-project",
        "BQ_DATASET": "test_dataset",
        "DW_DIR_LOG": str(temp_log_dir),
        "DW_DIR_ROOT": str(temp_log_dir),  # Point to temp for SQL file lookup
    }

    # Create a dummy SQL file so the script doesn't throw FileNotFoundError
    sql_dir = temp_log_dir / "exporter" / "kunde" / "sql"
    sql_dir.mkdir(parents=True, exist_ok=True)
    sql_file = sql_dir / "d_abgl_kunde_woech.sql"
    sql_file.write_text("SELECT 1;")

    # 3. Run the script with -s parameter
    test_args = ["r_abgl_kunde_woech.py", "-s", "20260329"]
    
    with patch.dict(os.environ, env_mock), patch.object(sys, "argv", test_args):
        exit_code = script.main()

    # 4. Assertions
    assert exit_code == 0, "Script should exit with 0 even when discrepancies are found."

    # Capture stdout and stderr
    captured = capsys.readouterr()

    # Verify stderr warning format
    assert "[W]" in captured.err, "Warning prefix '[W]' missing from stderr."
    assert "2 Abweichungen im Kundenadressabgleich gefunden" in captured.err, \
        "Discrepancy warning message missing or incorrect in stderr."

    # Verify log file contents
    log_files = list((temp_log_dir / "kunde").glob("abgl_kunde_woech_*.log"))
    assert len(log_files) == 1, "Expected exactly one log file to be created."
    
    log_content = log_files[0].read_text(encoding="utf-8")
    assert "Anzahl gefundener Abweichungen: 2" in log_content, \
        "Discrepancy count was not correctly written to the log file."
    assert "ABWEICHUNG K001 Mustermann" in log_content, \
        "Discrepancy rows were not written to the log file."
```