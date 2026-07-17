Here is a comprehensive suite of migration-validation tests designed to prove that the migrated Google Cloud Platform (Cloud Composer, BigQuery, Dataform) components are behaviorally equivalent to the legacy Automic/UC4, KornShell, and Oracle SQL*Plus workflow.

---

# Test Suite: `DW_KUNDE_ABGL_WOECHENTLICH` Migration Validation

This test suite validates the migration of the weekly customer address alignment workflow (`DW.DWH_KUNDE_ABGL_WOECHENTLICH_JP`) from Oracle/UC4 to GCP. It contains automated validation scripts, SQL assertions, and execution checks.

---

## Section 1: Output Parity & Transformation Correctness

### Test Case 1.1: Address Discrepancy Detection & Null Handling
#### Purpose
Verify that the migrated BigQuery SQL / Dataform logic identifies the exact same address discrepancies as the legacy Oracle SQL script, specifically validating:
*   Mismatches in `PLZ` (Postal Code), `ORT` (City), or `STRASSE` (Street).
*   Correct handling of `NULL` values (using `COALESCE` in BigQuery vs. `nvl` in Oracle).
*   Filtering based on the reporting cutoff date (`AKTUALISIERT_AM <= stichtag`).

#### Setup
1.  Create temporary test tables in BigQuery mimicking the production schema:
    *   `DWH_KERN.T_KUNDE`
    *   `STAMMDATEN.T_KUNDE_REFERENZ`
2.  Populate both tables with identical test cases containing matching records, mismatched records, and `NULL` values.

#### Action
Execute the following validation query in BigQuery for `stichtag = '20260301'`:

```sql
DECLARE target_stichtag DATE DEFAULT DATE('2026-03-01');

WITH test_kunde AS (
  -- Row 1: Perfect Match
  SELECT 'K001' AS KUNDE, 'Müller' AS NACHNAME, 'Hans' AS VORNAME, '12345' AS PLZ, 'Berlin' AS ORT, 'Hauptstr. 1' AS STRASSE, DATE('2026-02-20') AS AKTUALISIERT_AM UNION ALL
  -- Row 2: Mismatched PLZ
  SELECT 'K002', 'Schmidt', 'Anna', '54321', 'Hamburg', 'Elbchaussee 2', DATE('2026-02-25') UNION ALL
  -- Row 3: Mismatched ORT (with NULL in source)
  SELECT 'K003', 'Fischer', 'Fritz', '80331', NULL, 'Marienplatz 5', DATE('2026-02-28') UNION ALL
  -- Row 4: Mismatched STRASSE (with NULL in reference)
  SELECT 'K004', 'Weber', 'Julia', '50667', 'Köln', 'Domplatz 1', DATE('2026-02-28') UNION ALL
  -- Row 5: Match but updated AFTER stichtag (Should be excluded)
  SELECT 'K005', 'Meyer', 'Stefan', '60311', 'Frankfurt', 'Zeil 10', DATE('2026-03-02')
),
test_referenz AS (
  SELECT 'K001' AS KUNDE, '12345' AS PLZ, 'Berlin' AS ORT, 'Hauptstr. 1' AS STRASSE UNION ALL
  SELECT 'K002', '99999', 'Hamburg', 'Elbchaussee 2' UNION ALL -- Diff PLZ
  SELECT 'K003', '80331', 'München', 'Marienplatz 5' UNION ALL -- Diff ORT (Source was NULL)
  SELECT 'K004', '50667', 'Köln', NULL UNION ALL             -- Diff STRASSE (Ref is NULL)
  SELECT 'K005', '60311', 'Frankfurt', 'Zeil 99'               -- Diff STRASSE (But excluded by date)
)

SELECT
  k.KUNDE,
  COALESCE(k.PLZ, 'x') != COALESCE(r.PLZ, 'x') AS plz_diff,
  COALESCE(k.ORT, 'x') != COALESCE(r.ORT, 'x') AS ort_diff,
  COALESCE(k.STRASSE, 'x') != COALESCE(r.STRASSE, 'x') AS strasse_diff
FROM test_kunde k
JOIN test_referenz r ON r.KUNDE = k.KUNDE
WHERE k.AKTUALISIERT_AM <= target_stichtag
  AND (
        COALESCE(k.PLZ, 'x')     != COALESCE(r.PLZ, 'x')
     OR COALESCE(k.ORT, 'x')     != COALESCE(r.ORT, 'x')
     OR COALESCE(k.STRASSE, 'x') != COALESCE(r.STRASSE, 'x')
  );
```

#### Pass/Fail Criterion
*   **Pass**: The query returns exactly 3 rows: `K002` (PLZ mismatch), `K003` (ORT mismatch), and `K004` (STRASSE mismatch). `K001` (perfect match) and `K005` (updated after stichtag) are excluded.
*   **Fail**: Any other row count or mismatch combination is returned.

---

## Section 2: Log and Alert Parity (KornShell to Python)

### Test Case 2.1: Verbatim Log Output and Warning Thresholds
#### Purpose
Verify that the migrated Python execution script (`bin/r_abgl_kunde_woech.py`) produces the exact character-for-character log outputs and warning messages as the legacy KornShell script under different anomaly counts.

#### Setup
Install `pytest` and mock the Google Cloud BigQuery client to return specific row counts.

#### Action
Run the following `pytest` test suite:

```python
import pytest
from unittest.mock import MagicMock, patch
import datetime

# Import the migrated execution function
from bin.r_abgl_kunde_woech import run_reconciliation

@patch('bin.r_abgl_kunde_woech.bigquery.Client')
def test_reconciliation_logging_no_discrepancies(mock_bq_client, capsys):
    # Setup mock to return 0 discrepancies
    mock_results = []
    mock_client_instance = MagicMock()
    mock_client_instance.query.return_value.result.return_value = mock_results
    mock_bq_client.return_value = mock_client_instance

    # Run execution
    run_reconciliation(
        gcp_project="test-project",
        bq_dataset="test_dataset",
        l_stichtag="20260301",
        run_id="12345",
        lauf_woche="2026-09"
    )

    captured = capsys.readouterr()
    
    # Assertions for exact legacy string matches
    assert "Starte Adressabgleich Kundenstammdaten fuer Stichtag 20260301" in captured.out
    assert "Anzahl gefundener Abweichungen: 0" in captured.out
    assert "Adressabgleich Kundenstammdaten ohne erkennbare Fehler beendet" in captured.out
    assert "Kundenadressabgleich fuer Lauf 2026-09 angestossen" in captured.out
    assert "[W]" not in captured.out  # No warning should be printed


@patch('bin.r_abgl_kunde_woech.bigquery.Client')
def test_reconciliation_logging_with_discrepancies(mock_bq_client, capsys):
    # Setup mock to return 2 discrepancies
    RowMock = collections.namedtuple('Row', ['status_msg'])
    mock_results = [
        RowMock(status_msg="ABWEICHUNG: Kunde K002 hat abweichende Adresse."),
        RowMock(status_msg="ABWEICHUNG: Kunde K003 hat abweichende Adresse.")
    ]
    mock_client_instance = MagicMock()
    mock_client_instance.query.return_value.result.return_value = mock_results
    mock_bq_client.return_value = mock_client_instance

    import collections

    # Run execution
    run_reconciliation(
        gcp_project="test-project",
        bq_dataset="test_dataset",
        l_stichtag="20260301",
        run_id="12345",
        lauf_woche="2026-09"
    )

    captured = capsys.readouterr()
    
    # Assertions for exact legacy warning format
    assert "Starte Adressabgleich Kundenstammdaten fuer Stichtag 20260301" in captured.out
    assert "Anzahl gefundener Abweichungen: 2" in captured.out
    
    # Check warning format: [W] YYYY-MM-DD HH:MM:SS 2 Abweichungen im Kundenadressabgleich gefunden, siehe abgl_kunde_woech_12345.log
    assert "[W]" in captured.out
    assert "2 Abweichungen im Kundenadressabgleich gefunden, siehe abgl_kunde_woech_12345.log" in captured.out
    assert "Kundenadressabgleich fuer Lauf 2026-09 angestossen" in captured.out
```

#### Pass/Fail Criterion
*   **Pass**: Both test cases pass, proving that stdout matches the legacy KornShell output format character-for-character.
*   **Fail**: Any assertion fails, or the warning format deviates from the legacy pattern.

---

## Section 3: Orchestration & Dynamic Parameter Handling

### Test Case 3.1: Airflow Dynamic Date Fallback (Stichtag)
#### Purpose
Verify that the Airflow DAG correctly calculates the default `stichtag` parameter as "7 days ago" (matching the legacy shell script's `date -d '7 days ago' '+%Y%m%d'`) when no manual override is provided in the DAG run configuration.

#### Setup
Initialize a local Airflow unit testing context.

#### Action
Execute the following pytest test to validate parameter resolution:

```python
import datetime
from airflow.models import DagRun, TaskInstance
from airflow.utils.types import DagRunType
from airflow.utils.state import State
import pytest

# Import the wrapper function
from dags.dw_dwh_kunde_abgl_woechentlich import execute_reconciliation_wrapper

@patch('dags.dw_dwh_kunde_abgl_woechentlich.run_reconciliation')
@patch('dags.dw_dwh_kunde_abgl_woechentlich.Variable')
def test_airflow_stichtag_fallback(mock_variable, mock_run_reconciliation):
    # Mock Airflow Variables
    mock_variable.get.side_effect = lambda key, default_var=None: {
        "GCP_PROJECT": "prod-project",
        "BQ_DATASET_DWH_KERN": "dwh_kern"
    }.get(key, default_var)

    # Mock Context
    execution_date = datetime.datetime(2026, 3, 10, 6, 0, 0)
    context = {
        'execution_date': execution_date,
        'ds': '2026-03-10',
        'ds_nodash': '20260310',
        'run_id': 'scheduled__2026-03-10T06:00:00+00:00',
        'dag_run': MagicMock(conf={})  # Empty configuration to trigger fallback
    }

    # Execute wrapper
    execute_reconciliation_wrapper(**context)

    # Verify that run_reconciliation was called with stichtag = 7 days before 2026-03-10 (which is 20260303)
    mock_run_reconciliation.assert_called_once_with(
        gcp_project="prod-project",
        bq_dataset="dwh_kern",
        l_stichtag="20260303",  # 2026-03-10 minus 7 days
        run_id="scheduled__2026-03-10T06:00:00+00:00",
        lauf_woche="2026-03-10",
        sql_path=pytest.any
    )
```

#### Pass/Fail Criterion
*   **Pass**: The wrapper correctly calculates `20260303` as the `l_stichtag` and passes it to the execution script.
*   **Fail**: The wrapper passes any other date or fails to resolve.

---

## Section 4: Data Quality & Schema Assertions

### Test Case 4.1: Dataform Output Schema and Partitioning Validation
#### Purpose
Ensure that the Dataform-generated table `work.wrk_kunden_abweichungen` matches the target schema specification, is correctly partitioned by `stichtag`, and enforces idempotency (pre-operations cleanup).

#### Setup
Deploy the Dataform model to a test environment.

#### Action
Execute the following metadata validation queries in BigQuery:

```sql
-- Assertion 1: Verify Column Names and Types
SELECT column_name, data_type, is_nullable
FROM `work.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'wrk_kunden_abweichungen'
ORDER BY ordinal_position;

-- Assertion 2: Verify Partitioning
SELECT is_partitioning_column, partitioning_type
FROM `work.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'wrk_kunden_abweichungen' AND column_name = 'stichtag';
```

#### Pass/Fail Criterion
*   **Pass**: 
    *   Assertion 1 returns columns: `MARKER` (STRING), `KUNDE` (STRING), `NACHNAME` (STRING), `VORNAME` (STRING), `PLZ` (STRING), `ORT` (STRING), `STRASSE` (STRING), `REF_PLZ` (STRING), `REF_ORT` (STRING), `REF_STRASSE` (STRING), `stichtag` (DATE).
    *   Assertion 2 confirms `stichtag` is the partitioning column with `partitioning_type = 'DAY'`.
*   **Fail**: Schema mismatches are detected, or the table is not partitioned.