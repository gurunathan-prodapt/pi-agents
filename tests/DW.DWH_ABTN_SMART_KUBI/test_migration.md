Here is a comprehensive suite of migration-validation tests designed to prove that the migrated BigQuery/Airflow pipeline for `DW.DWH_ABTN_SMART_KUBI` is behaviorally equivalent to the legacy UC4/Oracle implementation.

---

# Test Suite Overview

These tests validate the correctness of the migration across four key dimensions:
1. **Orchestration & Parameter Parity**: Ensuring the Airflow DAG calculates the reporting month (`MONATSID`) identically to the legacy UC4 date logic.
2. **Transformation Correctness**: Verifying that the BigQuery SQL script performs joins, filters, aggregations, and conditional logic (such as `DECODE` to `CASE` translations) correctly.
3. **Null & Edge-Case Handling**: Testing boundary conditions for dates, null values, and specific business rules (e.g., `mp_geschaeftsfeld_id = 2`).
4. **Operational Telemetry & Error Handling**: Ensuring that the Python logging wrappers and BigQuery transaction rollbacks behave as designed.

---

## Test Case 1: Airflow DAG Parameter Calculation (`MONATSID`)

### Purpose
Verify that the Airflow DAG correctly calculates the reporting month (`MONATSID`) based on the logical execution date, matching the legacy UC4 script's day-of-month threshold logic.

### Setup
A Python environment with `pytest` and `apache-airflow` installed.

### Action
Execute a unit test that mocks the Airflow execution context with dates before and after the 15th of the month, calling the `run_abtn_smart_kubi` callable.

### Code Assertion (pytest)
```python
import pytest
from datetime import datetime
from local.home.gurunathan_t.kubi.DW_DWH_ABTN_SMART_KUBI import run_abtn_smart_kubi

@pytest.mark.parametrize(
    "logical_date, expected_monatsid",
    [
        # Case 1: Day is before the 15th -> Should return the previous month
        (datetime(2023, 10, 14), "202309"),
        (datetime(2023, 10, 1), "202309"),
        # Case 2: Day is on or after the 15th -> Should return the current month
        (datetime(2023, 10, 15), "202310"),
        (datetime(2023, 10, 31), "202310"),
        # Case 3: Leap year boundary check
        (datetime(2024, 3, 14), "202402"),
        (datetime(2024, 3, 15), "202403"),
    ]
)
def test_monatsid_calculation(logical_date, expected_monatsid):
    # Mock the Airflow context dictionary
    context = {"logical_date": logical_date}
    
    # Execute the migrated Python callable
    actual_monatsid = run_abtn_smart_kubi(**context)
    
    # Assert behavioral equivalence
    assert actual_monatsid == expected_monatsid, \
        f"Failed for logical date {logical_date.strftime('%Y-%m-%d')}. Expected {expected_monatsid}, got {actual_monatsid}"
```

### Pass/Fail Criterion
* **Pass**: The calculated `MONATSID` matches the expected value for all parameterized dates.
* **Fail**: Any calculated `MONATSID` deviates from the legacy logic rules.

---

## Test Case 2: Core SQL Transformation — Join & Filter Correctness

### Purpose
Verify that the BigQuery SQL script correctly filters fact records by date and KPI code, and performs standard ANSI `LEFT JOIN` operations equivalent to the legacy Oracle `(+)` outer joins.

### Setup
1. Create temporary test tables in a BigQuery test dataset:
   * `dwh_ta_f_d1_twvv_tn` (Fact)
   * `dwh_vi_l_map_fa_tarif` (Tarif Map View)
   * `bl_d_tarif` (Tarif Dimension)
   * `dwh_ta_c_vertrag` (Contract Dimension)
   * `dwh_ta_t_smart_kubi` (Target Table)
2. Populate these tables with mock records designed to test join boundaries.

### Action
Execute the BigQuery SQL script with query parameters `p_monats_id = 201509` and `p_eintrags_nr = 99999`.

### Code Assertion (Python + BigQuery Client)
```python
import os
from google.cloud import bigquery
import pytest

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client()

def test_sql_joins_and_filters(bq_client):
    dataset = os.environ.get("BQ_METADATA_DATASET", "test_dataset")
    
    # 1. Clean and Populate Mock Data
    setup_queries = f"""
    TRUNCATE TABLE `{dataset}.dwh_ta_f_d1_twvv_tn`;
    TRUNCATE TABLE `{dataset}.dwh_vi_l_map_fa_tarif`;
    TRUNCATE TABLE `{dataset}.bl_d_tarif`;
    TRUNCATE TABLE `{dataset}.dwh_ta_c_vertrag`;
    
    -- Insert Tariffs (temp CTE source)
    INSERT INTO `{dataset}.dwh_vi_l_map_fa_tarif` (tarif_id, dwh_tarif_id, gueltig_von, gueltig_bis) VALUES
    (101, 1001, DATE '2015-01-01', DATE '4712-12-31'),
    (102, 1002, DATE '2015-01-01', DATE '4712-12-31'),
    (103, 1003, DATE '2015-01-01', DATE '2015-08-31'); -- Expired tariff (should be filtered out of temp CTE)

    INSERT INTO `{dataset}.bl_d_tarif` (tarif_id, mp_geschaeftsfeld_id) VALUES
    (101, 1),
    (102, 2),
    (103, 1);

    -- Insert Contracts (dwh_ta_c_vertrag)
    -- l_monats_date for 201509 is 2015-10-01.
    -- Contract join condition: l_monats_date > gueltig_von AND l_monats_date <= gueltig_bis
    INSERT INTO `{dataset}.dwh_ta_c_vertrag` (dwh_vertrag_id, t_mobile_kundennummer, test_gp, gueltig_von, gueltig_bis) VALUES
    (5001, 'KUND_A', 'N', DATE '2015-09-01', DATE '2015-10-15'), -- Valid (2015-10-01 is within range)
    (5002, 'KUND_B', 'Y', DATE '2015-10-02', DATE '2015-11-01'), -- Invalid (2015-10-01 is not > gueltig_von)
    (5003, 'KUND_C', 'N', DATE '2015-08-01', DATE '2015-09-30'); -- Invalid (2015-10-01 is not <= gueltig_bis)

    -- Insert Fact Records
    INSERT INTO `{dataset}.dwh_ta_f_d1_twvv_tn` (gueltigkeitszeitpunkt, kennzahl_id, dwh_tarif_id_neu, dwh_tarif_id_alt, dwh_vertrag_id, zugang, vo_kenn, vo_kenn_bearb) VALUES
    -- Record 1: Valid date, valid KPI, valid joins
    (TIMESTAMP '2015-09-15 12:00:00', 'VVLREIN', 1001, 1002, 5001, 10, 'VO_01', 'VO_BEARB_01'),
    -- Record 2: Invalid date (should be filtered out)
    (TIMESTAMP '2015-10-01 00:00:00', 'VVLREIN', 1001, 1002, 5001, 5, 'VO_01', 'VO_BEARB_01'),
    -- Record 3: Invalid KPI (should be filtered out)
    (TIMESTAMP '2015-09-15 12:00:00', 'INVALID_KPI', 1001, 1002, 5001, 5, 'VO_01', 'VO_BEARB_01'),
    -- Record 4: Valid date/KPI, but unmatched contract (should outer join with NULLs)
    (TIMESTAMP '2015-09-20 12:00:00', 'VVLTWC2C', 1001, 1002, 9999, 2, 'VO_02', 'VO_BEARB_02');
    """
    bq_client.query(setup_queries).result()

    # 2. Run Migrated SQL Script
    with open("local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql", "r") as f:
        sql_script = f.read()

    # Replace table names with test dataset qualifiers if not dynamic
    sql_script = sql_script.replace("dwh_ta_t_smart_kubi", f"`{dataset}.dwh_ta_t_smart_kubi`")
    sql_script = sql_script.replace("dwh_ta_f_d1_twvv_tn", f"`{dataset}.dwh_ta_f_d1_twvv_tn`")
    sql_script = sql_script.replace("dwh_vi_l_map_fa_tarif", f"`{dataset}.dwh_vi_l_map_fa_tarif`")
    sql_script = sql_script.replace("bl_d_tarif", f"`{dataset}.bl_d_tarif`")
    sql_script = sql_script.replace("dwh_ta_c_vertrag", f"`{dataset}.dwh_ta_c_vertrag`")

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("p_monats_id", "INT64", 201509),
            bigquery.ScalarQueryParameter("p_eintrags_nr", "INT64", 99999),
        ]
    )
    bq_client.query(sql_script, job_config=job_config).result()

    # 3. Assert Results
    result_query = f"SELECT * FROM `{dataset}.dwh_ta_t_smart_kubi` ORDER BY kennzahl_id"
    rows = list(bq_client.query(result_query).result())

    # We expect exactly 2 records (Record 1 and Record 4)
    assert len(rows) == 2, f"Expected 2 rows, found {len(rows)}"
    
    # Verify Record 1 mappings
    rec1 = [r for r in rows if r.kennzahl_id == 'VVLREIN'][0]
    assert rec1.monats_id == 201509
    assert rec1.kundennummer == 'KUND_A'  # Joined successfully
    assert rec1.tarif_id == 101
    assert rec1.tarif_id_alt == 102
    assert rec1.test_gp == 'N'
    assert rec1.anzahl == 10

    # Verify Record 4 mappings (Outer join fallback)
    rec4 = [r for r in rows if r.kennzahl_id == 'VVLTWC2C'][0]
    assert rec4.kundennummer is None  # Contract did not join
    assert rec4.test_gp is None
    assert rec4.anzahl == 2
```

### Pass/Fail Criterion
* **Pass**: The target table contains exactly the records that match the date and KPI filters, and the outer-joined fields are populated correctly or set to `NULL` where unmatched.
* **Fail**: Records outside the date/KPI scope are inserted, or outer joins fail to resolve.

---

## Test Case 3: Core SQL Transformation — Conditional Logic & Aggregations

### Purpose
Verify that the BigQuery SQL script correctly executes conditional mappings (`DECODE` equivalents) and aggregates metrics (`SUM(zugang)`) correctly.

### Setup
Using the same BigQuery test dataset, populate the tables with specific edge cases for `mp_geschaeftsfeld_id` and `vo_kenn_bearb`.

### Action
Execute the BigQuery SQL script with query parameters `p_monats_id = 201509`.

### Code Assertion (SQL Assertion)
```sql
-- This query asserts that the conditional logic and aggregations match the legacy expectations.
-- It returns 0 rows if the migration is successful.

WITH expected AS (
  SELECT 201509 AS monats_id, '-1' AS kundennummer, 101 AS tarif_id, 102 AS tarif_id_alt, 'VO_FALLBACK' AS vo_kennung, 'N' AS test_gp, 15 AS anzahl, 'VVLREIN' AS kennzahl_id
  UNION ALL
  SELECT 201509, 'KUND_A', 101, 101, 'VO_BEARB', 'N', 5, 'VVLREIN'
)
SELECT 
  'MISMATCH' AS failure_reason,
  actual.*
FROM test_dataset.dwh_ta_t_smart_kubi actual
FULL OUTER JOIN expected
  ON actual.monats_id = expected.monats_id
  AND actual.kundennummer = expected.kundennummer
  AND actual.tarif_id = expected.tarif_id
  AND actual.tarif_id_alt = expected.tarif_id_alt
  AND actual.vo_kennung = expected.vo_kennung
  AND actual.test_gp = expected.test_gp
  AND actual.kennzahl_id = expected.kennzahl_id
WHERE actual.anzahl IS NULL 
   OR expected.anzahl IS NULL 
   OR actual.anzahl != expected.anzahl;
```

### Pass/Fail Criterion
* **Pass**: The assertion query returns 0 rows, proving that:
  * `mp_geschaeftsfeld_id = 2` correctly maps `kundennummer` to `-1`.
  * `vo_kenn_bearb` values of `NULL`, `''`, or `'#'` correctly fall back to `vo_kenn`.
  * Duplicate keys are aggregated correctly via `SUM(zugang)`.
* **Fail**: The assertion query returns mismatch rows.

---

## Test Case 4: Transactional Integrity & Exception Handling

### Purpose
Verify that the BigQuery SQL script handles runtime exceptions gracefully, rolls back any partial inserts, and logs the failure details to the metadata structure.

### Setup
Intentionally cause a runtime error during the execution of the SQL script (e.g., by passing an invalid date format or forcing a division by zero inside the transaction block).

### Action
Execute the BigQuery SQL script with a payload designed to fail.

### Code Assertion (pytest)
```python
from google.cloud.exceptions import GoogleCloudError
import pytest

def test_transaction_rollback_on_error(bq_client):
    dataset = "test_dataset"
    
    # 1. Insert a sentinel record into the target table
    bq_client.query(f"INSERT INTO `{dataset}.dwh_ta_t_smart_kubi` (monats_id) VALUES (999999)").result()
    
    # 2. Read the SQL script and inject a division-by-zero error inside the transaction
    with open("local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql", "r") as f:
        sql_script = f.read()

    # Inject error into the SELECT statement
    error_sql = sql_script.replace(
        "SUM(fact.zugang) AS anzahl,",
        "SUM(fact.zugang) / 0 AS anzahl,"
    )
    
    # Apply dataset qualifiers
    error_sql = error_sql.replace("dwh_ta_t_smart_kubi", f"`{dataset}.dwh_ta_t_smart_kubi`")
    error_sql = error_sql.replace("dwh_ta_f_d1_twvv_tn", f"`{dataset}.dwh_ta_f_d1_twvv_tn`")
    error_sql = error_sql.replace("dwh_vi_l_map_fa_tarif", f"`{dataset}.dwh_vi_l_map_fa_tarif`")
    error_sql = error_sql.replace("bl_d_tarif", f"`{dataset}.bl_d_tarif`")
    error_sql = error_sql.replace("dwh_ta_c_vertrag", f"`{dataset}.dwh_ta_c_vertrag`")

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("p_monats_id", "INT64", 201509),
            bigquery.ScalarQueryParameter("p_eintrags_nr", "INT64", 88888),
        ]
    )

    # 3. Execute and assert that it raises an exception
    with pytest.raises(GoogleCloudError):
        bq_client.query(error_sql, job_config=job_config).result()

    # 4. Verify that the transaction rolled back and the sentinel record is STILL there
    # (If TRUNCATE committed but the INSERT failed, the table would be empty. 
    #  Because of BEGIN TRANSACTION, the TRUNCATE must be rolled back.)
    result = list(bq_client.query(f"SELECT COUNT(*) FROM `{dataset}.dwh_ta_t_smart_kubi` WHERE monats_id = 999999").result())
    assert result[0][0] == 1, "Transaction failed to roll back! Sentinel record was lost."
```

### Pass/Fail Criterion
* **Pass**: The query execution fails with a database error, and the target table retains its pre-transaction state (the sentinel record is not lost).
* **Fail**: The script fails but commits the `TRUNCATE` command, leaving the table empty.

---

## Test Case 5: End-to-End Wrapper Execution (`r_sqlscript.py`)

### Purpose
Verify that the Python utility wrapper `r_sqlscript.py` correctly parses arguments, resolves script paths, registers metadata entries, and propagates exit codes.

### Setup
A local testing directory containing:
* `kubi/r_sqlscript.py`
* `sql/d_abtn_x_smart_kubi.sql` (A mock SQL script that executes successfully)

### Action
Execute `r_sqlscript.py` via a subprocess call with valid and invalid parameters.

### Code Assertion (pytest)
```python
import subprocess
import os
import pytest

def test_r_sqlscript_execution_success():
    # Run wrapper with a valid script path
    result = subprocess.run(
        [
            "python3", "local/home/gurunathan_t/kubi/r_sqlscript.py",
            "-f", "d_abtn_x_smart_kubi.sql",
            "-j", "ABTN_SMART_KUBI",
            "-i", "201509"
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    # Assert successful exit code
    assert result.returncode == 0, f"Execution failed: {result.stderr}"
    assert "Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet" in result.stdout

def test_r_sqlscript_missing_arguments():
    # Run wrapper without mandatory -f parameter
    result = subprocess.run(
        [
            "python3", "local/home/gurunathan_t/kubi/r_sqlscript.py",
            "-j", "ABTN_SMART_KUBI"
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    # Assert failure exit code matching legacy getopts error (193)
    assert result.returncode == 193
    assert "ERROR: EintragsNr=0, Severity=E, ErrNr=193" in result.stderr
```

### Pass/Fail Criterion
* **Pass**: The wrapper script returns exit code `0` on successful execution, and returns the correct legacy error codes (e.g., `193` for missing arguments) on validation failures.
* **Fail**: The wrapper script returns incorrect exit codes or fails to propagate execution logs.