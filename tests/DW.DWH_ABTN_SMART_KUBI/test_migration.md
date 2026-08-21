# Migration Validation Test Suite: DW.DWH_ABTN_SMART_KUBI

This test suite validates the migration of the `DW.DWH_ABTN_SMART_KUBI` workflow from UC4/Oracle to Apache Airflow/Google Cloud BigQuery. It ensures behavioral equivalence, transformation correctness, robust error handling, and data quality.

---

## Test Case 1: Date Calculation Logic (`MONATSID` calculation)

### Purpose
Prove that the Python date calculation logic in the migrated Airflow DAG matches the legacy UC4 date-subtraction logic exactly across all boundary dates (specifically around the 15th of the month).

### Setup
A Python environment with `pytest` and `freezegun` (or standard datetime mocking) installed.

### Action
Execute the date calculation function with logical execution dates representing boundary conditions:
1. **Before the 15th** (e.g., August 14th): Should return the previous month (`YYYYMM`).
2. **On the 15th** (e.g., August 15th): Should return the current month (`YYYYMM`).
3. **After the 15th** (e.g., August 31st): Should return the current month (`YYYYMM`).
4. **Leap Year / Year Boundary** (e.g., January 10th): Should return the previous year's December (`YYYY12`).

### Code (Pytest)

```python
import pytest
from datetime import datetime
from unittest.mock import MagicMock

# Import the callable from the migrated DAG
from local.home.gurunathan_t.kubi.dw_dwh_abtn_smart_kubi import calculate_monatsid_callable

@pytest.mark.parametrize(
    "logical_date_str, expected_monatsid",
    [
        ("2026-08-14", "202607"),  # Before 15th -> Previous Month
        ("2026-08-15", "202608"),  # On 15th -> Current Month
        ("2026-08-31", "202608"),  # After 15th -> Current Month
        ("2026-01-01", "202512"),  # Year Boundary, Before 15th -> Dec of Prev Year
        ("2024-03-14", "202402"),  # Leap Year, Before 15th -> Feb of Leap Year
    ]
)
def test_calculate_monatsid(logical_date_str, expected_monatsid):
    # Mock Airflow context
    logical_date = datetime.strptime(logical_date_str, "%Y-%m-%d")
    context = {"logical_date": logical_date}
    
    # Execute the migrated logic
    actual_monatsid = calculate_monatsid_callable(**context)
    
    assert actual_monatsid == expected_monatsid, \
        f"Failed for logical date {logical_date_str}: Expected {expected_monatsid}, got {actual_monatsid}"
```

### Pass/Fail Criterion
* **Pass**: All boundary dates yield the exact `MONATSID` matching the legacy UC4 logic.
* **Fail**: Any boundary date yields an incorrect month identifier.

---

## Test Case 2: SQL Transformation & Output Parity

### Purpose
Prove that the migrated BigQuery SQL script (`d_abtn_x_smart_kubi.sql`) produces identical outputs to the legacy Oracle PL/SQL script under identical source data conditions. This validates:
1. Conversion of Oracle outer joins `(+)` to ANSI `LEFT OUTER JOIN` with date boundaries.
2. `DECODE` and `NVL` mapping to `CASE WHEN` and `COALESCE`.
3. Oracle-to-BigQuery string handling (specifically `TRIM` and empty string vs. `NULL` handling).
4. Aggregation (`SUM`) and filtering on `kennzahl_id`.

### Setup
1. Create isolated test tables in a BigQuery sandbox dataset:
   * `dw.dwh_ta_f_d1_twvv_tn`
   * `dw.dwh_vi_l_map_fa_tarif`
   * `dw.bl_d_tarif`
   * `dw.dwh_ta_c_vertrag`
   * `dw.dwh_ta_t_smart_kubi` (Target table)
2. Populate the source tables with test cases designed to trigger all conditional branches:
   * **Branch A**: `mp_geschaeftsfeld_id = 2` (should map `kundennummer` to `'-1'`).
   * **Branch B**: `mp_geschaeftsfeld_id != 2` (should map `kundennummer` to `t_mobile_kundennummer`).
   * **Branch C**: `vo_kenn_bearb` is `NULL`, empty string `''`, or `'#'` (should fall back to `vo_kenn`).
   * **Branch D**: `vo_kenn_bearb` is a valid string (should use `vo_kenn_bearb`).
   * **Branch E**: Missing tariff mappings (should fall back to `0` via `COALESCE`).
   * **Branch F**: Date boundary checks (`l_monats_date > gueltig_von` and `l_monats_date <= gueltig_bis`).

### Action
1. Execute the migrated BigQuery SQL script using the BigQuery Python client, passing `@monats_id = 202608` and `@eintrags_nr = 99999`.
2. Query the target table `dw.dwh_ta_t_smart_kubi` and compare the results against the expected output dataset.

### Code (Pytest + BigQuery Client)

```python
import os
import pytest
from google.cloud import bigquery

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client()

def test_sql_transformation_parity(bq_client):
    dataset_id = os.environ.get("BQ_DATASET", "dw")
    project_id = bq_client.project
    
    # 1. Clean and Populate Source Tables
    setup_queries = [
        f"TRUNCATE TABLE `{project_id}.{dataset_id}.dwh_ta_f_d1_twvv_tn`",
        f"TRUNCATE TABLE `{project_id}.{dataset_id}.dwh_vi_l_map_fa_tarif`",
        f"TRUNCATE TABLE `{project_id}.{dataset_id}.bl_d_tarif`",
        f"TRUNCATE TABLE `{project_id}.{dataset_id}.dwh_ta_c_vertrag`",
        
        # Insert test data into fact table
        f"""
        INSERT INTO `{project_id}.{dataset_id}.dwh_ta_f_d1_twvv_tn` 
        (gueltigkeitszeitpunkt, kennzahl_id, dwh_tarif_id_neu, dwh_tarif_id_alt, dwh_vertrag_id, vo_kenn, vo_kenn_bearb, zugang)
        VALUES
        -- Case 1: Standard mapping, mp_geschaeftsfeld_id = 2 (kundennummer -> '-1')
        (TIMESTAMP('2026-08-10 12:00:00'), 'VVLREIN', 'TAR_NEW_1', 'TAR_OLD_1', 'CON_1', 'VO_A', 'VO_B', 10),
        -- Case 2: Standard mapping, mp_geschaeftsfeld_id != 2 (kundennummer -> contract kundennummer)
        (TIMESTAMP('2026-08-11 12:00:00'), 'VVLTWC2C', 'TAR_NEW_2', 'TAR_OLD_2', 'CON_2', 'VO_A', '   ', 5),
        -- Case 3: vo_kenn_bearb is '#' (should fall back to vo_kenn)
        (TIMESTAMP('2026-08-12 12:00:00'), 'MIGP2CBF', 'TAR_NEW_2', 'TAR_OLD_2', 'CON_2', 'VO_A', '#', 3),
        -- Case 4: Non-matching kennzahl_id (should be filtered out)
        (TIMESTAMP('2026-08-12 12:00:00'), 'INVALID', 'TAR_NEW_2', 'TAR_OLD_2', 'CON_2', 'VO_A', 'VO_B', 100)
        """,
        
        # Insert tariff mappings
        f"""
        INSERT INTO `{project_id}.{dataset_id}.dwh_vi_l_map_fa_tarif` (tarif_id, dwh_tarif_id, gueltig_von, gueltig_bis)
        VALUES
        (101, 'TAR_NEW_1', DATE '2020-01-01', DATE '4712-12-31'),
        (102, 'TAR_OLD_1', DATE '2020-01-01', DATE '4712-12-31'),
        (201, 'TAR_NEW_2', DATE '2020-01-01', DATE '4712-12-31'),
        (202, 'TAR_OLD_2', DATE '2020-01-01', DATE '4712-12-31')
        """,
        
        f"""
        INSERT INTO `{project_id}.{dataset_id}.bl_d_tarif` (tarif_id, mp_geschaeftsfeld_id)
        VALUES
        (101, 2),
        (102, 1),
        (201, 5),
        (202, 5)
        """,
        
        # Insert contracts (gueltig_von/bis must cover l_monats_date = 2026-09-01)
        f"""
        INSERT INTO `{project_id}.{dataset_id}.dwh_ta_c_vertrag` (dwh_vertrag_id, t_mobile_kundennummer, test_gp, gueltig_von, gueltig_bis)
        VALUES
        ('CON_1', 'KUND_1', 'Y', DATE '2026-01-01', DATE '2026-12-31'),
        ('CON_2', 'KUND_2', 'N', DATE '2026-01-01', DATE '2026-12-31')
        """
    ]
    
    for query in setup_queries:
        bq_client.query(query).result()
        
    # 2. Execute Migrated SQL Script
    sql_path = "local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql"
    with open(sql_path, "r") as f:
        sql_script = f.read()
        
    # Replace dataset references if parameterized, or run directly
    query_job = bq_client.query(
        sql_script,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("monats_id", "INT64", 202608),
                bigquery.ScalarQueryParameter("eintrags_nr", "INT64", 99999)
            ]
        )
    )
    query_job.result()
    
    # 3. Assert Output Parity
    result_query = f"SELECT * FROM `{project_id}.{dataset_id}.dwh_ta_t_smart_kubi` ORDER BY anzahl DESC"
    results = list(bq_client.query(result_query).result())
    
    assert len(results) == 3, f"Expected 3 rows in target table, found {len(results)}"
    
    # Row 1: Case 1 (mp_geschaeftsfeld_id = 2 -> kundennummer = '-1')
    assert results[0]["monats_id"] == 202608
    assert results[0]["kundennummer"] == "-1"
    assert results[0]["tarif_id"] == 101
    assert results[0]["tarif_id_alt"] == 102
    assert results[0]["vo_kennung"] == "VO_B"
    assert results[0]["test_gp"] == "Y"
    assert results[0]["anzahl"] == 10
    assert results[0]["kennzahl_id"] == "VVLREIN"

    # Row 2: Case 2 (mp_geschaeftsfeld_id != 2 -> kundennummer = 'KUND_2', vo_kenn_bearb was empty spaces -> 'VO_A')
    assert results[1]["kundennummer"] == "KUND_2"
    assert results[1]["vo_kennung"] == "VO_A"
    assert results[1]["anzahl"] == 5
    assert results[1]["kennzahl_id"] == "VVLTWC2C"

    # Row 3: Case 3 (vo_kenn_bearb was '#' -> 'VO_A')
    assert results[2]["kundennummer"] == "KUND_2"
    assert results[2]["vo_kennung"] == "VO_A"
    assert results[2]["anzahl"] == 3
    assert results[2]["kennzahl_id"] == "MIGP2CBF"
```

### Pass/Fail Criterion
* **Pass**: The target table contains exactly the expected rows, verifying correct join logic, date boundaries, aggregation, and null/empty string handling.
* **Fail**: Row count mismatch, incorrect aggregation values, or failure to map empty strings/special characters correctly.

---

## Test Case 3: Error Handling & Logging Stored Procedure Integration

### Purpose
Prove that the Python execution wrapper (`r_sqlscript.py`) and SQL helper (`h_alis_sqlplus.py`) correctly trap database execution errors and invoke the BigQuery logging stored procedures (`dwpa_meldung_fehler` and `dwpa_meldung_setze_status_abbruch`) with the correct parameters.

### Setup
1. Deploy mock stored procedures in the BigQuery metadata dataset to capture calls:
   * `dwpa_meldung_erzeuge_eintrag`
   * `dwpa_meldung_fehler`
   * `dwpa_meldung_setze_status_abbruch`
2. Create a temporary, intentionally broken SQL script (e.g., referencing a non-existent table).

### Action
1. Run `r_sqlscript.py` passing the broken SQL script.
2. Verify that the script exits with a non-zero code.
3. Query the mock logging tables to verify that the error was logged with `FehlerNr = -20001` and the status was updated to aborted.

### Code (Pytest + Subprocess)

```python
import os
import pytest
import subprocess
from google.cloud import bigquery

def test_error_trapping_and_logging(bq_client):
    dataset_id = os.environ.get("BQ_DATASET", "dwpa_meldung")
    project_id = bq_client.project
    
    # 1. Create a broken SQL script
    broken_sql_path = "local/home/gurunathan_t/kubi/broken_script.sql"
    with open(broken_sql_path, "w") as f:
        f.write("""
        BEGIN
          -- Intentionally referencing a non-existent table to trigger EXCEPTION
          SELECT * FROM `non_existent_dataset.non_existent_table`;
        EXCEPTION WHEN ERROR THEN
          CALL `dwpa_meldung.dwpa_meldung_fehler`('F', @eintrags_nr, -20001, @@error.message, CAST(@@error.code AS STRING));
          ERROR @@error.message;
        END;
        """)
        
    # 2. Clear mock audit log tables
    bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.audit_log`").result()
    
    # 3. Run the wrapper script
    env = os.environ.copy()
    env["GCP_PROJECT"] = project_id
    env["BQ_DATASET"] = dataset_id
    
    result = subprocess.run(
        ["python3", "local/home/gurunathan_t/kubi/r_sqlscript.py", "-f", "broken_script.sql", "-j", "TEST_ERR_JOB"],
        env=env,
        capture_output=True,
        text=True
    )
    
    # Clean up broken script
    if os.path.exists(broken_sql_path):
        os.remove(broken_sql_path)
        
    # Assert wrapper exited with failure
    assert result.returncode != 0, "Wrapper script should have exited with a non-zero code"
    
    # 4. Verify Stored Procedure Calls via Audit Log Table
    # (Assuming the mock stored procedures write to an audit_log table for validation)
    audit_query = f"SELECT * FROM `{project_id}.{dataset_id}.audit_log` ORDER BY timestamp DESC"
    logs = list(bq_client.query(audit_query).result())
    
    assert len(logs) > 0, "No audit logs found. Stored procedures were not called."
    
    # Verify that dwpa_meldung_fehler was called
    error_logs = [log for log in logs if log["procedure_name"] == "dwpa_meldung_fehler"]
    assert len(error_logs) > 0, "dwpa_meldung_fehler was not called"
    assert error_logs[0]["severity"] == "F"
    assert error_logs[0]["fehler_nr"] == -20001
    
    # Verify that status was set to aborted
    abort_logs = [log for log in logs if log["procedure_name"] == "dwpa_meldung_setze_status_abbruch"]
    assert len(abort_logs) > 0, "dwpa_meldung_setze_status_abbruch was not called"
```

### Pass/Fail Criterion
* **Pass**: The wrapper script exits with a non-zero code, calls `dwpa_meldung_fehler` with severity `'F'`, and sets the final execution status to aborted.
* **Fail**: The wrapper script exits with `0`, fails to log the error to BigQuery, or fails to set the status to aborted.

---

## Test Case 4: Schema and Data Quality Assertions

### Purpose
Ensure that the target table `dwh_ta_t_smart_kubi` conforms to strict schema definitions, contains no duplicate records for the same reporting month/customer/tariff combination, and respects non-null constraints.

### Setup
The target table `dwh_ta_t_smart_kubi` has been populated by a successful run of the pipeline.

### Action
Execute data quality and schema validation queries against the BigQuery target table.

### Code (SQL Assertions)

```sql
-- Assertion 1: Verify no NULL values exist in mandatory business keys
SELECT
  'NULL_KEY_ERROR' AS failure_type,
  COUNT(*) AS failure_count
FROM
  `dw.dwh_ta_t_smart_kubi`
WHERE
  monats_id IS NULL
  OR kundennummer IS NULL
  OR tarif_id IS NULL
  OR tarif_id_alt IS NULL
  OR kennzahl_id IS NULL;

-- Assertion 2: Verify uniqueness constraint (monats_id, kundennummer, tarif_id, tarif_id_alt, vo_kennung, kennzahl_id)
SELECT
  'DUPLICATE_RECORD_ERROR' AS failure_type,
  COUNT(*) AS failure_count
FROM (
  SELECT
    monats_id,
    kundennummer,
    tarif_id,
    tarif_id_alt,
    vo_kennung,
    kennzahl_id,
    COUNT(*)
  FROM
    `dw.dwh_ta_t_smart_kubi`
  GROUP BY
    1, 2, 3, 4, 5, 6
  HAVING COUNT(*) > 1
);

-- Assertion 3: Verify data types and logical boundaries
SELECT
  'LOGICAL_VALUE_ERROR' AS failure_type,
  COUNT(*) AS failure_count
FROM
  `dw.dwh_ta_t_smart_kubi`
WHERE
  anzahl <= 0  -- Aggregated counts must be positive
  OR monats_id < 190001 
  OR monats_id > 210012;
```

### Pass/Fail Criterion
* **Pass**: All three assertion queries return a `failure_count` of `0`.
* **Fail**: Any assertion query returns a `failure_count` greater than `0`, indicating data corruption, duplicate loads, or constraint violations.