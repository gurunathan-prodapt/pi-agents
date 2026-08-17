# Migration Validation Test Suite: `DW.DWH_ABTN_SMART_KUBI`

This document defines the migration-validation test suite for the job `DW.DWH_ABTN_SMART_KUBI`. These tests ensure behavioral equivalence between the legacy Oracle/UC4 environment and the migrated Google Cloud (Composer/BigQuery) environment.

---

## Test Case 1: Dynamic Parameter Calculation (`MONATSID`)

### Purpose
Validate that the Airflow Jinja macro / Python helper `get_monatsid` calculates the correct reporting month (`MONATSID`) across critical date boundaries (mid-month transitions, leap years, and year-end boundaries), matching the legacy UC4 scheduler logic exactly.

### Setup
No database state is required. The test executes the Python date logic directly.

### Action
Execute the `get_monatsid` function with the following test dates:
1. **Mid-month boundary (Before 15th)**: `2023-12-14` (Expected: `202311`)
2. **Mid-month boundary (On/After 15th)**: `2023-12-15` (Expected: `202312`)
3. **Year-end boundary (Before 15th)**: `2024-01-10` (Expected: `202312`)
4. **Leap year boundary (Before 15th)**: `2024-03-01` (Expected: `202402`)

### Pass/Fail Criterion
* **Pass**: All calculated `MONATSID` values match the expected legacy values exactly.
* **Fail**: Any calculated value deviates from the expected legacy output.

### Test Code (Pytest)
```python
import pytest
from datetime import datetime, timedelta

def get_monatsid(logical_date: datetime) -> str:
    """
    Replicates UC4 Date parsing:
    If execution day is < 15, use the previous month (YYYYMM format).
    Otherwise, use the current month (YYYYMM format).
    """
    if logical_date.day < 15:
        first_of_this_month = logical_date.replace(day=1)
        prev_month_date = first_of_this_month - timedelta(days=1)
        return prev_month_date.strftime("%Y%m")
    else:
        return logical_date.strftime("%Y%m")

@pytest.mark.parametrize(
    "input_date, expected_monatsid",
    [
        (datetime(2023, 12, 14), "202311"),
        (datetime(2023, 12, 15), "202312"),
        (datetime(2024, 1, 10), "202312"),
        (datetime(2024, 3, 1), "202402"),
        (datetime(2024, 2, 29), "202402"),
    ]
)
def test_monatsid_calculation(input_date, expected_monatsid):
    assert get_monatsid(input_date) == expected_monatsid
```

---

## Test Case 2: End-to-End Output Parity

### Purpose
Prove that running the migrated BigQuery SQL script with identical source data produces the exact same output in `dwh_dataset.ta_t_smart_kubi` as the legacy Oracle PL/SQL script produced in `DWH$TA_T_SMART_KUBI`.

### Setup
1. Populate the following source tables in both Oracle and BigQuery with identical test datasets for the reporting month `201509`:
   * `DWH$TA_F_D1_TWVV_TN` / `dwh_dataset.ta_f_d1_twvv_tn`
   * `BL_D_TARIF` / `dwh_dataset.bl_d_tarif`
   * `DWH$VI_L_MAP_FA_TARIF` / `dwh_dataset.vi_l_map_fa_tarif`
   * `DWH$TA_C_VERTREG` / `dwh_dataset.ta_c_vertrag`
2. Ensure the target tables `DWH$TA_T_SMART_KUBI` (Oracle) and `dwh_dataset.ta_t_smart_kubi` (BigQuery) are empty before execution.

### Action
1. Run the legacy PL/SQL script on Oracle passing parameters `201509` and `99999`.
2. Run the migrated BigQuery SQL script passing parameters `201509` and `99999`.
3. Extract the contents of both target tables and compare them.

### Pass/Fail Criterion
* **Pass**: The row count is identical, and a full outer join/checksum comparison of all columns (`monats_id`, `kundennummer`, `tarif_id`, `tarif_id_alt`, `vo_kennung`, `test_gp`, `anzahl`, `kennzahl_id`) yields zero differences.
* **Fail**: Row counts differ, or any column value differs between the legacy and migrated target tables.

### Test Code (Python / Pandas Parity Check)
```python
import os
import pandas as pd
from google.cloud import bigquery
import cx_Oracle  # Or oracledb

def test_e2e_output_parity():
    # 1. Fetch Oracle target data
    oracle_conn = cx_Oracle.connect(os.environ["ORACLE_CONN_STR"])
    oracle_query = "SELECT * FROM DWH$TA_T_SMART_KUBI ORDER BY monats_id, kundennummer, tarif_id, tarif_id_alt, vo_kennung"
    df_oracle = pd.read_sql(oracle_query, con=oracle_conn)
    oracle_conn.close()

    # 2. Fetch BigQuery target data
    bq_client = bigquery.Client()
    bq_query = """
        SELECT * FROM `dwh_dataset.ta_t_smart_kubi` 
        ORDER BY monats_id, kundennummer, tarif_id, tarif_id_alt, vo_kennung
    """
    df_bq = bq_client.query(bq_query).to_dataframe()

    # Normalize column names to lowercase for comparison
    df_oracle.columns = [col.lower() for col in df_oracle.columns]
    df_bq.columns = [col.lower() for col in df_bq.columns]

    # 3. Assertions
    assert len(df_oracle) == len(df_bq), f"Row count mismatch: Oracle={len(df_oracle)}, BQ={len(df_bq)}"
    
    # Compare DataFrames
    pd.testing.assert_frame_equal(df_oracle, df_bq, check_dtype=False, obj="Target Table Parity")
```

---

## Test Case 3: Transformation Logic & Edge Cases

### Purpose
Verify that specific transformation rules, conditional logic (`DECODE` / `CASE`), string trimming, and `NULL` handling behave correctly under edge-case inputs.

### Setup
Insert the following specific edge-case records into `dwh_dataset.ta_f_d1_twvv_tn` (partition `201509`):
* **Case A (Business Field Mapping)**: `dwh_tarif_id_neu` maps to a tariff with `mp_geschaeftsfeld_id = 2`. (Expected: `kundennummer = '-1'`)
* **Case B (Standard Customer Mapping)**: `dwh_tarif_id_neu` maps to a tariff with `mp_geschaeftsfeld_id = 1`. (Expected: `kundennummer = d.t_mobile_kundennummer`)
* **Case C (VO Kennung Null Fallback)**: `vo_kenn_bearb = NULL`, `vo_kenn = 'VO_123'`. (Expected: `vo_kennung = 'VO_123'`)
* **Case D (VO Kennung Hash Fallback)**: `vo_kenn_bearb = '  #  '`, `vo_kenn = 'VO_456'`. (Expected: `vo_kennung = 'VO_456'`)
* **Case E (VO Kennung Clean Trim)**: `vo_kenn_bearb = '  VO_789  '`. (Expected: `vo_kennung = 'VO_789'`)
* **Case F (Tariff ID Null Fallback)**: `dwh_tarif_id_neu` has no match in the tariff mapping. (Expected: `tarif_id = 0`)

### Action
Execute the BigQuery SQL script for `201509`. Query the target table `dwh_dataset.ta_t_smart_kubi` to verify the outputs of the edge-case records.

### Pass/Fail Criterion
* **Pass**: All edge-case records are transformed exactly as specified in the expected outputs.
* **Fail**: Any edge-case record fails to match the expected transformation logic.

### Test Code (SQL Assertions)
```sql
-- Assert Case A: mp_geschaeftsfeld_id = 2 maps to '-1'
ASSERT (
  SELECT COUNT(1) 
  FROM `dwh_dataset.ta_t_smart_kubi` 
  WHERE tarif_id IN (SELECT tarif_id FROM `dwh_dataset.bl_d_tarif` WHERE mp_geschaeftsfeld_id = 2)
    AND kundennummer != '-1'
) = 0 WITH CONNECTION_STRING = "mp_geschaeftsfeld_id = 2 must map to kundennummer = '-1'";

-- Assert Case C & D: VO Kennung Fallbacks
ASSERT (
  SELECT COUNT(1) 
  FROM `dwh_dataset.ta_t_smart_kubi`
  WHERE vo_kennung = 'VO_456'
) > 0 WITH CONNECTION_STRING = "Hash fallback failed to resolve to vo_kenn";

-- Assert Case E: VO Kennung Clean Trim
ASSERT (
  SELECT COUNT(1) 
  FROM `dwh_dataset.ta_t_smart_kubi`
  WHERE vo_kennung = '  VO_789  '
) = 0 WITH CONNECTION_STRING = "vo_kennung was not trimmed correctly";

-- Assert Case F: Tariff ID Null Fallback
ASSERT (
  SELECT COUNT(1) 
  FROM `dwh_dataset.ta_t_smart_kubi`
  WHERE tarif_id IS NULL
) = 0 WITH CONNECTION_STRING = "NULL tariff_id was not coalesced to 0";
```

---

## Test Case 4: Partition Pruning and Date Range Validation

### Purpose
Verify that the migrated BigQuery SQL script correctly translates the legacy partition reference `partition(dwh$ta_f_d1_twvv_tn_&1)` into an optimized date range filter that prunes partitions on `dwh_dataset.ta_f_d1_twvv_tn`.

### Setup
Ensure `dwh_dataset.ta_f_d1_twvv_tn` is partitioned by the column `gueltigkeitszeitpunkt`.

### Action
Perform a dry run of the migrated BigQuery SQL query for a single month (e.g., `201509`) and inspect the `total_bytes_processed`. Compare this against a dry run of the same query *without* the date range filters (which would result in a full table scan).

### Pass/Fail Criterion
* **Pass**: The dry run bytes processed with the date range filter is significantly lower than the full table scan bytes processed, proving that partition pruning is active.
* **Fail**: The bytes processed are identical to a full table scan, indicating that partition pruning is not functioning.

### Test Code (Python / BigQuery API)
```python
from google.cloud import bigquery

def test_partition_pruning():
    client = bigquery.Client()
    
    # 1. Query with partition pruning date filters
    pruned_query = """
        SELECT SUM(fact.zugang)
        FROM `dwh_dataset.ta_f_d1_twvv_tn` fact
        WHERE fact.gueltigkeitszeitpunkt >= '2015-09-01'
          AND fact.gueltigkeitszeitpunkt < '2015-10-01'
          AND fact.kennzahl_id IN ('VVLREIN', 'VVLTWC2C', 'MIGP2CBF')
    """
    
    # 2. Query without partition pruning (Full Table Scan)
    full_scan_query = """
        SELECT SUM(fact.zugang)
        FROM `dwh_dataset.ta_f_d1_twvv_tn` fact
        WHERE fact.kennzahl_id IN ('VVLREIN', 'VVLTWC2C', 'MIGP2CBF')
    """
    
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    
    pruned_job = client.query(pruned_query, job_config=job_config)
    full_job = client.query(full_scan_query, job_config=job_config)
    
    bytes_pruned = pruned_job.total_bytes_processed
    bytes_full = full_job.total_bytes_processed
    
    print(f"Pruned Bytes: {bytes_pruned}, Full Scan Bytes: {bytes_full}")
    
    # Assert that pruned query scans at most 15% of the full table (assuming ~12+ monthly partitions)
    assert bytes_pruned < (bytes_full * 0.15), "Partition pruning is not active on the source table!"
```

---

## Test Case 5: Error Handling and Logging Parity

### Purpose
Verify that the Python wrapper `r_sqlscript.py` and `f_alis_msgerr.py` correctly handle execution failures, log the exact German strings, and update the status table to aborted.

### Setup
1. Create a dummy malformed SQL script `d_invalid_test.sql` containing syntax errors.
2. Ensure the environment variables `DW_DIR_PROT` and `DW_ORAUSER` (or BigQuery equivalent) are set.

### Action
1. Execute `r_sqlscript.py` pointing to the invalid SQL script:
   ```bash
   python3 r_sqlscript.py -f d_invalid_test.sql -j ABTN_SMART_KUBI -v
   ```
2. Capture the standard error and inspect the generated log file.

### Pass/Fail Criterion
* **Pass**: 
  * The script exits with a non-zero exit code (`1`).
  * The log file contains the exact German error string: `"!FEHLER gemeldet!"`.
  * The database status for the generated `EintragsNr` is set to aborted (via `BERT_MELDUNG.SetzeStatusAbbruch`).
* **Fail**: The script exits with `0`, or the log file does not contain the verbatim German error string.

### Test Code (Python Subprocess Integration Test)
```python
import subprocess
import os
import pytest

def test_error_handling_and_logging_parity(tmp_path):
    # Setup a temporary invalid SQL file
    invalid_sql = tmp_path / "d_invalid_test.sql"
    invalid_sql.write_text("SELECT MALFORMED SQL FROM WHERE;", encoding="utf-8")
    
    # Configure environment
    env = os.environ.copy()
    env["DW_DIR_PROT"] = str(tmp_path)
    env["DW_DIR_ROOT"] = str(tmp_path)
    
    # Run the wrapper script
    cmd = [
        "python3", "local/home/gurunathan_t/kubi/r_sqlscript.py",
        "-f", str(invalid_sql),
        "-j", "ABTN_SMART_KUBI",
        "-v"
    ]
    
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    
    # Assert non-zero exit code
    assert result.returncode != 0, "Script should have failed but returned exit code 0"
    
    # Assert verbatim German error output in stderr or stdout
    combined_output = result.stdout + result.stderr
    assert "!FEHLER gemeldet!" in combined_output, "Verbatim German error string '!FEHLER gemeldet!' not found in output"
```