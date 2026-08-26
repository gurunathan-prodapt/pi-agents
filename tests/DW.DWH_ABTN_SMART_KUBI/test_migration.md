An elegant, production-grade test suite is detailed below to validate the migration of the **DW.DWH_ABTN_SMART_KUBI** pipeline from UC4/Oracle to Apache Airflow/BigQuery. 

These tests are designed to be executed by a QA automation framework (such as `pytest` combined with the BigQuery client SDK) to guarantee functional, behavioral, and data-level equivalence.

---

# Test Suite Overview: DW.DWH_ABTN_SMART_KUBI Migration Validation

The validation strategy is divided into five key testing areas:
1. **Date Logic Validation (`MONATSID` Calculation)**: Verifies the boundary conditions of the reporting month logic.
2. **End-to-End Output Parity (Reconciliation)**: Compares Oracle legacy outputs with BigQuery target outputs using identical inputs.
3. **Transformation Correctness (Edge Cases)**: Validates `DECODE`, `NVL`/`COALESCE`, `TRIM`, and outer join logic.
4. **Idempotency & Truncation**: Ensures the target table is safely truncated before reloading.
5. **Schema & Data Quality Assertions**: Enforces structural integrity and column-level constraints.

---

## Section 1: Date Logic Validation (`MONATSID` Calculation)

### Purpose
To verify that the dynamic calculation of `MONATSID` (reporting month) in the migrated Airflow DAG matches the legacy UC4 script logic exactly, specifically testing the boundary conditions around the 15th day of the month.

### Setup
A Python environment with `pytest` and `apache-airflow` installed. We will unit-test the Jinja template expression used in the migrated DAG:
`{{ (logical_date - macros.timedelta(days=15)).strftime('%Y%m') }}`

### Action
Execute a test script that evaluates the Jinja expression against various mock execution dates (`logical_date`).

```python
import pytest
from datetime import datetime
from jinja2 import Environment, DebugUndefined

def evaluate_monatsid(logical_date_str: str) -> str:
    # Emulate Airflow's Jinja context evaluation
    logical_date = datetime.strptime(logical_date_str, "%Y-%m-%d")
    # Emulate: (logical_date - timedelta(days=15)).strftime('%Y%m')
    calculated_date = logical_date - datetime.timedelta(days=15)
    return calculated_date.strftime("%Y%m")

@pytest.mark.parametrize(
    "execution_date, expected_monatsid",
    [
        # Boundary Case 1: Run on the 14th of the month -> Should yield previous month
        ("2023-10-14", "202309"),
        ("2023-01-14", "202212"),  # Year boundary
        # Boundary Case 2: Run on the 15th of the month -> Should yield current month
        ("2023-10-15", "202310"),
        ("2023-01-15", "202301"),
        # General Cases
        ("2023-10-01", "202309"),
        ("2023-10-31", "202310"),
    ]
)
def test_monatsid_calculation_parity(execution_date, expected_monatsid):
    actual_monatsid = evaluate_monatsid(execution_date)
    assert actual_monatsid == expected_monatsid, (
        f"Failed for execution date {execution_date}. "
        f"Expected {expected_monatsid}, got {actual_monatsid}"
    )
```

### Pass/Fail Criterion
* **Pass**: The calculated `MONATSID` matches the expected value across all boundary dates (14th vs. 15th, and year-end transitions).
* **Fail**: Any calculated value deviates from the legacy logic.

---

## Section 2: End-to-End Output Parity (Reconciliation)

### Purpose
To prove that running the migrated BigQuery SQL script with a controlled input dataset yields the exact same output rows as the legacy Oracle PL/SQL block.

### Setup
1. Create a sandboxed dataset in both Oracle (Legacy) and BigQuery (Target).
2. Populate the source tables (`dwh$vi_l_map_fa_tarif`, `bl_d_tarif`, `dwh$ta_f_d1_twvv_tn`, and `dwh$ta_c_vertrag`) with identical, deterministic test records for `MONATSID = 202309`.
3. Ensure the target tables (`dwh$ta_t_smart_kubi`) are empty in both environments before execution.

### Action
1. Execute the legacy PL/SQL block in Oracle passing parameters `202309` and `99999`.
2. Execute the migrated BigQuery SQL script in GCP passing parameters `l_monats_id = 202309` and `EintragsNr = 99999`.
3. Extract the contents of both target tables and run a full outer join comparison.

```python
import os
from google.cloud import bigquery
import oracledb
import pytest

def test_reconciliation_oracle_vs_bigquery():
    # 1. Connect to Oracle and fetch legacy target data
    oracle_conn = oracledb.connect(os.environ["ORACLE_CONN_STR"])
    oracle_cursor = oracle_conn.cursor()
    oracle_cursor.execute("""
        SELECT monats_id, kundennummer, tarif_id, tarif_id_alt, vo_kennung, test_gp, anzahl, kennzahl_id 
        FROM dwh$ta_t_smart_kubi 
        ORDER BY kundennummer, tarif_id, tarif_id_alt, vo_kennung, kennzahl_id
    """)
    oracle_rows = oracle_cursor.fetchall()
    oracle_conn.close()

    # 2. Connect to BigQuery and fetch migrated target data
    bq_client = bigquery.Client()
    bq_query = """
        SELECT monats_id, kundennummer, tarif_id, tarif_id_alt, vo_kennung, test_gp, anzahl, kennzahl_id 
        FROM `dwh.dwh$ta_t_smart_kubi`
        ORDER BY kundennummer, tarif_id, tarif_id_alt, vo_kennung, kennzahl_id
    """
    bq_rows = [list(row.values()) for row in bq_client.query(bq_query).result()]

    # 3. Assert exact row count and value parity
    assert len(oracle_rows) == len(bq_rows), f"Row count mismatch! Oracle: {len(oracle_rows)}, BQ: {len(bq_rows)}"
    
    for idx, (o_row, bq_row) in enumerate(zip(oracle_rows, bq_rows)):
        # Convert Decimals/Floats to standard types for comparison
        o_row_normalized = [int(val) if isinstance(val, float) else val for val in o_row]
        bq_row_normalized = [int(val) if isinstance(val, float) else val for val in bq_row]
        
        assert o_row_normalized == bq_row_normalized, (
            f"Row mismatch at index {idx}!\n"
            f"Oracle: {o_row_normalized}\n"
            f"BigQuery: {bq_row_normalized}"
        )
```

### Pass/Fail Criterion
* **Pass**: The row counts are identical, and every column value matches exactly between the legacy and migrated target tables.
* **Fail**: Any row count mismatch or value discrepancy is detected.

---

## Section 3: Transformation Correctness (Edge Cases)

### Purpose
To verify that the complex conditional logic (`DECODE`, `NVL`/`COALESCE`, `TRIM`, and outer joins) behaves identically in BigQuery compared to the legacy Oracle implementation.

### Setup
Populate the BigQuery source tables with specific edge-case records:
* **Edge Case A**: `mp_geschaeftsfeld_id = 2` (Should map `kundennummer` to `'-1'`).
* **Edge Case B**: `mp_geschaeftsfeld_id = 3` (Should map `kundennummer` to `d.t_mobile_kundennummer`).
* **Edge Case C**: `vo_kenn_bearb` is `NULL` or `'#'` (Should fall back to `vo_kenn`).
* **Edge Case D**: `vo_kenn_bearb` has leading/trailing spaces (Should be trimmed).
* **Edge Case E**: Unmatched outer joins on `t_new` and `t_old` (Should default `tarif_id` and `tarif_id_alt` to `0`).

### Action
Run the BigQuery SQL script and execute validation queries to assert correct transformations.

```sql
-- Test Query to validate Edge Cases in BigQuery Target Table
SELECT
  -- Assert Edge Case A & B
  CASE 
    WHEN kundennummer = '-1' THEN 'PASS_GESCHAEFTSFELD_2'
    WHEN kundennummer = 'CUST_12345' THEN 'PASS_GESCHAEFTSFELD_OTHER'
    ELSE 'FAIL_KUNDENNUMMER'
  END AS kundennummer_test,

  -- Assert Edge Case C & D
  CASE 
    WHEN vo_kennung = 'VO_FALLBACK' THEN 'PASS_VO_NULL_OR_HASH'
    WHEN vo_kennung = 'VO_CLEAN' THEN 'PASS_VO_TRIMMED'
    ELSE 'FAIL_VO_KENNUNG'
  END AS vo_kennung_test,

  -- Assert Edge Case E
  CASE 
    WHEN tarif_id = 0 AND tarif_id_alt = 0 THEN 'PASS_OUTER_JOIN_NULLS'
    ELSE 'FAIL_OUTER_JOIN'
  END AS outer_join_test
FROM `dwh.dwh$ta_t_smart_kubi`
WHERE monats_id = 202309;
```

### Pass/Fail Criterion
* **Pass**: All test columns return `'PASS_*'` values, proving that the `CASE WHEN` and `COALESCE` statements are behaviorally equivalent to Oracle's `DECODE` and `NVL`.
* **Fail**: Any test column returns a `'FAIL_*'` value.

---

## Section 4: Idempotency & Truncation Validation

### Purpose
To ensure that the target table `dwh.dwh$ta_t_smart_kubi` is completely truncated before the insert operation occurs, preventing duplicate data on pipeline reruns.

### Setup
1. Manually insert 5 dummy "poison" records into `dwh.dwh$ta_t_smart_kubi` with a distinct `monats_id = 999999`.
2. Ensure source tables contain valid data for `MONATSID = 202309`.

### Action
1. Execute the migrated BigQuery SQL script with `l_monats_id = 202309`.
2. Query the target table to check if the poison records still exist.

```python
def test_idempotency_and_truncation(bq_client):
    # 1. Insert poison records
    poison_query = """
        INSERT INTO `dwh.dwh$ta_t_smart_kubi` (monats_id, kundennummer, tarif_id, tarif_id_alt, vo_kennung, test_gp, anzahl, kennzahl_id)
        VALUES (999999, 'POISON_CUST', 999, 999, 'POISON_VO', 'Y', 10, 'VVLREIN')
    """
    bq_client.query(poison_query).result()

    # Verify poison record is there
    assert get_row_count(bq_client, "monats_id = 999999") == 1

    # 2. Run the migrated SQL script (which contains the TRUNCATE statement)
    run_migrated_script(bq_client, monats_id=202309)

    # 3. Assert that poison records are gone (Truncate worked)
    remaining_poison_count = get_row_count(bq_client, "monats_id = 999999")
    assert remaining_poison_count == 0, "Truncate failed! Poison records still exist."

    # 4. Assert that new records are loaded
    new_records_count = get_row_count(bq_client, "monats_id = 202309")
    assert new_records_count > 0, "Insert failed! No records loaded after truncate."

def get_row_count(client, condition):
    query = f"SELECT COUNT(1) as cnt FROM `dwh.dwh$ta_t_smart_kubi` WHERE {condition}"
    result = client.query(query).result()
    return list(result)[0].cnt
```

### Pass/Fail Criterion
* **Pass**: The poison records are completely removed (count = 0), and the target table only contains the newly processed records for the active reporting month.
* **Fail**: Poison records persist after execution, indicating the `TRUNCATE` step was bypassed or failed.

---

## Section 5: Schema and Data Quality Assertions

### Purpose
To enforce structural integrity, schema compliance, and basic data quality constraints on the target BigQuery table after migration.

### Setup
The target table `dwh.dwh$ta_t_smart_kubi` has been populated by a test run.

### Action
Execute structural and data quality validation queries against the BigQuery Information Schema and the target table.

```sql
-- DQ Assertion 1: Verify Column Data Types match the design specification
SELECT 
  column_name, 
  data_type,
  is_nullable
FROM `dwh.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'dwh$ta_t_smart_kubi'
  AND column_name IN ('monats_id', 'kundennummer', 'tarif_id', 'tarif_id_alt', 'vo_kennung', 'test_gp', 'anzahl', 'kennzahl_id');

-- DQ Assertion 2: Check for Null Violations in key fields
SELECT 
  COUNTIF(monats_id IS NULL) AS err_null_monats_id,
  COUNTIF(kundennummer IS NULL) AS err_null_kundennummer,
  COUNTIF(tarif_id IS NULL) AS err_null_tarif_id,
  COUNTIF(anzahl IS NULL) AS err_null_anzahl
FROM `dwh.dwh$ta_t_smart_kubi`;

-- DQ Assertion 3: Check for logical anomalies (e.g., negative counts)
SELECT 
  COUNTIF(anzahl < 0) AS err_negative_anzahl
FROM `dwh.dwh$ta_t_smart_kubi`;
```

### Pass/Fail Criterion
* **Pass**: 
  * Column types match exactly: `monats_id` (INT64), `kundennummer` (STRING), `tarif_id` (INT64), `tarif_id_alt` (INT64), `vo_kennung` (STRING), `test_gp` (STRING), `anzahl` (INT64), `kennzahl_id` (STRING).
  * Null violation counts are all `0`.
  * Negative count anomalies are `0`.
* **Fail**: Any schema mismatch is detected, or any data quality error count is greater than `0`.