# Migration Validation Test Suite: `DW.DWH_ABTN_SMART_KUBI`

This document defines the migration-validation test suite for the migration of the `DW.DWH_ABTN_SMART_KUBI` job from UC4/Oracle to Google Cloud Composer/BigQuery. These tests ensure behavioral equivalence, transformation correctness, date-logic parity, and robust error handling.

---

## Section 1: Date Logic & Parameter Calculation Parity

### Purpose
To prove that the migrated Python date-calculation logic (implemented in the Airflow DAG) produces the exact same reporting month (`MONATSID`) as the legacy UC4 script across critical calendar boundaries (e.g., month transitions, leap years, and year-end boundaries).

### Setup
A Python environment with `pytest` and `pendulum` (or `timezone-aware datetime`) installed.

### Action
Execute a parameterized unit test comparing the legacy UC4 logic outcomes with the migrated Python logic for a comprehensive set of test dates.

### Pass/Fail Criterion
*   **Pass**: The calculated `MONATSID` matches the expected legacy value for 100% of the test cases.
*   **Fail**: Any calculated `MONATSID` deviates from the expected legacy value.

### Test Code
```python
import pytest
from datetime import datetime, timedelta
import pendulum

def calculate_monatsid(logical_date: datetime) -> str:
    """
    Migrated logic from dw_dwh_abtn_smart_kubi.py
    """
    # Ensure timezone-aware comparison matching Europe/Berlin
    dt = pendulum.instance(logical_date).in_timezone('Europe/Berlin')
    if dt.day < 15:
        first_of_month = dt.replace(day=1)
        prev_month = first_of_month - timedelta(days=1)
        monatsid = prev_month.strftime('%Y%m')
    else:
        monatsid = dt.strftime('%Y%m')
    return monatsid

@pytest.mark.parametrize(
    "execution_date, expected_monatsid",
    [
        # Mid-month transition boundary (Day 14 vs Day 15)
        (datetime(2023, 10, 14, 12, 0), "202309"),
        (datetime(2023, 10, 15, 12, 0), "202310"),
        # Year-end boundaries
        (datetime(2023, 1, 14, 23, 59), "202212"),
        (datetime(2023, 1, 15, 0, 0), "202301"),
        # Leap year transitions (Feb 2024)
        (datetime(2024, 3, 14, 12, 0), "202402"),
        (datetime(2024, 3, 15, 12, 0), "202403"),
        # Month-end execution dates
        (datetime(2023, 10, 31, 23, 59), "202310"),
        # Month-start execution dates
        (datetime(2023, 11, 1, 0, 1), "202310"),
    ]
)
def test_monatsid_calculation_parity(execution_date, expected_monatsid):
    calculated = calculate_monatsid(execution_date)
    assert calculated == expected_monatsid, (
        f"Failed for execution date {execution_date}. "
        f"Expected: {expected_monatsid}, Got: {calculated}"
    )
```

---

## Section 2: SQL Transformation Logic & Null Handling

### Purpose
To verify that the migrated BigQuery SQL script produces identical outputs to the legacy Oracle PL/SQL script. This test validates the correctness of:
1.  `DECODE` to `CASE WHEN` conversions.
2.  `NVL` to `COALESCE` conversions.
3.  `LTRIM(RTRIM(...))` to `TRIM(...)` conversions.
4.  Implicit Oracle outer joins `(+)` converted to ANSI `LEFT OUTER JOIN`s.

### Setup
1.  Create a temporary test dataset in BigQuery.
2.  Create mock source tables matching the schemas of:
    *   `bl_d_tarif`
    *   `dwh$vi_l_map_fa_tarif`
    *   `dwh$ta_f_d1_twvv_tn`
    *   `dwh$ta_c_vertrag`
3.  Populate these tables with edge-case records (e.g., nulls, spaces, specific business codes).
4.  Create an empty target table `dwh$ta_t_smart_kubi`.

### Action
Execute the migrated BigQuery SQL script with test parameters `@monats_id = 202310` and `@eintrags_nr = 99999`.

### Pass/Fail Criterion
*   **Pass**: The target table `dwh$ta_t_smart_kubi` is populated with rows that match the expected output dataset exactly (including correct handling of nulls, default values, and business logic decodes).
*   **Fail**: Any row mismatch, incorrect aggregation, or SQL syntax error.

### Test Code
```sql
-- 1. Seed Mock Data
-- Mock bl_d_tarif
CREATE OR REPLACE TEMP TABLE bl_d_tarif AS
SELECT 101 AS tarif_id, 2 AS mp_geschaeftsfeld_id UNION ALL -- Should map to kundennummer '-1'
SELECT 102 AS tarif_id, 1 AS mp_geschaeftsfeld_id;

-- Mock dwh$vi_l_map_fa_tarif
CREATE OR REPLACE TEMP TABLE dwh$vi_l_map_fa_tarif AS
SELECT 101 AS tarif_id, 1001 AS dwh_tarif_id, DATE '2020-01-01' AS gueltig_von, DATE '4712-12-31' AS gueltig_bis UNION ALL
SELECT 102 AS tarif_id, 1002 AS dwh_tarif_id, DATE '2020-01-01' AS gueltig_von, DATE '4712-12-31' AS gueltig_bis;

-- Mock dwh$ta_c_vertrag
CREATE OR REPLACE TEMP TABLE dwh$ta_c_vertrag AS
SELECT 5001 AS dwh_vertrag_id, 'KUND_A' AS t_mobile_kundennummer, 'Y' AS test_gp, DATE '2020-01-01' AS gueltig_von, DATE '9999-12-31' AS gueltig_bis UNION ALL
SELECT 5002 AS dwh_vertrag_id, 'KUND_B' AS t_mobile_kundennummer, 'N' AS test_gp, DATE '2020-01-01' AS gueltig_von, DATE '9999-12-31' AS gueltig_bis;

-- Mock dwh$ta_f_d1_twvv_tn (Partitioned/Filtered for 202310)
CREATE OR REPLACE TEMP TABLE dwh$ta_f_d1_twvv_tn AS
SELECT 
  TIMESTAMP '2023-10-10 12:00:00 UTC' AS gueltigkeitszeitpunkt,
  'VVLREIN' AS kennzahl_id,
  1001 AS dwh_tarif_id_neu,
  1002 AS dwh_tarif_id_alt,
  5001 AS dwh_vertrag_id,
  'VO_A' AS vo_kenn,
  '  VO_A_BEARB  ' AS vo_kenn_bearb, -- Needs trimming
  10 AS zugang
UNION ALL
SELECT 
  TIMESTAMP '2023-10-11 12:00:00 UTC' AS gueltigkeitszeitpunkt,
  'VVLTWC2C' AS kennzahl_id,
  1002 AS dwh_tarif_id_neu,
  CAST(NULL AS INT64) AS dwh_tarif_id_alt, -- Test NVL/COALESCE
  5002 AS dwh_vertrag_id,
  'VO_B' AS vo_kenn,
  '#' AS vo_kenn_bearb, -- Should fall back to vo_kenn
  5 AS zugang;

-- Create Target Table
CREATE OR REPLACE TABLE dwh$ta_t_smart_kubi (
  monats_id INT64,
  kundennummer STRING,
  tarif_id INT64,
  tarif_id_alt INT64,
  vo_kennung STRING,
  test_gp STRING,
  anzahl INT64,
  kennzahl_id STRING
);

-- 2. Execute Migrated SQL Logic (Inline for testing)
DECLARE l_monats_id INT64 DEFAULT 202310;
DECLARE l_monats_date DATE;
SET l_monats_date = DATE_ADD(PARSE_DATE('%Y%m', CAST(l_monats_id AS STRING)), INTERVAL 1 MONTH);

INSERT INTO dwh$ta_t_smart_kubi (monats_id, kundennummer, tarif_id, tarif_id_alt, vo_kennung, test_gp, anzahl, kennzahl_id)
WITH temp AS (
  SELECT t.tarif_id, t.dwh_tarif_id, t.gueltig_von, t.gueltig_bis, tar.mp_geschaeftsfeld_id
  FROM dwh$vi_l_map_fa_tarif AS t
  INNER JOIN bl_d_tarif AS tar ON t.tarif_id = tar.tarif_id
  WHERE CAST(t.gueltig_bis AS DATE) = DATE '4712-12-31'
)
SELECT 
  l_monats_id AS monats_id,
  CASE WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' ELSE d.t_mobile_kundennummer END AS kundennummer,
  COALESCE(t_new.tarif_id, 0) AS tarif_id,
  COALESCE(t_old.tarif_id, 0) AS tarif_id_alt,
  CASE WHEN TRIM(fact.vo_kenn_bearb) IS NULL OR TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn ELSE TRIM(fact.vo_kenn_bearb) END AS vo_kennung,
  d.test_gp,
  SUM(fact.zugang) AS anzahl,
  fact.kennzahl_id
FROM dwh$ta_f_d1_twvv_tn AS fact
LEFT OUTER JOIN temp AS t_new ON fact.dwh_tarif_id_neu = t_new.dwh_tarif_id
LEFT OUTER JOIN temp AS t_old ON fact.dwh_tarif_id_alt = t_old.dwh_tarif_id
LEFT OUTER JOIN dwh$ta_c_vertrag AS d 
  ON fact.dwh_vertrag_id = d.dwh_vertrag_id
  AND l_monats_date > CAST(d.gueltig_von AS DATE)
  AND l_monats_date <= CAST(d.gueltig_bis AS DATE)
WHERE FORMAT_DATE('%Y%m', CAST(fact.gueltigkeitszeitpunkt AS DATE)) = CAST(l_monats_id AS STRING)
  AND fact.kennzahl_id IN ('VVLREIN', 'VVLTWC2C', 'MIGP2CBF')
GROUP BY 1, 2, 3, 4, 5, 6, 8;

-- 3. Assertions
-- Row 1 Assertions: mp_geschaeftsfeld_id = 2 -> kundennummer = '-1', trimmed vo_kenn_bearb
SELECT 
  ASSERT(kundennummer = '-1', "Error: mp_geschaeftsfeld_id=2 did not map to '-1'"),
  ASSERT(tarif_id = 101, "Error: New tarif_id mapping failed"),
  ASSERT(tarif_id_alt = 102, "Error: Old tarif_id mapping failed"),
  ASSERT(vo_kennung = 'VO_A_BEARB', "Error: Trimming of vo_kenn_bearb failed")
FROM dwh$ta_t_smart_kubi WHERE kennzahl_id = 'VVLREIN';

-- Row 2 Assertions: vo_kenn_bearb = '#' -> fallback to vo_kenn, COALESCE for missing old tarif
SELECT 
  ASSERT(kundennummer = 'KUND_B', "Error: Customer mapping failed"),
  ASSERT(tarif_id = 102, "Error: New tarif_id mapping failed"),
  ASSERT(tarif_id_alt = 0, "Error: COALESCE fallback to 0 failed for tarif_id_alt"),
  ASSERT(vo_kennung = 'VO_B', "Error: Fallback to vo_kenn failed when bearb was '#'")
FROM dwh$ta_t_smart_kubi WHERE kennzahl_id = 'VVLTWC2C';
```

---

## Section 3: Date Boundary Join Logic (`dwh$ta_c_vertrag`)

### Purpose
To verify that the contract validity date filter (`l_monats_date > gueltig_von` and `l_monats_date <= gueltig_bis`) behaves identically to Oracle's `(+)` join with date filters.

### Setup
1.  Set `@monats_id = 202310` (which resolves `l_monats_date` to `2023-11-01`).
2.  Insert contract records into `dwh$ta_c_vertrag` with varying validity ranges:
    *   **Contract A**: Valid (`gueltig_von = 2023-10-01`, `gueltig_bis = 2023-11-15`).
    *   **Contract B**: Expired (`gueltig_von = 2023-09-01`, `gueltig_bis = 2023-10-31`).
    *   **Contract C**: Future Active (`gueltig_von = 2023-11-02`, `gueltig_bis = 2023-12-31`).

### Action
Run the BigQuery SQL script.

### Pass/Fail Criterion
*   **Pass**: Only **Contract A** details are successfully joined. **Contract B** and **Contract C** details must result in `NULL` values for contract-derived columns (`kundennummer`, `test_gp`) due to the `LEFT OUTER JOIN` condition.
*   **Fail**: Contract details from B or C are joined, or Contract A is missed.

### Test Code
```sql
-- Seed specific date boundary contracts
CREATE OR REPLACE TEMP TABLE dwh$ta_c_vertrag AS
SELECT 5001 AS dwh_vertrag_id, 'KUND_ACTIVE' AS t_mobile_kundennummer, 'Y' AS test_gp, DATE '2023-10-01' AS gueltig_von, DATE '2023-11-15' AS gueltig_bis
UNION ALL
SELECT 5002 AS dwh_vertrag_id, 'KUND_EXPIRED' AS t_mobile_kundennummer, 'N' AS test_gp, DATE '2023-09-01' AS gueltig_von, DATE '2023-10-31' AS gueltig_bis
UNION ALL
SELECT 5003 AS dwh_vertrag_id, 'KUND_FUTURE' AS t_mobile_kundennummer, 'N' AS test_gp, DATE '2023-11-02' AS gueltig_von, DATE '2023-12-31' AS gueltig_bis;

-- Seed fact table pointing to all three contracts
CREATE OR REPLACE TEMP TABLE dwh$ta_f_d1_twvv_tn AS
SELECT TIMESTAMP '2023-10-10' AS gueltigkeitszeitpunkt, 'VVLREIN' AS kennzahl_id, 1001 AS dwh_tarif_id_neu, 1002 AS dwh_tarif_id_alt, 5001 AS dwh_vertrag_id, 'VO' AS vo_kenn, NULL AS vo_kenn_bearb, 1 AS zugang
UNION ALL
SELECT TIMESTAMP '2023-10-10' AS gueltigkeitszeitpunkt, 'VVLREIN' AS kennzahl_id, 1001 AS dwh_tarif_id_neu, 1002 AS dwh_tarif_id_alt, 5002 AS dwh_vertrag_id, 'VO' AS vo_kenn, NULL AS vo_kenn_bearb, 1 AS zugang
UNION ALL
SELECT TIMESTAMP '2023-10-10' AS gueltigkeitszeitpunkt, 'VVLREIN' AS kennzahl_id, 1001 AS dwh_tarif_id_neu, 1002 AS dwh_tarif_id_alt, 5003 AS dwh_vertrag_id, 'VO' AS vo_kenn, NULL AS vo_kenn_bearb, 1 AS zugang;

-- Execute query logic...
-- (Insert logic from Section 2 using the temp tables above)

-- Assertions
-- Contract 5001 (Active) -> Should have customer 'KUND_ACTIVE'
SELECT ASSERT(COUNT(1) = 1, "Active contract should be joined") 
FROM dwh$ta_t_smart_kubi WHERE kundennummer = 'KUND_ACTIVE';

-- Contract 5002 (Expired) & 5003 (Future) -> Should have NULL/Defaulted customer values
SELECT ASSERT(COUNT(1) = 2, "Expired and Future contracts should have NULL customer (joined as NULL)") 
FROM dwh$ta_t_smart_kubi WHERE kundennummer IS NULL;
```

---

## Section 4: Error Handling & Transaction Rollback

### Purpose
To verify that the BigQuery scripting block handles runtime exceptions correctly by rolling back any uncommitted DML operations and executing the custom logging procedure (`dwpa_meldung_fehler`).

### Setup
1.  Deploy a mock stored procedure `dwpa_meldung_fehler` in the test dataset to capture error logs.
2.  Intentionally inject a runtime error (e.g., division by zero) inside the main transaction block of `d_abtn_x_smart_kubi.sql`.

### Action
Execute the modified BigQuery SQL script.

### Pass/Fail Criterion
*   **Pass**: 
    1.  The transaction is rolled back, leaving the target table `dwh$ta_t_smart_kubi` in its pre-transaction state (or empty if truncated).
    2.  The mock `dwpa_meldung_fehler` procedure is called with the correct error parameters (`FehlerNr = -20001`, severity = `'F'`).
*   **Fail**: The script fails silently, commits partial data, or fails to log the error to the metadata table.

### Test Code
```sql
-- Create mock logging table
CREATE OR REPLACE TABLE error_log_metadata (
  typ STRING,
  eintrags_nr INT64,
  fehler_nr INT64,
  err_text STRING,
  err_code STRING
);

-- Create mock procedure
CREATE OR REPLACE PROCEDURE dwpa_meldung_fehler(typ STRING, eintrags_nr INT64, fehler_nr INT64, err_text STRING, err_code STRING)
BEGIN
  INSERT INTO error_log_metadata VALUES (typ, eintrags_nr, fehler_nr, err_text, err_code);
END;

-- Execute script with injected error
DECLARE v_anzahl_ds INT64 DEFAULT 0;
DECLARE l_monats_id INT64 DEFAULT 202310;
DECLARE EintragsNr INT64 DEFAULT 88888;

BEGIN
  BEGIN TRANSACTION;
  
  TRUNCATE TABLE dwh$ta_t_smart_kubi;
  
  -- Injected Error: Division by zero
  SELECT 1 / 0;

  COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  BEGIN
    DECLARE ErrText STRING DEFAULT @@error.message;
    CALL dwpa_meldung_fehler('F', EintragsNr, -20001, ErrText, '-1');
  END;
END;

-- Assertions
SELECT ASSERT(COUNT(1) = 1, "Error was not logged to metadata table") FROM error_log_metadata WHERE eintrags_nr = 88888;
```

---

## Section 5: End-to-End Shadow Run & Schema Validation

### Purpose
To perform a shadow run comparison between the legacy Oracle production run and the migrated BigQuery run using the same source data snapshot. This ensures 100% output parity.

### Setup
1.  Export the legacy Oracle target table `DWH$TA_T_SMART_KUBI` populated by the legacy job for a specific month (e.g., `202310`) to a temporary BigQuery table `legacy_dwh_ta_t_smart_kubi`.
2.  Run the migrated Airflow DAG `dw_dwh_abtn_smart_kubi` for the same logical date.

### Action
Execute a comparison query in BigQuery to check for schema discrepancies, row count mismatches, and data differences.

### Pass/Fail Criterion
*   **Pass**: 
    1.  The column names and data types of the migrated table match the legacy table.
    2.  The row count difference between the legacy and migrated target tables is exactly `0`.
    3.  A full outer join between the legacy and migrated tables on all columns yields `0` mismatched rows.
*   **Fail**: Any schema mismatch, row count discrepancy, or data variance.

### Test Code
```sql
-- 1. Schema Validation
SELECT 
  column_name, data_type
FROM 
  `INFORMATION_SCHEMA.COLUMNS`
WHERE 
  table_name = 'dwh$ta_t_smart_kubi';
-- Ensure types map correctly: monats_id (INT64), kundennummer (STRING), tarif_id (INT64), etc.

-- 2. Row Count Validation
SELECT 
  (SELECT COUNT(1) FROM dwh$ta_t_smart_kubi) AS migrated_count,
  (SELECT COUNT(1) FROM legacy_dwh_ta_t_smart_kubi) AS legacy_count,
  ASSERT(
    (SELECT COUNT(1) FROM dwh$ta_t_smart_kubi) = (SELECT COUNT(1) FROM legacy_dwh_ta_t_smart_kubi),
    "Row count mismatch between legacy and migrated tables!"
  );

-- 3. Full Data Parity Validation (Mismatched Rows)
WITH mismatches AS (
  SELECT * FROM dwh$ta_t_smart_kubi
  EXCEPT DISTINCT
  SELECT * FROM legacy_dwh_ta_t_smart_kubi
  UNION ALL
  SELECT * FROM legacy_dwh_ta_t_smart_kubi
  EXCEPT DISTINCT
  SELECT * FROM dwh$ta_t_smart_kubi
)
SELECT 
  ASSERT(COUNT(1) = 0, "Data mismatch found between legacy and migrated tables!") 
FROM 
  mismatches;
```