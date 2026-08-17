# Migration Validation Test Suite: DW.DWH_ABTN_SMART_KUBI

This document defines the migration-validation test suite to prove behavioral equivalence between the legacy UC4/Oracle workflow and the migrated Airflow/BigQuery pipeline for **DW.DWH_ABTN_SMART_KUBI**.

---

## Section 1: Orchestration & Parameter Calculation Parity

### Test Case 1.1: Dynamic `MONATSID` Calculation Boundary Test
#### Purpose
Verify that the Airflow Python logic calculates the reporting month ID (`MONATSID`) identically to the legacy UC4 script across all critical date boundaries (month start, day before 15th, 15th, day after 15th, leap years, and year transitions).

#### Setup
A local Python environment with `pytest` and `pendulum` installed.

#### Action
Execute a unit test suite that mocks the Airflow `logical_date` context and asserts the output of the `calculate_and_log_monatsid` function against a pre-calculated matrix of expected legacy outputs.

```python
import pytest
import pendulum
from datetime import timedelta

def calculate_monatsid(logical_date):
    """Implementation under test from the migrated DAG."""
    if logical_date.day < 15:
        first_of_month = logical_date.replace(day=1)
        prev_month = first_of_month - timedelta(days=1)
        monatsid = prev_month.strftime('%Y%m')
    else:
        monatsid = logical_date.strftime('%Y%m')
    return monatsid

@pytest.mark.parametrize(
    "execution_date, expected_monatsid",
    [
        # Mid-month transition boundaries
        ("2023-06-14T12:00:00Z", "202305"),  # Day before 15th -> Previous month
        ("2023-06-15T00:00:00Z", "202306"),  # On the 15th -> Current month
        ("2023-06-16T00:00:00Z", "202306"),  # Day after 15th -> Current month
        
        # Month start and end boundaries
        ("2023-06-01T00:00:00Z", "202305"),  # First day of month -> Previous month
        ("2023-06-30T23:59:59Z", "202306"),  # Last day of month -> Current month
        
        # Year transition boundaries
        ("2023-01-14T10:00:00Z", "202212"),  # Jan before 15th -> Dec of previous year
        ("2023-01-15T10:00:00Z", "202301"),  # Jan on/after 15th -> Jan of current year
        
        # Leap year boundary
        ("2024-03-14T00:00:00Z", "202402"),  # Leap year March 14 -> Feb 2024
        ("2024-03-15T00:00:00Z", "202403"),  # Leap year March 15 -> March 2024
    ]
)
def test_monatsid_calculation_parity(execution_date, expected_monatsid):
    logical_date = pendulum.parse(execution_date)
    calculated_id = calculate_monatsid(logical_date)
    assert calculated_id == expected_monatsid, \
        f"Failed for execution date {execution_date}. Expected {expected_monatsid}, got {calculated_id}"
```

#### Pass/Fail Criterion
* **Pass**: All test cases in the parameter matrix match the expected legacy outputs exactly.
* **Fail**: Any calculated `MONATSID` deviates from the expected legacy value.

---

## Section 2: SQL Transformation & Logic Parity

### Test Case 2.1: Oracle `(+)` Outer Join to BigQuery `LEFT JOIN` Parity
#### Purpose
Verify that the migrated BigQuery SQL correctly translates Oracle's proprietary outer join syntax `(+)` combined with date range checks, ensuring no rows are incorrectly filtered out.

#### Setup
1. Create temporary test tables in BigQuery:
   * `dwh_ta_f_d1_twvv_tn_test`
   * `dwh_ta_c_vertrag_test`
2. Populate them with a controlled set of records where some contracts match the date range, some fall outside, and some have no matching contract ID at all.

#### Action
Execute the following validation query in BigQuery to compare the behavior of the migrated join logic against expected logical outcomes.

```sql
-- Create mock data representing the source tables
WITH fact AS (
  SELECT 'VVLREIN' AS kennzahl_id, 100 AS dwh_vertrag_id, DATE '2023-06-15' AS gueltigkeitszeitpunkt, 10 AS zugang UNION ALL
  SELECT 'VVLREIN' AS kennzahl_id, 200 AS dwh_vertrag_id, DATE '2023-06-15' AS gueltigkeitszeitpunkt, 20 AS zugang UNION ALL
  SELECT 'VVLREIN' AS kennzahl_id, 300 AS dwh_vertrag_id, DATE '2023-06-15' AS gueltigkeitszeitpunkt, 30 AS zugang
),
vertrag AS (
  -- Contract 100: Active during the reporting month boundary (l_monats_date = 2023-07-01)
  SELECT 100 AS dwh_vertrag_id, DATE '2023-01-01' AS gueltig_von, DATE '2023-12-31' AS gueltig_bis, 'KUNDE_A' AS t_mobile_kundennummer, 'GP1' AS test_gp UNION ALL
  -- Contract 200: Expired before the reporting month boundary
  SELECT 200 AS dwh_vertrag_id, DATE '2023-01-01' AS gueltig_von, DATE '2023-06-30' AS gueltig_bis, 'KUNDE_B' AS t_mobile_kundennummer, 'GP2' AS test_gp
  -- Contract 300: Missing entirely (should result in NULLs due to LEFT JOIN)
),
test_execution AS (
  SELECT 
    202306 AS l_monats_id,
    DATE '2023-07-01' AS l_monats_date -- ADD_MONTHS(202306, 1)
)
SELECT 
  fact.dwh_vertrag_id,
  d.t_mobile_kundennummer,
  d.test_gp,
  fact.zugang
FROM fact
CROSS JOIN test_execution
LEFT JOIN vertrag d 
  ON fact.dwh_vertrag_id = d.dwh_vertrag_id
 AND test_execution.l_monats_date > d.gueltig_von
 AND test_execution.l_monats_date <= d.gueltig_bis;
```

#### Pass/Fail Criterion
* **Pass**: The query returns exactly 3 rows:
  1. `dwh_vertrag_id = 100` has `t_mobile_kundennummer = 'KUNDE_A'` and `test_gp = 'GP1'`.
  2. `dwh_vertrag_id = 200` has `t_mobile_kundennummer = NULL` and `test_gp = NULL` (since `l_monats_date` (2023-07-01) is not `<= gueltig_bis` (2023-06-30)).
  3. `dwh_vertrag_id = 300` has `t_mobile_kundennummer = NULL` and `test_gp = NULL`.
* **Fail**: Row 200 or 300 is missing from the output (indicating an inner join behavior), or the date boundary check fails to nullify contract 200.

---

## Section 3: Edge Case & Null Handling Validation

### Test Case 3.1: String Trimming and Null/Hash Handling (`vo_kennung`)
#### Purpose
Verify that the complex nested `DECODE(LTRIM(RTRIM(...)))` logic for `vo_kennung` is perfectly replicated by the BigQuery `CASE` statement.

#### Setup
Create a mock dataset for `dwh_ta_f_d1_twvv_tn` containing various permutations of `vo_kenn` and `vo_kenn_bearb`.

#### Action
Run the following SQL assertion query in BigQuery:

```sql
WITH test_data AS (
  SELECT 'VO_VAL' AS vo_kenn, CAST(NULL AS STRING) AS vo_kenn_bearb UNION ALL -- Null bearb
  SELECT 'VO_VAL' AS vo_kenn, '   ' AS vo_kenn_bearb UNION ALL               -- Whitespace bearb
  SELECT 'VO_VAL' AS vo_kenn, '#' AS vo_kenn_bearb UNION ALL                 -- Hash bearb
  SELECT 'VO_VAL' AS vo_kenn, '  #  ' AS vo_kenn_bearb UNION ALL             -- Hash with whitespace bearb
  SELECT 'VO_VAL' AS vo_kenn, 'VO_BEARB' AS vo_kenn_bearb                    -- Standard bearb
)
SELECT 
  vo_kenn,
  vo_kenn_bearb,
  CASE 
    WHEN TRIM(vo_kenn_bearb) IS NULL THEN vo_kenn
    WHEN TRIM(vo_kenn_bearb) = '#' THEN vo_kenn
    ELSE vo_kenn_bearb
  END AS migrated_vo_kennung
FROM test_data;
```

#### Pass/Fail Criterion
* **Pass**: The output matches the expected mapping:
  | `vo_kenn` | `vo_kenn_bearb` | `migrated_vo_kennung` |
  | :--- | :--- | :--- |
  | 'VO_VAL' | NULL | 'VO_VAL' |
  | 'VO_VAL' | '   ' | 'VO_VAL' |
  | 'VO_VAL' | '#' | 'VO_VAL' |
  | 'VO_VAL' | '  #  ' | 'VO_VAL' |
  | 'VO_VAL' | 'VO_BEARB' | 'VO_BEARB' |
* **Fail**: Any row produces a value for `migrated_vo_kennung` that deviates from the mapping above.

### Test Case 3.2: Business Unit Mapping (`kundennummer` Override)
#### Purpose
Verify that when the new tariff's business unit (`mp_geschaeftsfeld_id`) is `2`, the customer number (`kundennummer`) is overridden to `-1` regardless of the contract's actual customer number.

#### Setup
Create mock tables for `temp` (tariffs) and `dwh_ta_c_vertrag` (contracts).

#### Action
Execute the following query to validate the conditional override:

```sql
WITH temp_mock AS (
  SELECT 1 AS dwh_tarif_id, 2 AS mp_geschaeftsfeld_id UNION ALL -- Business Unit 2
  SELECT 2 AS dwh_tarif_id, 1 AS mp_geschaeftsfeld_id           -- Business Unit 1
),
vertrag_mock AS (
  SELECT 'KUNDE_12345' AS t_mobile_kundennummer
)
SELECT 
  t_new.mp_geschaeftsfeld_id,
  d.t_mobile_kundennummer,
  CASE 
    WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' 
    ELSE d.t_mobile_kundennummer 
  END AS migrated_kundennummer
FROM vertrag_mock d
CROSS JOIN temp_mock t_new;
```

#### Pass/Fail Criterion
* **Pass**: The output contains exactly two rows:
  1. For `mp_geschaeftsfeld_id = 2`, `migrated_kundennummer` is `-1`.
  2. For `mp_geschaeftsfeld_id = 1`, `migrated_kundennummer` is `KUNDE_12345`.
* **Fail**: The override fails to apply, or applies to non-target business units.

---

## Section 4: End-to-End Data Parity & Reconciliation

### Test Case 4.1: Historical Run A/B Reconciliation
#### Purpose
Ensure that executing the migrated BigQuery SQL script on a production-like historical dataset produces identical results to the legacy Oracle run for the same reporting period.

#### Setup
1. Identify a historical reporting month (e.g., `201707`).
2. Ensure the legacy target table state for that month is preserved in a comparison table: `oracle_reconciliation.dwh_ta_t_smart_kubi_201707`.
3. Run the migrated BigQuery SQL script for `l_monats_id = 201707` to populate the target BigQuery table `dwh_ta_t_smart_kubi`.

#### Action
Execute a full outer join reconciliation query in BigQuery to detect any discrepancies in row counts, keys, or metrics.

```sql
WITH legacy AS (
  SELECT 
    monats_id, 
    kundennummer, 
    tarif_id, 
    tarif_id_alt, 
    vo_kennung, 
    test_gp, 
    anzahl, 
    kennzahl_id
  FROM `oracle_reconciliation.dwh_ta_t_smart_kubi_201707`
),
migrated AS (
  SELECT 
    monats_id, 
    kundennummer, 
    tarif_id, 
    tarif_id_alt, 
    vo_kennung, 
    test_gp, 
    anzahl, 
    kennzahl_id
  FROM `your-gcp-project.your_dataset.dwh_ta_t_smart_kubi`
  WHERE monats_id = 201707
),
reconciliation AS (
  SELECT
    COALESCE(l.monats_id, m.monats_id) AS monats_id,
    COALESCE(l.kundennummer, m.kundennummer) AS kundennummer,
    COALESCE(l.tarif_id, m.tarif_id) AS tarif_id,
    COALESCE(l.tarif_id_alt, m.tarif_id_alt) AS tarif_id_alt,
    COALESCE(l.vo_kennung, m.vo_kennung) AS vo_kennung,
    COALESCE(l.test_gp, m.test_gp) AS test_gp,
    COALESCE(l.kennzahl_id, m.kennzahl_id) AS kennzahl_id,
    l.anzahl AS legacy_anzahl,
    m.anzahl AS migrated_anzahl,
    (COALESCE(l.anzahl, 0) - COALESCE(m.anzahl, 0)) AS delta_anzahl
  FROM legacy l
  FULL OUTER JOIN migrated m
    ON  l.monats_id = m.monats_id
    AND COALESCE(l.kundennummer, 'NULL_VAL') = COALESCE(m.kundennummer, 'NULL_VAL')
    AND l.tarif_id = m.tarif_id
    AND l.tarif_id_alt = m.tarif_id_alt
    AND COALESCE(l.vo_kennung, 'NULL_VAL') = COALESCE(m.vo_kennung, 'NULL_VAL')
    AND COALESCE(l.test_gp, 'NULL_VAL') = COALESCE(m.test_gp, 'NULL_VAL')
    AND l.kennzahl_id = m.kennzahl_id
)
SELECT 
  COUNT(*) AS total_mismatched_rows,
  SUM(CASE WHEN legacy_anzahl IS NULL THEN 1 ELSE 0 END) AS extra_rows_in_migrated,
  SUM(CASE WHEN migrated_anzahl IS NULL THEN 1 ELSE 0 END) AS missing_rows_in_migrated,
  SUM(CASE WHEN delta_anzahl != 0 THEN 1 ELSE 0 END) AS metric_mismatches
FROM reconciliation
WHERE legacy_anzahl IS NULL 
   OR migrated_anzahl IS NULL 
   OR delta_anzahl != 0;
```

#### Pass/Fail Criterion
* **Pass**: The reconciliation query returns exactly `0` for all output columns (`total_mismatched_rows = 0`, `extra_rows_in_migrated = 0`, `missing_rows_in_migrated = 0`, `metric_mismatches = 0`).
* **Fail**: Any non-zero value is returned, indicating schema, key, or aggregation mismatches.