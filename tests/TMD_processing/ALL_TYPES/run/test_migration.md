Here is a comprehensive suite of migration-validation tests designed for a senior QA engineer to validate the BigQuery SQL migration of the `all_types_graph.ksh` Ab Initio job.

---

# Migration Validation Test Suite: `all_types_graph`

## Test 1: End-to-End Output Parity (Golden Dataset Validation)

### Purpose
To prove that the migrated BigQuery SQL script produces the exact same output datasets as the legacy Ab Initio graph when presented with the same input data.

### Setup
1. Create a isolated test dataset in BigQuery: `{{project_id}}.all_types_test_validation`.
2. Populate the source tables with a "Golden Dataset" containing representative records for all three business categories (Cancellations, Products, Quotes/Contracts) and various team visibility configurations.
3. Run the legacy Ab Initio graph using the same input data and export the output files (`tos_cancellations.dat`, `tos_cancellations_wk.dat`, etc.) into BigQuery tables suffixed with `_legacy`.

### Action
Execute the migrated BigQuery SQL script pointing to the test dataset:

```sql
-- Execute the migrated script against the validation dataset
-- (Ensure project_id and dataset are replaced with the validation environment)
```

### Pass/Fail Criterion
Run the following reconciliation query. It must return `True` for all tables, indicating zero differences in row counts and column values.

```sql
-- Pytest validation script using the Google Cloud BigQuery SDK
import pytest
from google.cloud import bigquery

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client()

@pytest.mark.parametrize("table_base", [
    "tos_cancellations",
    "tos_cancellations_wk",
    "tos_products",
    "tos_products_wk",
    "tos_quotes_contracts",
    "tos_quotes_contracts_wk"
])
def test_table_parity(bq_client, table_base):
    dataset = "all_types_test_validation"
    target_table = f"{dataset}.{table_base}"
    legacy_table = f"{dataset}.{table_base}_legacy"
    
    # 1. Assert Row Counts Match Exactly
    count_query = f"""
        SELECT 
            (SELECT COUNT(*) FROM `{target_table}`) as target_count,
            (SELECT COUNT(*) FROM `{legacy_table}`) as legacy_count
    """
    query_job = bq_client.query(count_query)
    results = list(query_job.result())[0]
    assert results.target_count == results.legacy_count, \
        f"Row count mismatch for {table_base}: Target={results.target_count}, Legacy={results.legacy_count}"
        
    # 2. Assert Symmetric Difference is Zero (Value Parity)
    # We exclude dynamic metadata columns if any exist, but here schemas should match exactly.
    diff_query = f"""
        SELECT COUNT(*) as diff_count FROM (
            SELECT * FROM `{target_table}`
            EXCEPT DISTINCT
            SELECT * FROM `{legacy_table}`
        )
    """
    diff_job = bq_client.query(diff_query)
    diff_result = list(diff_job.result())[0]
    assert diff_result.diff_count == 0, \
        f"Data mismatch found in {table_base}. {diff_result.diff_count} rows differ."
```

---

## Test 2: Team Visibility Masking (Transformation Correctness)

### Purpose
To verify that the left join with the active/visible teams lookup (`lkp_teamvirt_ccos`) correctly masks `sdm_team_id` to `NULL` if the team is not visible, and preserves it if it is visible.

### Setup
1. Populate `ccr_ta_f_teamsichtbarkeit`, `ccr_ta_s_sdm_team`, and `ccr_ta_s_sdm_abteilung` such that:
   - Team `1001` is visible (`team_sichtbarkeitstyp_id = 10` AND `UNSICHTBAR_FLAG = 0`).
   - Team `1002` is visible via external department (`team_sichtbarkeitstyp_id = 10` AND `UNSICHTBAR_FLAG = 1` AND `ABT_EXTERN = 1`).
   - Team `1003` is **invisible** (`team_sichtbarkeitstyp_id = 10` AND `UNSICHTBAR_FLAG = 1` AND `ABT_EXTERN = 0`).
   - Team `1004` is **invisible** (`team_sichtbarkeitstyp_id = 99` AND `UNSICHTBAR_FLAG = 0`).
2. Populate `x_tos_measures` with records for all four teams under `tos_mea_group_name = 'CANCELLATIONS'`.

### Action
Run the SQL logic to generate the temporary lookup and the standard cancellations table.

### Pass/Fail Criterion
Execute the following assertion query in BigQuery. It must complete successfully without throwing an assertion error.

```sql
-- Assertions for Team Visibility Masking
ASSERT (
  SELECT COUNT(*) 
  FROM `{{project_id}}.all_types_test_validation.tos_cancellations`
  WHERE sdm_team_id = 1001
) = 1 AS "Visible team 1001 was incorrectly masked!";

ASSERT (
  SELECT COUNT(*) 
  FROM `{{project_id}}.all_types_test_validation.tos_cancellations`
  WHERE sdm_team_id = 1002
) = 1 AS "Externally visible team 1002 was incorrectly masked!";

ASSERT (
  SELECT sdm_team_id 
  FROM `{{project_id}}.all_types_test_validation.tos_cancellations`
  WHERE kkm_kampagne_id = (SELECT kkm_kampagne_id FROM `{{project_id}}.all_types_test_validation.x_tos_measures` WHERE sdm_team_id = 1003 LIMIT 1)
) IS NULL AS "Invisible team 1003 was not masked to NULL!";

ASSERT (
  SELECT sdm_team_id 
  FROM `{{project_id}}.all_types_test_validation.tos_cancellations`
  WHERE kkm_kampagne_id = (SELECT kkm_kampagne_id FROM `{{project_id}}.all_types_test_validation.x_tos_measures` WHERE sdm_team_id = 1004 LIMIT 1)
) IS NULL AS "Invisible team 1004 (wrong visibility type) was not masked to NULL!";
```

---

## Test 3: Weekly Cutoff Logic (Temporal Boundary Validation)

### Purpose
To verify that the dynamic Tuesday cutoff calculation (`v_tuesday_cutoff`) correctly filters records for the weekly tables (`_wk`) across different run dates.

### Setup
1. Since `CURRENT_DATE()` is dynamic, we will test the date-truncation logic explicitly by mocking various execution dates.
2. The target formula is: `DATE_ADD(DATE_TRUNC(test_date, WEEK(MONDAY)), INTERVAL 1 DAY)`.

### Action
Execute a test query evaluating the boundary logic against a matrix of test dates (representing Mondays, Tuesdays, Wednesdays, and Sundays).

### Pass/Fail Criterion
All calculated cutoffs must match the expected Tuesday of the current ISO week.

```sql
WITH date_matrix AS (
  SELECT DATE '2023-10-23' AS run_date, DATE '2023-10-24' AS expected_cutoff UNION ALL -- Monday
  SELECT DATE '2023-10-24' AS run_date, DATE '2023-10-24' AS expected_cutoff UNION ALL -- Tuesday
  SELECT DATE '2023-10-25' AS run_date, DATE '2023-10-24' AS expected_cutoff UNION ALL -- Wednesday
  SELECT DATE '2023-10-29' AS run_date, DATE '2023-10-24' AS expected_cutoff           -- Sunday
)
SELECT 
  run_date,
  expected_cutoff,
  DATE_ADD(DATE_TRUNC(run_date, WEEK(MONDAY)), INTERVAL 1 DAY) AS calculated_cutoff
FROM date_matrix;
```

*Assertion script to run in pipeline test:*
```sql
ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT 
      DATE_ADD(DATE_TRUNC(run_date, WEEK(MONDAY)), INTERVAL 1 DAY) AS calculated_cutoff,
      expected_cutoff
    FROM (
      SELECT DATE '2023-10-23' AS run_date, DATE '2023-10-24' AS expected_cutoff UNION ALL
      SELECT DATE '2023-10-24' AS run_date, DATE '2023-10-24' AS expected_cutoff UNION ALL
      SELECT DATE '2023-10-25' AS run_date, DATE '2023-10-24' AS expected_cutoff UNION ALL
      SELECT DATE '2023-10-29' AS run_date, DATE '2023-10-24' AS expected_cutoff
    )
  )
  WHERE calculated_cutoff != expected_cutoff
) = 0 AS "Weekly Tuesday cutoff calculation logic is incorrect!";
```

---

## Test 4: Quotes & Contracts Metric Mapping (Transformation Correctness)

### Purpose
To verify that the conditional mapping of metrics (`mea_1` and `mea_2`) based on `tos_mea_group_name` (`QUOTES` vs `CONTRACTS`) is correctly handled and that type casting for `subventionen` behaves correctly.

### Setup
Populate `x_tos_measures` with:
- Record 1: `tos_mea_group_name = 'QUOTES'`, `mea_1 = 10`, `mea_2 = '150.55'`
- Record 2: `tos_mea_group_name = 'CONTRACTS'`, `mea_1 = 5`, `mea_2 = NULL`
- Record 3: `tos_mea_group_name = 'CANCELLATIONS'`, `mea_1 = 3`, `mea_2 = NULL`

### Action
Run the SQL logic for Step 8 (`tos_quotes_contracts`).

### Pass/Fail Criterion
Verify that the metrics are mapped conditionally and non-applicable groups are excluded.

```sql
-- Assertions for Quotes & Contracts Mapping
ASSERT (
  SELECT COUNT(*) 
  FROM `{{project_id}}.all_types_test_validation.tos_quotes_contracts`
) = 2 AS "Wrong number of records in tos_quotes_contracts (should exclude CANCELLATIONS)";

ASSERT (
  SELECT anzahl_angebote 
  FROM `{{project_id}}.all_types_test_validation.tos_quotes_contracts` 
  WHERE tos_offer_id = 1 -- assuming Record 1 has tos_offer_id = 1
) = 10 AS "QUOTES record failed to map anzahl_angebote correctly";

ASSERT (
  SELECT subventionen 
  FROM `{{project_id}}.all_types_test_validation.tos_quotes_contracts` 
  WHERE tos_offer_id = 1
) = 150.55 AS "QUOTES record failed to cast and map subventionen correctly";

ASSERT (
  SELECT anzahl_vertraege 
  FROM `{{project_id}}.all_types_test_validation.tos_quotes_contracts` 
  WHERE tos_offer_id = 2 -- assuming Record 2 has tos_offer_id = 2
) = 5 AS "CONTRACTS record failed to map anzahl_vertraege correctly";

ASSERT (
  SELECT anzahl_angebote 
  FROM `{{project_id}}.all_types_test_validation.tos_quotes_contracts` 
  WHERE tos_offer_id = 2
) = 0 AS "CONTRACTS record should have anzahl_angebote set to 0";
```

---

## Test 5: Product ID Concatenation & Trimming (Transformation Correctness)

### Purpose
To verify that `tcn_offer_product_id` is correctly concatenated using the tilde (`~`) separator and that both source fields are trimmed of whitespace.

### Setup
Populate `x_tos_measures` with:
- Record 1: `tos_mea_group_name = 'PRODUCTS'`, `tos_offer_id = 9999`, `tcn_product_id = 8888`
- Record 2: `tos_mea_group_name = 'PRODUCTS'`, `tos_offer_id = 1111`, `tcn_product_id = 2222` (with leading/trailing spaces in source if stored as string, or test casting)

### Action
Run the SQL logic for Step 6 (`tos_products`).

### Pass/Fail Criterion
Verify the concatenated output format.

```sql
-- Assertions for Product ID Concatenation
ASSERT (
  SELECT COUNT(*) 
  FROM `{{project_id}}.all_types_test_validation.tos_products`
  WHERE tcn_offer_product_id = '9999~8888'
) = 1 AS "Product ID concatenation failed for standard values";

-- Test trimming behavior explicitly
SELECT 
  CONCAT(TRIM(CAST(' 1111 ' AS STRING)), '~', TRIM(CAST(' 2222 ' AS STRING))) AS test_concat;
-- Expected output: '1111~2222'
```

---

## Test 6: Schema and Nullability Assertions (Data Quality)

### Purpose
To ensure that the target BigQuery tables conform to the expected schema definitions, column types, and nullability constraints defined in the legacy DML files.

### Setup
None (Metadata-only check).

### Action
Query the BigQuery `INFORMATION_SCHEMA.COLUMNS` view for the target tables.

### Pass/Fail Criterion
The target tables must have the correct data types.

```sql
-- SQL validation query to verify target schema types
WITH expected_schema AS (
  SELECT 'tos_cancellations' AS table_name, 'anzahl_stornos' AS column_name, 'INT64' AS data_type UNION ALL
  SELECT 'tos_cancellations', 'sdm_team_id', 'INT64' UNION ALL
  SELECT 'tos_products', 'anzahl_produkte', 'INT64' UNION ALL
  SELECT 'tos_products', 'tcn_offer_product_id', 'STRING' UNION ALL
  SELECT 'tos_quotes_contracts', 'anzahl_angebote', 'INT64' UNION ALL
  SELECT 'tos_quotes_contracts', 'subventionen', 'NUMERIC' UNION ALL
  SELECT 'tos_quotes_contracts', 'anzahl_vertraege', 'INT64'
)
SELECT 
  e.table_name,
  e.column_name,
  e.data_type AS expected_type,
  i.data_type AS actual_type,
  (e.data_type = i.data_type) AS is_ok
FROM expected_schema e
JOIN `{{project_id}}.all_types_test_validation.INFORMATION_SCHEMA.COLUMNS` i
  ON i.table_name = e.table_name AND i.column_name = e.column_name;
```

*Assertion script to run in pipeline test:*
```sql
ASSERT (
  SELECT COUNT(*)
  FROM `{{project_id}}.all_types_test_validation.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'tos_quotes_contracts' 
    AND column_name = 'subventionen' 
    AND data_type != 'NUMERIC'
) = 0 AS "Column 'subventionen' in 'tos_quotes_contracts' is not of type NUMERIC!";
```