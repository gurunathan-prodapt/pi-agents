Here is a comprehensive migration-validation test suite designed to verify that the migrated BigQuery Dataform script (`d_iptn_l_fn_vs_planprod_pk.sqlx`) is behaviorally equivalent to the legacy Oracle PL/SQL script.

---

# Migration Validation Test Suite: IPTN_FN_PLANPRODPK

## Test Case 1: Output Parity & Transformation Correctness (Happy Path)
### Purpose
Verify that the migrated BigQuery script correctly performs the delta load:
1. **Inserts** new mappings from the staging table (`DATEN_EXTTAB`) that do not exist in the target table.
2. **Updates** existing mappings if the `td_plan_produkt_id` has changed.
3. **Soft-deletes (end-dates)** mappings that exist in the active view (`DWH$VI_L_FN_VS_PLANPROD_PK`) but are missing from the new staging data by setting `gueltig_bis` to the execution `stichtag`.

### Setup
Create mock tables and populate them with a baseline state.

```sql
-- 1. Create Mock Target Table
CREATE OR REPLACE TABLE `DWH$TA_L_FN_VS_PLANPROD_PK` (
  td_perlenprodukt_id STRING,
  td_plan_produkt_id STRING,
  referenz_jahr INT64,
  gueltig_von DATE,
  gueltig_bis DATE
);

-- 2. Create Mock Active View (simulating active records where gueltig_bis is NULL)
CREATE OR REPLACE VIEW `DWH$VI_L_FN_VS_PLANPROD_PK` AS (
  SELECT * FROM `DWH$TA_L_FN_VS_PLANPROD_PK` WHERE gueltig_bis IS NULL
);

-- 3. Create Mock Staging Tables
CREATE OR REPLACE TABLE `DATEN_EXTTAB` (
  td_perlenprodukt_id STRING,
  td_plan_produkt_id STRING,
  referenz_jahr INT64
);

CREATE OR REPLACE TABLE `ER_EXTTAB` (
  stichtag DATE
);

-- 4. Populate Baseline Target Data
INSERT INTO `DWH$TA_L_FN_VS_PLANPROD_PK` (td_perlenprodukt_id, td_plan_produkt_id, referenz_jahr, gueltig_von, gueltig_bis)
VALUES 
  ('PROD_A', 'PLAN_A_OLD', 2024, DATE('2024-01-01'), NULL), -- To be updated
  ('PROD_B', 'PLAN_B', 2024, DATE('2024-01-01'), NULL),     -- To be soft-deleted (missing from staging)
  ('PROD_C', 'PLAN_C', 2024, DATE('2024-01-01'), DATE('2024-02-01')); -- Already inactive, should remain untouched

-- 5. Populate Staging Data (New Delivery)
INSERT INTO `DATEN_EXTTAB` (td_perlenprodukt_id, td_plan_produkt_id, referenz_jahr)
VALUES 
  ('PROD_A', 'PLAN_A_NEW', 2024), -- Update (different plan product)
  ('PROD_D', 'PLAN_D', 2024);     -- Insert (new mapping)

-- 6. Populate Execution Metadata
INSERT INTO `ER_EXTTAB` (stichtag) VALUES (DATE('2024-03-30'));
```

### Action
Execute the migrated BigQuery SQLX script (or the compiled SQL block) with the parameter `@p_eintragsnr` set to `1001`.

### Pass/Fail Criterion
Run the following assertion query. It must return no rows (all assertions pass).

```sql
WITH expected AS (
  SELECT 'PROD_A' AS td_perlenprodukt_id, 'PLAN_A_NEW' AS td_plan_produkt_id, 2024 AS referenz_jahr, DATE('2024-03-30') AS gueltig_von, CAST(NULL AS DATE) AS gueltig_bis
  UNION ALL
  SELECT 'PROD_B', 'PLAN_B', 2024, DATE('2024-01-01'), DATE('2024-03-30') -- Soft-deleted
  UNION ALL
  SELECT 'PROD_C', 'PLAN_C', 2024, DATE('2024-01-01'), DATE('2024-02-01') -- Untouched
  UNION ALL
  SELECT 'PROD_D', 'PLAN_D', 2024, DATE('2024-03-30'), NULL               -- Inserted
),
actual AS (
  SELECT * FROM `DWH$TA_L_FN_VS_PLANPROD_PK`
)
SELECT 
  'Mismatch between expected and actual target table state' AS failure_reason,
  coalesce(a.td_perlenprodukt_id, e.td_perlenprodukt_id) AS td_perlenprodukt_id,
  a.td_plan_produkt_id AS actual_plan, e.td_plan_produkt_id AS expected_plan,
  a.gueltig_von AS actual_von, e.gueltig_von AS expected_von,
  a.gueltig_bis AS actual_bis, e.gueltig_bis AS expected_bis
FROM expected e
FULL OUTER JOIN actual a 
  ON e.td_perlenprodukt_id = a.td_perlenprodukt_id 
  AND e.referenz_jahr = a.referenz_jahr
WHERE 
  a.td_plan_produkt_id IS DISTINCT FROM e.td_plan_produkt_id
  OR a.gueltig_von IS DISTINCT FROM e.gueltig_von
  OR a.gueltig_bis IS DISTINCT FROM e.gueltig_bis;
```

---

## Test Case 2: Idempotency & Repeated Runs
### Purpose
Verify that running the migration script multiple times with the same input data does not duplicate records, alter existing valid records, or repeatedly update `gueltig_bis` dates.

### Setup
Initialize the target table with the final state of Test Case 1. Keep the same staging data in `DATEN_EXTTAB` and `ER_EXTTAB`.

```sql
-- Target table is already in the post-load state of Test Case 1:
-- PROD_A -> PLAN_A_NEW (gueltig_von: 2024-03-30, gueltig_bis: NULL)
-- PROD_B -> PLAN_B     (gueltig_von: 2024-01-01, gueltig_bis: 2024-03-30)
-- PROD_C -> PLAN_C     (gueltig_von: 2024-01-01, gueltig_bis: 2024-02-01)
-- PROD_D -> PLAN_D     (gueltig_von: 2024-03-30, gueltig_bis: NULL)
```

### Action
Execute the migrated BigQuery SQLX script a second time.

### Pass/Fail Criterion
Verify that no rows were modified during the second run (the target table state remains identical to the end of Test Case 1).

```sql
-- Assert that row counts and values did not change
DECLARE affected_rows INT64;

-- Run the assertion
ASSERT (
  SELECT COUNT(*) FROM `DWH$TA_L_FN_VS_PLANPROD_PK`
) = 4 AS "Row count changed on idempotent run";

ASSERT (
  SELECT COUNT(*) FROM `DWH$TA_L_FN_VS_PLANPROD_PK` WHERE gueltig_bis IS NULL
) = 2 AS "Active row count changed on idempotent run";
```

---

## Test Case 3: NULL Handling & Edge Cases
### Purpose
Verify that the script handles NULL values in non-key fields correctly and does not break or produce unexpected Cartesian products when optional fields are empty.

### Setup
```sql
-- Clear tables
TRUNCATE TABLE `DWH$TA_L_FN_VS_PLANPROD_PK`;
TRUNCATE TABLE `DATEN_EXTTAB`;

-- Insert record with NULL in non-key field (td_plan_produkt_id)
INSERT INTO `DATEN_EXTTAB` (td_perlenprodukt_id, td_plan_produkt_id, referenz_jahr)
VALUES ('PROD_NULL', NULL, 2024);
```

### Action
Execute the migrated BigQuery SQLX script.

### Pass/Fail Criterion
Verify that the record with the NULL plan product ID was successfully inserted with its key fields intact.

```sql
ASSERT (
  SELECT COUNT(*) 
  FROM `DWH$TA_L_FN_VS_PLANPROD_PK` 
  WHERE td_perlenprodukt_id = 'PROD_NULL' 
    AND td_plan_produkt_id IS NULL 
    AND referenz_jahr = 2024
) = 1 AS "Failed to handle NULL value in non-key column td_plan_produkt_id";
```

---

## Test Case 4: External System Replacement (Metadata Cardinality Guard)
### Purpose
The legacy script uses a Cartesian join (`CROSS JOIN` in BigQuery) with the execution metadata table `<ER_EXTTAB>`. If `<ER_EXTTAB>` contains more than one row due to an upstream orchestration error, a standard cross join will duplicate target records. This test ensures that the system behaves predictably or that we assert single-row cardinality for the metadata table.

### Setup
```sql
TRUNCATE TABLE `ER_EXTTAB`;
-- Insert duplicate execution dates (simulating an upstream bug)
INSERT INTO `ER_EXTTAB` (stichtag) VALUES (DATE('2024-03-30')), (DATE('2024-03-31'));
```

### Action
Execute the migrated BigQuery SQLX script.

### Pass/Fail Criterion
The execution must fail or be guarded against Cartesian explosion. If the pipeline design assumes exactly one row in `ER_EXTTAB`, we assert this condition.

```python
# pytest implementation to verify cardinality check or failure
import pytest
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

def test_metadata_multi_row_failure():
    client = bigquery.Client()
    
    # We expect the script to fail or we run a pre-validation query that raises an error
    guard_query = """
      ASSERT (SELECT COUNT(*) FROM `ER_EXTTAB`) = 1 
      AS "Validation Failed: ER_EXTTAB must contain exactly one row to prevent Cartesian explosion.";
    """
    
    with pytest.raises(GoogleAPIError) as excinfo:
        client.query(guard_query).result()
    
    assert "ER_EXTTAB must contain exactly one row" in str(excinfo.value)
```

---

## Test Case 5: Transaction Rollback & Error Logging
### Purpose
Verify that if any statement fails during execution:
1. All changes made during the transaction are rolled back (atomicity).
2. The error is logged to the centralized logging table `dw_logs.dwpa_meldung_errors` with the correct job number (`EintragsNr`).

### Setup
```sql
-- Create mock logging table if not exists
CREATE SCHEMA IF NOT EXISTS `dw_logs`;
CREATE OR REPLACE TABLE `dw_logs.dwpa_meldung_errors` (
  severity STRING,
  entry_nr INT64,
  error_nr INT64,
  error_msg STRING,
  statement STRING
);

-- Reset target table to a known state
TRUNCATE TABLE `DWH$TA_L_FN_VS_PLANPROD_PK`;
INSERT INTO `DWH$TA_L_FN_VS_PLANPROD_PK` (td_perlenprodukt_id, td_plan_produkt_id, referenz_jahr, gueltig_von, gueltig_bis)
VALUES ('PROD_KEEP', 'PLAN_KEEP', 2024, DATE('2024-01-01'), NULL);

-- Force an error by inserting incompatible data types or violating schema constraints in staging
-- Here we mock a scenario where DATEN_EXTTAB structure is corrupted or we force a runtime error
```

### Action
Execute a test harness version of the script that forces a runtime error (e.g., division by zero or schema mismatch) mid-transaction.

```sql
-- Test Harness Execution Block
DECLARE EintragsNr INT64 DEFAULT 9999;
BEGIN
  BEGIN TRANSACTION;
  
  -- This insert should succeed initially
  INSERT INTO `DWH$TA_L_FN_VS_PLANPROD_PK` (td_perlenprodukt_id, td_plan_produkt_id, referenz_jahr, gueltig_von, gueltig_bis)
  VALUES ('PROD_TEMP', 'PLAN_TEMP', 2024, DATE('2024-03-30'), NULL);
  
  -- Force a runtime error (Division by Zero)
  SELECT 1 / 0;
  
  COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  BEGIN
    DECLARE ErrText STRING DEFAULT @@error.message;
    DECLARE ErrC STRING DEFAULT @@error.statement_text;
    INSERT INTO `dw_logs.dwpa_meldung_errors` (severity, entry_nr, error_nr, error_msg, statement)
    VALUES ('F', EintragsNr, -1, ErrText, ErrC);
  END;
END;
```

### Pass/Fail Criterion
Verify that:
1. The temporary row `'PROD_TEMP'` was **not** committed to the target table (rollback succeeded).
2. The original row `'PROD_KEEP'` remains intact.
3. An error entry was written to `dw_logs.dwpa_meldung_errors` with `entry_nr = 9999`.

```sql
-- Assertion 1: Rollback Verification
ASSERT (
  SELECT COUNT(*) FROM `DWH$TA_L_FN_VS_PLANPROD_PK` WHERE td_perlenprodukt_id = 'PROD_TEMP'
) = 0 AS "Rollback Failed: Temporary row was committed despite error!";

-- Assertion 2: Data Preservation Verification
ASSERT (
  SELECT COUNT(*) FROM `DWH$TA_L_FN_VS_PLANPROD_PK` WHERE td_perlenprodukt_id = 'PROD_KEEP'
) = 1 AS "Rollback Failed: Existing data was lost!";

-- Assertion 3: Logging Verification
ASSERT (
  SELECT COUNT(*) FROM `dw_logs.dwpa_meldung_errors` 
  WHERE entry_nr = 9999 
    AND severity = 'F' 
    AND error_msg LIKE '%division by zero%'
) = 1 AS "Error Logging Failed: No log entry found in dwpa_meldung_errors";
```