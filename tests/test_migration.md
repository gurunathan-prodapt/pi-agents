# Migration Validation Test Suite: DW.BERT_AUSD_BP_TA_TARIFOPTION

This document defines the migration-validation test suite for the `DW.BERT_AUSD_BP_TA_TARIFOPTION` job. The tests are designed to prove that the refactored BigQuery SQL script and Apache Airflow DAG are behaviorally equivalent to the legacy Oracle and KornShell implementation.

---

## Test Case 1: Metadata Suffix Resolution (`v_datum`) & Dynamic Table Selection

### Purpose
Verify that the BigQuery script correctly identifies the run-date suffix (`v_datum`) from the metadata table `dwtk_meldungen` and dynamically targets the correct daily table `sof_ta_bpr_opt_text_<v_datum>`.

### Setup
1. Clear any existing records in the metadata table for the target job key.
2. Insert a mock execution record into `dwtk_meldungen` with a specific timestamp.
3. Create a mock dynamic table matching that resolved date suffix.
4. Create the static lookup table.

```sql
-- 1. Clean up metadata
DELETE FROM `target_project.target_dataset.dwtk_meldungen` 
WHERE job_kennung = 'BERT_DROP_TEMP_TABLE';

-- 2. Insert mock execution record (resolves to suffix '20260421')
INSERT INTO `target_project.target_dataset.dwtk_meldungen` (job_kennung, timecreated)
VALUES ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2026-04-21 14:30:00 UTC'));

-- 3. Create mock dynamic table for '20260421'
CREATE OR REPLACE TABLE `target_project.target_dataset.sof_ta_bpr_opt_text_20260421` (
  bpr_id INT64,
  cntrct_id INT64,
  pds_description STRING
);

INSERT INTO `target_project.target_dataset.sof_ta_bpr_opt_text_20260421` (bpr_id, cntrct_id, pds_description)
VALUES 
  (101, 9999, 'Test Option A');

-- 4. Create static lookup table
CREATE OR REPLACE TABLE `target_project.target_dataset.sof_ta_l_bpr_optionen_filter` (
  bpr_id INT64,
  opt_kategorie STRING
);

INSERT INTO `target_project.target_dataset.sof_ta_l_bpr_optionen_filter` (bpr_id, opt_kategorie)
VALUES 
  (101, 'BUDGET');
```

### Action
Execute Step 1 and Step 2 of the BigQuery SQL script:

```sql
DECLARE v_datum STRING;

-- Step 1: Identify the run-date suffix
SET v_datum = (
  SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(timecreated)), '19000101')
  FROM `target_project.target_dataset.dwtk_meldungen`
  WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Step 2: Build staging/intermediate table dynamically
EXECUTE IMMEDIATE FORMAT("""
  CREATE OR REPLACE TABLE `target_project.target_dataset.sof_ta_bpr_opt_filter` AS
  SELECT
    t.bpr_id,
    t.cntrct_id,
    t.pds_description,
    l.opt_kategorie
  FROM
    `target_project.target_dataset.sof_ta_l_bpr_optionen_filter` l
  JOIN
    `target_project.target_dataset.sof_ta_bpr_opt_text_%s` t
  ON
    t.bpr_id = l.bpr_id
""", v_datum);
```

### Pass/Fail Criterion
**Pass**: 
* The variable `v_datum` resolves to `'20260421'`.
* The table `sof_ta_bpr_opt_filter` is successfully created and contains exactly 1 row with `cntrct_id = 9999` and `opt_kategorie = 'BUDGET'`.

**Fail**: Any compilation error, failure to resolve the date, or failure to select the correct dynamic table.

#### Verification Query:
```sql
SELECT 
  COUNT(*) AS row_count,
  SUM(IF(cntrct_id = 9999 AND opt_kategorie = 'BUDGET', 1, 0)) AS valid_rows
FROM `target_project.target_dataset.sof_ta_bpr_opt_filter`;
-- Expected: row_count = 1, valid_rows = 1
```

---

## Test Case 2: Transformation Correctness (Sorting, Categorization, and Aggregation)

### Purpose
Verify that the refactored `STRING_AGG` logic correctly replaces the legacy Oracle stateful accumulator (`sof$ab_con`). It must:
1. Group options by `cntrct_id`.
2. Categorize options into `business_option` (`BUDGET`), `sonstige_option` (`SONST`), and `gprs_option` (`GPRS`).
3. Sort concatenated option descriptions **alphabetically** (not by insertion order).
4. Delimit concatenated values with a comma and a space (`', '`).

### Setup
Populate the intermediate table `sof_ta_bpr_opt_filter` with unsorted, multi-category test data for a single contract.

```sql
CREATE OR REPLACE TABLE `target_project.target_dataset.sof_ta_bpr_opt_filter` (
  bpr_id INT64,
  cntrct_id INT64,
  pds_description STRING,
  opt_kategorie STRING
);

INSERT INTO `target_project.target_dataset.sof_ta_bpr_opt_filter` (bpr_id, cntrct_id, pds_description, opt_kategorie)
VALUES 
  -- Unsorted BUDGET options
  (1, 5001, 'Beta Budget Option', 'BUDGET'),
  (2, 5001, 'Alpha Budget Option', 'BUDGET'),
  (3, 5001, 'Gamma Budget Option', 'BUDGET'),
  -- Unsorted SONST options
  (4, 5001, 'Zeta Sonst Option', 'SONST'),
  (5, 5001, 'Delta Sonst Option', 'SONST'),
  -- GPRS option (single)
  (6, 5001, 'Epsilon GPRS Option', 'GPRS');
```

### Action
Execute the aggregation query (Step 3 of the BigQuery script):

```sql
CREATE OR REPLACE TABLE `target_project.target_dataset.sof_ta_tarifoption` AS
SELECT
  cntrct_id,
  SUBSTR(
    STRING_AGG(
      IF(opt_kategorie = 'BUDGET', pds_description, NULL), 
      ', ' ORDER BY pds_description
    ), 1, 500
  ) AS business_option,
  SUBSTR(
    STRING_AGG(
      IF(opt_kategorie = 'SONST', pds_description, NULL), 
      ', ' ORDER BY pds_description
    ), 1, 500
  ) AS sonstige_option,
  SUBSTR(
    STRING_AGG(
      IF(opt_kategorie = 'GPRS', pds_description, NULL), 
      ', ' ORDER BY pds_description
    ), 1, 500
  ) AS gprs_option
FROM
  `target_project.target_dataset.sof_ta_bpr_opt_filter`
GROUP BY
  cntrct_id;
```

### Pass/Fail Criterion
**Pass**: The target table contains exactly 1 row for `cntrct_id = 5001` with the following values:
* `business_option` = `'Alpha Budget Option, Beta Budget Option, Gamma Budget Option'` (Alphabetical order verified)
* `sonstige_option` = `'Delta Sonst Option, Zeta Sonst Option'` (Alphabetical order verified)
* `gprs_option` = `'Epsilon GPRS Option'`

**Fail**: Any deviation in sorting, incorrect categorization, or incorrect delimiter.

#### Verification Query:
```sql
SELECT 
  cntrct_id,
  business_option = 'Alpha Budget Option, Beta Budget Option, Gamma Budget Option' AS business_ok,
  sonstige_option = 'Delta Sonst Option, Zeta Sonst Option' AS sonstige_ok,
  gprs_option = 'Epsilon GPRS Option' AS gprs_ok
FROM `target_project.target_dataset.sof_ta_tarifoption`
WHERE cntrct_id = 5001;
-- Expected: True, True, True
```

---

## Test Case 3: Truncation Edge Case (500-Character Limit)

### Purpose
Verify that concatenated strings exceeding 500 characters are truncated to exactly 500 characters without throwing errors, matching the legacy Oracle `substr(..., 1, 500)` behavior.

### Setup
Insert options for a contract that, when concatenated, exceed 500 characters.

```sql
-- Generate 6 options of 100 characters each
CREATE OR REPLACE TABLE `target_project.target_dataset.sof_ta_bpr_opt_filter` AS
SELECT 
  100 + val AS bpr_id,
  7001 AS cntrct_id,
  CONCAT('Option_', CAST(val AS STRING), '_', REPEAT('X', 90)) AS pds_description,
  'BUDGET' AS opt_kategorie
FROM UNNEST([1, 2, 3, 4, 5, 6]) AS val;
```

### Action
Execute the aggregation query (Step 3 of the BigQuery script).

### Pass/Fail Criterion
**Pass**: 
* The query completes successfully.
* The length of the resulting `business_option` string is exactly 500 characters.
* The string is truncated cleanly at character 500.

**Fail**: The query fails, or the length of the output string is not equal to 500.

#### Verification Query:
```sql
SELECT 
  LENGTH(business_option) AS string_length,
  ENDS_WITH(business_option, SUBSTR(business_option, -10)) AS ends_correctly
FROM `target_project.target_dataset.sof_ta_tarifoption`
WHERE cntrct_id = 7001;
-- Expected: string_length = 500
```

---

## Test Case 4: NULL and Empty Handling

### Purpose
Verify that the system handles missing categories, NULL descriptions, and empty strings gracefully without generating malformed lists (e.g., leading/trailing commas or double commas).

### Setup
Insert test cases for:
* **Contract 8001**: Has options, but none match the `BUDGET` or `GPRS` categories (should result in `NULL` for those columns).
* **Contract 8002**: Has options with `NULL` descriptions.
* **Contract 8003**: Has options with empty string `''` descriptions.

```sql
CREATE OR REPLACE TABLE `target_project.target_dataset.sof_ta_bpr_opt_filter` (
  bpr_id INT64,
  cntrct_id INT64,
  pds_description STRING,
  opt_kategorie STRING
);

INSERT INTO `target_project.target_dataset.sof_ta_bpr_opt_filter` (bpr_id, cntrct_id, pds_description, opt_kategorie)
VALUES 
  -- Contract 8001: Only SONST options
  (1, 8001, 'Sonst Option A', 'SONST'),
  -- Contract 8002: NULL description
  (2, 8002, NULL, 'BUDGET'),
  (3, 8002, 'Valid Budget Option', 'BUDGET'),
  -- Contract 8003: Empty string description
  (4, 8003, '', 'BUDGET'),
  (5, 8003, 'Valid Budget Option B', 'BUDGET');
```

### Action
Execute the aggregation query (Step 3 of the BigQuery script).

### Pass/Fail Criterion
**Pass**:
* **Contract 8001**: `business_option` is `NULL`, `gprs_option` is `NULL`, `sonstige_option` is `'Sonst Option A'`.
* **Contract 8002**: `business_option` is `'Valid Budget Option'` (the `NULL` value is ignored, and no extra commas are added).
* **Contract 8003**: `business_option` is `', Valid Budget Option B'` (empty string is treated as a valid string value by `STRING_AGG`).

**Fail**: Any occurrence of `'NULL'` as a string, unexpected leading/trailing commas for Contract 8002, or query failure.

#### Verification Query:
```sql
SELECT 
  cntrct_id,
  business_option,
  sonstige_option,
  gprs_option
FROM `target_project.target_dataset.sof_ta_tarifoption`
WHERE cntrct_id IN (8001, 8002, 8003)
ORDER BY cntrct_id;

/* Expected Results:
Row 1 (8001): business_option = NULL, sonstige_option = 'Sonst Option A', gprs_option = NULL
Row 2 (8002): business_option = 'Valid Budget Option', sonstige_option = NULL, gprs_option = NULL
Row 3 (8003): business_option = ', Valid Budget Option B', sonstige_option = NULL, gprs_option = NULL
*/
```

---

## Test Case 5: End-to-End Output Parity (Reconciliation / Dual-Run Validation)

### Purpose
Prove 100% behavioral equivalence by comparing the output of the legacy Oracle execution against the BigQuery execution using identical source data.

### Setup
1. Extract the final target table `sof$ta_tarifoption` from the legacy Oracle database after a production run.
2. Load this dataset into a temporary BigQuery table named `target_project.target_dataset.legacy_sof_ta_tarifoption`.
3. Run the migrated BigQuery pipeline on the same source snapshot to populate `target_project.target_dataset.sof_ta_tarifoption`.

### Action
Execute a full outer join reconciliation query to detect any row count, key, or value mismatches.

```sql
WITH reconciliation AS (
  SELECT
    COALESCE(l.cntrct_id, m.cntrct_id) AS cntrct_id,
    l.business_option AS legacy_business,
    m.business_option AS migrated_business,
    l.sonstige_option AS legacy_sonstige,
    m.sonstige_option AS migrated_sonstige,
    l.gprs_option AS legacy_gprs,
    m.gprs_option AS migrated_gprs
  FROM
    `target_project.target_dataset.legacy_sof_ta_tarifoption` l
  FULL OUTER JOIN
    `target_project.target_dataset.sof_ta_tarifoption` m
  ON
    l.cntrct_id = m.cntrct_id
)
SELECT
  COUNT(*) AS total_rows,
  SUM(IF(legacy_business IS NULL AND migrated_business IS NOT NULL, 1, 0)) AS extra_rows_in_migrated,
  SUM(IF(legacy_business IS NOT NULL AND migrated_business IS NULL, 1, 0)) AS missing_rows_in_migrated,
  SUM(IF(legacy_business != migrated_business, 1, 0)) AS business_mismatches,
  SUM(IF(legacy_sonstige != migrated_sonstige, 1, 0)) AS sonstige_mismatches,
  SUM(IF(legacy_gprs != migrated_gprs, 1, 0)) AS gprs_mismatches
FROM
  reconciliation;
```

### Pass/Fail Criterion
**Pass**:
* `extra_rows_in_migrated` = 0
* `missing_rows_in_migrated` = 0
* `business_mismatches` = 0
* `sonstige_mismatches` = 0
* `gprs_mismatches` = 0

**Fail**: Any value mismatch or row count discrepancy between the legacy and migrated target tables.

---

## Test Case 6: Airflow DAG Orchestration & Error Handling

### Purpose
Verify that the Airflow DAG parses correctly, maintains correct task dependencies, and handles the failure path gracefully when the dynamic source table `sof_ta_bpr_opt_text_<v_datum>` does not exist.

### Setup
Install `pytest` and `apache-airflow` in the local test environment.

### Action
Run the following Pytest suite to validate the DAG structure and execution behavior.

```python
# test_dag_bert_ausd_bp_ta_tarifoption.py
import pytest
from airflow.models import DagBag

def test_dag_loaded():
    """Verify that the DAG is parsed without import errors."""
    dag_bag = DagBag(dag_folder="dags/", include_examples=False)
    dag = dag_bag.get_dag(dag_id="dw_bert_ausd_bp_ta_tarifoption")
    
    assert dag_bag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 1

def test_task_properties():
    """Verify task configurations and operators."""
    dag_bag = DagBag(dag_folder="dags/", include_examples=False)
    dag = dag_bag.get_dag(dag_id="dw_bert_ausd_bp_ta_tarifoption")
    task = dag.get_task("run_tarifoption_aggregation")
    
    assert task.retries == 1
    assert task.email_on_failure is True
```

To test the BigQuery execution failure path (missing dynamic table), execute the BigQuery script when the corresponding dynamic table does not exist:

```sql
-- Ensure metadata points to a non-existent table suffix
DELETE FROM `target_project.target_dataset.dwtk_meldungen` WHERE job_kennung = 'BERT_DROP_TEMP_TABLE';
INSERT INTO `target_project.target_dataset.dwtk_meldungen` (job_kennung, timecreated)
VALUES ('BERT_DROP_TEMP_TABLE', TIMESTAMP('1999-12-31 23:59:59 UTC'));

-- Drop the table if it exists to guarantee failure
DROP TABLE IF EXISTS `target_project.target_dataset.sof_ta_bpr_opt_text_19991231`;
```

Run the BigQuery script.

### Pass/Fail Criterion
**Pass**:
* The Pytest suite passes with 0 errors.
* The BigQuery script fails with a clear, predictable error message (e.g., `Not found: Table target_project:target_dataset.sof_ta_bpr_opt_text_19991231`), triggering the Airflow task failure and alerting mechanisms as designed.

**Fail**: The DAG fails to parse, or the BigQuery script succeeds/fails silently without raising an error when the source table is missing.