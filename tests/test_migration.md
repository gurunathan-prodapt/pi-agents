Here is a comprehensive suite of migration-validation tests designed to prove that the migrated BigQuery/Airflow pipeline `ausd_bp_ta_bpr_beschr` is behaviorally equivalent to the legacy Oracle implementation.

---

# Migration Validation Test Suite: `ausd_bp_ta_bpr_beschr`

This test suite is organized into four main categories:
1. **Schema & Structural Assertions** (Validating the target table structure)
2. **Functional Transformation & Edge-Case Tests** (Validating joins, filters, NULL handling, and audit logic)
3. **Idempotency & Operational Tests** (Validating rerun safety and Airflow DAG execution)
4. **End-to-End Output Parity Tests** (Validating 100% data alignment between Oracle and BigQuery)

---

## Section 1: Schema & Structural Assertions

### Test Case 1.1: Target Table Schema Validation
* **Purpose**: Verify that the target table `sof_ta_bpr_beschr` is created with the correct BigQuery data types, nullability, and column names, matching the design specification (converting Oracle `$` to `_`).
* **Setup**: Ensure the target table has been deployed to the test environment (`gcp-bert-prod.isbert_schema.sof_ta_bpr_beschr`).
* **Action**: Query the BigQuery `INFORMATION_SCHEMA.COLUMNS` view to assert the schema structure.
* **Pass/Fail Criterion**: The query must return exactly 0 rows. Any mismatch in column names, data types, or nullability will fail the test.

```sql
WITH expected_schema AS (
  SELECT 'BPR_ID' AS column_name, 'INT64' AS data_type, 'NO' AS is_nullable UNION ALL
  SELECT 'PDS_DESCRIPTION', 'STRING', 'YES'
)
SELECT 
  actual.column_name AS actual_col,
  actual.data_type AS actual_type,
  actual.is_nullable AS actual_null,
  expected.column_name AS exp_col,
  expected.data_type AS exp_type,
  expected.is_nullable AS exp_null
FROM 
  `gcp-bert-prod.isbert_schema.INFORMATION_SCHEMA.COLUMNS` actual
FULL OUTER JOIN 
  expected_schema expected
ON 
  actual.column_name = expected.column_name
WHERE 
  actual.table_name = 'sof_ta_bpr_beschr'
  AND (
    actual.data_type != expected.data_type 
    OR actual.is_nullable != expected.is_nullable
    OR actual.column_name IS NULL 
    OR expected.column_name IS NULL
  );
```

---

## Section 2: Functional Transformation & Edge-Case Tests

These tests use isolated, sandboxed test tables to verify the transformation logic without polluting production data.

### Test Case 2.1: Filter Logic (`modified_at IS NULL` and `is_production = 1`)
* **Purpose**: Prove that only active production base products (`is_production = 1`) that have not been modified/deprecated (`modified_at IS NULL`) are loaded into the target table.
* **Setup**: Populate sandboxed source tables with test cases covering all permutations of the filter criteria.
* **Action**: Run the transformation query targeting the sandboxed tables.
* **Pass/Fail Criterion**: Only the record with `is_production = 1` and `modified_at IS NULL` must exist in the target table.

```sql
-- 1. Setup Sandboxed Test Tables
CREATE OR REPLACE TABLE `gcp-bert-prod.isbert_schema.test_pds_ta_bpr` AS
SELECT 101 AS bpr_id, CAST(NULL AS TIMESTAMP) AS modified_at, 1 AS is_production, 1001 AS pds_description_id UNION ALL -- Should Pass
SELECT 102 AS bpr_id, TIMESTAMP('2023-10-27 12:00:00') AS modified_at, 1 AS is_production, 1002 AS pds_description_id UNION ALL -- Fail (modified_at is NOT NULL)
SELECT 103 AS bpr_id, CAST(NULL AS TIMESTAMP) AS modified_at, 0 AS is_production, 1003 AS pds_description_id UNION ALL -- Fail (is_production is 0)
SELECT 104 AS bpr_id, TIMESTAMP('2023-10-27 12:00:00') AS modified_at, 0 AS is_production, 1004 AS pds_description_id; -- Fail (Both fail)

CREATE OR REPLACE TABLE `gcp-bert-prod.isbert_schema.test_pds_ta_care_description` AS
SELECT 1001 AS pds_description_id, 'Valid Active Product' AS pds_description UNION ALL
SELECT 1002 AS pds_description_id, 'Modified Product' AS pds_description UNION ALL
SELECT 1003 AS pds_description_id, 'Non-Production Product' AS pds_description UNION ALL
SELECT 1004 AS pds_description_id, 'Modified Non-Prod Product' AS pds_description;

CREATE OR REPLACE TABLE `gcp-bert-prod.isbert_schema.test_sof_ta_bpr_beschr` (
  BPR_ID INT64 NOT NULL,
  PDS_DESCRIPTION STRING
);

-- 2. Action: Run Transformation
INSERT INTO `gcp-bert-prod.isbert_schema.test_sof_ta_bpr_beschr` (BPR_ID, PDS_DESCRIPTION)
SELECT bp.bpr_id, dbp.pds_description
FROM `gcp-bert-prod.isbert_schema.test_pds_ta_bpr` bp
INNER JOIN `gcp-bert-prod.isbert_schema.test_pds_ta_care_description` dbp
  ON bp.pds_description_id = dbp.pds_description_id
WHERE bp.modified_at IS NULL AND bp.is_production = 1;

-- 3. Assertion
ASSERT (
  SELECT COUNT(*) FROM `gcp-bert-prod.isbert_schema.test_sof_ta_bpr_beschr`
) = 1 
AS "Error: Filter logic failed to restrict records correctly.";

ASSERT (
  SELECT BPR_ID FROM `gcp-bert-prod.isbert_schema.test_sof_ta_bpr_beschr`
) = 101 
AS "Error: Incorrect record was loaded.";
```

### Test Case 2.2: Inner Join Integrity & Missing Descriptions
* **Purpose**: Verify that the `INNER JOIN` correctly drops base products that do not have a matching description ID in `pds_ta_care_description` (standard relational integrity check).
* **Setup**: Insert a base product that has a `pds_description_id` not present in the description table.
* **Action**: Run the transformation query.
* **Pass/Fail Criterion**: The orphaned base product must be excluded from the target table.

```sql
-- 1. Setup Sandboxed Test Tables
CREATE OR REPLACE TABLE `gcp-bert-prod.isbert_schema.test_pds_ta_bpr` AS
SELECT 201 AS bpr_id, CAST(NULL AS TIMESTAMP) AS modified_at, 1 AS is_production, 9999 AS pds_description_id; -- 9999 does not exist in description table

CREATE OR REPLACE TABLE `gcp-bert-prod.isbert_schema.test_pds_ta_care_description` AS
SELECT 1001 AS pds_description_id, 'Some Description' AS pds_description;

CREATE OR REPLACE TABLE `gcp-bert-prod.isbert_schema.test_sof_ta_bpr_beschr` (
  BPR_ID INT64 NOT NULL,
  PDS_DESCRIPTION STRING
);

-- 2. Action: Run Transformation
INSERT INTO `gcp-bert-prod.isbert_schema.test_sof_ta_bpr_beschr` (BPR_ID, PDS_DESCRIPTION)
SELECT bp.bpr_id, dbp.pds_description
FROM `gcp-bert-prod.isbert_schema.test_pds_ta_bpr` bp
INNER JOIN `gcp-bert-prod.isbert_schema.test_pds_ta_care_description` dbp
  ON bp.pds_description_id = dbp.pds_description_id
WHERE bp.modified_at IS NULL AND bp.is_production = 1;

-- 3. Assertion
ASSERT (
  SELECT COUNT(*) FROM `gcp-bert-prod.isbert_schema.test_sof_ta_bpr_beschr`
) = 0 
AS "Error: Inner join allowed an orphaned base product to be loaded.";
```

### Test Case 2.3: Audit Metadata Extraction (`dwtk_meldungen`)
* **Purpose**: Verify that the query correctly extracts the audit date from `dwtk_meldungen` and handles the fallback to `'19000101'` when no matching record exists.
* **Setup**: Test both scenarios: (A) when `job_kennung = 'BERT_DROP_TEMP_TABLE'` exists, and (B) when it does not.
* **Action**: Run the script's variable assignment block and capture the output.
* **Pass/Fail Criterion**: Scenario A must return the formatted timestamp of the max `timecreated`. Scenario B must return `'19000101'`.

```sql
-- Scenario A: Record Exists
CREATE OR REPLACE TABLE `gcp-bert-prod.isbert_schema.test_dwtk_meldungen` AS
SELECT 'BERT_DROP_TEMP_TABLE' AS job_kennung, TIMESTAMP('2023-10-27 15:30:00 UTC') AS timecreated UNION ALL
SELECT 'BERT_DROP_TEMP_TABLE' AS job_kennung, TIMESTAMP('2023-10-28 09:15:00 UTC') AS timecreated UNION ALL
SELECT 'OTHER_JOB' AS job_kennung, TIMESTAMP('2023-10-29 10:00:00 UTC') AS timecreated;

DECLARE v_datum_a STRING;
SET v_datum_a = (
  SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')
  FROM `gcp-bert-prod.isbert_schema.test_dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

ASSERT v_datum_a = '20231028' AS "Error: Audit date extraction failed to find the maximum timestamp.";

-- Scenario B: No Record Exists
TRUNCATE TABLE `gcp-bert-prod.isbert_schema.test_dwtk_meldungen`;

DECLARE v_datum_b STRING;
SET v_datum_b = (
  SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')
  FROM `gcp-bert-prod.isbert_schema.test_dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

ASSERT v_datum_b = '19000101' AS "Error: Audit date fallback failed to default to 19000101.";
```

---

## Section 3: Idempotency & Operational Tests

### Test Case 3.1: Rerun Safety (Truncate-Load Pattern)
* **Purpose**: Prove that running the SQL script multiple times sequentially results in the exact same target state without duplicating rows or leaving orphaned records.
* **Setup**: Populate source tables with 5 valid records.
* **Action**: Run the target SQL script twice.
* **Pass/Fail Criterion**: The target table must contain exactly 5 records after the first run, and exactly 5 records after the second run.

```sql
-- 1. Setup Source Data
CREATE OR REPLACE TABLE `gcp-bert-prod.isbert_schema.test_pds_ta_bpr` AS
SELECT x AS bpr_id, CAST(NULL AS TIMESTAMP) AS modified_at, 1 AS is_production, x*10 AS pds_description_id
FROM UNNEST(GENERATE_ARRAY(1, 5)) x;

CREATE OR REPLACE TABLE `gcp-bert-prod.isbert_schema.test_pds_ta_care_description` AS
SELECT x*10 AS pds_description_id, FORMAT("Product Description %d", x) AS pds_description
FROM UNNEST(GENERATE_ARRAY(1, 5)) x;

-- Initialize Target Table
CREATE OR REPLACE TABLE `gcp-bert-prod.isbert_schema.test_sof_ta_bpr_beschr` (
  BPR_ID INT64 NOT NULL,
  PDS_DESCRIPTION STRING
);

-- 2. Action: Run Load 1
TRUNCATE TABLE `gcp-bert-prod.isbert_schema.test_sof_ta_bpr_beschr`;
INSERT INTO `gcp-bert-prod.isbert_schema.test_sof_ta_bpr_beschr` (BPR_ID, PDS_DESCRIPTION)
SELECT bp.bpr_id, dbp.pds_description
FROM `gcp-bert-prod.isbert_schema.test_pds_ta_bpr` bp
INNER JOIN `gcp-bert-prod.isbert_schema.test_pds_ta_care_description` dbp ON bp.pds_description_id = dbp.pds_description_id
WHERE bp.modified_at IS NULL AND bp.is_production = 1;

ASSERT (SELECT COUNT(*) FROM `gcp-bert-prod.isbert_schema.test_sof_ta_bpr_beschr`) = 5 
AS "Error: First run did not load exactly 5 records.";

-- 3. Action: Run Load 2 (Simulating a rerun)
TRUNCATE TABLE `gcp-bert-prod.isbert_schema.test_sof_ta_bpr_beschr`;
INSERT INTO `gcp-bert-prod.isbert_schema.test_sof_ta_bpr_beschr` (BPR_ID, PDS_DESCRIPTION)
SELECT bp.bpr_id, dbp.pds_description
FROM `gcp-bert-prod.isbert_schema.test_pds_ta_bpr` bp
INNER JOIN `gcp-bert-prod.isbert_schema.test_pds_ta_care_description` dbp ON bp.pds_description_id = dbp.pds_description_id
WHERE bp.modified_at IS NULL AND bp.is_production = 1;

-- 4. Assertion
ASSERT (SELECT COUNT(*) FROM `gcp-bert-prod.isbert_schema.test_sof_ta_bpr_beschr`) = 5 
AS "Error: Idempotency check failed. Rerun caused duplicate or missing records.";
```

### Test Case 3.2: Airflow DAG Compilation & Parameter Validation
* **Purpose**: Verify that the migrated Airflow DAG compiles without syntax errors, correctly resolves environment variables, and generates the expected SQL payload.
* **Setup**: A Python environment with `pytest` and `apache-airflow` installed.
* **Action**: Execute a unit test using `pytest` to parse the DAG and check its structure.
* **Pass/Fail Criterion**: The DAG must load with no import errors, contain the correct task dependency chain, and render the SQL template with the correct project and dataset variables.

```python
# Save this file as tests/test_dag_ausd_bp_ta_bpr_beschr.py
import pytest
from airflow.models import DagBag, Variable
from airflow.models.connection import Connection
from airflow import settings

@pytest.fixture(scope="session", autouse=True)
def setup_airflow_env():
    """Set up mock Airflow variables and connections for testing."""
    settings.configure_vars()
    Variable.set("gcp_project_id", "gcp-bert-test")
    Variable.set("bq_dataset_isbert", "isbert_schema_test")

def test_dag_loads_with_no_errors():
    """Verify that the DAG compiles without import errors."""
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"
    
    dag = dag_bag.get_dag(dag_id="ausd_bp_ta_bpr_beschr")
    assert dag is not None
    assert len(dag.tasks) == 3

def test_dag_structure():
    """Verify the task dependency chain."""
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    dag = dag_bag.get_dag(dag_id="ausd_bp_ta_bpr_beschr")
    
    start_task = dag.get_task("start_process")
    transform_task = dag.get_task("execute_bpr_transform")
    end_task = dag.get_task("end_process")
    
    assert transform_task in start_task.downstream_list
    assert end_task in transform_task.downstream_list

def test_sql_generation():
    """Verify that the SQL builder function injects variables correctly."""
    from dags.ausd_bp_ta_bpr_beschr import build_transform_sql
    
    sql = build_transform_sql(project_id="gcp-bert-test", dataset="isbert_schema_test")
    
    # Assert that variables are correctly interpolated
    assert "`gcp-bert-test.isbert_schema_test.sof_ta_bpr_beschr`" in sql
    assert "`gcp-bert-test.isbert_schema_test.pds_ta_bpr`" in sql
    assert "`gcp-bert-test.isbert_schema_test.pds_ta_care_description`" in sql
    assert "TRUNCATE TABLE" in sql
```

---

## Section 4: End-to-End Output Parity Tests

### Test Case 4.1: Side-by-Side Reconciliation (Oracle vs. BigQuery)
* **Purpose**: Prove that the migrated BigQuery pipeline produces the exact same output dataset as the legacy Oracle pipeline when run against the same source data snapshot.
* **Setup**: 
  1. Extract the output of the legacy Oracle job run on a specific date into a temporary BigQuery table `gcp-bert-prod.isbert_schema.legacy_oracle_sof_ta_bpr_beschr`.
  2. Ensure the BigQuery staging tables (`pds_ta_bpr`, `pds_ta_care_description`) represent the exact same snapshot of the Oracle source tables at that run time.
* **Action**: Run the migrated BigQuery SQL script to populate `gcp-bert-prod.isbert_schema.sof_ta_bpr_beschr`. Run a symmetric difference (`EXCEPT DISTINCT`) query between the legacy output and the new BigQuery output.
* **Pass/Fail Criterion**: Both `EXCEPT DISTINCT` queries must return 0 rows, proving 100% data parity.

```sql
-- 1. Check for rows in BigQuery target that do not exist in Oracle legacy target
WITH bq_minus_oracle AS (
  SELECT BPR_ID, PDS_DESCRIPTION 
  FROM `gcp-bert-prod.isbert_schema.sof_ta_bpr_beschr`
  EXCEPT DISTINCT
  SELECT BPR_ID, PDS_DESCRIPTION 
  FROM `gcp-bert-prod.isbert_schema.legacy_oracle_sof_ta_bpr_beschr`
),

-- 2. Check for rows in Oracle legacy target that do not exist in BigQuery target
oracle_minus_bq AS (
  SELECT BPR_ID, PDS_DESCRIPTION 
  FROM `gcp-bert-prod.isbert_schema.legacy_oracle_sof_ta_bpr_beschr`
  EXCEPT DISTINCT
  SELECT BPR_ID, PDS_DESCRIPTION 
  FROM `gcp-bert-prod.isbert_schema.sof_ta_bpr_beschr`
)

-- 3. Combine results
SELECT 'In BQ but not in Oracle' AS discrepancy_type, * FROM bq_minus_oracle
UNION ALL
SELECT 'In Oracle but not in BQ' AS discrepancy_type, * FROM oracle_minus_bq;
```

### Test Case 4.2: Row Count and Nullability Assertions
* **Purpose**: Ensure that the total row count matches exactly and that no critical fields (`BPR_ID`) contain unexpected NULL values.
* **Setup**: Run the migrated pipeline on production-replicated data.
* **Action**: Execute validation queries to check row counts and NULL constraints.
* **Pass/Fail Criterion**: 
  * Row count of target table must equal the count of matched active records in the source tables.
  * Zero records in the target table must have a NULL `BPR_ID`.

```sql
-- Assertion A: Row Count Match
DECLARE v_source_count INT64;
DECLARE v_target_count INT64;

SET v_source_count = (
  SELECT COUNT(*)
  FROM `gcp-bert-prod.isbert_schema.pds_ta_bpr` bp
  INNER JOIN `gcp-bert-prod.isbert_schema.pds_ta_care_description` dbp
    ON bp.pds_description_id = dbp.pds_description_id
  WHERE bp.modified_at IS NULL AND bp.is_production = 1;
);

SET v_target_count = (
  SELECT COUNT(*) FROM `gcp-bert-prod.isbert_schema.sof_ta_bpr_beschr`
);

ASSERT v_source_count = v_target_count 
AS FORMAT("Row count mismatch! Source: %d, Target: %d", v_source_count, v_target_count);

-- Assertion B: Nullability Check on Primary Key
ASSERT (
  SELECT COUNT(*) 
  FROM `gcp-bert-prod.isbert_schema.sof_ta_bpr_beschr` 
  WHERE BPR_ID IS NULL
) = 0 
AS "Error: Found NULL values in REQUIRED column BPR_ID.";
```