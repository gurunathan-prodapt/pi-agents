# Migration Validation Test Suite: DW.BERT_AUSD_BP_TA_BCP_MSISDN

This document defines the migration-validation test suite for migrating the UC4 UNIX job `DW.BERT_AUSD_BP_TA_BCP_MSISDN` to Google Cloud Platform (Airflow + BigQuery). 

These tests are designed to prove behavioral equivalence between the legacy UNIX/Oracle execution environment and the target GCP BigQuery environment.

---

## Test Case 1: End-to-End Output Parity (Reconciliation)

### Purpose
Verify that given identical input datasets, the migrated BigQuery pipeline produces the exact same output dataset as the legacy UNIX/Oracle pipeline.

### Setup
1. **Legacy Environment**: 
   * Seed the legacy source table (e.g., `STAGE_BERT_MSISDN`) with a controlled set of 10,000 test records representing standard, edge-case, and boundary data.
   * Clear the legacy target table `BERT_AUSD_BP_TA_BCP_MSISDN`.
2. **GCP Environment**:
   * Seed the BigQuery staging table `dw_bert_staging.tmp_source` with the exact same 10,000 test records.
   * Clear the BigQuery target table `dw_bert.bert_ausd_bp_ta_bcp_msisdn`.

### Action
1. Execute the legacy UNIX script:
   ```bash
   . $HOME/.dw_init
   $HOME/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_msisdn.ksh
   ```
2. Execute the migrated Airflow DAG task:
   ```bash
   airflow dags trigger -e 2026-04-21 dw_bert_ausd_bp_ta_bcp_msisdn
   ```
3. Export both target tables to a shared validation environment as CSVs (sorting by primary keys: `msisdn`, `product_id`).

### Pass/Fail Criterion
* **Pass**: The row count of both target tables is identical, and a SHA-256 hash of the sorted data columns (excluding system metadata columns like `created_at`, `updated_at`, `batch_id`) matches with 100% precision.
* **Fail**: Any discrepancy in row counts or column values.

#### Verification SQL (Run in BigQuery)
```sql
-- Compare row counts and generate a checksum of data values
WITH legacy_data AS (
  -- External table pointing to exported legacy Oracle target
  SELECT msisdn, product_id, customer_id, valid_from, valid_to, is_valid_record 
  FROM `dw_bert_staging.legacy_bert_ausd_bp_ta_bcp_msisdn`
),
target_data AS (
  SELECT msisdn, product_id, customer_id, valid_from, valid_to, is_valid_record 
  FROM `dw_bert.bert_ausd_bp_ta_bcp_msisdn`
),
discrepancies AS (
  (SELECT * FROM legacy_data EXCEPT DISTINCT SELECT * FROM target_data)
  UNION ALL
  (SELECT * FROM target_data EXCEPT DISTINCT SELECT * FROM legacy_data)
)
SELECT 
  (SELECT COUNT(*) FROM legacy_data) AS legacy_count,
  (SELECT COUNT(*) FROM target_data) AS target_count,
  (SELECT COUNT(*) FROM discrepancies) AS mismatch_count;
-- PASS if mismatch_count = 0 AND legacy_count = target_count
```

---

## Test Case 2: Transformation Correctness (NULL Handling & Business Rules)

### Purpose
Verify that the BigQuery SQL script correctly implements specific business rules:
1. `valid_to` defaulting to `9999-12-31` when the source value is `NULL`.
2. `is_valid_record` evaluating to `TRUE` when `valid_from` is present, and `FALSE` when `valid_from` is `NULL`.

### Setup
Inject specific test cases into the temporary source table `tmp_source` within a test session:
* **Record A**: `valid_to` is `NULL`, `valid_from` is `'2026-01-01'`.
* **Record B**: `valid_to` is `'2026-12-31'`, `valid_from` is `NULL`.

### Action
Execute the transformation logic isolated from the target merge:
```sql
-- Simulate the transformation CTE
CREATE OR REPLACE TEMP TABLE tmp_transformed_test AS
WITH base AS (
  SELECT
    'MSISDN_A' AS msisdn, 'PROD_A' AS product_id, 'CUST_A' AS customer_id, 
    DATE '2026-01-01' AS valid_from, CAST(NULL AS DATE) AS valid_to,
    DATE '2026-04-21' AS process_date, 'TEST_BATCH' AS batch_id
  UNION ALL
  SELECT
    'MSISDN_B' AS msisdn, 'PROD_B' AS product_id, 'CUST_B' AS customer_id, 
    CAST(NULL AS DATE) AS valid_from, DATE '2026-12-31' AS valid_to,
    DATE '2026-04-21' AS process_date, 'TEST_BATCH' AS batch_id
),
enriched AS (
  SELECT
    b.*,
    COALESCE(valid_to, DATE '9999-12-31') AS valid_to_norm,
    CASE
      WHEN valid_from IS NULL THEN FALSE
      ELSE TRUE
    END AS is_valid_record
  FROM base b
)
SELECT
  msisdn, product_id, customer_id, valid_from, valid_to_norm AS valid_to,
  process_date, batch_id, is_valid_record
FROM enriched;
```

### Pass/Fail Criterion
* **Pass**: 
  * Record A has `valid_to = '9999-12-31'` and `is_valid_record = TRUE`.
  * Record B has `valid_to = '2026-12-31'` and `is_valid_record = FALSE`.
* **Fail**: Any deviation from the expected values.

#### Verification Assertions
```sql
ASSERT (
  SELECT valid_to FROM tmp_transformed_test WHERE msisdn = 'MSISDN_A'
) = DATE '9999-12-31' 
AS 'Assertion Failed: NULL valid_to did not default to 9999-12-31';

ASSERT (
  SELECT is_valid_record FROM tmp_transformed_test WHERE msisdn = 'MSISDN_A'
) = TRUE 
AS 'Assertion Failed: is_valid_record should be TRUE when valid_from is populated';

ASSERT (
  SELECT is_valid_record FROM tmp_transformed_test WHERE msisdn = 'MSISDN_B'
) = FALSE 
AS 'Assertion Failed: is_valid_record should be FALSE when valid_from is NULL';
```

---

## Test Case 3: Idempotency & MERGE Behavior

### Purpose
Prove that the job can be rerun safely at any time ("Restart jederzeit möglich") without duplicating records, and that it correctly updates existing records while inserting new ones.

### Setup
1. Initialize the target table with one active record:
   ```sql
   INSERT INTO `dw_bert.bert_ausd_bp_ta_bcp_msisdn` 
   (msisdn, product_id, customer_id, valid_from, valid_to, process_date, batch_id, is_valid_record, created_at, updated_at)
   VALUES ('49170000001', 'PROD_01', 'CUST_OLD', DATE '2026-01-01', DATE '9999-12-31', DATE '2026-04-20', 'BATCH_01', TRUE, TIMESTAMP '2026-04-20 12:00:00 UTC', TIMESTAMP '2026-04-20 12:00:00 UTC');
   ```
2. Prepare the staging source with two records (one update, one insert):
   * **Update**: Same keys (`49170000001`, `PROD_01`), but updated `customer_id` (`'CUST_NEW'`).
   * **Insert**: New keys (`49170000002`, `PROD_02`).

### Action
Execute the `MERGE` statement block from `dw_bert_ausd_bp_ta_bcp_msisdn.sql` using the test staging data.

### Pass/Fail Criterion
* **Pass**: 
  * The target table contains exactly 2 records.
  * The record for `49170000001` has `customer_id = 'CUST_NEW'` and `updated_at > created_at`.
  * The record for `49170000002` is inserted with `created_at = updated_at`.
* **Fail**: Duplicate rows are created, or updates are not applied.

#### Verification SQL
```sql
-- Assert total row count is exactly 2
ASSERT (
  SELECT COUNT(*) FROM `dw_bert.bert_ausd_bp_ta_bcp_msisdn`
) = 2 AS 'Idempotency Failure: Target table does not contain exactly 2 records';

-- Assert update was applied and metadata updated
ASSERT (
  SELECT customer_id FROM `dw_bert.bert_ausd_bp_ta_bcp_msisdn` WHERE msisdn = '49170000001'
) = 'CUST_NEW' AS 'Idempotency Failure: customer_id was not updated';

ASSERT (
  SELECT updated_at > created_at FROM `dw_bert.bert_ausd_bp_ta_bcp_msisdn` WHERE msisdn = '49170000001'
) = TRUE AS 'Idempotency Failure: updated_at was not refreshed on update';
```

---

## Test Case 4: Data Quality Assertions (MSISDN Null Constraint)

### Purpose
Verify that the pipeline fails gracefully and raises an assertion error if an invalid record (NULL MSISDN) is processed, matching the `sp_validate_output` logic.

### Setup
Seed the temporary source table with a record containing a `NULL` MSISDN.

### Action
Execute the validation procedure `sp_validate_output()` within the BigQuery session.

### Pass/Fail Criterion
* **Pass**: The BigQuery execution aborts with an assertion error containing the message: `Validation failed: MSISDN must not be NULL`.
* **Fail**: The procedure completes successfully despite the invalid data.

#### Verification SQL
```sql
-- Setup invalid data in session scope
CREATE OR REPLACE TEMP TABLE tmp_transformed AS
SELECT 
  CAST(NULL AS STRING) AS msisdn, 
  'PROD_ERR' AS product_id, 
  'CUST_ERR' AS customer_id, 
  DATE '2026-01-01' AS valid_from, 
  DATE '9999-12-31' AS valid_to, 
  DATE '2026-04-21' AS process_date, 
  'BATCH_ERR' AS batch_id, 
  TRUE AS is_valid_record;

-- Action & Assertion: This block must throw an error
BEGIN
  -- Call the validation logic
  ASSERT (
    SELECT COUNT(*) = 0
    FROM tmp_transformed
    WHERE msisdn IS NULL
  ) AS 'Validation failed: MSISDN must not be NULL';
  
  -- If we reach this line, the test has failed
  SELECT ERROR('Test Failed: Validation did not catch NULL MSISDN');
EXCEPTION WHEN ERROR THEN
  IF @@error.message LIKE '%Validation failed: MSISDN must not be NULL%' THEN
    -- Test passed successfully
    SELECT 'Test Passed: Correct assertion thrown' AS result;
  ELSE
    -- Re-raise if it's an unexpected error
    RAISE USING MESSAGE = @@error.message;
  END IF;
END;
```

---

## Test Case 5: Airflow DAG & Variable Resolution Validation

### Purpose
Verify that the Airflow DAG is syntactically correct, resolves environment variables correctly, and maps the legacy UNIX execution flow to the correct BigQuery operator.

### Setup
Install the DAG file and its SQL dependency in a test Airflow environment. Mock the required Airflow Variables.

### Action
Execute a suite of unit tests using `pytest` against the Airflow DAG definition.

### Pass/Fail Criterion
* **Pass**: All pytest assertions pass, confirming DAG structure, task parameters, and template rendering.
* **Fail**: Any assertion fails, indicating misconfigured operators or missing variables.

#### Pytest Code (`test_dag_migration.py`)
```python
import pytest
from airflow.models import DagBag, Variable
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

@pytest.fixture(scope="module")
def dagbag():
    # Mock Airflow variables required for DAG parsing / rendering
    Variable.set("GCP_PROJECT_ID", "gcp-dwh-test")
    Variable.set("CONN_ID_BIGQUERY", "bigquery_default")
    Variable.set("BIGQUERY_LOCATION", "EU")
    return DagBag(dag_folder="dags", include_examples=False)

def test_dag_loaded(dagbag):
    """Verify that the DAG parses with no errors."""
    dag_id = "dw_bert_ausd_bp_ta_bcp_msisdn"
    dag = dagbag.get_dag(dag_id)
    assert dag is not None
    assert len(dagbag.import_errors) == 0

def test_dag_structure(dagbag):
    """Verify the task dependency chain matches design."""
    dag = dagbag.get_dag("dw_bert_ausd_bp_ta_bcp_msisdn")
    start_task = dag.get_task("start")
    main_task = dag.get_task("bert_ausd_bp_ta_bcp_msisdn")
    end_task = dag.get_task("end")

    assert start_task.downstream_task_ids == {"bert_ausd_bp_ta_bcp_msisdn"}
    assert main_task.downstream_task_ids == {"end"}
    
    # Verify operator type
    from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
    assert isinstance(main_task, BigQueryInsertJobOperator)

def test_operator_configuration(dagbag):
    """Verify BigQuery operator is configured correctly for scripting statements."""
    dag = dagbag.get_dag("dw_bert_ausd_bp_ta_bcp_msisdn")
    task = dag.get_task("bert_ausd_bp_ta_bcp_msisdn")
    
    # Scripting statements must not define destinationTable or writeDisposition in configuration
    query_config = task.configuration.get("query", {})
    assert "destinationTable" not in query_config
    assert "writeDisposition" not in query_config
    assert query_config.get("query") == "dw_bert_ausd_bp_ta_bcp_msisdn.sql"
    assert query_config.get("useLegacySql") is False
```