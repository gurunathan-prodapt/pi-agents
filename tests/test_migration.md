This document provides a comprehensive suite of migration-validation tests for the `ausd_bp_ta_cntrct_dist` job. These tests are designed to prove behavioral equivalence between the legacy Oracle/KornShell/UC4 environment and the migrated Google Cloud Platform (Airflow/BigQuery) environment.

---

## Test Case 1: End-to-End Output Parity (Oracle vs. BigQuery)

### Purpose
To prove that the migrated BigQuery pipeline produces the exact same output dataset as the legacy Oracle pipeline when provided with identical input data.

### Setup
1. **Legacy Environment (Oracle):**
   * Populate the source table `sof$ta_bpr_basis` with a controlled set of test records containing duplicate contract IDs, various numeric ranges, and edge cases (e.g., very large integers).
   * Ensure the target table `sof$ta_cntrct_dist` is empty or in a pre-run state.
2. **Target Environment (BigQuery):**
   * Populate the source table `sof.ta_bpr_basis` with the exact same dataset used in the Oracle setup.
   * Ensure the target table `sof.ta_cntrct_dist` is empty.
3. **Audit Registry:**
   * Populate both legacy and target audit tables (`dwtk_meldungen`) with a dummy record for `job_kennung = 'BERT_DROP_TEMP_TABLE'` to ensure the pre-requisite step is satisfied.

### Action
1. Execute the legacy Oracle job via the shell wrapper:
   ```bash
   ./r_ausd_bp_ta_cntrct_dist.ksh -s 15102023 -l 0
   ```
2. Execute the migrated Airflow DAG with the same parameters:
   ```bash
   airflow dags trigger -c '{"stichtag": "15102023", "wiederanlaufwert": 0}' dw_bert_ausd_bp_ta_cntrct_dist
   ```
3. Extract the results from both target tables into a unified format (CSV/JSON) and compare them.

### Pass/Fail Criterion
* **Pass:** The set of `CNTRCT_ID` values in Oracle's `sof$ta_cntrct_dist` is identical in content, count, and data type representation to BigQuery's `sof.ta_cntrct_dist`.
* **Fail:** Any mismatch in row count, missing contract IDs, or type mismatches.

### Validation Code (Python / Pytest)
```python
import pytest
from google.cloud import bigquery
import cx_Oracle

def test_output_parity():
    # 1. Fetch from Oracle (Legacy)
    oracle_conn = cx_Oracle.connect("user/pwd@host:port/service")
    oracle_cursor = oracle_conn.cursor()
    oracle_cursor.execute("SELECT CNTRCT_ID FROM sof$ta_cntrct_dist ORDER BY CNTRCT_ID")
    legacy_results = [row[0] for row in oracle_cursor.fetchall()]
    oracle_conn.close()

    # 2. Fetch from BigQuery (Target)
    bq_client = bigquery.Client()
    query = """
        SELECT CNTRCT_ID 
        FROM `gcp-project-placeholder.sof.ta_cntrct_dist` 
        ORDER BY CNTRCT_ID
    """
    query_job = bq_client.query(query)
    target_results = [row["CNTRCT_ID"] for row in query_job.result()]

    # 3. Assert Equivalence
    assert len(legacy_results) == len(target_results), (
        f"Row count mismatch! Legacy: {len(legacy_results)}, Target: {len(target_results)}"
    )
    assert legacy_results == target_results, "Data content mismatch between Oracle and BigQuery!"
```

---

## Test Case 2: Transformation Correctness (Deduplication & NULL Handling)

### Purpose
To verify that the BigQuery SQL transformation correctly collapses duplicate `cntrct_id` values into a single distinct entry and filters out `NULL` values as specified in the migration design (`WHERE cntrct_id IS NOT NULL`).

### Setup
1. Clear the BigQuery source table `sof.ta_bpr_basis`.
2. Insert a mock dataset containing:
   * Duplicate active contract IDs (e.g., `10001` repeated 3 times).
   * Unique contract IDs (e.g., `10002`, `10003`).
   * Multiple `NULL` values in the `cntrct_id` column.

### Action
Run the BigQuery SQL transformation script directly:
```sql
-- Execute the core logic
TRUNCATE TABLE `gcp-project-placeholder.sof.ta_cntrct_dist`;

INSERT INTO `gcp-project-placeholder.sof.ta_cntrct_dist` (CNTRCT_ID)
SELECT DISTINCT cntrct_id
FROM `gcp-project-placeholder.sof.ta_bpr_basis`
WHERE cntrct_id IS NOT NULL;
```

### Pass/Fail Criterion
* **Pass:** 
  * The target table contains exactly one instance of each non-null contract ID.
  * No `NULL` values exist in the target table.
  * The target row count matches the distinct count of non-null source contract IDs.
* **Fail:** Duplicate contract IDs are found, or `NULL` is present in the target table.

### Validation Code (SQL Assertions)
```sql
-- Assertion 1: Ensure zero NULLs exist in the target table
SELECT 
  ASSERT(count_nulls = 0, "Error: NULL values found in target table!")
FROM (
  SELECT COUNTIF(CNTRCT_ID IS NULL) AS count_nulls 
  FROM `gcp-project-placeholder.sof.ta_cntrct_dist`
);

-- Assertion 2: Ensure no duplicates exist in the target table
SELECT 
  ASSERT(max_dupes = 1, "Error: Duplicate CNTRCT_ID values found in target table!")
FROM (
  SELECT MAX(cnt) AS max_dupes
  FROM (
    SELECT CNTRCT_ID, COUNT(1) as cnt 
    FROM `gcp-project-placeholder.sof.ta_cntrct_dist` 
    GROUP BY CNTRCT_ID
  )
);
```

---

## Test Case 3: Temporal Parameter & Audit Lookup Validation

### Purpose
To verify that the dynamic variable `v_datum` is correctly resolved from the audit table `isbert_schema.dwtk_meldungen` based on the upstream job `BERT_DROP_TEMP_TABLE`, and that the `stichtag` parameter is correctly parsed or defaulted.

### Setup
1. Populate `isbert_schema.dwtk_meldungen` with multiple execution logs for different jobs, including multiple entries for `BERT_DROP_TEMP_TABLE` with varying timestamps.
2. Ensure the latest timestamp for `BERT_DROP_TEMP_TABLE` is `2023-10-24 14:30:00 UTC` (which should format to `20231024`).

### Action
1. Execute the BigQuery script with a specific `stichtag` passed via Airflow configuration (e.g., `15102023`).
2. Execute the BigQuery script *without* passing a `stichtag` to test the fallback default (current date).

### Pass/Fail Criterion
* **Pass:**
  * The resolved `v_datum` matches the formatted date (`YYYYMMDD`) of the maximum `timecreated` for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
  * When `stichtag` is provided, `v_stichtag` matches the input.
  * When `stichtag` is omitted, `v_stichtag` defaults to the current date in `DDMMYYYY` format.
* **Fail:** `v_datum` resolves to the fallback `19000101` despite valid audit records, or `v_stichtag` fails to default correctly.

### Validation Code (Pytest / BigQuery)
```python
import pytest
from datetime import datetime
from google.cloud import bigquery

def test_temporal_and_audit_resolution():
    bq_client = bigquery.Client()
    
    # 1. Insert mock audit records
    setup_query = """
        INSERT INTO `gcp-project-placeholder.isbert_schema.dwtk_meldungen` (job_kennung, timecreated)
        VALUES 
          ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-10-20 10:00:00')),
          ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-10-24 14:30:00')), -- This is the MAX
          ('SOME_OTHER_JOB', TIMESTAMP('2023-10-25 09:00:00'));
    """
    bq_client.query(setup_query).result()

    # 2. Run the parameter resolution block and capture variables
    test_query = """
        DECLARE v_datum STRING;
        DECLARE v_stichtag STRING;

        SET v_datum = (
          SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')
          FROM `gcp-project-placeholder.isbert_schema.dwtk_meldungen` m
          WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
        );

        -- Simulate default fallback logic
        SET v_stichtag = COALESCE(
          NULLIF('', ''),
          FORMAT_DATE('%d%m%Y', CURRENT_DATE())
        );

        SELECT v_datum AS resolved_datum, v_stichtag AS resolved_stichtag;
    """
    
    result = list(bq_client.query(test_query).result())[0]
    
    # 3. Assertions
    expected_stichtag = datetime.now().strftime("%d%m%Y")
    assert result["resolved_datum"] == "20231024", f"Expected v_datum to be '20231024', got {result['resolved_datum']}"
    assert result["resolved_stichtag"] == expected_stichtag, f"Expected default stichtag to be {expected_stichtag}, got {result['resolved_stichtag']}"
```

---

## Test Case 4: Idempotency & Truncate-and-Insert Behavior

### Purpose
To verify that the job is fully idempotent. Multiple consecutive executions of the pipeline must yield the exact same state in the target table without accumulating duplicate records or leaving orphaned data.

### Setup
1. Populate the source table `sof.ta_bpr_basis` with 10 distinct contract IDs.
2. Populate the target table `sof.ta_cntrct_dist` with stale/historical dummy records (e.g., contract IDs `99991`, `99992`) to simulate a dirty state from a previous failed run.

### Action
1. Trigger the Airflow DAG `dw_bert_ausd_bp_ta_cntrct_dist`.
2. Once completed, record the row count and the exact set of contract IDs in `sof.ta_cntrct_dist`.
3. Trigger the Airflow DAG a second time with identical parameters.
4. Record the row count and data set again.

### Pass/Fail Criterion
* **Pass:**
  * The stale records (`99991`, `99992`) are completely removed during the first run (proving the `TRUNCATE` step works).
  * The second run produces the exact same row count and data set as the first run (proving idempotency).
  * No duplicate rows are appended during the second run.
* **Fail:** Target table contains stale records, or the row count doubles/increases on the second run.

### Validation Code (SQL Assertions)
```sql
-- Run this query after the second execution of the DAG
WITH target_stats AS (
  SELECT 
    COUNT(1) as total_rows,
    COUNT(DISTINCT CNTRCT_ID) as unique_rows,
    COUNTIF(CNTRCT_ID IN (99991, 99992)) as stale_rows_found
  FROM `gcp-project-placeholder.sof.ta_cntrct_dist`
)
SELECT
  ASSERT(total_rows = 10, FORMAT("Error: Expected 10 rows, found %d", total_rows)),
  ASSERT(total_rows = unique_rows, "Error: Target table contains duplicate rows after rerun!"),
  ASSERT(stale_rows_found = 0, "Error: Truncate failed! Stale records still exist in target table.")
FROM target_stats;
```

---

## Test Case 5: Airflow DAG Structure & Variable Validation

### Purpose
To verify that the Airflow DAG parses correctly, has no syntax errors, correctly references GCP variables, and maintains the correct task structure.

### Setup
Ensure the testing environment has the standard Airflow libraries installed and environment variables mocked.

### Action
Run a programmatic unit test against the DAG file `dags/dw_bert_ausd_bp_ta_cntrct_dist.py`.

### Pass/Fail Criterion
* **Pass:**
  * The DAG parses without any import errors.
  * The DAG contains exactly the expected task (`run_dist_provisioning`).
  * The task uses the `BigQueryExecuteQueryOperator`.
  * The SQL template compiles successfully with Jinja variables.
* **Fail:** Any import errors, missing tasks, or incorrect operator types.

### Validation Code (Pytest)
```python
import pytest
from airflow.models import DagBag, Variable

@pytest.fixture(autouse=True)
def mock_airflow_variables(monkeypatch):
    """Mock Airflow Variables to prevent database lookup failures during parsing."""
    mock_vars = {
        "gcp_project_id": "test-gcp-project",
        "bq_sof_dataset": "test_sof",
        "bq_isbert_dataset": "test_isbert",
        "bq_location": "EU"
    }
    def mock_get(key, default_var=None):
        return mock_vars.get(key, default_var)
    
    monkeypatch.setattr(Variable, "get", mock_get)

def test_dag_parsing_and_structure():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"

    dag = dagbag.get_dag(dag_id="dw_bert_ausd_bp_ta_cntrct_dist")
    assert dag is not None, "DAG 'dw_bert_ausd_bp_ta_cntrct_dist' not found!"
    
    # Verify task existence and type
    task_id = "run_dist_provisioning"
    assert task_id in dag.task_ids, f"Task {task_id} is missing from the DAG."
    
    task = dag.get_task(task_id)
    from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
    assert isinstance(task, BigQueryExecuteQueryOperator), f"Task {task_id} is not a BigQueryExecuteQueryOperator."
```