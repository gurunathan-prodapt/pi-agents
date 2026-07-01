# Migration Validation Test Suite: `ausd_bp_ta_bpr_opt_text`

This document defines the migration-validation test suite to prove behavioral equivalence between the legacy Oracle PL/SQL execution and the migrated Google Cloud Composer (Airflow) / BigQuery implementation for the job `ausd_bp_ta_bpr_opt_text`.

---

## Test Case 1: Schema and Structure Validation

### Purpose
Verify that the target table structure in BigQuery matches the legacy Oracle table structure (with the specified name translation of `$` to `_`) in terms of column names, data types, and nullability.

### Setup
*   Access to the legacy Oracle database schema containing `sof$ta_bpr_opt_text`.
*   Access to the target BigQuery dataset containing `sof_ta_bpr_opt_text`.

### Action
Execute a comparison script that queries the metadata catalog of both systems and asserts structural equivalence.

```python
import pytest
from google.cloud import bigquery
import cx_Oracle  # Or alternative Oracle driver

def test_schema_equivalence():
    # 1. Fetch BigQuery Schema
    bq_client = bigquery.Client()
    bq_table = bq_client.get_table("your_project.your_dataset.sof_ta_bpr_opt_text")
    bq_columns = {field.name.upper(): field.field_type for field in bq_table.schema}

    # 2. Expected Schema Mapping (Oracle to BigQuery)
    # Oracle: CNTRCT_ID (NUMBER) -> BQ: INTEGER/INT64
    # Oracle: BPR_ID (NUMBER) -> BQ: INTEGER/INT64
    # Oracle: PDS_DESCRIPTION (VARCHAR2) -> BQ: STRING
    expected_schema = {
        "CNTRCT_ID": "INTEGER",
        "BPR_ID": "INTEGER",
        "PDS_DESCRIPTION": "STRING"
    }

    # 3. Assertions
    for col_name, expected_type in expected_schema.items():
        assert col_name in bq_columns, f"Column {col_name} missing in BigQuery target table."
        
        # Normalize INT64/INTEGER naming variations
        actual_type = bq_columns[col_name]
        if expected_type == "INTEGER" and actual_type == "INT64":
            actual_type = "INTEGER"
            
        assert actual_type == expected_type, f"Column {col_name} type mismatch. Expected {expected_type}, got {actual_type}."
```

### Pass/Fail Criterion
*   **Pass**: All target columns exist in BigQuery with compatible data types matching the legacy specification.
*   **Fail**: Any column is missing, misspelled, or has an incompatible data type.

---

## Test Case 2: Output Parity (Golden Dataset Comparison)

### Purpose
Prove that running the migrated BigQuery SQL on a snapshot of legacy source data produces the exact same output as the legacy Oracle PL/SQL run.

### Setup
1.  Prepare a "Golden Dataset" containing a representative sample of records (including edge cases, special characters, and nulls) in the source tables:
    *   `sof_ta_bpr_optionen` / `sof$ta_bpr_optionen`
    *   `sof_ta_bpr_beschr` / `sof$ta_bpr_beschr`
2.  Populate these tables in both a test Oracle schema and a test BigQuery dataset.
3.  Clear the target tables in both environments.

### Action
1.  Run the legacy Oracle PL/SQL script `d_ausd_bp_ta_bpr_opt_text.sql`.
2.  Run the migrated BigQuery SQL script `gcp/bigquery/sql/d_ausd_bp_ta_bpr_opt_text.sql`.
3.  Extract the contents of both target tables, sort them by primary keys (`CNTRCT_ID`, `BPR_ID`), and compare them.

```python
import pandas as pd
from google.cloud import bigquery
import sqlalchemy

def test_golden_dataset_parity():
    # 1. Extract from Oracle Target
    oracle_engine = sqlalchemy.create_engine("oracle+cx_oracle://user:pass@host:port/db")
    oracle_df = pd.read_sql(
        "SELECT CNTRCT_ID, BPR_ID, PDS_DESCRIPTION FROM sof$ta_bpr_opt_text ORDER BY CNTRCT_ID, BPR_ID", 
        con=oracle_engine
    )

    # 2. Extract from BigQuery Target
    bq_client = bigquery.Client()
    query = """
        SELECT CNTRCT_ID, BPR_ID, PDS_DESCRIPTION 
        FROM `your_project.your_dataset.sof_ta_bpr_opt_text` 
        ORDER BY CNTRCT_ID, BPR_ID
    """
    bq_df = bq_client.query(query).to_dataframe()

    # 3. Assert Equivalence
    pd.testing.assert_frame_equal(
        oracle_df.reset_index(drop=True), 
        bq_df.reset_index(drop=True), 
        check_dtype=False,
        obj="Oracle vs BigQuery Target Dataframe"
    )
```

### Pass/Fail Criterion
*   **Pass**: The dataframes are identical in row count, column values, and ordering.
*   **Fail**: Any differences in row counts, column values, or string encodings are detected.

---

## Test Case 3: Transformation Correctness (Inner Join & Null Handling)

### Purpose
Verify that the inner join logic between `sof_ta_bpr_optionen` and `sof_ta_bpr_beschr` correctly handles:
1.  Matching keys (successful join).
2.  Unmatched keys (filtered out).
3.  `NULL` values in join keys (filtered out, no Cartesian product or errors).

### Setup
Insert the following controlled test data into the BigQuery source tables:

**`sof_ta_bpr_optionen` (bp)**:
| CNTRCT_ID | BPR_ID | Note |
| :--- | :--- | :--- |
| 1001 | 500 | Valid matching record |
| 1002 | 600 | Unmatched BPR_ID |
| 1003 | NULL | NULL BPR_ID |

**`sof_ta_bpr_beschr` (bs)**:
| BPR_ID | PDS_DESCRIPTION | Note |
| :--- | :--- | :--- |
| 500 | 'Option 500 Description' | Valid matching record |
| 700 | 'Option 700 Description' | Unmatched BPR_ID |
| NULL | 'NULL Description' | NULL BPR_ID |

### Action
1.  Execute the BigQuery SQL script.
2.  Query the target table `sof_ta_bpr_opt_text`.

```sql
-- Validation Query
SELECT CNTRCT_ID, BPR_ID, PDS_DESCRIPTION 
FROM `your_project.your_dataset.sof_ta_bpr_opt_text`;
```

### Pass/Fail Criterion
*   **Pass**: The target table contains exactly **one** row:
    *   `1001`, `500`, `'Option 500 Description'`
*   **Fail**: Any of the following occur:
    *   Unmatched or NULL key records are loaded.
    *   The query fails due to NULL comparison errors.
    *   Duplicate rows are generated.

---

## Test Case 4: Idempotency and Restartability (Truncate Validation)

### Purpose
Verify that the job is fully idempotent and can be safely restarted or run multiple times on the same day without duplicating or corrupting data.

### Setup
*   Populate source tables with initial test data.
*   Ensure target table contains pre-existing data from a previous run.

### Action
1.  Execute the BigQuery SQL script.
2.  Record the row count of the target table ($N_1$).
3.  Without changing the source data, execute the BigQuery SQL script a second time.
4.  Record the row count of the target table ($N_2$).
5.  Modify one source record, execute the BigQuery SQL script a third time, and verify the modification is reflected without duplicating rows.

```python
def test_idempotency(bq_client):
    dataset_ref = "your_project.your_dataset"
    
    # Run 1
    job_1 = bq_client.query(f"CALL `{dataset_ref}.run_migration_procedure`()") # or direct script execution
    job_1.result()
    count_1 = list(bq_client.query(f"SELECT COUNT(*) FROM `{dataset_ref}.sof_ta_bpr_opt_text`").result())[0][0]
    
    # Run 2 (Immediate restart)
    job_2 = bq_client.query(f"CALL `{dataset_ref}.run_migration_procedure`()")
    job_2.result()
    count_2 = list(bq_client.query(f"SELECT COUNT(*) FROM `{dataset_ref}.sof_ta_bpr_opt_text`").result())[0][0]
    
    assert count_1 == count_2, f"Data duplicated on restart. Run 1 count: {count_1}, Run 2 count: {count_2}"
    assert count_1 > 0, "Target table is empty after execution."
```

### Pass/Fail Criterion
*   **Pass**: The target table row count remains identical ($N_1 = N_2$) across consecutive runs, and no duplicate records are created.
*   **Fail**: Row count increases on subsequent runs, indicating a failure of the `TRUNCATE TABLE` step.

---

## Test Case 5: Metadata Audit Trail (`dwtk_meldungen` & `v_datum` handling)

### Purpose
Verify that the calculation of `v_datum` from `dwtk_meldungen` executes without error, handles empty/missing metadata gracefully (coalescing to `'19000101'`), and matches the legacy date extraction logic.

### Setup
Create two test scenarios in the BigQuery environment:
*   **Scenario A**: `dwtk_meldungen` contains valid execution logs for `BERT_DROP_TEMP_TABLE`.
*   **Scenario B**: `dwtk_meldungen` is empty or does not contain any records for `BERT_DROP_TEMP_TABLE`.

### Action
Execute the variable declaration block of the BigQuery SQL script and capture the resolved value of `v_datum`.

```sql
-- Test Script for Scenario A
INSERT INTO `your_project.your_dataset.dwtk_meldungen` (job_kennung, timecreated)
VALUES ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2024-10-24 14:30:00 UTC'));

-- Execute extraction logic
DECLARE v_datum STRING;
SET v_datum = (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(timecreated))), '19000101')
  FROM `your_project.your_dataset.dwtk_meldungen`
  WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
);
SELECT v_datum; -- Expected: '20241024'

-- Test Script for Scenario B (After truncating dwtk_meldungen)
TRUNCATE TABLE `your_project.your_dataset.dwtk_meldungen`;

DECLARE v_datum STRING;
SET v_datum = (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(timecreated))), '19000101')
  FROM `your_project.your_dataset.dwtk_meldungen`
  WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
);
SELECT v_datum; -- Expected: '19000101'
```

### Pass/Fail Criterion
*   **Pass**: 
    *   Scenario A correctly extracts and formats the maximum date as `'20241024'`.
    *   Scenario B gracefully defaults to `'19000101'` without throwing null pointer or query execution errors.
*   **Fail**: The query fails, or returns incorrect date formats.

---

## Test Case 6: Airflow DAG Orchestration & Parameter Resolution

### Purpose
Verify that the Airflow DAG correctly parses environment variables (`GCP_PROJECT_ID`, `BQ_DATASET`), resolves the SQL file path, substitutes variables, and executes the BigQuery operator successfully.

### Setup
*   An Airflow/Cloud Composer environment (local development or test environment).
*   The DAG file `dag_ausd_bp_ta_bpr_opt_text.py` placed in the DAGs folder.

### Action
Run a unit test using the Airflow testing framework to validate DAG structure, task dependencies, and template rendering.

```python
from airflow.models import DagBag

def test_dag_loading_and_dependencies():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag_id = "dag_ausd_bp_ta_bpr_opt_text"
    
    # 1. Assert DAG loads without import errors
    assert dag_id in dagbag.dags, f"DAG {dag_id} failed to load."
    dag = dagbag.get_dag(dag_id)
    assert len(dagbag.import_errors) == 0, f"Import errors detected: {dagbag.import_errors}"

    # 2. Assert Task Structure
    build_task = dag.get_task("build_sql_query")
    bq_task = dag.get_task("process_basisprodukte_bq")
    
    assert build_task.downstream_task_ids == {"process_basisprodukte_bq"}
    assert bq_task.upstream_task_ids == {"build_sql_query"}

def test_sql_variable_resolution():
    from dags.dag_ausd_bp_ta_bpr_opt_text import resolve_sql_query
    from unittest.mock import MagicMock

    # Mock Airflow Context
    context = {
        "dag_run": MagicMock()
    }
    context["dag_run"].conf = {
        "gcp_project_id": "test-project-123",
        "target_dataset": "test_dataset_abc"
    }

    # Execute resolution
    resolved_sql = resolve_sql_query(**context)

    # Assert placeholders are replaced
    assert "${gcp_project_id}" not in resolved_sql
    assert "${target_dataset}" not in resolved_sql
    assert "test-project-123" in resolved_sql
    assert "test_dataset_abc" in resolved_sql
```

### Pass/Fail Criterion
*   **Pass**: The DAG loads cleanly, task dependencies are correctly configured, and SQL parameters are successfully resolved from the Airflow context configuration.
*   **Fail**: Import errors are thrown, tasks are misaligned, or SQL variable substitution leaves unresolved placeholders.