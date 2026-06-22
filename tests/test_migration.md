As a senior data-migration QA engineer, I have reviewed the migration design document and the generated Airflow DAG and BigQuery SQL for `DW.BERT_AUSD_V_TA_CNTRCT_CRS2`. Below are the migration validation tests designed to ensure behavioral equivalence, transformation correctness, and data integrity.

---

## Migration Validation Tests for DW.BERT_AUSD_V_TA_CNTRCT_CRS2

### 1. Output Parity

#### Test Case 1.1: Full Data Parity (End-to-End)

*   **Purpose:** To verify that, given identical source data, the migrated BigQuery job produces an identical target table to the legacy Oracle job. This is the ultimate check for behavioral equivalence.
*   **Setup:**
    1.  Ensure that the source tables (`sof$ta_cntrct_crs`, `isbert_schema.dwtk_meldungen` in Oracle; `project_id.source_dataset.ta_cntrct_crs`, `project_id.source_dataset.dwtk_meldungen` in BigQuery) contain an identical, representative dataset. This dataset should include various scenarios for `cntrct_ty`, `cntrct_parent`, and `contract_number` to thoroughly test the join and filter logic.
    2.  Execute the legacy Oracle job (`DW.BERT_AUSD_V_TA_CNTRCT_CRS2`) to populate `sof$ta_cntrct_crs2`.
    3.  Execute the migrated Airflow DAG (`dw_bert_ausd_v_ta_cntrct_crs2`) to populate `project_id.dwh_dataset.ta_cntrct_crs2`.
*   **Action:**
    1.  Extract all data from the legacy target table (`sof$ta_cntrct_crs2`) into a canonical format (e.g., CSV, JSON, or a temporary table).
    2.  Extract all data from the migrated target table (`project_id.dwh_dataset.ta_cntrct_crs2`) into the same canonical format.
    3.  Compare the two datasets row by row, column by column. A robust comparison can be done using SQL `EXCEPT` or by hashing rows.
*   **Pass/Fail Criterion:**
    *   **Pass:** The two datasets are identical in terms of row count, column values, and data types (allowing for BigQuery's equivalent types). No differences are found.
    *   **Fail:** Any discrepancy in row count or data values indicates a migration issue.

```sql
-- Example SQL for BigQuery comparison (assuming Oracle data is loaded into a temp BQ table 'legacy_ta_cntrct_crs2')
-- This assumes column names and types are identical or compatible.

-- Check for rows in migrated table not in legacy
SELECT 'Migrated_Only' AS source, * FROM `project_id.dwh_dataset.ta_cntrct_crs2`
EXCEPT DISTINCT
SELECT 'Legacy_Only' AS source, * FROM `project_id.temp_dataset.legacy_ta_cntrct_crs2`;

-- Check for rows in legacy table not in migrated
SELECT 'Legacy_Only' AS source, * FROM `project_id.temp_dataset.legacy_ta_cntrct_crs2`
EXCEPT DISTINCT
SELECT 'Migrated_Only' AS source, * FROM `project_id.dwh_dataset.ta_cntrct_crs2`;

-- Pass if both queries return 0 rows.
```

### 2. Transformation Correctness

#### Test Case 2.1: `LEFT OUTER JOIN` and `cr.cntrct_ty` Filter Logic

*   **Purpose:** To verify that the BigQuery translation of Oracle's `(+)` outer join and the associated `cr.cntrct_ty (+) = 10` filter condition correctly handles parent contracts and their types.
*   **Setup:**
    1.  Populate `project_id.source_dataset.ta_cntrct_crs` with specific test data covering the following scenarios for `c.cntrct_ty <> 10`:
        *   **Scenario A:** `c.cntrct_parent` matches `cr.cntrct_id` where `cr.cntrct_ty = 10`. (Expected: `RV_NUM` populated with `cr.contract_number`).
        *   **Scenario B:** `c.cntrct_parent` matches `cr.cntrct_id` where `cr.cntrct_ty <> 10`. (Expected: `RV_NUM` is NULL).
        *   **Scenario C:** `c.cntrct_parent` is NULL. (Expected: `RV_NUM` is NULL).
        *   **Scenario D:** `c.cntrct_parent` does not match any `cr.cntrct_id`. (Expected: `RV_NUM` is NULL).
    2.  Ensure `dwtk_meldungen` has data for `get_v_datum` task, though `v_datum` is not used in the main SQL.
    3.  Execute the migrated Airflow DAG.
*   **Action:**
    1.  Query `project_id.dwh_dataset.ta_cntrct_crs2` to retrieve `cntrct_id`, `cntrct_parent`, `cntrct_ty`, and `RV_NUM` for the test data.
    2.  Assert the `RV_NUM` values against the expected outcomes for each scenario.
*   **Pass/Fail Criterion:**
    *   **Pass:** `RV_NUM` values in the target table precisely match the expected outcomes for all test scenarios.
    *   **Fail:** Any mismatch indicates an incorrect translation of the join or filter logic.

```sql
-- Example Pytest with SQL assertions (conceptual)
import pytest
from google.cloud import bigquery

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client()

def test_join_and_filter_logic(bq_client):
    target_table = "`your-gcp-dwh-project-id.dwh_dataset.ta_cntrct_crs2`"

    # Scenario A: Parent exists and is type 10
    query_a = f"""
    SELECT RV_NUM FROM {target_table}
    WHERE cntrct_id = 'CONTRACT_A_ID' AND cntrct_ty <> 10
    """
    rows_a = list(bq_client.query(query_a).result())
    assert len(rows_a) == 1
    assert rows_a[0].RV_NUM == 'PARENT_A_CONTRACT_NUMBER'

    # Scenario B: Parent exists but is not type 10
    query_b = f"""
    SELECT RV_NUM FROM {target_table}
    WHERE cntrct_id = 'CONTRACT_B_ID' AND cntrct_ty <> 10
    """
    rows_b = list(bq_client.query(query_b).result())
    assert len(rows_b) == 1
    assert rows_b[0].RV_NUM is None

    # Scenario C: cntrct_parent is NULL
    query_c = f"""
    SELECT RV_NUM FROM {target_table}
    WHERE cntrct_id = 'CONTRACT_C_ID' AND cntrct_ty <> 10
    """
    rows_c = list(bq_client.query(query_c).result())
    assert len(rows_c) == 1
    assert rows_c[0].RV_NUM is None

    # Scenario D: cntrct_parent does not exist
    query_d = f"""
    SELECT RV_NUM FROM {target_table}
    WHERE cntrct_id = 'CONTRACT_D_ID' AND cntrct_ty <> 10
    """
    rows_d = list(bq_client.query(query_d).result())
    assert len(rows_d) == 1
    assert rows_d[0].RV_NUM is None
```

#### Test Case 2.2: `c.cntrct_ty <> 10` Filter

*   **Purpose:** To verify that contracts with `cntrct_ty = 10` from the primary `c` alias are correctly excluded from the output.
*   **Setup:**
    1.  Populate `project_id.source_dataset.ta_cntrct_crs` with at least one row where `cntrct_ty = 10`.
    2.  Execute the migrated Airflow DAG.
*   **Action:**
    1.  Query `project_id.dwh_dataset.ta_cntrct_crs2` to check for any rows where `cntrct_ty = 10`.
*   **Pass/Fail Criterion:**
    *   **Pass:** The query returns 0 rows, confirming no contracts of type 10 from the primary source are included in the target.
    *   **Fail:** Any row found with `cntrct_ty = 10` indicates an incorrect filter application.

```sql
SELECT COUNT(*) FROM `your-gcp-dwh-project-id.dwh_dataset.ta_cntrct_crs2`
WHERE cntrct_ty = 10;
-- Expected result: 0
```

#### Test Case 2.3: NULL Handling for `cntrct_parent`

*   **Purpose:** To verify that when `c.cntrct_parent` is NULL, the `RV_NUM` column in the target table is also NULL, as expected from a `LEFT OUTER JOIN` where no match is found.
*   **Setup:**
    1.  Populate `project_id.source_dataset.ta_cntrct_crs` with rows where `cntrct_parent` is explicitly NULL, and `cntrct_ty <> 10`.
    2.  Execute the migrated Airflow DAG.
*   **Action:**
    1.  Query `project_id.dwh_dataset.ta_cntrct_crs2` for rows corresponding to the setup data.
    2.  Check the `RV_NUM` column for these rows.
*   **Pass/Fail Criterion:**
    *   **Pass:** For all rows where the original `c.cntrct_parent` was NULL, the `RV_NUM` column in the target table is NULL.
    *   **Fail:** If `RV_NUM` contains a non-NULL value for such rows, it indicates an issue with NULL handling in the join.

```sql
SELECT COUNT(*) FROM `your-gcp-dwh-project-id.dwh_dataset.ta_cntrct_crs2`
WHERE cntrct_parent IS NULL AND RV_NUM IS NOT NULL;
-- Expected result: 0
```

#### Test Case 2.4: Data Type Fidelity

*   **Purpose:** To ensure that data types are preserved or correctly cast during migration, preventing data loss, truncation, or unexpected behavior.
*   **Setup:**
    1.  Obtain the precise schema (column names, data types, nullability, lengths/precision) of `sof$ta_cntrct_crs` and `sof$ta_cntrct_crs2` in Oracle.
    2.  Populate `project_id.source_dataset.ta_cntrct_crs` with test data that pushes the boundaries of each column's data type (e.g., maximum string lengths, min/max date values, large/small numbers, NULLs).
    3.  Execute the migrated Airflow DAG.
*   **Action:**
    1.  Compare the BigQuery schema of `project_id.dwh_dataset.ta_cntrct_crs2` against the Oracle schema of `sof$ta_cntrct_crs2`.
    2.  Query the migrated target table and inspect the data values for the boundary test cases.
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   The BigQuery target table schema (column names, order, and data types) is a faithful and functionally equivalent representation of the Oracle source schema.
        *   All data values, especially boundary cases, are preserved without truncation, loss of precision, or unexpected type conversion errors.
    *   **Fail:** Any schema mismatch that impacts data integrity or any data value corruption indicates a data type handling issue.

```sql
-- Example SQL to check a specific column's type and value
-- (This would be part of a larger schema/data validation script)
SELECT
    column_name,
    data_type
FROM
    `your-gcp-dwh-project-id.dwh_dataset.INFORMATION_SCHEMA.COLUMNS`
WHERE
    table_name = 'ta_cntrct_crs2' AND column_name = 'valid_from';
-- Expected: data_type should be DATE or TIMESTAMP, matching Oracle's type.

-- Example to check for string truncation (if original was VARCHAR2(50), new is STRING)
SELECT COUNT(*) FROM `your-gcp-dwh-project-id.dwh_dataset.ta_cntrct_crs2`
WHERE LENGTH(some_string_column) > 50; -- Assuming original max length was 50
-- Expected result: 0
```

### 3. External-System Replacements

#### Test Case 3.1: `TRUNCATE TABLE` Replacement

*   **Purpose:** To verify that the BigQuery DDL `TRUNCATE TABLE` executed by the `truncate_target_table` PythonOperator correctly replaces the Oracle `DWPA_UTIL_SKRIPT.runstatement` call for table truncation.
*   **Setup:**
    1.  Manually insert a few dummy rows into `project_id.dwh_dataset.ta_cntrct_crs2`.
    2.  Run the Airflow DAG, but only up to and including the `truncate_target_table` task.
*   **Action:**
    1.  After the `truncate_target_table` task completes, query `project_id.dwh_dataset.ta_cntrct_crs2` to count its rows.
*   **Pass/Fail Criterion:**
    *   **Pass:** The table `project_id.dwh_dataset.ta_cntrct_crs2` contains 0 rows.
    *   **Fail:** If the table still contains rows, the truncation mechanism is not working as expected.

```sql
SELECT COUNT(*) FROM `your-gcp-dwh-project-id.dwh_dataset.ta_cntrct_crs2`;
-- Expected result after 'truncate_target_table' task: 0
```

#### Test Case 3.2: `v_datum` Retrieval Replacement

*   **Purpose:** To verify that the `get_v_datum` PythonOperator correctly fetches the `v_datum` value from `dwtk_meldungen` and pushes it to Airflow XCom.
*   **Setup:**
    1.  Populate `project_id.source_dataset.dwtk_meldungen` with specific test data:
        *   One row with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and `timecreated` set to a known date (e.g., `2023-03-15 10:00:00 UTC`).
        *   Other rows with different `job_kennung` or earlier `timecreated` values.
        *   A scenario where no rows match `job_kennung = 'BERT_DROP_TEMP_TABLE'` to test the '19000101' default.
    2.  Run the Airflow DAG, but only up to and including the `get_v_datum` task.
*   **Action:**
    1.  Inspect the Airflow UI for the `get_v_datum` task's XComs.
    2.  Retrieve the value associated with the key `'v_datum'`.
*   **Pass/Fail Criterion:**
    *   **Pass:** The `v_datum` value retrieved from XCom matches the expected `YYYYMMDD` format of the `MAX(timecreated)` for `job_kennung = 'BERT_DROP_TEMP_TABLE'` (e.g., '20230315'). If no matching rows, it should be '19000101'.
    *   **Fail:** Any discrepancy in the fetched value or if the XCom is not populated indicates an issue with the `get_v_datum` task.

```python
# Example Pytest for Airflow XCom (requires Airflow testing setup)
import pytest
from airflow.models import DagBag
from airflow.utils.session import provide_session
from airflow.utils.state import State
from airflow.utils.db import create_session
from airflow.models import XCom

@pytest.fixture(scope="module")
def dag_bag():
    return DagBag(dag_folder='path/to/your/dags', include_examples=False)

@provide_session
def test_get_v_datum_xcom(dag_bag, session):
    dag_id = 'dw_bert_ausd_v_ta_cntrct_crs2'
    dag = dag_bag.get_dag(dag_id)
    assert dag is not None

    # Simulate running the task
    task = dag.get_task('get_v_datum')
    ti = task.create_task_instance(session=session, execution_date=dag.start_date)
    ti.run(session=session)

    # Check XCom value
    v_datum_value = XCom.get_one(
        ti.execution_date,
        key='v_datum',
        task_id='get_v_datum',
        dag_id=dag_id,
        session=session
    )
    # Assert against expected value based on your setup data
    assert v_datum_value == '20230315' # Or '19000101' if no data
```

#### Test Case 3.3: DB-Link `@pcrs1` Resolution (Implicit)

*   **Purpose:** To confirm that the assumption in the design document (that `sof$ta_cntrct_crs` already contains all necessary data, including what `@pcrs1` might have provided) holds true, and no direct DB-link replacement is needed in BigQuery.
*   **Setup:** This is primarily a design validation and is implicitly covered by other tests. The key is to ensure that the BigQuery source table `project_id.source_dataset.ta_cntrct_crs` is a complete and accurate replica of the Oracle `sof$ta_cntrct_crs`, including any data that the Oracle job might have pulled via `@pcrs1`.
*   **Action:**
    1.  Perform a thorough data profiling and comparison between the Oracle `sof$ta_cntrct_crs` and BigQuery `project_id.source_dataset.ta_cntrct_crs` tables.
    2.  Execute the full migrated job.
*   **Pass/Fail Criterion:**
    *   **Pass:** The data profiling confirms that `project_id.source_dataset.ta_cntrct_crs` is a faithful and complete representation of `sof$ta_cntrct_crs`. The full data parity test (Test 1.1) passes, indicating no data loss or discrepancies related to the `@pcrs1` dependency.
    *   **Fail:** If data profiling reveals missing data in the BigQuery source table that was present in Oracle (and potentially sourced via `@pcrs1`), or if Test 1.1 fails due to such discrepancies, then the `@pcrs1` dependency was not correctly resolved.

### 4. Data-Quality / Row-Count / Schema Assertions

#### Test Case 4.1: Row Count Parity

*   **Purpose:** To verify that the total number of rows in the target table is identical between the legacy and migrated jobs, ensuring no rows are unexpectedly added or dropped.
*   **Setup:**
    1.  Execute the legacy Oracle job.
    2.  Execute the migrated Airflow DAG.
*   **Action:**
    1.  Count the rows in `sof$ta_cntrct_crs2` (Oracle).
    2.  Count the rows in `project_id.dwh_dataset.ta_cntrct_crs2` (BigQuery).
*   **Pass/Fail Criterion:**
    *   **Pass:** The row counts from both target tables are exactly equal.
    *   **Fail:** Any difference in row count indicates a data loss or unexpected data generation.

```sql
-- BigQuery
SELECT COUNT(*) FROM `your-gcp-dwh-project-id.dwh_dataset.ta_cntrct_crs2`;
-- Compare this count to the count from Oracle's sof$ta_cntrct_crs2
```

#### Test Case 4.2: Schema Parity

*   **Purpose:** To verify that the schema (column names, data types, nullability) of the target table is identical between the legacy and migrated systems.
*   **Setup:**
    1.  Obtain the schema definition for `sof$ta_cntrct_crs2` from Oracle.
    2.  Obtain the schema definition for `project_id.dwh_dataset.ta_cntrct_crs2` from BigQuery.
*   **Action:**
    1.  Compare column names, their order, data types (allowing for BigQuery equivalents), and nullability constraints.
*   **Pass/Fail Criterion:**
    *   **Pass:** Column names, their logical order, and data types (or their BigQuery functional equivalents) match. Nullability constraints are also consistent.
    *   **Fail:** Any discrepancy in schema that could lead to data integrity issues or application breaks.

```sql
-- BigQuery SQL to retrieve schema information
SELECT
    column_name,
    data_type,
    is_nullable
FROM
    `your-gcp-dwh-project-id.dwh_dataset.INFORMATION_SCHEMA.COLUMNS`
WHERE
    table_name = 'ta_cntrct_crs2'
ORDER BY
    ordinal_position;
-- Compare this output to the Oracle schema definition.
```

#### Test Case 4.3: Data Integrity (NULLs, Duplicates)

*   **Purpose:** To ensure that the migration does not introduce unexpected NULLs in critical columns or duplicate primary keys.
*   **Setup:**
    1.  Execute the migrated Airflow DAG.
*   **Action:**
    1.  Check for unexpected NULLs in columns that are expected to be NOT NULL (e.g., `cntrct_id`).
    2.  Check for duplicate `cntrct_id` values in the target table, assuming `cntrct_id` is a unique identifier.
    3.  Verify that `RV_NUM` is NULL when `c.cntrct_parent` does not match any `cr.cntrct_id` with `cr.cntrct_ty = 10` (already covered in 2.1, but good to re-emphasize).
*   **Pass/Fail Criterion:**
    *   **Pass:** No unexpected NULLs are found in non-nullable columns. No duplicate `cntrct_id` values are found.
    *   **Fail:** Presence of unexpected NULLs or duplicate primary keys indicates a data integrity issue.

```sql
-- Check for NULLs in cntrct_id (assuming it's NOT NULL)
SELECT COUNT(*) FROM `your-gcp-dwh-project-id.dwh_dataset.ta_cntrct_crs2`
WHERE cntrct_id IS NULL;
-- Expected result: 0

-- Check for duplicate cntrct_id (assuming it's unique)
SELECT cntrct_id, COUNT(*)
FROM `your-gcp-dwh-project-id.dwh_dataset.ta_cntrct_crs2`
GROUP BY cntrct_id
HAVING COUNT(*) > 1;
-- Expected result: 0 rows
```

#### Test Case 4.4: `v_datum` Unused Variable Check (Design Validation)

*   **Purpose:** To confirm that the `v_datum` variable, while fetched by the `get_v_datum` task, is indeed *not* used in the main SQL transformation (`d_ausd_v_ta_cntrct_crs2.bqsql`), as observed in the provided migrated code. This is a check against potential missing logic in the migration if the original Oracle SQL *did* use it.
*   **Setup:**
    1.  Review the `d_ausd_v_ta_cntrct_crs2.bqsql` file.
    2.  Review the `dw_bert_ausd_v_ta_cntrct_crs2_dag.py` file.
*   **Action:**
    1.  Manually inspect the `d_ausd_v_ta_cntrct_crs2.bqsql` content for any Jinja templating variables like `{{ params.v_datum }}` or any direct SQL references to a `v_datum` variable.
    2.  Confirm that the `execute_main_sql` task in the DAG does not explicitly pass `v_datum` as a parameter to the SQL if it's not templated.
*   **Pass/Fail Criterion:**
    *   **Pass:** The `d_ausd_v_ta_cntrct_crs2.bqsql` file does *not* contain any reference to `v_datum` or `params.v_datum`. This confirms that the `get_v_datum` task, while present, is not currently feeding a parameter to the main SQL. (If the original Oracle SQL *did* use `v_datum` in its `INSERT...SELECT` and this test passes, it indicates a critical functional gap in the migration that needs to be addressed.)
    *   **Fail:** If `v_datum` *is* referenced in the SQL, then the `get_v_datum` task is correctly feeding it, and the design document's SQL extract was incomplete in showing its usage. This would not be a migration bug, but a documentation/design discrepancy.

---