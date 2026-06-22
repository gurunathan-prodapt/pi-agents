The following migration validation tests are designed to ensure the `DW.BERT_AUSD_V_TA_CNTRCT_CRS2` job, migrated to Google Cloud Platform, is behaviourally equivalent to its legacy Oracle counterpart. These tests cover output parity, transformation correctness, external system replacements, and data quality assertions.

---

### Test Setup Prerequisites

Before running any tests, ensure the following:

1.  **BigQuery Tables Created**:
    *   `isbert_schema.dwtk_meldungen`
    *   `sof_ta_cntrct_crs`
    *   `sof_ta_cntrct_crs2` (target table, initially empty)
    *   All tables should have schemas matching their Oracle counterparts, including column names, data types, and nullability.
2.  **GCS Deployment**:
    *   The PySpark script (`r_ausd_v_ta_cntrct_crs2.py`) is uploaded to `gs://YOUR_BUCKET_NAME/dataproc_jobs/`.
    *   The BigQuery SQL script (`d_ausd_v_ta_cntrct_crs2_bq.sql`) is uploaded to `gs://YOUR_BUCKET_NAME/dataproc_jobs/`.
3.  **Airflow DAG Deployed**:
    *   The `dw_bert_ausd_v_ta_cntrct_crs2_dag.py` is deployed to your Cloud Composer environment.
    *   All GCP placeholders (`GCP_PROJECT_ID`, `GCP_DATAPROC_REGION`, `GCP_DATAPROC_CLUSTER_NAME`, `GCS_BUCKET_NAME`) in the DAG are correctly configured.
4.  **Test Data Management**: For each test, source tables will be populated with specific data. It's crucial to clear and re-populate source tables between test runs to ensure isolation.

---

### Test Case 1: Schema Validation of Target Table

**Purpose:** Verify that the target BigQuery table `sof_ta_cntrct_crs2` has the correct schema (column names, data types, and nullability) as expected from the migration design and the legacy Oracle table.

**Setup:**
1.  Ensure the `sof_ta_cntrct_crs2` table exists in BigQuery.
2.  Obtain the schema definition of the legacy `sof$ta_cntrct_crs2` table from Oracle.

**Action:**
1.  Query the schema of the BigQuery table `sof_ta_cntrct_crs2`.

**Expected Result:**
The BigQuery table schema should precisely match the legacy Oracle table schema, including:
*   All expected column names.
*   Corresponding BigQuery data types (e.g., `NUMBER` to `INT64` or `BIGNUMERIC`, `VARCHAR2` to `STRING`, `DATE` to `DATE` or `TIMESTAMP`).
*   Correct nullability constraints for each column.

**Pass/Fail Criterion:**
The BigQuery schema for `sof_ta_cntrct_crs2` is identical to the legacy Oracle schema in terms of column names, data types, and nullability.

**Test Code (Conceptual SQL Assertion):**

```sql
-- Query BigQuery schema
SELECT
    column_name,
    data_type,
    is_nullable
FROM
    `YOUR_GCP_PROJECT_ID`.sof_ta_cntrct_crs2.INFORMATION_SCHEMA.COLUMNS
WHERE
    table_name = 'sof_ta_cntrct_crs2'
ORDER BY
    ordinal_position;

-- Compare this output manually or programmatically with the Oracle schema:
-- SELECT COLUMN_NAME, DATA_TYPE, NULLABLE FROM ALL_TAB_COLUMNS WHERE OWNER = 'YOUR_SCHEMA' AND TABLE_NAME = 'SOF$TA_CNTRCT_CRS2';
```

---

### Test Case 2: Row Count Parity - Empty Source Tables

**Purpose:** Verify that when source tables are empty, the migrated job correctly results in an empty target table, demonstrating proper handling of empty inputs and the `TRUNCATE` operation.

**Setup:**
1.  Clear all data from `sof_ta_cntrct_crs`.
2.  Clear all data from `isbert_schema.dwtk_meldungen`.
3.  Clear all data from `sof_ta_cntrct_crs2`.

**Action:**
1.  Trigger the Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs2`.

**Expected Result:**
The `sof_ta_cntrct_crs2` table should remain empty after the job execution.

**Pass/Fail Criterion:**
The row count of `sof_ta_cntrct_crs2` is 0.

**Test Code (Pytest with BigQuery client):**

```python
import pytest
from google.cloud import bigquery

PROJECT_ID = "YOUR_GCP_PROJECT_ID"
TARGET_TABLE = f"{PROJECT_ID}.your_dataset.sof_ta_cntrct_crs2"
SOURCE_TABLE_CRS = f"{PROJECT_ID}.your_dataset.sof_ta_cntrct_crs"
SOURCE_TABLE_MELDUNGEN = f"{PROJECT_ID}.isbert_schema.dwtk_meldungen"

@pytest.fixture(scope="module", autouse=True)
def setup_empty_tables():
    client = bigquery.Client(project=PROJECT_ID)
    client.query(f"TRUNCATE TABLE `{SOURCE_TABLE_CRS}`").result()
    client.query(f"TRUNCATE TABLE `{SOURCE_TABLE_MELDUNGEN}`").result()
    client.query(f"TRUNCATE TABLE `{TARGET_TABLE}`").result()
    yield
    # Teardown (optional): clear tables after tests
    client.query(f"TRUNCATE TABLE `{SOURCE_TABLE_CRS}`").result()
    client.query(f"TRUNCATE TABLE `{SOURCE_TABLE_MELDUNGEN}`").result()
    client.query(f"TRUNCATE TABLE `{TARGET_TABLE}`").result()

def test_empty_source_tables_result_in_empty_target(setup_empty_tables):
    client = bigquery.Client(project=PROJECT_ID)

    # Trigger Airflow DAG (conceptual step, would be an API call or manual trigger)
    # For automated testing, you might mock the DAG run or use Airflow's REST API
    print("Triggering Airflow DAG 'dw_bert_ausd_v_ta_cntrct_crs2'...")
    # Assume DAG execution completes here.

    # Verify target table is empty
    query_job = client.query(f"SELECT COUNT(*) FROM `{TARGET_TABLE}`")
    result = query_job.result()
    row_count = [row[0] for row in result][0]
    assert row_count == 0, f"Expected target table to be empty, but found {row_count} rows."
```

---

### Test Case 3: Row Count Parity - Full Source Data

**Purpose:** Verify that the total number of rows inserted into the target table matches the expected count from the legacy job's output, ensuring no rows are unexpectedly dropped or duplicated.

**Setup:**
1.  Populate `sof_ta_cntrct_crs` with a diverse set of test data, including:
    *   Contracts with `cntrct_ty <> 10` and `cntrct_parent` matching a `cntrct_ty = 10` parent.
    *   Contracts with `cntrct_ty <> 10` and `cntrct_parent` matching a `cntrct_ty <> 10` parent.
    *   Contracts with `cntrct_ty <> 10` and `cntrct_parent` as NULL.
    *   Contracts with `cntrct_ty = 10` (these should be filtered out).
2.  Populate `isbert_schema.dwtk_meldungen` with at least one record where `job_kennung = 'BERT_DROP_TEMP_TABLE'` and a `timecreated` value.
3.  Clear `sof_ta_cntrct_crs2`.
4.  Determine the expected row count by running the legacy Oracle job with the same data and counting the output rows, or by manually calculating based on the transformation logic.

**Action:**
1.  Trigger the Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs2`.

**Expected Result:**
The `sof_ta_cntrct_crs2` table should contain the exact number of rows as produced by the legacy Oracle job.

**Pass/Fail Criterion:**
The row count of `sof_ta_cntrct_crs2` equals the `expected_row_count_from_legacy`.

**Test Code (Conceptual SQL Assertion):**

```sql
-- After DAG execution:
SELECT COUNT(*) FROM `YOUR_GCP_PROJECT_ID`.your_dataset.sof_ta_cntrct_crs2;

-- Assert this count against the known expected count from legacy.
```

---

### Test Case 4: Data Parity - Full Source Data (Happy Path)

**Purpose:** Verify that for a representative set of source data, the content of the target table `sof_ta_cntrct_crs2` is identical to the output of the legacy Oracle job, ensuring full output parity.

**Setup:**
1.  Use the same diverse dataset as in Test Case 3 for `sof_ta_cntrct_crs` and `isbert_schema.dwtk_meldungen`.
2.  Clear `sof_ta_cntrct_crs2`.
3.  Generate the expected output data for `sof_ta_cntrct_crs2` by running the legacy Oracle job with the same input data and extracting its output. Store this expected output in a temporary BigQuery table or a file.

**Action:**
1.  Trigger the Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs2`.

**Expected Result:**
Every column of every row in `sof_ta_cntrct_crs2` should exactly match the corresponding data in the expected output.

**Pass/Fail Criterion:**
A row-by-row comparison (e.g., using `EXCEPT DISTINCT` or checksums) between `sof_ta_cntrct_crs2` and the expected output table yields no differences.

**Test Code (BigQuery SQL Assertion):**

```sql
-- Assume 'expected_sof_ta_cntrct_crs2' is a temporary table containing the legacy output.
-- This query should return 0 rows if the tables are identical.
SELECT 'Difference Found'
FROM (
    SELECT * FROM `YOUR_GCP_PROJECT_ID`.your_dataset.sof_ta_cntrct_crs2
    EXCEPT DISTINCT
    SELECT * FROM `YOUR_GCP_PROJECT_ID`.your_dataset.expected_sof_ta_cntrct_crs2
)
UNION ALL
SELECT 'Difference Found'
FROM (
    SELECT * FROM `YOUR_GCP_PROJECT_ID`.your_dataset.expected_sof_ta_cntrct_crs2
    EXCEPT DISTINCT
    SELECT * FROM `YOUR_GCP_PROJECT_ID`.your_dataset.sof_ta_cntrct_crs2
);
```

---

### Test Case 5: Transformation Correctness - `c.cntrct_ty <> 10` Filter

**Purpose:** Verify that the main filter `c.cntrct_ty <> 10` correctly excludes contracts of type 10 from the primary source table (`c`).

**Setup:**
1.  Populate `sof_ta_cntrct_crs` with:
    *   Contract A: `cntrct_id=1`, `cntrct_ty=1`
    *   Contract B: `cntrct_id=2`, `cntrct_ty=10`
    *   Contract C: `cntrct_id=3`, `cntrct_ty=11`
2.  Populate `isbert_schema.dwtk_meldungen` as needed.
3.  Clear `sof_ta_cntrct_crs2`.

**Action:**
1.  Trigger the Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs2`.

**Expected Result:**
Only Contract A and Contract C should be present in `sof_ta_cntrct_crs2`. Contract B (with `cntrct_ty = 10`) should be excluded.

**Pass/Fail Criterion:**
`SELECT COUNT(*) FROM sof_ta_cntrct_crs2 WHERE cntrct_id = 2` returns 0.
`SELECT COUNT(*) FROM sof_ta_cntrct_crs2 WHERE cntrct_ty = 10` returns 0.

**Test Code (BigQuery SQL Assertion):**

```sql
-- After DAG execution:
SELECT COUNT(*) FROM `YOUR_GCP_PROJECT_ID`.your_dataset.sof_ta_cntrct_crs2 WHERE cntrct_ty = 10;
-- Expected result: 0

SELECT cntrct_id FROM `YOUR_GCP_PROJECT_ID`.your_dataset.sof_ta_cntrct_crs2 ORDER BY cntrct_id;
-- Expected result: [1, 3]
```

---

### Test Case 6: Transformation Correctness - `LEFT JOIN` with `cr.cntrct_ty = 10` Filter

**Purpose:** Verify the correct behavior of the `LEFT JOIN` and the filter applied to the right-hand side (`cr.cntrct_ty = 10`) for populating the `rv_num` column.

**Setup:**
1.  Populate `sof_ta_cntrct_crs` with:
    *   Contract 1: `cntrct_id=1`, `cntrct_ty=1`, `cntrct_parent=100`
    *   Contract 2: `cntrct_id=2`, `cntrct_ty=2`, `cntrct_parent=200`
    *   Contract 3: `cntrct_id=3`, `cntrct_ty=3`, `cntrct_parent=NULL`
    *   Parent 100: `cntrct_id=100`, `cntrct_ty=10`, `contract_number='P100_NUM'`
    *   Parent 200: `cntrct_id=200`, `cntrct_ty=20`, `contract_number='P200_NUM'`
    *   Parent 300: `cntrct_id=300`, `cntrct_ty=10`, `contract_number='P300_NUM'` (no child refers to this)
2.  Populate `isbert_schema.dwtk_meldungen` as needed.
3.  Clear `sof_ta_cntrct_crs2`.

**Action:**
1.  Trigger the Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs2`.

**Expected Result:**
*   Contract 1 (`cntrct_id=1`) should have `rv_num = 'P100_NUM'` (parent 100 has `cntrct_ty=10`).
*   Contract 2 (`cntrct_id=2`) should have `rv_num = NULL` (parent 200 exists but `cntrct_ty=20`, so the join condition `cr.cntrct_ty = 10` fails for the right side).
*   Contract 3 (`cntrct_id=3`) should have `rv_num = NULL` (no parent).

**Pass/Fail Criterion:**
Assert the `rv_num` values for `cntrct_id` 1, 2, and 3 match the expected values.

**Test Code (BigQuery SQL Assertion):**

```sql
-- After DAG execution:
SELECT cntrct_id, rv_num
FROM `YOUR_GCP_PROJECT_ID`.your_dataset.sof_ta_cntrct_crs2
WHERE cntrct_id IN (1, 2, 3)
ORDER BY cntrct_id;

-- Expected result:
-- cntrct_id | rv_num
-- ----------|---------
-- 1         | P100_NUM
-- 2         | NULL
-- 3         | NULL
```

---

### Test Case 7: Transformation Correctness - `cntrct_parent` NULL Handling

**Purpose:** Verify that contracts with a `NULL` value in `cntrct_parent` are correctly handled by the `LEFT JOIN`, resulting in a `NULL` `rv_num`.

**Setup:**
1.  Populate `sof_ta_cntrct_crs` with:
    *   Contract A: `cntrct_id=1`, `cntrct_ty=1`, `cntrct_parent=NULL`
    *   Contract B: `cntrct_id=2`, `cntrct_ty=2`, `cntrct_parent=NULL`
2.  Populate `isbert_schema.dwtk_meldungen` as needed.
3.  Clear `sof_ta_cntrct_crs2`.

**Action:**
1.  Trigger the Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs2`.

**Expected Result:**
Both Contract A and Contract B should be present in `sof_ta_cntrct_crs2`, and their `rv_num` column should be `NULL`.

**Pass/Fail Criterion:**
`SELECT COUNT(*) FROM sof_ta_cntrct_crs2 WHERE cntrct_parent IS NULL AND rv_num IS NOT NULL` returns 0.

**Test Code (BigQuery SQL Assertion):**

```sql
-- After DAG execution:
SELECT cntrct_id, rv_num
FROM `YOUR_GCP_PROJECT_ID`.your_dataset.sof_ta_cntrct_crs2
WHERE cntrct_id IN (1, 2)
ORDER BY cntrct_id;

-- Expected result:
-- cntrct_id | rv_num
-- ----------|---------
-- 1         | NULL
-- 2         | NULL
```

---

### Test Case 8: Transformation Correctness - `v_datum` Derivation (Max Date)

**Purpose:** Verify that the `v_datum` derivation correctly identifies the maximum `timecreated` for `job_kennung = 'BERT_DROP_TEMP_TABLE'` and formats it as `YYYYMMDD`.

**Setup:**
1.  Populate `isbert_schema.dwtk_meldungen` with:
    *   `job_kennung='BERT_DROP_TEMP_TABLE'`, `timecreated='2023-01-01 10:00:00 UTC'`
    *   `job_kennung='OTHER_JOB'`, `timecreated='2023-01-02 11:00:00 UTC'`
    *   `job_kennung='BERT_DROP_TEMP_TABLE'`, `timecreated='2023-01-03 12:00:00 UTC'` (This should be the max)
2.  Populate `sof_ta_cntrct_crs` with some dummy data to allow the job to run.
3.  Clear `sof_ta_cntrct_crs2`.

**Action:**
1.  Trigger the Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs2`.
    *   *Note*: The `v_datum` variable is declared and used internally within the BigQuery SQL script but not inserted into the target table. Direct assertion of its value in the final output is not possible. This test conceptually validates the `SELECT` statement used for its derivation. If `v_datum` were used in the `INSERT` statement, this test would directly verify its impact on the output. For this test, we assume the `DECLARE` statement's logic is correct if the `SELECT` query it contains is correct.

**Expected Result (Conceptual):**
The `v_datum` variable, if it were inspectable, would hold the value `'20230103'`.

**Pass/Fail Criterion:**
The `SELECT` statement used to derive `v_datum` (when run in isolation) returns the expected maximum formatted date.

**Test Code (BigQuery SQL Assertion for the derivation logic):**

```sql
-- Run this query in isolation to verify the v_datum derivation logic:
SELECT COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101') AS derived_v_datum
FROM `YOUR_GCP_PROJECT_ID`.isbert_schema.dwtk_meldungen m
WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';

-- Expected result: '20230103'
```

---

### Test Case 9: Transformation Correctness - `v_datum` Derivation (No Match/Default)

**Purpose:** Verify that the `v_datum` derivation correctly defaults to `'19000101'` when no matching `job_kennung` is found or when the `dwtk_meldungen` table is empty.

**Setup:**
1.  **Scenario A (No Matching Job)**: Populate `isbert_schema.dwtk_meldungen` with records, but none where `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
2.  **Scenario B (Empty Table)**: Clear all data from `isbert_schema.dwtk_meldungen`.
3.  Populate `sof_ta_cntrct_crs` with some dummy data.
4.  Clear `sof_ta_cntrct_crs2`.

**Action:**
1.  Trigger the Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs2` for each scenario.

**Expected Result (Conceptual):**
The `v_datum` variable, if inspectable, would hold the value `'19000101'` for both scenarios.

**Pass/Fail Criterion:**
The `SELECT` statement used to derive `v_datum` (when run in isolation for each scenario) returns `'19000101'`.

**Test Code (BigQuery SQL Assertion for the derivation logic):**

```sql
-- Scenario A: No matching job_kennung
-- Populate dwtk_meldungen with:
-- job_kennung='OTHER_JOB_1', timecreated='2023-01-01 10:00:00 UTC'
-- job_kennung='OTHER_JOB_2', timecreated='2023-01-02 11:00:00 UTC'
SELECT COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101') AS derived_v_datum
FROM `YOUR_GCP_PROJECT_ID`.isbert_schema.dwtk_meldungen m
WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
-- Expected result: '19000101'

-- Scenario B: Empty dwtk_meldungen table
-- TRUNCATE TABLE `YOUR_GCP_PROJECT_ID`.isbert_schema.dwtk_meldungen;
SELECT COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101') AS derived_v_datum
FROM `YOUR_GCP_PROJECT_ID`.isbert_schema.dwtk_meldungen m
WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
-- Expected result: '19000101'
```

---

### Test Case 10: External System Replacement - Truncate Table Operation

**Purpose:** Verify that the BigQuery `TRUNCATE TABLE` statement correctly replaces the Oracle `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` call, ensuring the target table is cleared before new data is inserted.

**Setup:**
1.  Populate `sof_ta_cntrct_crs2` with 5 dummy rows (e.g., `cntrct_id` 9001-9005).
2.  Populate `sof_ta_cntrct_crs` with 3 valid rows that should be inserted into `sof_ta_cntrct_crs2` (e.g., `cntrct_id` 1, 2, 3).
3.  Populate `isbert_schema.dwtk_meldungen` as needed.

**Action:**
1.  Record the initial row count of `sof_ta_cntrct_crs2`.
2.  Trigger the Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs2`.
3.  Record the final row count of `sof_ta_cntrct_crs2`.

**Expected Result:**
The initial 5 dummy rows should be removed, and only the 3 valid rows from `sof_ta_cntrct_crs` should be present in `sof_ta_cntrct_crs2`. The final row count should be 3.

**Pass/Fail Criterion:**
The final row count of `sof_ta_cntrct_crs2` is 3, and `SELECT COUNT(*) FROM sof_ta_cntrct_crs2 WHERE cntrct_id BETWEEN 9001 AND 9005` returns 0.

**Test Code (BigQuery SQL Assertion):**

```sql
-- Before DAG execution:
INSERT INTO `YOUR_GCP_PROJECT_ID`.your_dataset.sof_ta_cntrct_crs2 (cntrct_id, ...)
VALUES (9001, ...), (9002, ...), (9003, ...), (9004, ...), (9005, ...);
SELECT COUNT(*) FROM `YOUR_GCP_PROJECT_ID`.your_dataset.sof_ta_cntrct_crs2; -- Should be 5

-- After DAG execution:
SELECT COUNT(*) FROM `YOUR_GCP_PROJECT_ID`.your_dataset.sof_ta_cntrct_crs2; -- Should be 3
SELECT COUNT(*) FROM `YOUR_GCP_PROJECT_ID`.your_dataset.sof_ta_cntrct_crs2 WHERE cntrct_id BETWEEN 9001 AND 9005; -- Should be 0
```

---

### Test Case 11: Idempotency Check

**Purpose:** Verify that running the migrated job multiple times with the same source data produces the exact same result in the target table, ensuring idempotency.

**Setup:**
1.  Populate `sof_ta_cntrct_crs` and `isbert_schema.dwtk_meldungen` with a consistent, representative dataset.
2.  Clear `sof_ta_cntrct_crs2`.

**Action:**
1.  Trigger the Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs2` (Run 1).
2.  Calculate a checksum or hash of the entire `sof_ta_cntrct_crs2` table content (or export and compare).
3.  Trigger the Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs2` again (Run 2).
4.  Calculate a checksum or hash of the `sof_ta_cntrct_crs2` table content after Run 2.

**Expected Result:**
The content of `sof_ta_cntrct_crs2` should be identical after both runs.

**Pass/Fail Criterion:**
The checksums/hashes of `sof_ta_cntrct_crs2` after Run 1 and Run 2 are identical. The row count is also identical.

**Test Code (Conceptual BigQuery SQL for checksum):**

```sql
-- After each run, execute this to get a content hash:
SELECT FARM_FINGERPRINT(TO_JSON_STRING(t)) AS table_hash
FROM (
    SELECT *
    FROM `YOUR_GCP_PROJECT_ID`.your_dataset.sof_ta_cntrct_crs2
    ORDER BY cntrct_id, obj_version -- Order by primary key(s) for consistent hashing
) t;

-- Compare the hash values from Run 1 and Run 2.
```

---

### Test Case 12: Data Quality - NULLs and Data Types

**Purpose:** Verify that NULL values are handled correctly across all columns and that data types are preserved or correctly cast during the migration, preventing data corruption or unexpected type changes.

**Setup:**
1.  Populate `sof_ta_cntrct_crs` with records that include:
    *   NULL values in various nullable columns (e.g., `cntrct_parent`, `commitment_reference_date`, `order_number`).
    *   Values that test data type boundaries (e.g., max/min integers, long strings).
2.  Populate `isbert_schema.dwtk_meldungen` as needed.
3.  Clear `sof_ta_cntrct_crs2`.
4.  Generate expected output from legacy Oracle job for this specific data.

**Action:**
1.  Trigger the Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs2`.

**Expected Result:**
All NULL values in the source are correctly represented as NULL in the target. All data types are correctly mapped and values are not truncated or altered due to type mismatches.

**Pass/Fail Criterion:**
A row-by-row comparison between `sof_ta_cntrct_crs2` and the expected output table (from legacy) shows no discrepancies in NULL handling or data values.

**Test Code (BigQuery SQL Assertion - similar to Test Case 4):**

```sql
-- Assume 'expected_sof_ta_cntrct_crs2_null_types' is a temporary table with legacy output.
SELECT 'Difference Found in NULLs or Types'
FROM (
    SELECT * FROM `YOUR_GCP_PROJECT_ID`.your_dataset.sof_ta_cntrct_crs2
    EXCEPT DISTINCT
    SELECT * FROM `YOUR_GCP_PROJECT_ID`.your_dataset.expected_sof_ta_cntrct_crs2_null_types
)
UNION ALL
SELECT 'Difference Found in NULLs or Types'
FROM (
    SELECT * FROM `YOUR_GCP_PROJECT_ID`.your_dataset.expected_sof_ta_cntrct_crs2_null_types
    EXCEPT DISTINCT
    SELECT * FROM `YOUR_GCP_PROJECT_ID`.your_dataset.sof_ta_cntrct_crs2
);
```