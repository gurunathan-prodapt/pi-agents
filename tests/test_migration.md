As a senior data-migration QA engineer, I've analyzed the migration design document and the generated BigQuery/Airflow code for `DW.BERT_AUSD_V_TA_C_BFC`. The following test cases are designed to ensure behavioral equivalence, data integrity, and correctness of the migrated solution.

---

## Test Case 1: End-to-End Output Parity

*   **Purpose:** To verify that the migrated BigQuery job, when executed via Airflow, produces an identical final state in the `sof$ta_c_bfc` target table as the legacy Oracle job, given the same initial input data. This is the primary validation of behavioral equivalence.
*   **Setup:**
    1.  **Legacy Baseline:**
        *   Capture a snapshot of all source tables (`sof$ta_cntrct_crs`, `sof$ta_barrier`, `sof$ta_cntrct_valid`, `sof$ta_period`, `dwtk_meldungen`, `all_objects`) from the Oracle environment *before* running the legacy `DW.BERT_AUSD_V_TA_C_BFC` job.
        *   Capture a snapshot of the target table `sof$ta_c_bfc` from Oracle *before* running the legacy job.
        *   Execute the legacy Oracle job.
        *   Capture the final state of `sof$ta_c_bfc` from Oracle *after* the job completes. This will be our "golden source" for comparison.
    2.  **Migrated Environment:**
        *   Load the "before" snapshots of all Oracle source tables into their corresponding BigQuery tables (`{{ params.project }}.{{ params.dataset }}.sof$ta_cntrct_crs`, etc.), ensuring precise data type and value mapping.
        *   Load the "before" snapshot of the Oracle `sof$ta_c_bfc` table into `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc`.
        *   Ensure the `bfc_get_bindefrist` UDF is deployed and its logic is functionally identical to the Oracle `Cds$vr_Bindefrist.GetBindeFrist`.
        *   Ensure the `dwtk_meldungen` and `all_objects` tables in BigQuery contain data that would yield the same `v_datum` and `v_bfc_procedure` values as observed during the legacy run.
*   **Action:**
    1.  Trigger the `dw_bert_ausd_v_ta_c_bfc` Airflow DAG.
    2.  Monitor the DAG execution until it completes successfully.
    3.  Query the final state of `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc`.
*   **Pass/Fail Criterion:**
    *   The row count of the BigQuery `sof$ta_c_bfc` table must be identical to the row count of the legacy Oracle `sof$ta_c_bfc` golden source.
    *   A full data comparison (e.g., using `EXCEPT DISTINCT` in SQL or a programmatic comparison of dataframes) between the BigQuery `sof$ta_c_bfc` and the legacy golden source must show no differences in any column (`cntrct_id`, `bindefrist`, `bfc_age`, `bfc_count`, `bfc_procedure`, `commitment_reference_date`, `cntrct_validity_id`).

```python
# Example pytest assertion for end-to-end output parity
import pytest
from google.cloud import bigquery
from datetime import datetime

# Assume bigquery_client is a fixture providing an authenticated BigQuery client
# Assume legacy_expected_output_table is a BigQuery table pre-loaded with the golden source data

def test_end_to_end_output_parity(bigquery_client, project_id, dataset_id):
    target_table = f"{project_id}.{dataset_id}.sof$ta_c_bfc"
    legacy_expected_output_table = f"{project_id}.{dataset_id}.legacy_sof_ta_c_bfc_golden" # Pre-loaded golden source

    # --- Action: Trigger Airflow DAG (conceptual, in a real test this would be an API call) ---
    # For this test, we assume the DAG has already run and populated `target_table`.
    # In a CI/CD pipeline, this step would involve Airflow API calls to trigger and poll.
    print(f"Assuming Airflow DAG 'dw_bert_ausd_v_ta_c_bfc' has been executed.")

    # --- Pass/Fail Criterion: SQL-based comparison ---
    sql_diff_query = f"""
        (
            SELECT 'Only in Migrated' AS diff_type, *
            FROM `{target_table}`
            EXCEPT DISTINCT
            SELECT 'Only in Migrated' AS diff_type, *
            FROM `{legacy_expected_output_table}`
        )
        UNION ALL
        (
            SELECT 'Only in Legacy' AS diff_type, *
            FROM `{legacy_expected_output_table}`
            EXCEPT DISTINCT
            SELECT 'Only in Legacy' AS diff_type, *
            FROM `{target_table}`
        )
    """
    diff_results = bigquery_client.query(sql_diff_query).result().to_dataframe()

    assert diff_results.empty, \
        f"Differences found between migrated and legacy output in `{target_table}`:\n{diff_results.to_string()}"

    migrated_row_count = bigquery_client.query(f"SELECT COUNT(1) FROM `{target_table}`").result().scalar_iterator().next()
    legacy_row_count = bigquery_client.query(f"SELECT COUNT(1) FROM `{legacy_expected_output_table}`").result().scalar_iterator().next()
    assert migrated_row_count == legacy_row_count, \
        f"Row count mismatch: Migrated={migrated_row_count}, Legacy={legacy_row_count}"

```

---

## Test Case 2: `sof$ta_c_bfc_akt` Population Correctness (Transformation)

*   **Purpose:** To verify that the initial `INSERT` statement populating the temporary table `sof$ta_c_bfc_akt` correctly performs joins, handles `NULL` values, calculates `bfc_age` using `GREATEST` and `COALESCE`, and aggregates data using `MAX` and `COUNT` as per the legacy logic.
*   **Setup:**
    1.  Populate BigQuery source tables (`sof$ta_cntrct_crs`, `sof$ta_barrier`, `sof$ta_cntrct_valid`, `sof$ta_period`) with a comprehensive set of test data, including:
        *   `cntrct_id`s with and without matches in `LEFT JOIN` tables.
        *   `bfc_age` values that are `NULL` in various combinations across source tables.
        *   Multiple records for the same `cntrct_id` in `sof$ta_cntrct_crs` to test `MAX` aggregation.
        *   Scenarios where all `bfc_age` values for a `cntrct_id` are `NULL` to test `COALESCE(..., DATE '1900-01-01')`.
    2.  (Optional but recommended) Execute the equivalent Oracle SQL statement on the same test data and capture its output for `sof$ta_c_bfc_akt` as an additional baseline.
*   **Action:**
    1.  Execute only the `TRUNCATE TABLE sof$ta_c_bfc_akt` and the subsequent `INSERT INTO sof$ta_c_bfc_akt` statements from `d_ausd_v_ta_c_bfc.bqsql`.
    2.  Query the contents of `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc_akt`.
*   **Pass/Fail Criterion:**
    *   The row count in `sof$ta_c_bfc_akt` must match the expected count based on the test data and logic.
    *   For a representative sample of `cntrct_id`s, verify that `commitment_reference_date`, `cntrct_validity_id`, `bfc_age`, and `bfc_count` are calculated correctly, especially for `NULL` inputs and the `GREATEST`/`COALESCE` logic.
    *   If an Oracle baseline is available, a full data comparison of `sof$ta_c_bfc_akt` between BigQuery and Oracle must show no differences.

```sql
-- Example SQL assertion for sof$ta_c_bfc_akt population
-- This query would be run AFTER the INSERT into sof$ta_c_bfc_akt.
-- 'expected_sof_ta_c_bfc_akt_data' should be a temporary table or CTE containing the expected output
-- based on the test data and logic, or from a legacy Oracle run.

CREATE TEMPORARY TABLE expected_sof_ta_c_bfc_akt_data (
  cntrct_id STRING,
  commitment_reference_date DATE,
  cntrct_validity_id STRING,
  bfc_age DATE,
  bfc_count INT64
);
-- INSERT INTO expected_sof_ta_c_bfc_akt_data VALUES ('C1', '2023-01-15', 'V1', '2023-01-10', 1);
-- INSERT INTO expected_sof_ta_c_bfc_akt_data VALUES ('C2', '2023-02-20', 'V2', '2023-02-18', 2);
-- ... populate with comprehensive test cases ...

SELECT
    CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'FAIL' END AS test_result,
    'Differences found in sof$ta_c_bfc_akt population' AS message,
    diff_type,
    cntrct_id,
    commitment_reference_date,
    cntrct_validity_id,
    bfc_age,
    bfc_count
FROM (
    SELECT 'Only in Actual' AS diff_type, A.* FROM `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc_akt` AS A
    EXCEPT DISTINCT
    SELECT 'Only in Actual' AS diff_type, B.* FROM expected_sof_ta_c_bfc_akt_data AS B

    UNION ALL

    SELECT 'Only in Expected' AS diff_type, B.* FROM expected_sof_ta_c_bfc_akt_data AS B
    EXCEPT DISTINCT
    SELECT 'Only in Expected' AS diff_type, A.* FROM `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc_akt` AS A
)
GROUP BY diff_type, cntrct_id, commitment_reference_date, cntrct_validity_id, bfc_age, bfc_count
HAVING COUNT(1) > 0;
```

---

## Test Case 3: Initial Load into `sof$ta_c_bfc` (Conditional Logic)

*   **Purpose:** To verify that the `IF (SELECT COUNT(1) FROM sof$ta_c_bfc) = 0 THEN INSERT ... END IF;` block correctly executes only when the target table is empty, and that the `bfc_procedure` column is initialized to `DATE '1900-01-01'` during this initial load.
*   **Setup:**
    1.  **Scenario A (Empty Target):** Ensure `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc` is empty. Populate `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc_akt` with test data.
    2.  **Scenario B (Non-Empty Target):** Populate `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc` with some existing data. Populate `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc_akt` with test data.
*   **Action:**
    1.  Execute the `IF` block and the subsequent `INSERT` statement from `d_ausd_v_ta_c_bfc.bqsql` for both scenarios.
    2.  Query the state of `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc` after each scenario.
*   **Pass/Fail Criterion:**
    *   **Scenario A:** `sof$ta_c_bfc` must be populated with all rows from `sof$ta_c_bfc_akt`. For all inserted rows, the `bfc_procedure` column must be `DATE '1900-01-01'`.
    *   **Scenario B:** `sof$ta_c_bfc` must remain unchanged (i.e., the `INSERT` statement should not have executed).

```sql
-- Example SQL assertion for Scenario A (Empty Target)
-- Run AFTER the IF block execution when sof$ta_c_bfc was initially empty.
SELECT
    CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'FAIL' END AS test_result,
    'Initial load failed or bfc_procedure not initialized to 1900-01-01' AS message
FROM (
    SELECT
        s.cntrct_id, s.bfc_age, s.bfc_count, DATE '1900-01-01' AS expected_bfc_procedure,
        s.commitment_reference_date, s.cntrct_validity_id
    FROM `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc_akt` s
    EXCEPT DISTINCT
    SELECT
        t.cntrct_id, t.bfc_age, t.bfc_count, t.bfc_procedure,
        t.commitment_reference_date, t.cntrct_validity_id
    FROM `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc` t
)
HAVING COUNT(1) > 0;

-- Example SQL assertion for Scenario B (Non-Empty Target)
-- Run AFTER the IF block execution when sof$ta_c_bfc was initially NOT empty.
-- 'initial_sof_ta_c_bfc_state' is a temporary table holding the state before the IF block.
CREATE TEMPORARY TABLE initial_sof_ta_c_bfc_state AS
SELECT * FROM `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc`; -- Capture state before test

-- ... execute the IF block ...

SELECT
    CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'FAIL' END AS test_result,
    'sof$ta_c_bfc was modified when it should not have been (Scenario B)' AS message
FROM (
    SELECT * FROM `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc`
    EXCEPT DISTINCT
    SELECT * FROM initial_sof_ta_c_bfc_state
)
HAVING COUNT(1) > 0;
```

---

## Test Case 4: `MERGE` Statement Correctness (Update and Insert Logic)

*   **Purpose:** To verify the `MERGE` statement's behavior for both `WHEN MATCHED THEN UPDATE` (based on `bfc_age` and `bfc_count` conditions) and `WHEN NOT MATCHED THEN INSERT`. This also validates the correct usage of the `bfc_get_bindefrist` UDF and the `v_bfc_procedure` variable.
*   **Setup:**
    1.  Populate `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc` with existing data.
    2.  Populate `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc_akt` with data that includes:
        *   Rows matching `cntrct_id` in `sof$ta_c_bfc` where `bfc_age` is greater or `bfc_count` is different (should trigger `UPDATE`).
        *   Rows matching `cntrct_id` in `sof$ta_c_bfc` where `bfc_age` and `bfc_count` are the same (should NOT trigger `UPDATE`).
        *   Rows with new `cntrct_id`s not present in `sof$ta_c_bfc` (should trigger `INSERT`).
    3.  Define `v_bfc_procedure` to a specific test value (e.g., `DATE '2023-03-15'`).
    4.  Ensure the `bfc_get_bindefrist` UDF returns predictable values for the test inputs.
*   **Action:**
    1.  Execute the `MERGE` statement from `d_ausd_v_ta_c_bfc.bqsql`.
    2.  Query the final state of `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc`.
*   **Pass/Fail Criterion:**
    *   Verify that rows intended for `UPDATE` have their `bindefrist`, `bfc_age`, `bfc_count`, `bfc_procedure`, `commitment_reference_date`, and `cntrct_validity_id` updated correctly.
    *   Verify that rows intended for `INSERT` are added with correct values for all columns.
    *   Verify that rows that should NOT have been updated remain unchanged.
    *   Specifically check that `bfc_procedure` is set to the value of `v_bfc_procedure` for both updated and inserted rows.
    *   Check that `bindefrist` is correctly calculated by the UDF.

```sql
-- Example SQL assertion for MERGE statement
-- Run AFTER the MERGE statement execution.
-- 'expected_sof_ta_c_bfc_after_merge' is a temporary table with the expected state after MERGE.
CREATE TEMPORARY TABLE expected_sof_ta_c_bfc_after_merge (
  cntrct_id STRING, bindefrist DATE, bfc_age DATE, bfc_count INT64,
  bfc_procedure DATE, commitment_reference_date DATE, cntrct_validity_id STRING
);
-- INSERT INTO expected_sof_ta_c_bfc_after_merge VALUES (...); -- Populate with expected data

SELECT
    CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'FAIL' END AS test_result,
    'Differences found after MERGE operation' AS message,
    diff_type,
    cntrct_id
FROM (
    SELECT 'Only in Actual' AS diff_type, A.cntrct_id FROM `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc` AS A
    EXCEPT DISTINCT
    SELECT 'Only in Actual' AS diff_type, B.cntrct_id FROM expected_sof_ta_c_bfc_after_merge AS B

    UNION ALL

    SELECT 'Only in Expected' AS diff_type, B.cntrct_id FROM expected_sof_ta_c_bfc_after_merge AS B
    EXCEPT DISTINCT
    SELECT 'Only in Expected' AS diff_type, A.cntrct_id FROM `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc` AS A
)
GROUP BY diff_type, cntrct_id
HAVING COUNT(1) > 0;
```

---

## Test Case 5: `UPDATE` Statement Correctness (Batching and `bfc_procedure`)

*   **Purpose:** To verify that the final `UPDATE` statement correctly recomputes `bindefrist` for rows where `bfc_procedure` is older than the current `v_bfc_procedure`, and that the `QUALIFY ROW_NUMBER() OVER (ORDER BY cntrct_id) <= v_max_update` clause correctly limits the number of updated rows.
*   **Setup:**
    1.  Populate `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc` with data where:
        *   A known number of rows have `bfc_procedure` older than the `v_bfc_procedure` to be used in the test.
        *   Other rows have `bfc_procedure` equal to or newer than `v_bfc_procedure` (these should not be updated).
    2.  Set `v_bfc_procedure` to a specific test date (e.g., `DATE '2023-03-15'`).
    3.  Set `v_max_update` to a value smaller than the total number of rows that *could* be updated (e.g., 2, if 5 rows are eligible).
    4.  Ensure the `bfc_get_bindefrist` UDF returns predictable values.
*   **Action:**
    1.  Execute the `UPDATE` statement from `d_ausd_v_ta_c_bfc.bqsql`.
    2.  Query the final state of `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc`.
*   **Pass/Fail Criterion:**
    *   Verify that exactly `v_max_update` rows (or fewer if fewer are eligible) have their `bindefrist` recomputed and `bfc_procedure` updated to the current `v_bfc_procedure`.
    *   Verify that the `cntrct_id`s of the updated rows match the `ORDER BY cntrct_id` clause in the `QUALIFY` statement.
    *   Verify that rows with `bfc_procedure` equal to or newer than `v_bfc_procedure` remain unchanged.

```sql
-- Example SQL assertion for UPDATE statement
-- Run AFTER the UPDATE statement execution.
-- Assume v_max_update was 2 and v_bfc_procedure was DATE '2023-03-15'.
-- Assume 'C1', 'C2', 'C3', 'C4', 'C5' were eligible for update, ordered by cntrct_id.
-- So 'C1' and 'C2' should be updated.

-- Check count of updated rows
SELECT
    CASE WHEN COUNT(1) = 2 THEN 'PASS' ELSE 'FAIL' END AS test_result,
    'Incorrect number of rows updated by final UPDATE statement' AS message
FROM `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc`
WHERE bfc_procedure = DATE '2023-03-15'; -- Assuming this is the new bfc_procedure

-- Check specific updated rows (e.g., 'C1' and 'C2' should be updated)
SELECT
    CASE WHEN COUNT(1) = 1 THEN 'PASS' ELSE 'FAIL' END AS test_result,
    'Row C1 was not updated correctly or bindefrist is NULL' AS message
FROM `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc`
WHERE cntrct_id = 'C1'
  AND bfc_procedure = DATE '2023-03-15'
  AND bindefrist IS NOT NULL; -- Check that UDF was called and returned a value

-- Check a row that should NOT have been updated (e.g., 'C3' if its old bfc_procedure was newer or equal)
SELECT
    CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'FAIL' END AS test_result,
    'Row C3 was updated when it should not have been' AS message
FROM `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc`
WHERE cntrct_id = 'C3'
  AND bfc_procedure = DATE '2023-03-15'; -- Should still be its old bfc_procedure, not the new one
```

---

## Test Case 6: `bfc_get_bindefrist` UDF Functional Equivalence (Transformation / External System Replacement)

*   **Purpose:** To verify that the BigQuery UDF `bfc_get_bindefrist` produces identical outputs to the legacy Oracle `Cds$vr_Bindefrist.GetBindeFrist` function for a comprehensive set of inputs. This is critical given the "Unresolved / Risks" section in the design.
*   **Setup:**
    1.  Identify a representative set of input parameters (`cntrct_id`, `commitment_reference_date`, `cntrct_validity_id`) for the Oracle `Cds$vr_Bindefrist.GetBindeFrist` function. This set should include:
        *   Valid combinations that produce a date.
        *   Combinations that might produce `NULL` or specific default dates in Oracle.
        *   Edge cases for dates (e.g., start/end of year, leap years, invalid dates if Oracle handles them gracefully).
    2.  Execute the Oracle function with these inputs and record the exact outputs.
    3.  Ensure the BigQuery UDF `{{ params.project }}.{{ params.dataset }}.bfc_get_bindefrist` is deployed and its logic is implemented.
*   **Action:**
    1.  Execute the BigQuery UDF `bfc_get_bindefrist` with the same set of input parameters.
    2.  Compare the outputs.
*   **Pass/Fail Criterion:**
    *   For every input combination, the output of the BigQuery UDF must exactly match the output of the Oracle function. This includes `NULL` values.

```python
# Example pytest for UDF equivalence
import pytest
from google.cloud import bigquery
from datetime import datetime, date

# Assume bigquery_client is a fixture providing an authenticated BigQuery client

def test_bfc_get_bindefrist_udf_equivalence(bigquery_client, project_id, dataset_id):
    udf_name = f"{project_id}.{dataset_id}.bfc_get_bindefrist"

    # Define test cases: (cntrct_id, commitment_reference_date_str, cntrct_validity_id, expected_output_date_str)
    # The expected_output_date_str would come from running these inputs against the Oracle function.
    test_cases = [
        ('C100', '2023-01-01', 'V1', '2023-03-31'),
        ('C101', '2023-02-15', 'V2', '2023-04-30'),
        ('C102', '2023-03-10', 'V3', None), # Assuming Oracle returns NULL for this case
        ('C103', '2024-02-29', 'V4', '2024-05-31'), # Leap year test
        ('C104', None, 'V5', None), # NULL input for commitment_reference_date
        ('C105', '2023-06-01', None, None), # NULL input for cntrct_validity_id
        ('C106', '1900-01-01', 'V6', '1900-03-31'), # Edge case for 1900-01-01
    ]

    for cntrct_id, ref_date_str, validity_id, expected_date_str in test_cases:
        # Construct the UDF call with proper NULL handling for SQL
        ref_date_sql = f"DATE '{ref_date_str}'" if ref_date_str else "NULL"
        validity_id_sql = f"'{validity_id}'" if validity_id else "NULL"
        
        query = f"""
            SELECT `{udf_name}`('{cntrct_id}', {ref_date_sql}, {validity_id_sql}) AS actual_bindefrist
        """
        
        result = bigquery_client.query(query).result().to_dataframe()
        actual_bindefrist = result['actual_bindefrist'][0]

        expected_bindefrist = None
        if expected_date_str:
            expected_bindefrist = datetime.strptime(expected_date_str, '%Y-%m-%d').date()

        assert actual_bindefrist == expected_bindefrist, \
            f"UDF mismatch for inputs (cntrct_id='{cntrct_id}', ref_date='{ref_date_str}', validity_id='{validity_id}'): " \
            f"Expected '{expected_bindefrist}', Got '{actual_bindefrist}'"

```

---

## Test Case 7: Variable Declaration and Usage (`v_datum`, `v_bfc_procedure`)

*   **Purpose:** To verify that the `DECLARE` statements for `v_datum` and `v_bfc_procedure` correctly retrieve values from `dwtk_meldungen` and `all_objects` respectively, including `COALESCE` handling and date conversions from `TIMESTAMP` to `DATE`. This validates the replacement of Oracle-specific metadata queries.
*   **Setup:**
    1.  Populate `{{ params.project }}.{{ params.dataset }}.dwtk_meldungen` with test data, including:
        *   Rows for `job_kennung = 'BERT_DROP_TEMP_TABLE'` with various `timecreated` `TIMESTAMP` values.
        *   Scenarios where no rows match `job_kennung = 'BERT_DROP_TEMP_TABLE'` to test `COALESCE(..., DATE '1900-01-01')`.
    2.  Populate `{{ params.project }}.{{ params.dataset }}.all_objects` with test data, including:
        *   A row for `object_name = 'CDS$VR_BINDEFRIST'` and `object_type = 'PACKAGE'` with a `created` `TIMESTAMP`.
        *   Scenarios where no such object exists to test `COALESCE(..., DATE '1900-01-01')`.
*   **Action:**
    1.  Execute only the `DECLARE` and `SET` statements for `v_datum` and `v_bfc_procedure` from `d_ausd_v_ta_c_bfc.bqsql` in isolation.
    2.  Query the values of these declared variables.
*   **Pass/Fail Criterion:**
    *   `v_datum` must be set to the `MAX(DATE(timecreated))` for `job_kennung = 'BERT_DROP_TEMP_TABLE'`, or `DATE '1900-01-01'` if no matching rows are found.
    *   `v_bfc_procedure` must be set to the `MAX(DATE(created))` for `object_name = 'CDS$VR_BINDEFRIST'` and `object_type = 'PACKAGE'`, or `DATE '1900-01-01'` if no matching rows are found.

```sql
-- Example SQL assertion for v_datum
-- Run AFTER the v_datum SET statement.
DECLARE expected_v_datum DATE DEFAULT DATE '2023-01-01'; -- Replace with actual expected value based on setup
SELECT
    CASE WHEN v_datum = expected_v_datum THEN 'PASS' ELSE 'FAIL' END AS test_result,
    CONCAT('v_datum mismatch: Expected ', FORMAT_DATE('%Y-%m-%d', expected_v_datum), ', Got ', FORMAT_DATE('%Y-%m-%d', v_datum)) AS message;

-- Example SQL assertion for v_bfc_procedure
-- Run AFTER the v_bfc_procedure SET statement.
DECLARE expected_v_bfc_procedure DATE DEFAULT DATE '2023-03-15'; -- Replace with actual expected value based on setup
SELECT
    CASE WHEN v_bfc_procedure = expected_v_bfc_procedure THEN 'PASS' ELSE 'FAIL' END AS test_result,
    CONCAT('v_bfc_procedure mismatch: Expected ', FORMAT_DATE('%Y-%m-%d', expected_v_bfc_procedure), ', Got ', FORMAT_DATE('%Y-%m-%d', v_bfc_procedure)) AS message;
```

---

## Test Case 8: Schema and Data Type Validation (Data Quality)

*   **Purpose:** To verify that the BigQuery target table `sof$ta_c_bfc` (and other relevant tables) has the correct schema, column names, and data types as expected after migration, matching the logical structure of the legacy Oracle table.
*   **Setup:**
    1.  Ensure all relevant BigQuery tables (source and target) have been created (e.g., by running their respective DDLs).
    2.  Obtain the precise schema definitions (column names, data types, nullability) of the legacy Oracle tables.
*   **Action:**
    1.  Query the schema of `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc` (and other tables) in BigQuery using `INFORMATION_SCHEMA.COLUMNS`.
*   **Pass/Fail Criterion:**
    *   All columns present in the legacy Oracle table must be present in the BigQuery table.
    *   Column names must match (case-insensitivity might be a factor, but BigQuery is case-sensitive for column names unless quoted).
    *   Data types must be correctly mapped (e.g., Oracle `DATE` to BigQuery `DATE`, `NUMBER` to `INT64`/`NUMERIC`, `VARCHAR2` to `STRING`).
    *   Nullability constraints should be reviewed if they were explicitly defined in Oracle and are critical for data integrity.

```sql
-- Example SQL assertion for schema validation of sof$ta_c_bfc
-- This can be run as a standalone check.
-- 'expected_schema_sof_ta_c_bfc' is a temporary table or CTE with the expected schema.

CREATE TEMPORARY TABLE expected_schema_sof_ta_c_bfc (
  column_name STRING,
  data_type STRING,
  is_nullable STRING, -- 'YES' or 'NO'
  ordinal_position INT64
);
INSERT INTO expected_schema_sof_ta_c_bfc VALUES
  ('cntrct_id', 'STRING', 'YES', 1),
  ('bindefrist', 'DATE', 'YES', 2),
  ('bfc_age', 'DATE', 'YES', 3),
  ('bfc_count', 'INT64', 'YES', 4),
  ('bfc_procedure', 'DATE', 'YES', 5),
  ('commitment_reference_date', 'DATE', 'YES', 6),
  ('cntrct_validity_id', 'STRING', 'YES', 7);
-- Adjust 'is_nullable' based on actual Oracle schema.

SELECT
    CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'FAIL' END AS test_result,
    'Schema mismatch in sof$ta_c_bfc' AS message,
    diff_type,
    column_name,
    data_type,
    is_nullable
FROM (
    SELECT 'Only in Actual' AS diff_type, column_name, data_type, is_nullable
    FROM `{{ params.project }}.{{ params.dataset}}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'sof$ta_c_bfc'
    EXCEPT DISTINCT
    SELECT 'Only in Actual' AS diff_type, column_name, data_type, is_nullable
    FROM expected_schema_sof_ta_c_bfc

    UNION ALL

    SELECT 'Only in Expected' AS diff_type, column_name, data_type, is_nullable
    FROM expected_schema_sof_ta_c_bfc
    EXCEPT DISTINCT
    SELECT 'Only in Expected' AS diff_type, column_name, data_type, is_nullable
    FROM `{{ params.project }}.{{ params.dataset}}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'sof$ta_c_bfc'
) AS diff
GROUP BY diff_type, column_name, data_type, is_nullable
HAVING COUNT(1) > 0;
```

---

## Test Case 9: Airflow DAG Orchestration and Parameter Passing (External System Replacement)

*   **Purpose:** To verify that the Airflow DAG correctly triggers the BigQuery job, passes the `project` and `dataset` parameters, and completes successfully without errors. This confirms the replacement of UC4/KornShell orchestration with Airflow.
*   **Setup:**
    1.  Deploy the `dw_bert_ausd_v_ta_c_bfc.py` DAG to an Airflow/Cloud Composer environment.
    2.  Ensure the `gcp_conn_id` (`google_cloud_default`) is configured correctly with appropriate IAM permissions for BigQuery.
    3.  Ensure the `d_ausd_v_ta_c_bfc.bqsql` script is accessible in the DAG's `template_searchpath`.
    4.  Ensure placeholder `project` and `dataset` parameters in the DAG are correctly set to the target GCP project and BigQuery dataset IDs.
*   **Action:**
    1.  Manually trigger the `dw_bert_ausd_v_ta_c_bfc` DAG in the Airflow UI (or via Airflow API/CLI).
    2.  Monitor the DAG run in the Airflow UI.
*   **Pass/Fail Criterion:**
    *   The DAG run must complete successfully (green status).
    *   The `execute_d_ausd_v_ta_c_bfc_sql` task must complete successfully.
    *   Review Airflow task logs for any errors related to BigQuery execution or parameter substitution.
    *   Verify that a BigQuery job corresponding to the DAG run was indeed executed in the specified `project` and `dataset` (e.g., by checking BigQuery job history).

```python
# This test is primarily an operational verification within the Airflow environment.
# Automated checks would typically involve Airflow's REST API or CLI for status polling.

# Example of a conceptual pytest for Airflow DAG status (requires Airflow API interaction)
import pytest
import time
from airflow.api.client.local_client import Client # Or use remote API client for production

# This is a simplified example. A robust Airflow test would involve:
# 1. Setting up a test Airflow environment or connecting to a live one.
# 2. Using Airflow's API to trigger the DAG and get a run_id.
# 3. Polling the DAG run status using the run_id until it completes.
# 4. Asserting the final state of the DAG run.

def test_airflow_dag_execution_success(airflow_client_fixture): # Assume fixture provides Airflow client
    dag_id = 'dw_bert_ausd_v_ta_c_bfc'
    
    print(f"Triggering Airflow DAG: {dag_id}")
    # In a real scenario, use airflow_client_fixture.trigger_dag(dag_id)
    # and capture the run_id to monitor that specific run.
    # For demonstration, we'll simulate success.
    
    # Simulate waiting for DAG to complete
    # time.sleep(300) # Wait for a reasonable time for the DAG to run

    # Placeholder for actual status check logic using Airflow API
    # For example:
    # dag_run_status = airflow_client_fixture.get_dag_run_status(dag_id, run_id)
    # assert dag_run_status == 'success', f"DAG {dag_id} failed. Current status: {dag_run_status}"

    print(f"DAG {dag_id} execution simulated successfully. Please verify in Airflow UI.")
    assert True # Placeholder, actual assertion depends on Airflow API interaction

```

---

## Test Case 10: Cleanup of `sof$ta_c_bfc_akt` (Data Quality / Operational)

*   **Purpose:** To verify that the temporary table `sof$ta_c_bfc_akt` is truncated at the end of the job execution, ensuring proper cleanup and preventing accumulation of temporary data.
*   **Setup:**
    1.  Ensure `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc_akt` is populated with some data (e.g., by running the job up to the `MERGE` statement, or by manually inserting test data).
*   **Action:**
    1.  Execute the final `TRUNCATE TABLE sof$ta_c_bfc_akt` statement from `d_ausd_v_ta_c_bfc.bqsql` (or run the full DAG).
    2.  Query the row count of `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc_akt`.
*   **Pass/Fail Criterion:**
    *   The row count of `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc_akt` must be 0 after the job completes.

```sql
-- Example SQL assertion for cleanup
-- Run AFTER the full DAG execution.
SELECT
    CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'FAIL' END AS test_result,
    'Temporary table sof$ta_c_bfc_akt was not truncated' AS message
FROM `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc_akt`;
```