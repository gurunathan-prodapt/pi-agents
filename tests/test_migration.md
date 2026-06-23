As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the `k_ausd_v_ta_cntrct_crs.ksh` migration to BigQuery. These tests aim to ensure the migrated solution is functionally equivalent, robust, and adheres to the design specifications.

The tests are categorized by the validation areas requested and include purpose, setup, action, and concrete pass/fail criteria. Where applicable, runnable test code snippets (using `pytest` and BigQuery SQL) are provided.

---

## Migration Validation Tests: `k_ausd_v_ta_cntrct_crs.ksh`

**Test Environment Setup (Prerequisites):**

1.  **BigQuery Project:** A dedicated GCP project for testing (e.g., `test-project`).
2.  **BigQuery Datasets:** Ensure `test-project.isbert_schema`, `test-project.staging`, `test-project.source_cds`, and `test-project.job_control` datasets exist.
3.  **BigQuery Tables & Stored Procedures:** All DDLs and stored procedures provided in the "GENERATED MIGRATION CODE" section must be deployed to the `test-project` (e.g., `project.isbert_schema.dwtk_meldungen`, `project.staging.d_ausd_v_ta_cntrct_crs`, etc.).
4.  **Legacy Environment:** Access to the original Oracle database and the ability to execute the `k_ausd_v_ta_cntrct_crs.ksh` script with controlled input data.
5.  **Python Environment:** A Python environment with `pytest` and `google-cloud-bigquery` installed for running automated tests.
6.  **Test Data Generation Script:** A script (e.g., Python) to populate the BigQuery source tables (`test-project.source_cds.cds_ta_cntrct`, `test-project.isbert_schema.dwtk_meldungen`) and the Oracle source tables (`cds$ta_cntrct`, `dwtk_meldungen`) with identical, controlled test data for each scenario.

---

### 1. Output Parity Tests

These tests focus on ensuring that given the same input data, the migrated BigQuery job produces the exact same final output as the legacy KornShell job.

#### Test Case 1.1: Full Data Parity of Target Table

*   **Purpose:** To verify that the `sof_ta_cntrct_crs` table in BigQuery contains precisely the same data (all rows, all columns) as the `sof$ta_cntrct_crs` table in the legacy Oracle system after a full execution.
*   **Setup:**
    1.  Populate `test-project.source_cds.cds_ta_cntrct` and `test-project.isbert_schema.dwtk_meldungen` with a comprehensive dataset covering various filter conditions, NULLs, and edge cases (e.g., 100-1000 rows).
    2.  Populate the corresponding Oracle `cds$ta_cntrct` and `dwtk_meldungen` tables with *identical* data.
    3.  Ensure `test-project.staging.sof_ta_cntrct_crs` and Oracle `sof$ta_cntrct_crs` are empty before execution.
*   **Action:**
    1.  Execute the legacy `k_ausd_v_ta_cntrct_crs.ksh` script with specific `p_JobKennung` and `p_EintragsNr` parameters.
    2.  Execute the migrated `project.job_control.r_ausd_vertrag_control` BigQuery stored procedure with the same logical `p_JobKennung` and `p_EintragsNr` parameters.
*   **Pass/Fail Criterion:**
    *   The number of rows in `test-project.staging.sof_ta_cntrct_crs` must be identical to the number of rows in Oracle `sof$ta_cntrct_crs`.
    *   A full row-by-row, column-by-column comparison (e.g., using a hash of sorted rows, or `EXCEPT` queries) must show no differences between the two tables.

```python
# Example pytest assertion for row count and data parity
import pytest
from google.cloud import bigquery

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client(project="test-project")

def test_full_data_parity(bq_client):
    job_kennung = "BERT_TA_CNTRCT_CRS_JOB_PARITY"
    eintrags_nr = "1"

    # 1. Execute BigQuery job
    query = f"""
    CALL `test-project.job_control.r_ausd_vertrag_control`('{job_kennung}', '{eintrags_nr}');
    """
    bq_client.query(query).result()

    # 2. Extract BigQuery results
    bq_results_query = """
    SELECT * FROM `test-project.staging.sof_ta_cntrct_crs` ORDER BY cntrct_id, contract_number;
    """
    bq_rows = list(bq_client.query(bq_results_query).result())
    bq_data = [tuple(row.values()) for row in bq_rows]

    # 3. (Manual/Automated) Extract Legacy Oracle results
    # This part would involve connecting to Oracle and fetching data.
    # For demonstration, assume `legacy_data` is obtained from Oracle.
    # Example: legacy_data = fetch_from_oracle("SELECT * FROM sof$ta_cntrct_crs ORDER BY cntrct_id, contract_number;")
    legacy_data = [
        # ... populate with actual data from Oracle ...
        # Example: (101, 1, 'CNTRCT-001', 1, 1, date(2023,1,1), 'CV1', 1, 'VO1', date(2023,1,1), 5, None, 10, 'CC1', 'CCU1', date(2023,1,1), 'ORD1', date(2022,12,1))
    ]

    # Assert row counts
    assert len(bq_data) == len(legacy_data), \
        f"Row count mismatch: BigQuery has {len(bq_data)} rows, Legacy has {len(legacy_data)} rows."

    # Assert data content
    assert sorted(bq_data) == sorted(legacy_data), \
        "Data content mismatch between BigQuery and Legacy target tables."

    # Optional: Detailed comparison using EXCEPT
    # This requires loading legacy data into a temporary BQ table or direct comparison if possible.
    # For a robust comparison, you might load legacy data into a temp BQ table and run:
    # SELECT * FROM bq_target EXCEPT SELECT * FROM temp_legacy_target
    # SELECT * FROM temp_legacy_target EXCEPT SELECT * FROM bq_target
```

#### Test Case 1.2: Row Count Parity

*   **Purpose:** To quickly verify that the total number of records processed and inserted into the target table is consistent between the legacy and migrated jobs.
*   **Setup:** Same as Test Case 1.1.
*   **Action:** Same as Test Case 1.1.
*   **Pass/Fail Criterion:** The `record_count` logged in `test-project.job_control.job_result_log` for the migrated job must match the record count reported by the legacy job (e.g., from its log file or a direct count on the Oracle target table).

```sql
-- BigQuery assertion for record count
SELECT record_count
FROM `test-project.job_control.job_result_log`
WHERE job_kennung = 'BERT_TA_CNTRCT_CRS_JOB_PARITY'
  AND eintrags_nr = '1'
ORDER BY result_time DESC
LIMIT 1;
-- Expected result: <count from legacy job>
```

---

### 2. Transformation Correctness Tests

These tests focus on the internal logic of the `d_ausd_v_ta_cntrct_crs` stored procedure, covering specific filters, joins, aggregations, and data type handling.

#### Test Case 2.1: `v_datum` Determination Logic

*   **Purpose:** Verify that the `v_datum` variable is correctly determined from `dwtk_meldungen` or defaults to '1900-01-01'.
*   **Setup:**
    1.  **Scenario A (Normal):** Populate `test-project.isbert_schema.dwtk_meldungen` with:
        *   `('2023-01-10 10:00:00', 'OTHER_JOB')`
        *   `('2023-01-15 12:00:00', 'BERT_DROP_TEMP_TABLE')`
        *   `('2023-01-12 09:00:00', 'BERT_DROP_TEMP_TABLE')`
    2.  **Scenario B (No Match):** Empty `test-project.isbert_schema.dwtk_meldungen` or no entry for `BERT_DROP_TEMP_TABLE`.
*   **Action:**
    1.  Execute `project.job_control.r_ausd_vertrag_control` for both scenarios.
    2.  Inspect the `v_datum` value used within the `d_ausd_v_ta_cntrct_crs` procedure (this might require temporary logging or debugging features if not directly exposed).
*   **Pass/Fail Criterion:**
    *   **Scenario A:** `v_datum` must be `DATE '2023-01-15'`.
    *   **Scenario B:** `v_datum` must be `DATE '1900-01-01'`.

```sql
-- This check would typically be done by inspecting logs or adding a temporary SELECT v_datum;
-- within the r_ausd_vertrag_control procedure for testing purposes.
-- Or, by asserting the final output based on the expected v_datum.
```

#### Test Case 2.2: Filter Logic - `cntrct_st`

*   **Purpose:** Verify that only contracts with `cntrct_st` values 5 or 6 are selected.
*   **Setup:** Populate `test-project.source_cds.cds_ta_cntrct` with contracts having `cntrct_st` values: 5, 6, 1, 2, 7. Ensure all other filter conditions are met for these rows. Set `v_datum` to a date that includes all `insert_at` values.
*   **Action:** Execute `project.job_control.r_ausd_vertrag_control`.
*   **Pass/Fail Criterion:** `test-project.staging.sof_ta_cntrct_crs` must only contain rows where `cntrct_st` is 5 or 6.

```sql
-- After running the job:
SELECT COUNT(*)
FROM `test-project.staging.sof_ta_cntrct_crs`
WHERE cntrct_st NOT IN (5, 6);
-- Expected result: 0
```

#### Test Case 2.3: Filter Logic - `redundant_owner_id`

*   **Purpose:** Verify that only contracts with `redundant_owner_id = 1` are selected.
*   **Setup:** Populate `test-project.source_cds.cds_ta_cntrct` with contracts having `redundant_owner_id` values: 1, 2, 3. Ensure all other filter conditions are met.
*   **Action:** Execute `project.job_control.r_ausd_vertrag_control`.
*   **Pass/Fail Criterion:** `test-project.staging.sof_ta_cntrct_crs` must only contain rows where `redundant_owner_id` is 1.

```sql
-- After running the job:
SELECT COUNT(*)
FROM `test-project.staging.sof_ta_cntrct_crs`
WHERE redundant_owner_id <> 1;
-- Expected result: 0
```

#### Test Case 2.4: Filter Logic - Date Conditions (`insert_at`, `modified_at`, `valid_from`, `valid_to`)

*   **Purpose:** Verify correct handling of all date-based filters, including NULL values.
*   **Setup:** Populate `test-project.source_cds.cds_ta_cntrct` with various date scenarios relative to a chosen `v_datum` (e.g., `DATE '2023-01-15'`).
    *   `insert_at`: `< v_datum`, `= v_datum`, `> v_datum`
    *   `modified_at`: `IS NULL`, `> v_datum`, `<= v_datum`
    *   `valid_from`: `< v_datum`, `= v_datum`, `> v_datum`
    *   `valid_to`: `IS NULL`, `> v_datum`, `<= v_datum`
    *   Ensure other filters are met for these rows.
*   **Action:** Execute `project.job_control.r_ausd_vertrag_control`.
*   **Pass/Fail Criterion:** Only rows satisfying all date conditions (`c.insert_at <= v_datum`, `(c.modified_at IS NULL OR c.modified_at > v_datum)`, `c.valid_from <= v_datum`, `(c.valid_to IS NULL OR c.valid_to > v_datum)`) must be present in `test-project.staging.sof_ta_cntrct_crs`.

```sql
-- Example data setup for cds_ta_cntrct (assuming v_datum = '2023-01-15')
-- Row 1 (Should be selected): insert_at='2023-01-10', modified_at=NULL, valid_from='2023-01-01', valid_to=NULL
-- Row 2 (Should be selected): insert_at='2023-01-15', modified_at='2023-01-20', valid_from='2023-01-15', valid_to='2023-01-30'
-- Row 3 (Should NOT be selected - insert_at > v_datum): insert_at='2023-01-16', modified_at=NULL, valid_from='2023-01-01', valid_to=NULL
-- Row 4 (Should NOT be selected - modified_at <= v_datum): insert_at='2023-01-10', modified_at='2023-01-10', valid_from='2023-01-01', valid_to=NULL
-- Row 5 (Should NOT be selected - valid_from > v_datum): insert_at='2023-01-10', modified_at=NULL, valid_from='2023-01-16', valid_to=NULL
-- Row 6 (Should NOT be selected - valid_to <= v_datum): insert_at='2023-01-10', modified_at=NULL, valid_from='2023-01-01', valid_to='2023-01-10'

-- After running the job, verify specific cntrct_id are present/absent.
SELECT COUNT(*) FROM `test-project.staging.sof_ta_cntrct_crs` WHERE cntrct_id IN (3,4,5,6);
-- Expected result: 0
```

#### Test Case 2.5: Filter Logic - `is_production`

*   **Purpose:** Verify that only contracts with `is_production = 1` are selected.
*   **Setup:** Populate `test-project.source_cds.cds_ta_cntrct` with contracts having `is_production` values: 1, 0. Ensure all other filter conditions are met.
*   **Action:** Execute `project.job_control.r_ausd_vertrag_control`.
*   **Pass/Fail Criterion:** `test-project.staging.sof_ta_cntrct_crs` must only contain rows where `is_production` is 1.

```sql
-- After running the job:
SELECT COUNT(*)
FROM `test-project.staging.sof_ta_cntrct_crs`
WHERE is_production <> 1;
-- Expected result: 0
```

#### Test Case 2.6: Filter Logic - `cntrct_ty` and `cntrct_parent` (Complex OR)

*   **Purpose:** Verify the complex `(c.cntrct_ty NOT IN (1, 2, 5) OR c.cntrct_parent IS NOT NULL)` condition.
*   **Setup:** Populate `test-project.source_cds.cds_ta_cntrct` with various combinations:
    *   `cntrct_ty = 1`, `cntrct_parent IS NULL` (Should NOT be selected)
    *   `cntrct_ty = 2`, `cntrct_parent IS NULL` (Should NOT be selected)
    *   `cntrct_ty = 5`, `cntrct_parent IS NULL` (Should NOT be selected)
    *   `cntrct_ty = 1`, `cntrct_parent IS NOT NULL` (Should be selected)
    *   `cntrct_ty = 3`, `cntrct_parent IS NULL` (Should be selected)
    *   `cntrct_ty = 4`, `cntrct_parent IS NOT NULL` (Should be selected)
    *   Ensure all other filter conditions are met.
*   **Action:** Execute `project.job_control.r_ausd_vertrag_control`.
*   **Pass/Fail Criterion:** Only rows satisfying the complex OR condition must be present in `test-project.staging.sof_ta_cntrct_crs`.

```sql
-- After running the job, verify specific cntrct_id are present/absent.
SELECT COUNT(*)
FROM `test-project.staging.sof_ta_cntrct_crs`
WHERE (cntrct_ty IN (1, 2, 5) AND cntrct_parent IS NULL);
-- Expected result: 0
```

#### Test Case 2.7: Column Mapping - `bfc_age`

*   **Purpose:** Verify that the `bfc_age` column in the target table correctly maps to `c.insert_at` from the source.
*   **Setup:** Populate `test-project.source_cds.cds_ta_cntrct` with diverse `insert_at` values. Ensure all other filter conditions are met for these rows.
*   **Action:** Execute `project.job_control.r_ausd_vertrag_control`.
*   **Pass/Fail Criterion:** For every row in `test-project.staging.sof_ta_cntrct_crs`, its `bfc_age` value must be equal to the `insert_at` value of the corresponding source row in `test-project.source_cds.cds_ta_cntrct`.

```sql
-- After running the job:
SELECT COUNT(*)
FROM `test-project.staging.sof_ta_cntrct_crs` AS target
JOIN `test-project.source_cds.cds_ta_cntrct` AS source
  ON target.cntrct_id = source.cntrct_id -- Assuming cntrct_id is a unique identifier for comparison
WHERE target.bfc_age <> source.insert_at;
-- Expected result: 0
```

---

### 3. External-System Replacements Tests

These tests validate the behavior of the migrated job concerning its interactions with external data sources and logging mechanisms, which replace the Oracle reads and KornShell utility scripts.

#### Test Case 3.1: Job Control Table Updates (Start/End)

*   **Purpose:** Verify that `project.job_control.job_table` is correctly updated at the start and end of the job execution.
*   **Setup:** Ensure `test-project.job_control.job_table` is empty.
*   **Action:**
    1.  Call `project.job_control.r_ausd_vertrag_control` with `p_JobKennung = 'TEST_JOB_CONTROL'` and `p_EintragsNr = '1'`.
    2.  Immediately after the call returns, query `job_table`.
*   **Pass/Fail Criterion:**
    *   One row must exist for `('TEST_JOB_CONTROL', '1')` with `status = 'COMPLETED'`.
    *   `start_time` and `end_time` must be populated, and `end_time` must be after `start_time`.
    *   `message` should be 'Successfully completed'.

```sql
-- After running the job:
SELECT job_kennung, eintrags_nr, status, start_time, end_time, message
FROM `test-project.job_control.job_table`
WHERE job_kennung = 'TEST_JOB_CONTROL' AND eintrags_nr = '1'
ORDER BY start_time DESC
LIMIT 1;
-- Expected: status='COMPLETED', start_time IS NOT NULL, end_time IS NOT NULL, message='Successfully completed'
```

#### Test Case 3.2: Job Result Logging

*   **Purpose:** Verify that the `project.job_control.job_result_log` table accurately records the count of processed records.
*   **Setup:** Populate `test-project.source_cds.cds_ta_cntrct` such that a known number of rows (e.g., 50) will be selected by the transformation logic.
*   **Action:** Execute `project.job_control.r_ausd_vertrag_control` with `p_JobKennung = 'TEST_JOB_RESULT'` and `p_EintragsNr = '1'`.
*   **Pass/Fail Criterion:** `test-project.job_control.job_result_log` must contain an entry for `('TEST_JOB_RESULT', '1')` with `record_count = 50` and `status = 'SUCCESS'`.

```sql
-- After running the job:
SELECT record_count, status
FROM `test-project.job_control.job_result_log`
WHERE job_kennung = 'TEST_JOB_RESULT' AND eintrags_nr = '1'
ORDER BY result_time DESC
LIMIT 1;
-- Expected: record_count=50, status='SUCCESS'
```

#### Test Case 3.3: Error Logging - `v_datum` Determination Failure

*   **Purpose:** Verify that errors during `v_datum` determination are correctly logged to `project.job_control.error_log` and the job status is set to 'FAILED'.
*   **Setup:** Ensure `test-project.isbert_schema.dwtk_meldungen` is empty or does not contain any entry for `job_kennung = 'BERT_DROP_TEMP_TABLE'`, which would cause `v_datum` to be NULL (and trigger the `SIGNAL` statement).
*   **Action:** Attempt to execute `project.job_control.r_ausd_vertrag_control` with `p_JobKennung = 'TEST_ERROR_VDATUM'` and `p_EintragsNr = '1'`. The call should fail.
*   **Pass/Fail Criterion:**
    *   `test-project.job_control.error_log` must contain an entry for `('TEST_ERROR_VDATUM', '1')` with an error message indicating `v_datum` determination failure.
    *   `test-project.job_control.job_table` must have an entry for `('TEST_ERROR_VDATUM', '1')` with `status = 'FAILED'` and a relevant error `message`.

```sql
-- After attempting to run the job:
SELECT error_message
FROM `test-project.job_control.error_log`
WHERE job_kennung = 'TEST_ERROR_VDATUM' AND eintrags_nr = '1'
ORDER BY error_time DESC
LIMIT 1;
-- Expected: error_message LIKE '%Failed to determine v_datum%'

SELECT status, message
FROM `test-project.job_control.job_table`
WHERE job_kennung = 'TEST_ERROR_VDATUM' AND eintrags_nr = '1'
ORDER BY start_time DESC
LIMIT 1;
-- Expected: status='FAILED', message LIKE '%Error determining v_datum%'
```

#### Test Case 3.4: Error Logging - Data Transformation Failure

*   **Purpose:** Verify that errors during the data transformation (`d_ausd_v_ta_cntrct_crs`) are correctly logged and the job status is set to 'FAILED'.
*   **Setup:** This is harder to simulate directly without modifying the SP. One way is to temporarily introduce a data type mismatch in `cds_ta_cntrct` that would cause an `INSERT` error (e.g., try to insert a string into an INT64 column if BigQuery's strictness allows it). Alternatively, temporarily modify `d_ausd_v_ta_cntrct_crs` to `RAISE` an error.
*   **Action:** Execute `project.job_control.r_ausd_vertrag_control` with `p_JobKennung = 'TEST_ERROR_TRANSFORM'` and `p_EintragsNr = '1'`. The call should fail.
*   **Pass/Fail Criterion:**
    *   `test-project.job_control.error_log` must contain an entry for `('TEST_ERROR_TRANSFORM', '1')` with an error message related to the transformation failure.
    *   `test-project.job_control.job_table` must have an entry for `('TEST_ERROR_TRANSFORM', '1')` with `status = 'FAILED'` and a relevant error `message`.

```sql
-- After attempting to run the job:
SELECT error_message
FROM `test-project.job_control.error_log`
WHERE job_kennung = 'TEST_ERROR_TRANSFORM' AND eintrags_nr = '1'
ORDER BY error_time DESC
LIMIT 1;
-- Expected: error_message LIKE '%Error during data transformation%'

SELECT status, message
FROM `test-project.job_control.job_table`
WHERE job_kennung = 'TEST_ERROR_TRANSFORM' AND eintrags_nr = '1'
ORDER BY start_time DESC
LIMIT 1;
-- Expected: status='FAILED', message LIKE '%Error during data transformation%'
```

---

### 4. Data Quality / Row Count / Schema Assertions

These tests ensure the structural integrity and basic quality of the data in the target table.

#### Test Case 4.1: Target Table Schema Validation

*   **Purpose:** Verify that the schema of `test-project.staging.sof_ta_cntrct_crs` matches the expected DDL.
*   **Setup:** None (relies on deployed DDL).
*   **Action:** Query BigQuery's `INFORMATION_SCHEMA` for the table schema.
*   **Pass/Fail Criterion:** The column names, data types, and nullability (if specified in DDL) of `test-project.staging.sof_ta_cntrct_crs` must exactly match the provided DDL.

```sql
SELECT column_name, data_type, is_nullable
FROM `test-project.staging.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'sof_ta_cntrct_crs'
ORDER BY ordinal_position;
/* Expected Output (compare against DDL):
cntrct_id, INT64, YES
obj_version, INT64, YES
contract_number, STRING, YES
...
bfc_age, DATE, YES
*/
```

#### Test Case 4.2: Target Table Emptiness on Empty Source

*   **Purpose:** Verify that if the source `cds_ta_cntrct` table is empty, the target `sof_ta_cntrct_crs` table also remains empty after execution.
*   **Setup:**
    1.  Ensure `test-project.source_cds.cds_ta_cntrct` is empty.
    2.  Populate `test-project.isbert_schema.dwtk_meldungen` to allow `v_datum` to be determined (e.g., `('2023-01-01 00:00:00', 'BERT_DROP_TEMP_TABLE')`).
    3.  Ensure `test-project.staging.sof_ta_cntrct_crs` is empty.
*   **Action:** Execute `project.job_control.r_ausd_vertrag_control`.
*   **Pass/Fail Criterion:** `test-project.staging.sof_ta_cntrct_crs` must contain 0 rows.

```sql
-- After running the job:
SELECT COUNT(*) FROM `test-project.staging.sof_ta_cntrct_crs`;
-- Expected result: 0
```

#### Test Case 4.3: Data Type Integrity (Post-Transformation)

*   **Purpose:** Verify that the data types of the inserted values in `sof_ta_cntrct_crs` are correct and no implicit conversions have led to data loss or errors.
*   **Setup:** Populate `test-project.source_cds.cds_ta_cntrct` with data that tests the boundaries of data types (e.g., large INT64 values, specific date formats).
*   **Action:** Execute `project.job_control.r_ausd_vertrag_control`.
*   **Pass/Fail Criterion:**
    *   No data type conversion errors should occur during job execution.
    *   Querying the target table for specific values (e.g., `cntrct_id`, `bfc_age`) should return values matching the source data and expected types.

```sql
-- Example: Check a specific date column
SELECT COUNT(*)
FROM `test-project.staging.sof_ta_cntrct_crs`
WHERE bfc_age IS NOT NULL AND NOT SAFE.PARSE_DATE('%Y-%m-%d', CAST(bfc_age AS STRING)) IS NOT NULL;
-- Expected result: 0 (all non-NULL bfc_age values should be valid dates)

-- Example: Check for unexpected string values in an INT64 column (if possible, BQ is strict)
-- This is more about ensuring the source data is clean or the migration handles it.
-- If the source has '123A' in an INT64 column, the migration should fail or handle it.
```

---

This comprehensive test plan covers the critical aspects of the migration, from end-to-end output parity to granular transformation logic and system interactions. By systematically executing these tests and verifying their criteria, confidence in the migrated BigQuery solution can be established.