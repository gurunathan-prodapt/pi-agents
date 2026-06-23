As a senior data-migration QA engineer, I have reviewed the migration design document and the generated BigQuery code for `k_ausd_v_ta_discount_rr.ksh`. The following test cases are designed to validate the migrated solution against the specified requirements, covering output parity, transformation correctness, external system replacements, and data quality/schema assertions.

---

### Pre-requisites for all Tests:

Before executing any tests, ensure the following setup is complete:
1.  **BigQuery Environment:** A BigQuery project (`my_project`) and dataset (`my_dataset`) are created and accessible.
2.  **Logging Tables:** The `my_project.my_dataset.error_log` and `my_project.my_dataset.job_log` tables are created using the provided `bq_logging_tables.sql` DDL.
3.  **Target Table:** The `my_project.my_dataset.ta_discount_rr` table is created with the schema implied by `bq_d_ausd_v_ta_discount_rr.sql`.
4.  **Source Tables:** All source tables (`cds_ta_discount_bc_assoc`, `cds_ta_discount`, `cds_ta_care_description`, `cds_ta_disc_vector`, `cds_ta_disc_invoice_item`, `dwtk_meldungen`) are created in `my_project.my_dataset` with appropriate schemas matching their Oracle counterparts.
5.  **Stored Procedures:** The `my_project.my_dataset.d_ausd_v_ta_discount_rr` and `my_project.my_dataset.r_ausd_vertrag_control` stored procedures are deployed.
6.  **Test Data Management:** For each test, ensure logging tables and the target table (`ta_discount_rr`) are cleared before execution. Source tables should be populated with specific test data relevant to the test case.

---

### Test Case 1: Successful Execution & Output Parity

**Purpose:** Verify that the entire migration flow executes successfully with valid inputs, processes data correctly according to the transformation logic, and accurately logs the outcome. This is the primary test for output parity and overall functional correctness.

**Setup:**
1.  Clear `my_project.my_dataset.error_log`, `my_project.my_dataset.job_log`, and `my_project.my_dataset.ta_discount_rr`.
2.  Populate `my_project.my_dataset.dwtk_meldungen` with a record to set `v_process_date`. For example:
    ```sql
    INSERT INTO `my_project.my_dataset.dwtk_meldungen` (job_kennung, timecreated)
    VALUES ('BERT_DROP_TEMP_TABLE', '2023-01-15 10:00:00 UTC');
    ```
    This will set `v_process_date` to `DATE '2023-01-15'`.
3.  Populate all source tables (`cds_ta_discount_bc_assoc`, `cds_ta_discount`, `cds_ta_care_description`, `cds_ta_disc_vector`, `cds_ta_disc_invoice_item`) with a comprehensive set of test data. This data should include:
    *   Rows that satisfy all `WHERE` clause conditions (date ranges, `is_production=1`, `LANGUAGE=1`).
    *   Rows with `NULL` values for `modified_at` and `valid_to` that should be included.
    *   Rows that should be excluded by the `WHERE` clauses (e.g., `insert_at` too new, `is_production=0`, `LANGUAGE!=1`).
    *   Rows that test all `INNER JOIN` conditions.
4.  Based on the populated source data and the logic in `d_ausd_v_ta_discount_rr.sql`, pre-calculate the exact expected rows and their values for `my_project.my_dataset.ta_discount_rr`. This is your "golden dataset".

**Action:**
Execute the main control stored procedure with valid parameters:
```sql
CALL `my_project.my_dataset.r_ausd_vertrag_control`('JOB_A', 'ENTRY_123');
```

**Pass/Fail Criterion:**
*   The `CALL` statement completes successfully without raising any unhandled errors.
*   `my_project.my_dataset.error_log` contains **zero** entries.
*   `my_project.my_dataset.job_log` contains two entries for `job_kennung='JOB_A'` and `eintrags_nr='ENTRY_123'`:
    1.  One entry from `d_ausd_v_ta_discount_rr` with `status = 'SUCCESS'`, `message = 'Data processing completed successfully.'`, and `records_processed` matching the expected count.
    2.  One entry from `r_ausd_vertrag_control` with `status = 'SUCCESS'`, `message = 'Data processing completed.'`, and `records_processed` matching the expected count.
*   The number of rows in `my_project.my_dataset.ta_discount_rr` exactly matches the `records_processed` count in the `job_log` entries.
*   The data in `my_project.my_dataset.ta_discount_rr` (all columns, values, and NULLs) precisely matches the pre-calculated "golden dataset".

```python
# Example pytest assertion for output parity and successful execution
import pytest
from google.cloud import bigquery
from datetime import datetime

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client()

def test_successful_execution_and_output_parity(bq_client):
    project_id = "my_project"
    dataset_id = "my_dataset"
    job_kennung = "JOB_A"
    eintrags_nr = "ENTRY_123"
    process_date_str = "2023-01-15"

    # --- Setup: Clear tables and load minimal test data ---
    bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.error_log`").result()
    bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_log`").result()
    bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.ta_discount_rr`").result()
    bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.dwtk_meldungen`").result()
    bq_client.query(f"INSERT INTO `{project_id}.{dataset_id}.dwtk_meldungen` (job_kennung, timecreated) VALUES ('BERT_DROP_TEMP_TABLE', '{process_date_str} 10:00:00 UTC')").result()

    # Load source data for a single expected output row
    bq_client.query(f"""
        INSERT INTO `{project_id}.{dataset_id}.cds_ta_discount_bc_assoc` (cntrct_id, discount_id, disc_vector_ty, cntrct_obj_version, insert_at, modified_at) VALUES ('C1', 'D1', 'T1', 1, '2023-01-01', NULL);
        INSERT INTO `{project_id}.{dataset_id}.cds_ta_discount` (discount_id, disc_vector_ty, cntrct_template_id, disc_invoice_item_id, CDS_DESCRIPTION_ID, obj_version, insert_at, modified_at, valid_from, valid_to, is_production) VALUES ('D1', 'T1', 'TMP1', 'II1', 'CD1', 1, '2023-01-01', NULL, '2023-01-01', NULL, 1);
        INSERT INTO `{project_id}.{dataset_id}.cds_ta_care_description` (cds_description_id, language, cds_description) VALUES ('CD1', 1, 'Discount A');
        INSERT INTO `{project_id}.{dataset_id}.cds_ta_disc_vector` (discount_id, disc_vector_ty, discount_obj_version, CALC_RULE_VALUE, insert_at, modified_at) VALUES ('D1', 'T1', 1, 10.5, '2023-01-01', NULL);
        INSERT INTO `{project_id}.{dataset_id}.cds_ta_disc_invoice_item` (disc_invoice_item_id, CDS_DESCRIPTION_ID, insert_at, modified_at) VALUES ('II1', 'CD2', '2023-01-01', NULL);
        INSERT INTO `{project_id}.{dataset_id}.cds_ta_care_description` (cds_description_id, language, cds_description) VALUES ('CD2', 1, 'Invoice Item A');
    """).result()
    expected_records = 1

    # Define expected output (golden dataset)
    expected_output_rows = [
        ('C1', 'D1', 'T1', 1, 'TMP1', 'II1', 'Discount A', 10.5, 'Invoice Item A')
    ]

    # --- Action ---
    bq_client.query(f"CALL `{project_id}.{dataset_id}.r_ausd_vertrag_control`('{job_kennung}', '{eintrags_nr}')").result()

    # --- Assertions ---
    # 1. Error log is empty
    error_log_rows = list(bq_client.query(f"SELECT * FROM `{project_id}.{dataset_id}.error_log`").result())
    assert len(error_log_rows) == 0, "Error log should be empty on successful run."

    # 2. Job log entries
    job_log_query = f"SELECT status, records_processed, message FROM `{project_id}.{dataset_id}.job_log` WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}' ORDER BY log_timestamp"
    job_log_rows = list(bq_client.query(job_log_query).result())
    assert len(job_log_rows) == 2, f"Expected 2 job log entries, got {len(job_log_rows)}"

    # Check d_ausd_v_ta_discount_rr log entry
    d_log = [r for r in job_log_rows if 'Data processing completed successfully.' in r.message]
    assert len(d_log) == 1, "Expected one log entry for d_ausd_v_ta_discount_rr"
    assert d_log[0].status == 'SUCCESS'
    assert d_log[0].records_processed == expected_records
    assert d_log[0].message == 'Data processing completed successfully.'

    # Check r_ausd_vertrag_control log entry
    r_log = [r for r in job_log_rows if 'Data processing completed.' in r.message]
    assert len(r_log) == 1, "Expected one log entry for r_ausd_vertrag_control"
    assert r_log[0].status == 'SUCCESS'
    assert r_log[0].records_processed == expected_records
    assert r_log[0].message == 'Data processing completed.'

    # 3. Target table row count
    target_row_count_query = f"SELECT COUNT(*) FROM `{project_id}.{dataset_id}.ta_discount_rr`"
    target_row_count = bq_client.query(target_row_count_query).result().single_value
    assert target_row_count == expected_records, f"Expected {expected_records} rows in target table, got {target_row_count}."

    # 4. Output parity (data comparison)
    target_data_query = f"""
        SELECT cntrct_id, discount_id, disc_vector_ty, cntrct_obj_version, cntrct_template_id,
               disc_invoice_item_id, rabatt, rabatthoehe, rabattierte_rech_pos
        FROM `{project_id}.{dataset_id}.ta_discount_rr` ORDER BY cntrct_id
    """
    target_data_rows = [tuple(row) for row in bq_client.query(target_data_query).result()]
    assert target_data_rows == expected_output_rows, "Target table data does not match expected output."
```

---

### Test Case 2: Parameter Validation - Missing `p_job_kennung`

**Purpose:** Verify that the control script correctly identifies and handles a missing `p_job_kennung` parameter, logs an error, and terminates execution early.

**Setup:**
1.  Clear `my_project.my_dataset.error_log`, `my_project.my_dataset.job_log`, and `my_project.my_dataset.ta_discount_rr`.
2.  Populate `my_project.my_dataset.dwtk_meldungen` as in Test Case 1 to ensure date derivation doesn't cause an *earlier* error.

**Action:**
Attempt to execute the main control stored procedure with `p_job_kennung` as `NULL`:
```sql
CALL `my_project.my_dataset.r_ausd_vertrag_control`(NULL, 'ENTRY_123');
```

**Pass/Fail Criterion:**
*   The `CALL` statement raises a `BadRequest` error (or similar BigQuery error) with a message containing `'FEHLER: 193 p_job_kennung - Parameter p_job_kennung is missing.'`.
*   `my_project.my_dataset.error_log` contains exactly one entry with:
    *   `job_kennung` = `NULL`
    *   `eintrags_nr` = `'ENTRY_123'`
    *   `error_code` = `193`
    *   `error_argument` = `'p_job_kennung'`
    *   `message` = `'Parameter p_job_kennung is missing.'`
*   `my_project.my_dataset.job_log` contains one entry for `r_ausd_vertrag_control` with `status = 'FAILURE'` and a message indicating the parameter error.
*   `my_project.my_dataset.ta_discount_rr` remains empty (no data processing should have occurred).

---

### Test Case 3: Parameter Validation - Missing `p_eintrags_nr`

**Purpose:** Verify that the control script correctly identifies and handles a missing `p_eintrags_nr` parameter, logs an error, and terminates execution early.

**Setup:**
1.  Clear `my_project.my_dataset.error_log`, `my_project.my_dataset.job_log`, and `my_project.my_dataset.ta_discount_rr`.
2.  Populate `my_project.my_dataset.dwtk_meldungen` as in Test Case 1.

**Action:**
Attempt to execute the main control stored procedure with `p_eintrags_nr` as `NULL`:
```sql
CALL `my_project.my_dataset.r_ausd_vertrag_control`('JOB_A', NULL);
```

**Pass/Fail Criterion:**
*   The `CALL` statement raises a `BadRequest` error with a message containing `'FEHLER: 193 p_eintrags_nr - Parameter p_eintrags_nr is missing.'`.
*   `my_project.my_dataset.error_log` contains exactly one entry with:
    *   `job_kennung` = `'JOB_A'`
    *   `eintrags_nr` = `NULL`
    *   `error_code` = `193`
    *   `error_argument` = `'p_eintrags_nr'`
    *   `message` = `'Parameter p_eintrags_nr is missing.'`
*   `my_project.my_dataset.job_log` contains one entry for `r_ausd_vertrag_control` with `status = 'FAILURE'` and a message indicating the parameter error.
*   `my_project.my_dataset.ta_discount_rr` remains empty.

---

### Test Case 4: Process Date Derivation Failure

**Purpose:** Verify that the control script handles scenarios where `v_process_date` cannot be derived from `dwtk_meldungen`, logs an error, and terminates.

**Setup:**
1.  Clear `my_project.my_dataset.error_log`, `my_project.my_dataset.job_log`, and `my_project.my_dataset.ta_discount_rr`.
2.  Ensure `my_project.my_dataset.dwtk_meldungen` is either empty or does not contain a record with `job_kennung = 'BERT_DROP_TEMP_TABLE'`.

**Action:**
Execute the main control stored procedure with valid `p_job_kennung` and `p_eintrags_nr`:
```sql
CALL `my_project.my_dataset.r_ausd_vertrag_control`('JOB_D', 'ENTRY_000');
```

**Pass/Fail Criterion:**
*   The `CALL` statement raises a `BadRequest` error with a message containing `'FEHLER: -1 v_process_date - Could not derive process date from dwtk_meldungen.'`.
*   `my_project.my_dataset.error_log` contains exactly one entry with:
    *   `job_kennung` = `'JOB_D'`
    *   `eintrags_nr` = `'ENTRY_000'`
    *   `error_code` = `-1`
    *   `error_argument` = `'v_process_date'`
    *   `message` = `'Could not derive process date from dwtk_meldungen.'`
*   `my_project.my_dataset.job_log` contains one entry for `r_ausd_vertrag_control` with `status = 'FAILURE'` and a message indicating the date derivation error.
*   `my_project.my_dataset.ta_discount_rr` remains empty.

---

### Test Case 5: Data Transformation - Filtering Logic (Date Ranges and `is_production`)

**Purpose:** Verify the correctness of all `WHERE` clause filters in `d_ausd_v_ta_discount_rr.sql`, especially those involving date ranges (`insert_at`, `modified_at`, `valid_from`, `valid_to`) and the `is_production` flag, and `LANGUAGE` codes.

**Setup:**
1.  Clear `my_project.my_dataset.error_log`, `my_project.my_dataset.job_log`, and `my_project.my_dataset.ta_discount_rr`.
2.  Populate `my_project.my_dataset.dwtk_meldungen` to set `v_process_date` to `DATE '2023-01-15'`.
3.  Populate source tables with data specifically designed to test each filter condition:
    *   Rows that *should be included* (satisfy all conditions, including `NULL` for `modified_at` or `valid_to` where appropriate).
    *   Rows where `insert_at > '2023-01-15'`.
    *   Rows where `modified_at <= '2023-01-15'` (and `modified_at IS NOT NULL`).
    *   Rows where `valid_from > '2023-01-15'`.
    *   Rows where `valid_to <= '2023-01-15'` (and `valid_to IS NOT NULL`).
    *   Rows where `is_production = 0`.
    *   Rows where `LANGUAGE != 1`.
4.  Record the exact count and content of rows expected in `ta_discount_rr` after applying all filters.

**Action:**
Execute the main control stored procedure:
```sql
CALL `my_project.my_dataset.r_ausd_vertrag_control`('JOB_B', 'ENTRY_456');
```

**Pass/Fail Criterion:**
*   The `CALL` statement completes successfully.
*   `my_project.my_dataset.error_log` is empty.
*   `my_project.my_dataset.job_log` shows `SUCCESS` for both procedures, with `records_processed` matching the expected count.
*   The data in `my_project.my_dataset.ta_discount_rr` precisely matches the pre-calculated expected output, confirming correct application of all `WHERE` clause filters.

---

### Test Case 6: Data Transformation - Join Correctness and NULL Handling

**Purpose:** Verify that all `INNER JOIN` conditions correctly link records across tables and that `NULL` handling for `modified_at` and `valid_to` in the `WHERE` clause works as expected.

**Setup:**
1.  Clear `my_project.my_dataset.error_log`, `my_project.my_dataset.job_log`, and `my_project.my_dataset.ta_discount_rr`.
2.  Populate `my_project.my_dataset.dwtk_meldungen` to set `v_process_date` to `DATE '2023-01-15'`.
3.  Populate source tables with:
    *   Records that have matching keys across all `INNER JOIN`s and satisfy all `WHERE` conditions.
    *   Records in one table that *do not* have matching keys in a subsequent joined table (these should be excluded by the `INNER JOIN`).
    *   Records with `NULL` values for `modified_at` and `valid_to` that *should* be included based on the `OR` condition in the `WHERE` clause.
    *   Records with non-`NULL` `modified_at` or `valid_to` that *should* be excluded by the `WHERE` clause.
4.  Record the exact count and content of rows expected in `ta_discount_rr`.

**Action:**
Execute the main control stored procedure:
```sql
CALL `my_project.my_dataset.r_ausd_vertrag_control`('JOB_C', 'ENTRY_789');
```

**Pass/Fail Criterion:**
*   The `CALL` statement completes successfully.
*   `my_project.my_dataset.error_log` is empty.
*   `my_project.my_dataset.job_log` shows `SUCCESS` for both procedures, with `records_processed` matching the expected count.
*   The data in `my_project.my_dataset.ta_discount_rr` precisely matches the pre-calculated expected output, confirming correct join behavior and `NULL` handling.

---

### Test Case 7: Data Transformation - Empty Source Tables

**Purpose:** Verify that the job handles cases where one or more source tables are empty gracefully, resulting in an empty target table and successful execution.

**Setup:**
1.  Clear `my_project.my_dataset.error_log`, `my_project.my_dataset.job_log`, and `my_project.my_dataset.ta_discount_rr`.
2.  Populate `my_project.my_dataset.dwtk_meldungen` to set `v_process_date`.
3.  Ensure all source tables (`cds_ta_discount_bc_assoc`, `cds_ta_discount`, `cds_ta_care_description`, `cds_ta_disc_vector`, `cds_ta_disc_invoice_item`) are completely empty.

**Action:**
Execute the main control stored procedure:
```sql
CALL `my_project.my_dataset.r_ausd_vertrag_control`('JOB_D_EMPTY', 'ENTRY_EMPTY');
```

**Pass/Fail Criterion:**
*   The `CALL` statement completes successfully.
*   `my_project.my_dataset.error_log` is empty.
*   `my_project.my_dataset.job_log` shows `SUCCESS` for both procedures, with `records_processed` equal to `0`.
*   `my_project.my_dataset.ta_discount_rr` remains empty.

---

### Test Case 8: Data Transformation - SQL Error Handling

**Purpose:** Verify that errors occurring during the data manipulation (e.g., due to data type issues, schema mismatches, or unexpected data) are caught, logged, and propagated correctly.

**Setup:**
1.  Clear `my_project.my_dataset.error_log`, `my_project.my_dataset.job_log`, and `my_project.my_dataset.ta_discount_rr`.
2.  Populate `my_project.my_dataset.dwtk_meldungen` to set `v_process_date`.
3.  Introduce a data condition that would cause a BigQuery SQL error in `d_ausd_v_ta_discount_rr`. For example:
    *   Temporarily alter the target table `my_project.my_dataset.ta_discount_rr` to add a `NOT NULL` constraint on a column that can receive `NULL` values from the source query (e.g., `rabatt` if `cds_description` could be `NULL`).
    *   Alternatively, if `cds_ta_disc_vector.CALC_RULE_VALUE` was originally a `STRING` in Oracle and is mapped to `FLOAT64` in BigQuery, insert a non-numeric string (e.g., `'ABC'`) into `cds_ta_disc_vector.CALC_RULE_VALUE` for a specific record.

**Action:**
Execute the main control stored procedure:
```sql
CALL `my_project.my_dataset.r_ausd_vertrag_control`('JOB_E', 'ENTRY_ERR');
```

**Pass/Fail Criterion:**
*   The `CALL` statement raises a `BadRequest` error (or similar BigQuery error).
*   The error message from the `CALL` should indicate a data processing failure (e.g., "Data processing failed: Invalid NUMERIC value: 'ABC'" or "Cannot insert NULL into non-nullable column").
*   `my_project.my_dataset.error_log` contains at least one entry related to `JOB_E`, `ENTRY_ERR`, with `error_code = -1` (or similar custom code for SQL errors), `error_argument = 'SQL_EXECUTION_ERROR'`, and a detailed `message` from BigQuery's error.
*   `my_project.my_dataset.job_log` contains:
    *   One entry for `d_ausd_v_ta_discount_rr` with `status = 'FAILURE'` and an error message.
    *   One entry for `r_ausd_vertrag_control` with `status = 'FAILURE'` and a message indicating data processing failed.
*   `my_project.my_dataset.ta_discount_rr` should be empty, as BigQuery's `INSERT` is atomic and the `TRUNCATE` would have occurred before the failed `INSERT`.

---

### Test Case 9: External System Replacement - Oracle to BigQuery Data Integrity

**Purpose:** Verify that the data migrated from the legacy Oracle system to BigQuery source tables maintains its integrity and is correctly interpreted by the BigQuery job, thus proving the "External-system replacements" behave as designed.

**Setup:**
1.  Identify a representative subset of data from the *original Oracle source tables* (`cds_ta_discount_bc_assoc`, `cds_ta_discount`, etc.).
2.  Run the *legacy Oracle job* (`k_ausd_v_ta_discount_rr.ksh` and `d_ausd_v_ta_discount_rr.sql`) with this specific data subset and capture its exact output in the Oracle `ta_discount_rr` table. This is the "legacy golden dataset".
3.  Migrate *only this specific data subset* from Oracle to the corresponding BigQuery source tables (`my_project.my_dataset.cds_ta_discount_bc_assoc`, etc.). Ensure data types, NULLs, and values are preserved during this initial data load.
4.  Populate `my_project.my_dataset.dwtk_meldungen` to set `v_process_date` appropriately for the test data (matching the date used for the legacy run).
5.  Clear `my_project.my_dataset.error_log`, `my_project.my_dataset.job_log`, and `my_project.my_dataset.ta_discount_rr`.

**Action:**
Execute the main control stored procedure:
```sql
CALL `my_project.my_dataset.r_ausd_vertrag_control`('JOB_F', 'ENTRY_ORA');
```

**Pass/Fail Criterion:**
*   The `CALL` statement completes successfully.
*   `my_project.my_dataset.error_log` is empty.
*   `my_project.my_dataset.job_log` shows `SUCCESS` for both procedures.
*   The data in `my_project.my_dataset.ta_discount_rr` (BigQuery) exactly matches the "legacy golden dataset" captured from the Oracle `ta_discount_rr` table. This confirms that the BigQuery job processes the migrated data identically to how the Oracle job processed the original data, validating the external system replacement.

```sql
-- This comparison would typically be done using an external tool or script.
-- Example SQL for comparison (assuming 'legacy_oracle_golden_dataset_ta_discount_rr' is a temporary BigQuery table loaded with the Oracle output):

-- 1. Compare row counts
SELECT
    (SELECT COUNT(*) FROM `my_project.my_dataset.ta_discount_rr`) AS bq_count,
    (SELECT COUNT(*) FROM `my_project.my_dataset.legacy_oracle_golden_dataset_ta_discount_rr`) AS oracle_count;
-- Expected: bq_count = oracle_count

-- 2. Compare content (should return 0 rows if identical)
SELECT
    COUNT(*)
FROM
(
    SELECT * FROM `my_project.my_dataset.ta_discount_rr`
    EXCEPT DISTINCT
    SELECT * FROM `my_project.my_dataset.legacy_oracle_golden_dataset_ta_discount_rr`
) AS diff_bq_oracle;

SELECT
    COUNT(*)
FROM
(
    SELECT * FROM `my_project.my_dataset.legacy_oracle_golden_dataset_ta_discount_rr`
    EXCEPT DISTINCT
    SELECT * FROM `my_project.my_dataset.ta_discount_rr`
) AS diff_oracle_bq;
-- Expected: Both diff queries should return 0.
```

---

### Test Case 10: Data Quality - Schema and Data Type Integrity

**Purpose:** Verify that the target table `ta_discount_rr` in BigQuery has the correct schema (column names, data types, nullability) as defined by the migration and that data types are handled correctly during insertion.

**Setup:**
1.  Clear `my_project.my_dataset.error_log`, `my_project.my_dataset.job_log`, and `my_project.my_dataset.ta_discount_rr`.
2.  Populate `my_project.my_dataset.dwtk_meldungen` to set `v_process_date`.
3.  Populate source tables with data that covers all possible data types and potential edge cases (e.g., maximum length strings, boundary values for numbers, `NULL`s). This is similar to Test Case 1, but with an explicit focus on data type boundaries.

**Action:**
Execute the main control stored procedure:
```sql
CALL `my_project.my_dataset.r_ausd_vertrag_control`('JOB_G', 'ENTRY_SCHEMA');
```

**Pass/Fail Criterion:**
*   The `CALL` statement completes successfully.
*   `my_project.my_dataset.error_log` is empty.
*   `my_project.my_dataset.job_log` shows `SUCCESS`.
*   The schema of `my_project.my_dataset.ta_discount_rr` (column names, data types, and nullability) matches the expected BigQuery schema.
*   All inserted data conforms to the target column data types without truncation, conversion errors, or unexpected `NULL`s. For example, `rabatthoehe` (FLOAT64) should contain floating-point numbers, not strings.

```python
# Example pytest assertion for schema integrity
import pytest
from google.cloud import bigquery

def test_target_schema_integrity(bq_client):
    project_id = "my_project"
    dataset_id = "my_dataset"
    target_table_id = "ta_discount_rr"

    # Define the expected schema based on the bq_d_ausd_v_ta_discount_rr.sql INSERT statement
    expected_schema = [
        bigquery.SchemaField("cntrct_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("discount_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("disc_vector_ty", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("cntrct_obj_version", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("cntrct_template_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("disc_invoice_item_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("rabatt", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("rabatthoehe", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("rabattierte_rech_pos", "STRING", mode="NULLABLE"),
    ]

    table_ref = bq_client.dataset(dataset_id, project=project_id).table(target_table_id)
    table = bq_client.get_table(table_ref)

    # Compare field names, types, and modes
    actual_schema_fields = [(f.name, f.field_type, f.mode) for f in table.schema]
    expected_schema_fields = [(f.name, f.field_type, f.mode) for f in expected_schema]

    assert actual_schema_fields == expected_schema_fields, "Target table schema does not match expected schema."

    # Further checks would involve running the job (as in Test Case 1) and then
    # querying the target table to verify data types of inserted values.
    # E.g., SELECT rabatthoehe FROM ta_discount_rr WHERE TYPEOF(rabatthoehe) != 'FLOAT64'
    # (BigQuery's type system handles this implicitly, but explicit checks can be useful).
```

---

### Test Case 11: Missing Orchestration Logic (Active Job Handling) - *Behavioral Gap*

**Purpose:** Highlight a critical behavioral discrepancy: the migration design document states the original script's purpose includes "Ignoring already active jobs to prevent redundant execution" and "Deactivating old active jobs." However, the provided BigQuery `r_ausd_vertrag_control` stored procedure does not implement this logic. This test aims to confirm this gap.

**Setup:**
1.  Clear `my_project.my_dataset.error_log` and `my_project.my_dataset.ta_discount_rr`.
2.  Populate `my_project.my_dataset.dwtk_meldungen` to allow date derivation.
3.  Simulate an "active" job by manually inserting a `RUNNING` entry into `my_project.my_dataset.job_log` for a specific `job_kennung` and `eintrags_nr`:
    ```sql
    INSERT INTO `my_project.my_dataset.job_log`
    (job_kennung, eintrags_nr, start_timestamp, end_timestamp, status, records_processed, message)
    VALUES
    ('JOB_ACTIVE', 'ENTRY_ACTIVE', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), NULL, 'RUNNING', 0, 'Simulated active job from previous run.');
    ```
4.  Populate source tables with some data so that `d_ausd_v_ta_discount_rr` would process rows if executed.

**Action:**
Execute the main control stored procedure with the *same* `job_kennung` and `eintrags_nr` as the simulated active job:
```sql
CALL `my_project.my_dataset.r_ausd_vertrag_control`('JOB_ACTIVE', 'ENTRY_ACTIVE');
```

**Pass/Fail Criterion:**
*   **Expected Legacy Behavior (based on design document):** The legacy KornShell script would detect the existing active job and either ignore the current execution (exit gracefully without processing data) or manage/deactivate the old job. It would *not* proceed to create a new, concurrent "RUNNING" entry for the same job instance.
*   **Actual Migrated Behavior (from generated code):** The BigQuery `r_ausd_vertrag_control` procedure, as provided, does *not* contain logic to check for or manage active jobs. It will proceed with its execution flow, including:
    *   Inserting a *new* `RUNNING` entry into `my_project.my_dataset.job_log` for `JOB_ACTIVE`/`ENTRY_ACTIVE`.
    *   Calling `d_ausd_v_ta_discount_rr`, which will `TRUNCATE` and re-insert data into `ta_discount_rr`.
    *   Updating its own `job_log` entry to `SUCCESS` (or `FAILURE` if data processing fails).
*   **Pass/Fail for Migration Validation:**
    *   **FAIL (Behavioral Discrepancy):** The BigQuery procedure will *not* ignore the active job. It will proceed to execute, creating a new `RUNNING` entry in `job_log` and attempting to process data. This demonstrates a **behavioral discrepancy** from the stated purpose of the legacy script.
    *   **Recommendation:** This test case highlights a critical missing piece of functionality. The migration team needs to either:
        1.  Implement the active job checking/deactivation logic in `r_ausd_vertrag_control` (e.g., querying `job_log` for `status = 'RUNNING'` for the given `job_kennung`/`eintrags_nr` and raising an error or performing cleanup).
        2.  Explicitly document this as an intentional change in behavior or a deferred implementation, with justification, as it impacts job concurrency and data integrity.

```sql
-- SQL to verify the behavioral gap after running the action:
SELECT
    job_kennung,
    eintrags_nr,
    status,
    COUNT(*) AS entry_count
FROM `my_project.my_dataset.job_log`
WHERE job_kennung = 'JOB_ACTIVE' AND eintrags_nr = 'ENTRY_ACTIVE'
GROUP BY 1, 2, 3;

-- Expected result (if the gap exists):
-- job_kennung  | eintrags_nr  | status   | entry_count
-- -------------|--------------|----------|------------
-- JOB_ACTIVE   | ENTRY_ACTIVE | RUNNING  | 1  (from initial setup, but should be updated to FAILURE or SUCCESS by the new run)
-- JOB_ACTIVE   | ENTRY_ACTIVE | SUCCESS  | 1  (from the new run)
-- JOB_ACTIVE   | ENTRY_ACTIVE | RUNNING  | 1  (from d_ausd_v_ta_discount_rr's initial log)
-- This indicates that a new job instance ran concurrently or overwrote the previous one without proper handling.
-- A correct implementation would likely result in only one final entry for the job, or an error indicating concurrency.
```