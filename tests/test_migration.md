As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the `r_ausd_v_ta_notice.ksh` job migration. These tests aim to ensure behavioral equivalence between the legacy Oracle/KornShell system and the new BigQuery/Cloud Composer solution.

The tests are categorized to address output parity, transformation correctness, external system replacements, and data quality assertions, as per the requirements.

**Assumptions for Test Environment:**
*   **Legacy System Access:** The Oracle database is accessible for extracting baseline data.
*   **BigQuery Environment:** A BigQuery project (`your_gcp_project_id`) and datasets (`your_source_dataset`, `your_target_dataset`, `your_audit_dataset`, `legacy_snapshot_dataset`) are set up.
*   **Data Ingestion:** The `cds$ta_notice` and `dwtk_meldungen` tables have been ingested from Oracle into BigQuery as `your_source_dataset.cds_ta_notice` and `your_source_dataset.dwtk_meldungen` respectively.
*   **Migrated Code:** The Cloud Composer DAG (`r_ausd_v_ta_notice_dag.py`) and BigQuery Stored Procedure (`sp_process_ta_notice`) are deployed.
*   **Test Data:** Specific test data sets (e.g., `test_data_set_1`, `test_data_set_2`) are prepared in both Oracle and BigQuery to cover various scenarios.

---

## Migration Validation Test Plan: `r_ausd_v_ta_notice.ksh`

### I. Baseline Data Capture (Prerequisite for Data Validation Tests)

**Purpose:** To establish a definitive "golden source" and "golden output" from the legacy system against which the migrated BigQuery solution will be compared. This step is crucial for output parity and transformation correctness validation.

**Setup:**
1.  **Prepare Test Data:** Create a specific, controlled set of test data in the Oracle source tables (`oracle_schema.cds$ta_notice`, `oracle_schema.dwtk_meldungen`). This data should include:
    *   Rows that are expected to be inserted into `sof$ta_notice`.
    *   Rows that are expected to be filtered out by each condition (`insert_at`, `modified_at`, `valid_to`, `is_production`).
    *   Rows with `NULL` values in `modified_at` and `valid_to`.
    *   Rows that trigger the `v_datum` fallback to `1900-01-01`.
    *   A `dwtk_meldungen` entry for `BERT_DROP_TEMP_TABLE` with a specific `timecreated`.
2.  **Snapshot Oracle Source Data:** Extract the prepared data from `oracle_schema.cds$ta_notice` and `oracle_schema.dwtk_meldungen` into BigQuery temporary tables (e.g., `legacy_snapshot_dataset.cds_ta_notice_oracle_snapshot_pre_run`, `legacy_snapshot_dataset.dwtk_meldungen_oracle_snapshot_pre_run`).
3.  **Run Legacy Job:** Execute the original `r_ausd_v_ta_notice.ksh` job against the prepared Oracle data. Ensure the job completes successfully.
4.  **Snapshot Oracle Target Data:** Extract the resulting data from `oracle_schema.sof$ta_notice` into a BigQuery temporary table (e.g., `legacy_snapshot_dataset.sof_ta_notice_oracle_snapshot_post_run`).

**Action:**
This is a setup phase. The "action" is the execution of the legacy job and the subsequent data extraction.

**Pass/Fail Criterion:**
*   All Oracle source and target data is successfully extracted and loaded into the designated BigQuery snapshot tables.
*   The legacy job completes without errors.

---

### II. Prerequisite Data Ingestion Validation

**Purpose:** To verify that the data ingested from the Oracle source system into BigQuery is an exact replica of the original, ensuring that the migration starts with accurate source data. This addresses "External-system replacements" for Oracle reads.

**Setup:**
*   Oracle source tables (`oracle_schema.cds$ta_notice`, `oracle_schema.dwtk_meldungen`) populated with `test_data_set_1`.
*   BigQuery ingested tables (`your_source_dataset.cds_ta_notice`, `your_source_dataset.dwtk_meldungen`) populated from Oracle.
*   BigQuery snapshot tables from **I. Baseline Data Capture** are available.

**Action:**
Compare the contents of the BigQuery ingested tables with their Oracle snapshot counterparts.

**Pass/Fail Criterion:**
*   **Pass:** The row count and content (all columns) of `your_source_dataset.cds_ta_notice` exactly match `legacy_snapshot_dataset.cds_ta_notice_oracle_snapshot_pre_run`.
*   **Pass:** The row count and content (all columns) of `your_source_dataset.dwtk_meldungen` exactly match `legacy_snapshot_dataset.dwtk_meldungen_oracle_snapshot_pre_run`.
*   **Fail:** Any discrepancy in row count or data content.

```sql
-- Test 2.1: cds_ta_notice Ingestion Parity
SELECT
    CASE
        WHEN (SELECT COUNT(*) FROM `your_gcp_project_id.your_source_dataset.cds_ta_notice`) = (SELECT COUNT(*) FROM `your_gcp_project_id.legacy_snapshot_dataset.cds_ta_notice_oracle_snapshot_pre_run`)
        AND NOT EXISTS (
            (SELECT * FROM `your_gcp_project_id.your_source_dataset.cds_ta_notice` EXCEPT DISTINCT SELECT * FROM `your_gcp_project_id.legacy_snapshot_dataset.cds_ta_notice_oracle_snapshot_pre_run`)
            UNION ALL
            (SELECT * FROM `your_gcp_project_id.legacy_snapshot_dataset.cds_ta_notice_oracle_snapshot_pre_run` EXCEPT DISTINCT SELECT * FROM `your_gcp_project_id.your_source_dataset.cds_ta_notice`)
        )
        THEN 'PASS'
        ELSE 'FAIL'
    END AS cds_ta_notice_ingestion_parity_test;

-- Test 2.2: dwtk_meldungen Ingestion Parity
SELECT
    CASE
        WHEN (SELECT COUNT(*) FROM `your_gcp_project_id.your_source_dataset.dwtk_meldungen`) = (SELECT COUNT(*) FROM `your_gcp_project_id.legacy_snapshot_dataset.dwtk_meldungen_oracle_snapshot_pre_run`)
        AND NOT EXISTS (
            (SELECT * FROM `your_gcp_project_id.your_source_dataset.dwtk_meldungen` EXCEPT DISTINCT SELECT * FROM `your_gcp_project_id.legacy_snapshot_dataset.dwtk_meldungen_oracle_snapshot_pre_run`)
            UNION ALL
            (SELECT * FROM `your_gcp_project_id.legacy_snapshot_dataset.dwtk_meldungen_oracle_snapshot_pre_run` EXCEPT DISTINCT SELECT * FROM `your_gcp_project_id.your_source_dataset.dwtk_meldungen`)
        )
        THEN 'PASS'
        ELSE 'FAIL'
    END AS dwtk_meldungen_ingestion_parity_test;
```

---

### III. Orchestration & Parameter Handling

**Purpose:** To verify that the Cloud Composer DAG correctly orchestrates the job, handles parameters, and logs execution status, replacing the KornShell wrapper scripts. This covers "External-system replacements" for shell orchestration.

**Setup:**
*   Cloud Composer environment is running.
*   `r_ausd_v_ta_notice_dag.py` is deployed.
*   BigQuery audit tables (`your_audit_dataset.job_audit`, `your_audit_dataset.job_error_log`) are created.

**Action:**
1.  Trigger the `r_ausd_v_ta_notice_dag` in Cloud Composer with valid parameters (e.g., `p_JobKennung='TEST_JOB'`, `p_EintragsNr=123`).
2.  Trigger the DAG with missing/invalid parameters (e.g., `p_JobKennung` is `NULL` or too long).
3.  Trigger the DAG with a scenario that causes the underlying BigQuery Stored Procedure to fail (e.g., by temporarily revoking permissions or introducing a syntax error in the SP for testing purposes).

**Pass/Fail Criterion:**

**Test 3.1: DAG Execution and Logging (Valid Parameters)**
*   **Pass:** The DAG runs successfully, and an entry is recorded in `your_audit_dataset.job_audit` with `status = 'SUCCESS'` for the given `p_JobKennung` and `p_EintragsNr`. Cloud Logging shows expected execution messages.
*   **Fail:** DAG fails, or audit log entry is incorrect/missing.

**Test 3.2: Parameter Passing to Stored Procedure**
*   **Pass:** The `sp_process_ta_notice` stored procedure receives and correctly uses the `p_JobKennung` and `p_EintragsNr` passed from the DAG. This can be verified by checking audit logs or temporary debug outputs within the SP.
*   **Fail:** Parameters are not passed correctly or are misinterpreted.

**Test 3.3: Error Handling (Invalid Parameters / SP Failure)**
*   **Pass:** When triggered with invalid parameters, the DAG fails gracefully, and an entry is recorded in `your_audit_dataset.job_error_log` with appropriate error messages. When the SP fails, the DAG task fails, and an error is logged in `your_audit_dataset.job_error_log` and Cloud Logging.
*   **Fail:** DAG hangs, fails unexpectedly without proper logging, or logs incorrect error information.

```python
# Example pytest for triggering DAG and checking logs (conceptual)
import pytest
from airflow.models import DagBag
from google.cloud import bigquery
import time

# Assume Airflow environment is set up and DAGs are deployed
# This is a conceptual test; actual Airflow interaction might use Airflow API or specific test utilities

@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client(project="your_gcp_project_id")

def test_dag_execution_and_logging_success(bq_client):
    dag_id = "r_ausd_v_ta_notice_dag"
    job_kennung = "TEST_JOB_SUCCESS"
    eintrags_nr = 1001
    
    # Simulate triggering the DAG (e.g., via Airflow REST API or CLI)
    # For actual testing, you'd use Airflow's test utilities or trigger via API
    print(f"Triggering DAG {dag_id} with JobKennung={job_kennung}, EintragsNr={eintrags_nr}")
    # Placeholder for actual DAG trigger logic
    # e.g., subprocess.run(["airflow", "dags", "trigger", dag_id, "--conf", f"job_kennung={job_kennung},eintrags_nr={eintrags_nr}"])
    
    # Wait for DAG to complete (adjust sleep as needed)
    time.sleep(60) 
    
    # Check BigQuery audit log for success
    query = f"""
    SELECT status, error_message
    FROM `your_gcp_project_id.your_audit_dataset.job_audit`
    WHERE job_kennung = '{job_kennung}' AND eintrags_nr = {eintrags_nr}
    ORDER BY start_time DESC
    LIMIT 1
    """
    rows = list(bq_client.query(query).result())
    
    assert len(rows) == 1, "Audit log entry not found or multiple entries"
    assert rows[0].status == "SUCCESS", f"DAG did not succeed. Status: {rows[0].status}, Error: {rows[0].error_message}"
    print(f"DAG {dag_id} completed successfully and logged to audit table.")

def test_dag_execution_and_logging_failure(bq_client):
    dag_id = "r_ausd_v_ta_notice_dag"
    job_kennung = "TEST_JOB_FAILURE"
    eintrags_nr = 1002
    
    # To simulate failure, you might need to temporarily modify the SP or revoke permissions
    # For this test, assume a mechanism exists to force SP failure for this specific run.
    print(f"Triggering DAG {dag_id} to simulate failure with JobKennung={job_kennung}, EintragsNr={eintrags_nr}")
    # Placeholder for actual DAG trigger logic that leads to failure
    
    time.sleep(60) 
    
    # Check BigQuery error log for failure
    query = f"""
    SELECT error_message
    FROM `your_gcp_project_id.your_audit_dataset.job_error_log`
    WHERE job_kennung = '{job_kennung}' AND eintrags_nr = {eintrags_nr}
    ORDER BY error_time DESC
    LIMIT 1
    """
    rows = list(bq_client.query(query).result())
    
    assert len(rows) == 1, "Error log entry not found or multiple entries"
    assert "ERROR" in rows[0].error_message or "FAILURE" in rows[0].error_message, "Error message not found in log"
    print(f"DAG {dag_id} failed as expected and logged to error table.")

```

---

### IV. Core Transformation Logic Validation

**Purpose:** To verify that the BigQuery Stored Procedure (`sp_process_ta_notice`) correctly implements all transformation rules, including `v_datum` derivation, filtering, NULL handling, and data type conversions. This covers "Transformation correctness".

**Setup:**
*   BigQuery source tables (`your_source_dataset.cds_ta_notice`, `your_source_dataset.dwtk_meldungen`) populated with `test_data_set_1`.
*   BigQuery target table (`your_target_dataset.sof_ta_notice`) exists and is empty.
*   BigQuery snapshot tables from **I. Baseline Data Capture** are available.

**Action:**
Execute the `sp_process_ta_notice` stored procedure directly in BigQuery (or via the DAG, ensuring it's isolated for this test).

**Pass/Fail Criterion:**

**Test 4.1: `v_datum` Derivation Correctness**
*   **Purpose:** Verify `v_datum` is correctly derived from `dwtk_meldungen` or defaults to `1900-01-01`.
*   **Setup:** `dwtk_meldungen` contains a `BERT_DROP_TEMP_TABLE` entry with `timecreated = '2023-01-15 10:00:00 UTC'`.
*   **Action:** Run the SP. Inspect the `v_datum` value (if logged or accessible via debug).
*   **Pass:** `v_datum` is `DATE '2023-01-15'`.
*   **Setup (Fallback):** `dwtk_meldungen` is empty or has no `BERT_DROP_TEMP_TABLE` entry.
*   **Action:** Run the SP.
*   **Pass:** `v_datum` is `DATE '1900-01-01'`.

**Test 4.2: `TRUNCATE` Operation Correctness**
*   **Purpose:** Verify the target table `sof_ta_notice` is truncated before insertion.
*   **Setup:** `your_target_dataset.sof_ta_notice` contains 5 rows.
*   **Action:** Run the SP.
*   **Pass:** After the SP starts but before `INSERT`, `your_target_dataset.sof_ta_notice` has 0 rows. (Requires inspecting job history or adding debug steps to SP).

**Test 4.3 - 4.6: `INSERT` Filter Logic (Individual Conditions)**
*   **Purpose:** Verify each filter condition (`insert_at`, `modified_at`, `valid_to`, `is_production`) works as expected.
*   **Setup:** Populate `cds_ta_notice` with specific rows:
    *   One row where `insert_at > v_datum`.
    *   One row where `modified_at <= v_datum` (and not NULL).
    *   One row where `valid_to <= v_datum` (and not NULL).
    *   One row where `is_production = 0`.
    *   One row that satisfies all conditions.
*   **Action:** Run the SP.
*   **Pass:** Only the row satisfying all conditions is inserted into `your_target_dataset.sof_ta_notice`. The other specific rows are correctly filtered out.

**Test 4.7: Combined Filter Logic**
*   **Purpose:** Verify the combined effect of all filter conditions.
*   **Setup:** `cds_ta_notice` contains `test_data_set_1` which includes a mix of rows that should pass and fail the combined filter.
*   **Action:** Run the SP.
*   **Pass:** The set of rows inserted into `your_target_dataset.sof_ta_notice` matches the expected output based on the legacy job's logic for `test_data_set_1`.

**Test 4.8: NULL Handling in Source Columns**
*   **Purpose:** Verify `NULL` values in `modified_at` and `valid_to` are handled correctly as per `IS NULL` conditions.
*   **Setup:** `cds_ta_notice` contains:
    *   A row with `modified_at IS NULL` and `valid_to IS NULL`, satisfying other conditions.
    *   A row with `modified_at IS NULL` but `valid_to <= v_datum`.
*   **Action:** Run the SP.
*   **Pass:** The row with both `NULL`s is inserted. The row with `modified_at IS NULL` but invalid `valid_to` is filtered out.

**Test 4.9: Data Type Conversion Correctness**
*   **Purpose:** Verify that Oracle data types are correctly mapped to BigQuery types and values are preserved.
*   **Setup:** `cds_ta_notice` contains values at the boundaries of data types (e.g., max date, min date, large numbers if applicable).
*   **Action:** Run the SP.
*   **Pass:** All inserted values in `your_target_dataset.sof_ta_notice` match the expected types and values from the Oracle source, without truncation or conversion errors.

```sql
-- Example SQL for Test 4.7 (Combined Filter Logic) - after running the SP
-- This compares the actual output with the expected output based on the legacy snapshot.
SELECT
    CASE
        WHEN (SELECT COUNT(*) FROM `your_gcp_project_id.your_target_dataset.sof_ta_notice`) = (SELECT COUNT(*) FROM `your_gcp_project_id.legacy_snapshot_dataset.sof_ta_notice_oracle_snapshot_post_run`)
        AND NOT EXISTS (
            (SELECT * FROM `your_gcp_project_id.your_target_dataset.sof_ta_notice` EXCEPT DISTINCT SELECT * FROM `your_gcp_project_id.legacy_snapshot_dataset.sof_ta_notice_oracle_snapshot_post_run`)
            UNION ALL
            (SELECT * FROM `your_gcp_project_id.legacy_snapshot_dataset.sof_ta_notice_oracle_snapshot_post_run` EXCEPT DISTINCT SELECT * FROM `your_gcp_project_id.your_target_dataset.sof_ta_notice`)
        )
        THEN 'PASS'
        ELSE 'FAIL'
    END AS combined_filter_logic_test;

-- Example for Test 4.1 (v_datum derivation) - requires logging or temporary table in SP
-- If the SP logs v_datum to an audit table:
SELECT
    CASE
        WHEN (SELECT v_datum_logged FROM `your_gcp_project_id.your_audit_dataset.job_audit` WHERE job_kennung = 'TEST_JOB_VDATUM' ORDER BY start_time DESC LIMIT 1) = DATE '2023-01-15'
        THEN 'PASS'
        ELSE 'FAIL'
    END AS v_datum_derivation_test;
```

---

### V. End-to-End Output Parity

**Purpose:** To confirm that the final output table (`sof_ta_notice`) in BigQuery is identical to the output produced by the legacy Oracle job for the same input data. This directly addresses "Output parity".

**Setup:**
*   BigQuery source tables (`your_source_dataset.cds_ta_notice`, `your_source_dataset.dwtk_meldungen`) are populated with `test_data_set_1` (matching the Oracle baseline).
*   The `r_ausd_v_ta_notice_dag` has been executed successfully with `test_data_set_1`.
*   BigQuery snapshot table `legacy_snapshot_dataset.sof_ta_notice_oracle_snapshot_post_run` is available.

**Action:**
Compare the content of `your_target_dataset.sof_ta_notice` with `legacy_snapshot_dataset.sof_ta_notice_oracle_snapshot_post_run`.

**Pass/Fail Criterion:**

**Test 5.1: Full Output Parity (`sof_ta_notice`)**
*   **Pass:** The content (all columns and rows) of `your_target_dataset.sof_ta_notice` is an exact match to `legacy_snapshot_dataset.sof_ta_notice_oracle_snapshot_post_run`.
*   **Fail:** Any difference in data content.

**Test 5.2: Row Count Parity (`sof_ta_notice`)**
*   **Pass:** The row count of `your_target_dataset.sof_ta_notice` is identical to `legacy_snapshot_dataset.sof_ta_notice_oracle_snapshot_post_run`.
*   **Fail:** Different row counts.

```sql
-- Test 5.1 & 5.2: Full Output and Row Count Parity
SELECT
    CASE
        WHEN (SELECT COUNT(*) FROM `your_gcp_project_id.your_target_dataset.sof_ta_notice`) = (SELECT COUNT(*) FROM `your_gcp_project_id.legacy_snapshot_dataset.sof_ta_notice_oracle_snapshot_post_run`)
        AND NOT EXISTS (
            (SELECT * FROM `your_gcp_project_id.your_target_dataset.sof_ta_notice` EXCEPT DISTINCT SELECT * FROM `your_gcp_project_id.legacy_snapshot_dataset.sof_ta_notice_oracle_snapshot_post_run`)
            UNION ALL
            (SELECT * FROM `your_gcp_project_id.legacy_snapshot_dataset.sof_ta_notice_oracle_snapshot_post_run` EXCEPT DISTINCT SELECT * FROM `your_gcp_project_id.your_target_dataset.sof_ta_notice`)
        )
        THEN 'PASS'
        ELSE 'FAIL'
    END AS sof_ta_notice_output_parity_test;
```

---

### VI. Edge Cases

**Purpose:** To ensure the migrated job handles unusual or boundary conditions gracefully and correctly.

**Setup:**
*   BigQuery source tables (`your_source_dataset.cds_ta_notice`, `your_source_dataset.dwtk_meldungen`).
*   BigQuery target table (`your_target_dataset.sof_ta_notice`).

**Action:**
For each test, prepare the source data as described, then execute the `r_ausd_v_ta_notice_dag`.

**Pass/Fail Criterion:**

**Test 6.1: Empty Source Table (`cds_ta_notice`)**
*   **Setup:** `your_source_dataset.cds_ta_notice` is empty. `your_source_dataset.dwtk_meldungen` has a valid `v_datum` entry.
*   **Action:** Run the DAG.
*   **Pass:** `your_target_dataset.sof_ta_notice` is empty (0 rows). The job completes successfully.
*   **Fail:** Job fails, or `sof_ta_notice` contains unexpected data.

**Test 6.2: All Rows Filtered Out**
*   **Setup:** `your_source_dataset.cds_ta_notice` contains rows, but all of them fail at least one filter condition (e.g., all `is_production = 0`). `your_source_dataset.dwtk_meldungen` has a valid `v_datum` entry.
*   **Action:** Run the DAG.
*   **Pass:** `your_target_dataset.sof_ta_notice` is empty (0 rows). The job completes successfully.
*   **Fail:** Job fails, or `sof_ta_notice` contains unexpected data.

**Test 6.3: `dwtk_meldungen` Empty (fallback `v_datum`)**
*   **Setup:** `your_source_dataset.dwtk_meldungen` is empty or contains no `job_kennung = 'BERT_DROP_TEMP_TABLE'` entries. `your_source_dataset.cds_ta_notice` contains rows, some of which would pass if `v_datum` is `1900-01-01`.
*   **Action:** Run the DAG.
*   **Pass:** The job runs successfully, and `your_target_dataset.sof_ta_notice` contains data filtered correctly using `v_datum = DATE '1900-01-01'`.
*   **Fail:** Job fails, or `sof_ta_notice` contains incorrect data (e.g., if `v_datum` was not correctly defaulted).

```sql
-- Test 6.1: Empty Source Table
-- After running DAG with empty cds_ta_notice
SELECT
    CASE
        WHEN (SELECT COUNT(*) FROM `your_gcp_project_id.your_target_dataset.sof_ta_notice`) = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS empty_source_table_test;

-- Test 6.2: All Rows Filtered Out
-- After running DAG with all rows filtered out
SELECT
    CASE
        WHEN (SELECT COUNT(*) FROM `your_gcp_project_id.your_target_dataset.sof_ta_notice`) = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS all_rows_filtered_out_test;

-- Test 6.3: dwtk_meldungen Empty (fallback v_datum)
-- This requires a specific test dataset for cds_ta_notice where some rows pass with v_datum=1900-01-01
-- and others don't. Then compare the output with a pre-calculated expected result.
-- Example:
-- Expected count if v_datum = '1900-01-01' is X
SELECT
    CASE
        WHEN (SELECT COUNT(*) FROM `your_gcp_project_id.your_target_dataset.sof_ta_notice`) = X -- Replace X with expected count
        THEN 'PASS'
        ELSE 'FAIL'
    END AS fallback_v_datum_row_count_test;
```

---

### VII. Schema Assertions

**Purpose:** To ensure that the schema of the target table in BigQuery matches the expected schema, including column names, data types, and nullability constraints. This addresses "Data-quality / row-count / schema assertions".

**Setup:**
*   The `your_target_dataset.sof_ta_notice` table exists in BigQuery.
*   The expected schema (derived from Oracle `sof$ta_notice`) is documented.

**Action:**
Query BigQuery's `INFORMATION_SCHEMA` to retrieve the schema of `your_target_dataset.sof_ta_notice`.

**Pass/Fail Criterion:**
*   **Pass:** The schema of `your_target_dataset.sof_ta_notice` (column names, data types, and nullability) precisely matches the documented expected schema derived from the Oracle `sof$ta_notice` table.
*   **Fail:** Any discrepancy in column names, data types, or nullability.

```sql
-- Test 7.1: Target Table Schema Parity
-- This query retrieves the schema of the BigQuery table
SELECT
    column_name,
    data_type,
    is_nullable
FROM
    `your_gcp_project_id.your_target_dataset.INFORMATION_SCHEMA.COLUMNS`
WHERE
    table_name = 'sof_ta_notice'
ORDER BY
    ordinal_position;

-- Manual or automated comparison against expected schema:
-- Expected Schema (example, based on design doc's INSERT statement):
-- | column_name        | data_type | is_nullable |
-- |--------------------|-----------|-------------|
-- | cntrct_id          | STRING    | YES         | -- Assuming Oracle NUMBER/VARCHAR2 maps to STRING
-- | valid_from         | DATE      | YES         |
-- | valid_to           | DATE      | YES         |
-- | entry_date_of_notice | DATE      | YES         |

-- Pass/Fail: The output of the query above must match the expected schema.
```