As a senior data-migration QA engineer, I've reviewed the migration design document and the generated BigQuery code for `DW.BERT_AUSD_BP_TA_ICCID_VERTRAG`. Below is a comprehensive set of validation tests covering output parity, transformation correctness, external system replacements, and data quality assertions.

**Introduction:**

This test plan outlines the validation strategy for the `DW.BERT_AUSD_BP_TA_ICCID_VERTRAG` job migration. The primary goal is to ensure that the re-platformed ETL process on Google Cloud Platform (BigQuery/Airflow) is functionally equivalent to its legacy Oracle/KornShell/UC4 counterpart.

**Key Assumptions for Testing:**

*   **Legacy Baseline:** A "golden" dataset representing the output of the legacy job for specific input scenarios is available for comparison. This is crucial for output parity tests.
*   **Test Data:** The ability to inject specific, controlled test data into `project.source_dataset.sof_ta_iccid_einzeln` and `project.source_dataset.dwtk_meldungen` (if used) in BigQuery.
*   **Environment:** BigQuery tables, stored procedures, and the Airflow DAG are deployed and accessible in a test environment.
*   **`p_wiederanlaufWert` Discrepancy:** A critical observation is that the provided BigQuery stored procedures (`k_ausd_bp_ta_iccid_vertrag_sp`) perform a full `TRUNCATE` and `INSERT` operation, effectively ignoring the `p_wiederanlaufWert` parameter for data filtering, despite the design document's "Unresolved / Risks" section describing a restart logic involving `DWH_VERTRAG_ID`. This discrepancy will be explicitly tested and highlighted.

---

### Test Case 1: Schema Parity and Data Type Mapping

*   **Purpose:** To verify that the schema of the target BigQuery table `project.target_dataset.sof_ta_iccid_vertrag` precisely matches the legacy Oracle `SOF$TA_ICCID_VERTRAG` table, including column names, order, and data types.
*   **Setup:**
    1.  Ensure the `project.target_dataset.sof_ta_iccid_vertrag` table has been created using the provided DDL.
    2.  Obtain the definitive schema definition (column names, data types, nullability) of the legacy Oracle `SOF$TA_ICCID_VERTRAG` table.
*   **Action:**
    1.  Query the schema of `project.target_dataset.sof_ta_iccid_vertrag` in BigQuery using `INFORMATION_SCHEMA`.
    2.  Compare the retrieved BigQuery schema against the documented legacy Oracle schema.
*   **Pass/Fail Criterion:**
    *   **Pass:** All column names, their ordinal position, and their corresponding BigQuery data types (e.g., `VARCHAR2` to `STRING`, `DATE` to `DATE`, `NUMBER` to `INT64`) exactly match the legacy schema. Nullability constraints should also align.
    *   **Fail:** Any discrepancy in column names, order, data types, or nullability.

```sql
-- BigQuery SQL to retrieve schema for comparison
SELECT
    column_name,
    data_type,
    is_nullable
FROM
    `project.target_dataset.INFORMATION_SCHEMA.COLUMNS`
WHERE
    table_schema = 'target_dataset' AND table_name = 'sof_ta_iccid_vertrag'
ORDER BY
    ordinal_position;

-- Example legacy Oracle schema query (to be run in Oracle)
-- SELECT COLUMN_NAME, DATA_TYPE, NULLABLE FROM ALL_TAB_COLUMNS WHERE OWNER = 'ISBERT_SCHEMA' AND TABLE_NAME = 'SOF$TA_ICCID_VERTRAG' ORDER BY COLUMN_ID;
```

---

### Test Case 2: Full Load Output Parity - Standard Data

*   **Purpose:** To verify that the migrated job produces identical output data to the legacy job when processing a representative, standard set of input data, covering the core aggregation and pivoting logic. This is the primary output parity test.
*   **Setup:**
    1.  **Legacy Baseline:** Execute the legacy `DW.BERT_AUSD_BP_TA_ICCID_VERTRAG` job with a specific `Stichtag` (e.g., '20231026') and `Wiederanlaufwert` (e.g., 0) against a known, comprehensive `SOF$TA_ICCID_EINZELN` dataset. Export the resulting `SOF$TA_ICCID_VERTRAG` table as a "golden" CSV or JSON file.
    2.  **BigQuery Source Data:** Load the *exact same* `SOF$TA_ICCID_EINZELN` dataset into `project.source_dataset.sof_ta_iccid_einzeln`.
    3.  Ensure `project.target_dataset.sof_ta_iccid_vertrag` is empty before execution.
*   **Action:**
    1.  Trigger the Airflow DAG `dw_bert_ausd_bp_ta_iccid_vertrag` with `p_stichtag='20231026'` and `p_wiederanlaufWert=0`.
    2.  After successful completion, extract all data from `project.target_dataset.sof_ta_iccid_vertrag`.
    3.  Compare the extracted BigQuery data with the legacy "golden" dataset.
*   **Pass/Fail Criterion:**
    *   **Pass:** The number of rows in the BigQuery target table is identical to the legacy output. For every column, the data values for each `CNTRCT_ID` are identical between BigQuery and legacy outputs. This includes correct handling of NULLs and date formats.
    *   **Fail:** Any difference in row count or data values for any column.

```python
# Example Python (pytest) assertion for data parity
import pandas as pd
from google.cloud import bigquery

def test_full_load_output_parity_standard_data():
    client = bigquery.Client()
    
    # Load the "golden" output from the legacy system
    # Ensure this file is available in the test environment
    legacy_df = pd.read_csv("legacy_golden_data.csv").sort_values(by="CNTRCT_ID").reset_index(drop=True)

    # Trigger Airflow DAG (this step would typically be part of a test harness)
    # For this test, we assume the DAG has been triggered and completed successfully.
    # Example: call Airflow API to trigger DAG, then wait for completion.

    # Query BigQuery target table after DAG execution
    query = """
        SELECT * FROM `your-gcp-project-id.target_dataset.sof_ta_iccid_vertrag`
        ORDER BY CNTRCT_ID
    """
    bq_df = client.query(query).to_dataframe().sort_values(by="CNTRCT_ID").reset_index(drop=True)

    # Standardize date columns for robust comparison
    for col in bq_df.select_dtypes(include=['datetime64[ns]']).columns:
        bq_df[col] = bq_df[col].dt.strftime('%Y-%m-%d')
    for col in legacy_df.select_dtypes(include=['datetime64[ns]']).columns:
        legacy_df[col] = legacy_df[col].dt.strftime('%Y-%m-%d')

    # Ensure column order is the same for direct comparison
    bq_df = bq_df[legacy_df.columns]

    # Perform a deep comparison of the dataframes
    pd.testing.assert_frame_equal(legacy_df, bq_df, check_dtype=False, check_like=True)
```

---

### Test Case 3: Transformation Correctness - Pivoting Logic & NULL Handling

*   **Purpose:** To validate the complex pivoting logic (mapping `ICC_TYPE` to `TN_`, `TC_`, `TB_`, `MSx_` columns) and aggregation (`MAX`), as well as specific NULL handling, for various scenarios.
*   **Setup:**
    1.  **Legacy Baseline:** Prepare specific `SOF$TA_ICCID_EINZELN` data for each scenario below and run the legacy job. Export the results as "golden" files.
    2.  **BigQuery Source Data:** Load the corresponding test data into `project.source_dataset.sof_ta_iccid_einzeln`.
    3.  Ensure `project.target_dataset.sof_ta_iccid_vertrag` is empty before each scenario run.
*   **Action:**
    1.  For each scenario, trigger the Airflow DAG with `p_stichtag='20231026'` and `p_wiederanlaufWert=0`.
    2.  Query `project.target_dataset.sof_ta_iccid_vertrag` and compare against the legacy baseline or expected outcome.
*   **Scenarios & Pass/Fail Criterion:**

    *   **Scenario 3.1: Single ICCID per Contract (e.g., only TN type)**
        *   **Input:** `sof_ta_iccid_einzeln` contains one record for `CNTRCT_ID='C1'`, with `ICC_TYPE='TN'` and associated data.
        *   **Expected Output:** `sof_ta_iccid_vertrag` for `C1` should have `TN_ICCID` and related `TN_` fields populated, all other `ICCID` fields (TC, TB, MSx) should be NULL.
        *   **Pass/Fail:** Output matches legacy and expected logic.

    *   **Scenario 3.2: Multiple ICCIDs for a Contract (e.g., TN, TC, MS1)**
        *   **Input:** `sof_ta_iccid_einzeln` contains three records for `CNTRCT_ID='C2'`, with `ICC_TYPE='TN'`, `ICC_TYPE='TC'`, and `ICC_TYPE='MS1'`, each with distinct data.
        *   **Expected Output:** `sof_ta_iccid_vertrag` for `C2` should have `TN_`, `TC_`, and `MS1_` fields populated from their respective source records, and other `MSx_` fields NULL.
        *   **Pass/Fail:** Output matches legacy and expected logic.

    *   **Scenario 3.3: More than 10 ICCID types for a Contract (Edge Case)**
        *   **Input:** `sof_ta_iccid_einzeln` contains 12 records for `CNTRCT_ID='C3'`, with `ICC_TYPE` values like 'TN', 'TC', 'TB', 'MS1', ..., 'MS10', 'MS11', 'MS12'.
        *   **Expected Output:** `sof_ta_iccid_vertrag` for `C3` should have `TN_`, `TC_`, `TB_`, `MS1_` through `MS10_` fields populated. The data for `MS11` and `MS12` types should be *ignored* or *not mapped* as the target schema only goes up to `MS10`. This tests the implicit filtering/mapping of the pivot.
        *   **Pass/Fail:** Output matches legacy and expected logic (i.e., only MS1-MS10 are populated, MS11/MS12 data is not present in the target).

    *   **Scenario 3.4: NULL Handling for Date Fields (`NVL` to `COALESCE`)**
        *   **Input:** `sof_ta_iccid_einzeln` contains a record where `timecreated` (used for `VALID_TO` fields) is NULL.
        *   **Expected Output:** The corresponding `_VALID_TO` field in `sof_ta_iccid_vertrag` should be `DATE('1900-01-01')` due to the `COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')` logic.
        *   **Pass/Fail:** Output matches legacy and `COALESCE` logic.

```sql
-- Example BigQuery SQL for Scenario 3.3 (after running the job)
-- Verify that MS11 and MS12 data are not present by checking the last expected MS field
SELECT
    CNTRCT_ID,
    MS10_ICCID,
    MS10_VALID_TO
FROM
    `project.target_dataset.sof_ta_iccid_vertrag`
WHERE
    CNTRCT_ID = 'C3';

-- Example BigQuery SQL for Scenario 3.4 (after running the job)
-- Verify NULL handling for dates
SELECT
    CNTRCT_ID,
    TN_VALID_TO,
    TC_VALID_TO
FROM
    `project.target_dataset.sof_ta_iccid_vertrag`
WHERE
    CNTRCT_ID = 'C_NULL_DATE' -- Assuming a contract ID specifically for this test
    AND (TN_VALID_TO = DATE('1900-01-01') OR TC_VALID_TO = DATE('1900-01-01'));
```

---

### Test Case 4: `p_wiederanlaufWert` Parameter Handling (Critical Discrepancy)

*   **Purpose:** To explicitly test how the `p_wiederanlaufWert` parameter is handled in the migrated BigQuery code, given the discrepancy between the design document's description and the current implementation.
*   **Setup:**
    1.  **Legacy Baseline:**
        *   Populate `SOF$TA_ICCID_EINZELN` with data including `CNTRCT_ID` values both less than and greater than a chosen `p_wiederanlaufWert` (e.g., 100).
        *   Pre-populate `SOF$TA_ICCID_VERTRAG` with some data, including `CNTRCT_ID`s that would be affected by a restart logic (e.g., `CNTRCT_ID`s around 100).
        *   Run the legacy job with `p_wiederanlaufWert=100`. Observe and document the exact behavior (e.g., are records `>=100` deleted? Are only records `>100` processed?). Export the legacy `SOF$TA_ICCID_VERTRAG` output.
    2.  **BigQuery Source Data:** Load the *exact same* `sof_ta_iccid_einzeln` data into `project.source_dataset.sof_ta_iccid_einzeln`.
    3.  **BigQuery Target Data:** Pre-populate `project.target_dataset.sof_ta_iccid_vertrag` with the same initial data as the legacy target table.
*   **Action:**
    1.  Trigger the Airflow DAG with `p_stichtag='20231026'` and `p_wiederanlaufWert=100`.
    2.  Observe the contents of `project.target_dataset.sof_ta_iccid_vertrag` after the job completes.
*   **Pass/Fail Criterion:**
    *   **Pass (if current BigQuery code is accepted as-is):** The `project.target_dataset.sof_ta_iccid_vertrag` table is completely truncated and then repopulated with *all* data from `sof_ta_iccid_einzeln`, irrespective of `p_wiederanlaufWert`. The `p_wiederanlaufWert` parameter is passed to `k_ausd_bp_ta_iccid_vertrag_sp` but has no functional effect on the data transformation or filtering.
    *   **Fail (if legacy behavior strictly followed design document):** The `project.target_dataset.sof_ta_iccid_vertrag` table contains only data for `CNTRCT_ID > 100`, and any existing records with `CNTRCT_ID >= 100` were correctly handled (deleted/updated) according to the design's restart logic.
    *   **Critical Note:** This test is designed to highlight the discrepancy. If the legacy system *does* implement the restart logic described in the design document, then the current BigQuery code will *fail* this test, indicating a functional gap in the migration. This requires immediate discussion with the development team and business stakeholders.

```sql
-- BigQuery SQL to check target table content after running with p_wiederanlaufWert=100
-- (Assuming source has CNTRCT_ID from 1 to 200)
SELECT COUNT(*) FROM `project.target_dataset.sof_ta_iccid_vertrag`;
SELECT MIN(CNTRCT_ID), MAX(CNTRCT_ID) FROM `project.target_dataset.sof_ta_iccid_vertrag`;

-- If the current code is accepted, COUNT(*) should be 200, MIN(CNTRCT_ID) should be 1.
-- If the design's restart logic was implemented, COUNT(*) would be 100, MIN(CNTRCT_ID) would be 101.
```

---

### Test Case 5: Date Parameter Validation and Error Handling

*   **Purpose:** To verify that the stored procedures correctly validate the `p_stichtag` parameter (format `YYYYMMDD`) and handle invalid inputs gracefully by logging errors and failing the job.
*   **Setup:**
    1.  Ensure `project.audit_dataset.job_registry` and `project.audit_dataset.job_log` tables are empty.
*   **Action:**
    1.  **Scenario 5.1: Invalid `p_stichtag` format.** Trigger the Airflow DAG with `p_stichtag='2023-10-26'` (invalid format) and `p_wiederanlaufWert=0`.
    2.  **Scenario 5.2: Missing `p_stichtag`.** Trigger the Airflow DAG with `p_stichtag=NULL` (or an empty string if NULL is not directly supported by the operator) and `p_wiederanlaufWert=0`.
    3.  Monitor Airflow task status and query `job_registry` and `job_log` tables.
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   The Airflow task for `start_r_ausd_bp_ta_iccid_vertrag_sp` fails.
        *   An entry in `project.audit_dataset.job_registry` exists for the failed run with `status='FAILED'` and an informative `error_message` (e.g., "Invalid p_stichtag format").
        *   `project.audit_dataset.job_log` contains `ERROR` level messages detailing the invalid parameter.
        *   The `project.target_dataset.sof_ta_iccid_vertrag` table remains unchanged (no truncate/insert occurred).
    *   **Fail:** The job completes successfully despite invalid parameters, or error logging is incorrect/missing.

```sql
-- BigQuery SQL to check audit tables after an expected failure
SELECT
    job_id,
    job_name,
    start_time,
    end_time,
    status,
    error_message,
    parameters
FROM
    `project.audit_dataset.job_registry`
WHERE
    job_name = 'AUSD_BP_TA_ICCID_VERTRAG'
ORDER BY
    start_time DESC
LIMIT 1;

SELECT
    log_time,
    log_level,
    message
FROM
    `project.audit_dataset.job_log`
WHERE
    job_id = (SELECT job_id FROM `project.audit_dataset.job_registry` WHERE job_name = 'AUSD_BP_TA_ICCID_VERTRAG' ORDER BY start_time DESC LIMIT 1)
ORDER BY
    log_time ASC;
```

---

### Test Case 6: Audit Logging and Job Status Tracking

*   **Purpose:** To verify that the `job_registry` and `job_log` tables are correctly populated throughout the job execution lifecycle (start, info messages, success/failure). This validates the external system replacement for legacy logging.
*   **Setup:**
    1.  Ensure `project.source_dataset.sof_ta_iccid_einzeln` contains valid data.
    2.  Ensure `project.audit_dataset.job_registry` and `project.audit_dataset.job_log` tables are empty.
*   **Action:**
    1.  **Scenario 6.1: Successful Run.** Trigger the Airflow DAG with valid parameters (e.g., `p_stichtag='20231026'`, `p_wiederanlaufWert=0`).
    2.  **Scenario 6.2: Failed Run (e.g., simulate a SQL error).** Temporarily modify the `d_ausd_bp_ta_iccid_vertrag_insert.sql` content within `k_ausd_bp_ta_iccid_vertrag_sp` to introduce a syntax error (e.g., `SELECT non_existent_column FROM ...`). Trigger the DAG. Revert the change after testing.
    3.  Query `job_registry` and `job_log` tables after each scenario.
*   **Pass/Fail Criterion:**
    *   **Pass (Successful Run):**
        *   `job_registry` contains one entry with `status='SUCCEEDED'`, `start_time`, `end_time`, `processed_records` (matching actual row count), and `parameters` correctly populated.
        *   `job_log` contains `INFO` messages for job start, truncation, insert, and completion.
    *   **Pass (Failed Run):**
        *   `job_registry` contains one entry with `status='FAILED'`, `start_time`, `end_time`, and an `error_message` describing the SQL error. `processed_records` might be NULL or 0 depending on when the error occurred.
        *   `job_log` contains `INFO` messages up to the point of failure, followed by `ERROR` messages detailing the SQL error.
    *   **Fail:** Incorrect status, missing log entries, inaccurate record counts, or uninformative error messages.

```sql
-- BigQuery SQL to check audit tables after a successful run
SELECT
    job_id,
    job_name,
    start_time,
    end_time,
    status,
    processed_records,
    parameters
FROM
    `project.audit_dataset.job_registry`
WHERE
    job_name = 'AUSD_BP_TA_ICCID_VERTRAG'
ORDER BY
    start_time DESC
LIMIT 1;

SELECT
    log_time,
    log_level,
    message
FROM
    `project.audit_dataset.job_log`
WHERE
    job_id = (SELECT job_id FROM `project.audit_dataset.job_registry` WHERE job_name = 'AUSD_BP_TA_ICCID_VERTRAG' ORDER BY start_time DESC LIMIT 1)
ORDER BY
    log_time ASC;
```

---

### Test Case 7: Row Count Assertion

*   **Purpose:** To verify that the number of rows processed and inserted into the target table matches the expected count based on the source data and transformation logic.
*   **Setup:**
    1.  Prepare `project.source_dataset.sof_ta_iccid_einzeln` with a known number of distinct `CNTRCT_ID`s (e.g., 1000 unique contracts).
    2.  Ensure `project.target_dataset.sof_ta_iccid_vertrag` is empty.
*   **Action:**
    1.  Trigger the Airflow DAG with valid parameters.
    2.  After completion, query the row count of `project.target_dataset.sof_ta_iccid_vertrag`.
    3.  Query the `processed_records` from `project.audit_dataset.job_registry` for the latest run.
*   **Pass/Fail Criterion:**
    *   **Pass:** The row count in `project.target_dataset.sof_ta_iccid_vertrag` is equal to the number of distinct `CNTRCT_ID`s in the source `project.source_dataset.sof_ta_iccid_einzeln` table. The `processed_records` in `job_registry` also matches this count.
    *   **Fail:** Any discrepancy in row counts.

```sql
-- BigQuery SQL to get expected row count from source
SELECT COUNT(DISTINCT CNTRCT_ID) FROM `project.source_dataset.sof_ta_iccid_einzeln`;

-- BigQuery SQL to get actual row count from target
SELECT COUNT(*) FROM `project.target_dataset.sof_ta_iccid_vertrag`;

-- BigQuery SQL to get processed_records from audit log
SELECT processed_records
FROM `project.audit_dataset.job_registry`
WHERE job_name = 'AUSD_BP_TA_ICCID_VERTRAG'
ORDER BY start_time DESC
LIMIT 1;
```

---

### Test Case 8: Empty Source Table Handling

*   **Purpose:** To verify that the job handles an empty source table gracefully, resulting in an empty target table and correct logging.
*   **Setup:**
    1.  Ensure `project.source_dataset.sof_ta_iccid_einzeln` is empty.
    2.  Ensure `project.target_dataset.sof_ta_iccid_vertrag` is empty (or pre-populate it to ensure truncation works).
    3.  Ensure `project.audit_dataset.job_registry` and `project.audit_dataset.job_log` tables are empty.
*   **Action:**
    1.  Trigger the Airflow DAG with valid parameters.
    2.  Query `project.target_dataset.sof_ta_iccid_vertrag` and the audit tables.
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   `project.target_dataset.sof_ta_iccid_vertrag` is empty.
        *   `job_registry` shows `status='SUCCEEDED'` and `processed_records=0`.
        *   `job_log` contains `INFO` messages indicating successful completion with 0 records processed.
    *   **Fail:** The job fails, or the target table is not empty, or logging is incorrect.

```sql
-- BigQuery SQL to check target table and audit logs
SELECT COUNT(*) FROM `project.target_dataset.sof_ta_iccid_vertrag`;
SELECT status, processed_records FROM `project.audit_dataset.job_registry` WHERE job_name = 'AUSD_BP_TA_ICCID_VERTRAG' ORDER BY start_time DESC LIMIT 1;
SELECT message FROM `project.audit_dataset.job_log` WHERE job_id = (SELECT job_id FROM `project.audit_dataset.job_registry` WHERE job_name = 'AUSD_BP_TA_ICCID_VERTRAG' ORDER BY start_time DESC LIMIT 1) AND log_level = 'INFO' AND message LIKE '%Successfully inserted 0 records%';
```

---

### Test Case 9: Airflow Orchestration and Parameter Passing

*   **Purpose:** To verify that the Airflow DAG correctly triggers the top-level BigQuery stored procedure and passes parameters as expected, validating the UC4 to Airflow replacement.
*   **Setup:**
    1.  Ensure the Airflow DAG `dw_bert_ausd_bp_ta_iccid_vertrag` is deployed.
    2.  Ensure `project.audit_dataset.job_registry` is empty.
*   **Action:**
    1.  Manually trigger the Airflow DAG from the Airflow UI, providing specific values for `p_stichtag` (e.g., '20231101') and `p_wiederanlaufWert` (e.g., 1).
    2.  Observe the Airflow task logs for the `BigQueryStartStoredProcedureOperator` for any errors.
    3.  Query `project.audit_dataset.job_registry` to inspect the `parameters` JSON field for the latest run.
*   **Pass/Fail Criterion:**
    *   **Pass:** The Airflow task completes successfully. The `parameters` JSON in `job_registry` for the corresponding job run accurately reflects the `p_stichtag` and `p_wiederanlaufWert` values passed from Airflow.
    *   **Fail:** Airflow task fails to trigger the SP, or parameters are not correctly received/logged in BigQuery.

```sql
-- BigQuery SQL to check parameters in job_registry
SELECT
    JSON_VALUE(parameters, '$.p_stichtag') AS passed_stichtag,
    CAST(JSON_VALUE(parameters, '$.p_wiederanlaufWert') AS INT64) AS passed_wiederanlaufWert
FROM
    `project.audit_dataset.job_registry`
WHERE
    job_name = 'AUSD_BP_TA_ICCID_VERTRAG'
ORDER BY
    start_time DESC
LIMIT 1;

-- Expected output for the above query should match the values provided in Airflow.
```