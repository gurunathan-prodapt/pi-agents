As a senior data-migration QA engineer, I've developed a comprehensive suite of validation tests for the `DW.BERT_AUSD_V_TA_P_DISCOUNT` job migration. These tests are designed to ensure behavioral equivalence between the legacy Oracle/KornShell system and the new BigQuery/Airflow implementation, covering output parity, transformation correctness, external system integration, and data quality.

The migration design indicates two main job flows:
1.  `DW.BERT_AUSD_V_TA_P_DISCOUNT_RR` (for `ta_p_discount_rr` target table)
2.  `DW.BERT_AUSD_V_TA_P_DISCOUNT` (for `ta_p_discount` target table)

Both flows share similar orchestration, logging, and `Stichtag` (processing date) determination logic, but differ in their core SQL transformations and target tables. The tests below address both paths where applicable and highlight specific differences.

**Assumptions:**
*   The `sp_d_ausd_v_ta_p_discount_rr` and `sp_d_ausd_v_ta_p_discount` BigQuery stored procedures (not provided in the generated code) correctly implement the `INSERT INTO ... SELECT` logic described in the design document, including `Stichtag` determination from `dwtk_meldungen`.
*   BigQuery source tables (`dwtk_meldungen`, `ta_discount_rr`, `ta_cntrct_crs`, `ta_cntrct_templ`, `ta_disc_zusgf`) are populated with data identical to the legacy Oracle system for comparison.
*   BigQuery target tables (`ta_p_discount_rr`, `ta_p_discount`) and logging tables (`job_control`, `job_log`, `job_error_log`) are created with appropriate schemas.
*   `your-gcp-project` and `your_dataset` placeholders are replaced with actual project and dataset IDs.
*   Airflow is configured with the `google_cloud_default` connection.

---

## 1. Output Parity - `ta_p_discount_rr` (Full Data Comparison)

**Purpose:** To verify that the migrated `DW.BERT_AUSD_V_TA_P_DISCOUNT_RR` job produces identical output data in the `ta_p_discount_rr` target table as the legacy Oracle job, given the same source data. This covers the core transformation logic, including joins, column selections, and data type conversions.

**Setup:**
1.  **Legacy Baseline:**
    *   Populate the legacy Oracle source tables (`isbert_schema.dwtk_meldungen`, `sof$ta_discount_rr`, `sof$ta_cntrct_crs`, `sof$ta_cntrct_templ`) with a comprehensive dataset. This dataset should include:
        *   Records with matching `CONTRACT_ID` across all join tables.
        *   Records in `sof$ta_discount_rr` that do not have a match in `sof$ta_cntrct_crs` or `sof$ta_cntrct_templ` (to test join behavior).
        *   Records with `NULL` values in relevant columns (e.g., `CONTRACT_ID`, `DISCOUNT_NAME`).
        *   Specific `timecreated` entries in `isbert_schema.dwtk_meldungen` for `job_kennung = 'BERT_DROP_TEMP_TABLE'` to define a known `Stichtag`.
    *   Execute the legacy `DW.BERT_AUSD_V_TA_P_DISCOUNT_RR` job.
    *   Extract the final data from the legacy `sof$ta_p_discount_rr` table into a canonical format (e.g., CSV, JSON) or a dedicated baseline table in BigQuery.
2.  **Migrated Environment:**
    *   Create the BigQuery target table `your_dataset.ta_p_discount_rr` with the expected schema.
    *   Populate the BigQuery source tables (`your_dataset.dwtk_meldungen`, `your_dataset.ta_discount_rr`, `your_dataset.ta_cntrct_crs`, `your_dataset.ta_cntrct_templ`) with data *identical* to the legacy Oracle source tables used for the baseline.

**Action:**
1.  Trigger the `dw_bert_ausd_v_ta_p_discount_rr` Airflow DAG.
    *   Ensure the `p_processing_date` parameter (derived from `{{ ds }}`) is set to a specific, known date for consistency.

**Pass/Fail Criterion:**
*   The row count of `your_dataset.ta_p_discount_rr` must exactly match the row count of the legacy `sof$ta_p_discount_rr` baseline.
*   A full data comparison (e.g., using a checksum of sorted rows, or direct row-by-row comparison) between `your_dataset.ta_p_discount_rr` and the legacy baseline must show no differences. This includes all column values, considering appropriate data type conversions (e.g., Oracle `NUMBER` to BigQuery `INT64`/`NUMERIC`, Oracle `DATE` to BigQuery `DATE`/`TIMESTAMP`).

**Test Code (SQL Assertion Example):**

```sql
-- Step 1: Compare row counts
SELECT
    (SELECT COUNT(*) FROM `your-gcp-project.your_dataset.ta_p_discount_rr`) AS migrated_row_count,
    (SELECT COUNT(*) FROM `your-gcp-project.your_dataset.ta_p_discount_rr_LEGACY_BASELINE`) AS legacy_row_count
HAVING
    migrated_row_count = legacy_row_count;

-- Step 2: Compare data checksums (assuming a consistent ordering and hashing)
-- This requires a robust way to generate a comparable hash for each row.
-- Example using MD5 for a simplified scenario (adjust columns as needed):
SELECT
    (SELECT FARM_FINGERPRINT(ARRAY_AGG(TO_JSON_STRING(t) ORDER BY DISCOUNT_ID, CONTRACT_ID)) FROM `your-gcp-project.your_dataset.ta_p_discount_rr` AS t) AS migrated_checksum,
    (SELECT FARM_FINGERPRINT(ARRAY_AGG(TO_JSON_STRING(t) ORDER BY DISCOUNT_ID, CONTRACT_ID)) FROM `your-gcp-project.your_dataset.ta_p_discount_rr_LEGACY_BASELINE` AS t) AS legacy_checksum
HAVING
    migrated_checksum = legacy_checksum;

-- Step 3: Identify specific differences (if checksums don't match)
SELECT 'Migrated Only' AS source, * FROM `your-gcp-project.your_dataset.ta_p_discount_rr`
EXCEPT DISTINCT
SELECT 'Migrated Only' AS source, * FROM `your-gcp-project.your_dataset.ta_p_discount_rr_LEGACY_BASELINE`
UNION ALL
SELECT 'Legacy Only' AS source, * FROM `your-gcp-project.your_dataset.ta_p_discount_rr_LEGACY_BASELINE`
EXCEPT DISTINCT
SELECT 'Legacy Only' AS source, * FROM `your-gcp-project.your_dataset.ta_p_discount_rr`;
```

---

## 2. Output Parity - `ta_p_discount` (Full Data Comparison)

**Purpose:** To verify that the migrated `DW.BERT_AUSD_V_TA_P_DISCOUNT` job produces identical output data in the `ta_p_discount` target table as the legacy Oracle job, given the same source data. This covers the core transformation logic for this specific job flow.

**Setup:**
1.  **Legacy Baseline:**
    *   Populate the legacy Oracle source tables (`isbert_schema.dwtk_meldungen`, `sof$ta_disc_zusgf`, `sof$ta_cntrct_crs`) with a comprehensive dataset, similar to Test 1 but tailored for these sources.
    *   Execute the legacy `DW.BERT_AUSD_V_TA_P_DISCOUNT` job.
    *   Extract the final data from the legacy `sof$ta_p_discount` table into a canonical format or a dedicated baseline table in BigQuery.
2.  **Migrated Environment:**
    *   Create the BigQuery target table `your_dataset.ta_p_discount` with the expected schema.
    *   Populate the BigQuery source tables (`your_dataset.dwtk_meldungen`, `your_dataset.ta_disc_zusgf`, `your_dataset.ta_cntrct_crs`) with data *identical* to the legacy Oracle source tables used for the baseline.

**Action:**
1.  Trigger the `dw_bert_ausd_v_ta_p_discount` Airflow DAG.
    *   Ensure the `p_processing_date` parameter is set to a specific, known date.

**Pass/Fail Criterion:**
*   The row count of `your_dataset.ta_p_discount` must exactly match the row count of the legacy `sof$ta_p_discount` baseline.
*   A full data comparison between `your_dataset.ta_p_discount` and the legacy baseline must show no differences.

**Test Code (SQL Assertion Example):**
(Similar to Test 1, replacing `ta_p_discount_rr` with `ta_p_discount` and `ta_p_discount_rr_LEGACY_BASELINE` with `ta_p_discount_LEGACY_BASELINE`).

---

## 3. Transformation Correctness - `Stichtag` Determination

**Purpose:** To verify that the `PROCESSING_DATE` column in the target tables is correctly derived from `dwtk_meldungen` as per the legacy logic (`MAX(timecreated)` for `BERT_DROP_TEMP_TABLE` or '19000101' if no match). This specifically tests the external system replacement for `dwtk_meldungen` and the `Stichtag` logic.

**Setup:**
1.  **Case A: Multiple `dwtk_meldungen` entries:**
    *   Populate `your_dataset.dwtk_meldungen` with several records for `job_kennung = 'BERT_DROP_TEMP_TABLE'`, with varying `timecreated` values (e.g., '2023-01-01 10:00:00', '2023-01-05 12:30:00', '2023-01-03 08:00:00').
    *   Include other `job_kennung` values to ensure correct filtering.
    *   Populate other source tables (`ta_discount_rr`, `ta_cntrct_crs`, etc.) with minimal valid data to allow the job to run and produce output.
2.  **Case B: No `dwtk_meldungen` entries for `BERT_DROP_TEMP_TABLE`:**
    *   Ensure `your_dataset.dwtk_meldungen` either has no records or no records with `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    *   Populate other source tables with minimal valid data.

**Action:**
1.  Trigger both `dw_bert_ausd_v_ta_p_discount_rr` and `dw_bert_ausd_v_ta_p_discount` Airflow DAGs for Case A.
2.  Trigger both DAGs again for Case B (after resetting `dwtk_meldungen`).

**Pass/Fail Criterion:**
*   **Case A:** The `PROCESSING_DATE` column in all rows of `your_dataset.ta_p_discount_rr` and `your_dataset.ta_p_discount` must be '2023-01-05' (derived from the MAX `timecreated` '2023-01-05 12:30:00').
*   **Case B:** The `PROCESSING_DATE` column in all rows of `your_dataset.ta_p_discount_rr` and `your_dataset.ta_p_discount` must be '1900-01-01'.

**Test Code (SQL Assertion Example):**

```sql
-- For Case A:
SELECT COUNT(*) FROM `your-gcp-project.your_dataset.ta_p_discount_rr`
WHERE PROCESSING_DATE = DATE('2023-01-05');
-- Pass if COUNT(*) equals total rows in ta_p_discount_rr.

SELECT COUNT(*) FROM `your-gcp-project.your_dataset.ta_p_discount`
WHERE PROCESSING_DATE = DATE('2023-01-05');
-- Pass if COUNT(*) equals total rows in ta_p_discount.

-- For Case B:
SELECT COUNT(*) FROM `your-gcp-project.your_dataset.ta_p_discount_rr`
WHERE PROCESSING_DATE = DATE('1900-01-01');
-- Pass if COUNT(*) equals total rows in ta_p_discount_rr.

SELECT COUNT(*) FROM `your-gcp-project.your_dataset.ta_p_discount`
WHERE PROCESSING_DATE = DATE('1900-01-01');
-- Pass if COUNT(*) equals total rows in ta_p_discount.
```

---

## 4. Transformation Correctness - Join Logic and NULL Handling

**Purpose:** To verify that the join conditions and handling of `NULL` values in join keys or selected columns are correctly translated from Oracle SQL to BigQuery SQL. This ensures that records are correctly matched or excluded, and `NULL`s propagate as expected.

**Setup:**
1.  **Source Data Scenarios:** Populate BigQuery source tables with data to test:
    *   **Perfect Match:** All join keys (`CONTRACT_ID`) exist in all relevant tables.
    *   **No Match (Left Side):** Records in `ta_discount_rr` (or `ta_disc_zusgf`) where `CONTRACT_ID` does not exist in `ta_cntrct_crs` or `ta_cntrct_templ`.
    *   **No Match (Right Side):** Records in `ta_cntrct_crs` or `ta_cntrct_templ` where `CONTRACT_ID` does not exist in `ta_discount_rr` (or `ta_disc_zusgf`).
    *   **NULL Join Key:** Records where `CONTRACT_ID` is `NULL` in one or more source tables.
    *   **NULL Data Column:** Records where `CONTRACT_NUMBER` or `CONTRACT_TEMPLATE` are `NULL` in `ta_cntrct_crs` or `ta_cntrct_templ`.
2.  Ensure `dwtk_meldungen` has data for `Stichtag` determination.

**Action:**
1.  Trigger both `dw_bert_ausd_v_ta_p_discount_rr` and `dw_bert_ausd_v_ta_p_discount` Airflow DAGs.

**Pass/Fail Criterion:**
*   **Join Behavior:** Based on the design ("populates it by joining data from multiple source tables"), it implies `INNER JOIN` behavior.
    *   Records with no matching `CONTRACT_ID` in any of the joined tables should *not* appear in the target tables.
    *   Records with `NULL` `CONTRACT_ID` in any of the joined tables should *not* appear in the target tables (as `NULL` does not match `NULL` in standard SQL joins).
*   **NULL Propagation:** If a `CONTRACT_ID` matches, but `CONTRACT_NUMBER` or `CONTRACT_TEMPLATE` are `NULL` in their respective source tables, these `NULL`s must be correctly propagated to the target tables.
*   Verify row counts and specific column values for each scenario against the expected outcome based on `INNER JOIN` logic.

**Test Code (SQL Assertion Example):**

```sql
-- Example: Verify records with non-matching CONTRACT_ID are excluded (assuming INNER JOIN)
-- Setup: Insert a record into ta_discount_rr with CONTRACT_ID = 'NON_EXISTENT_ID'
SELECT COUNT(*) FROM `your-gcp-project.your_dataset.ta_p_discount_rr`
WHERE CONTRACT_ID = 'NON_EXISTENT_ID';
-- Pass if COUNT(*) = 0

-- Example: Verify NULL CONTRACT_ID records are excluded
-- Setup: Insert a record into ta_discount_rr with CONTRACT_ID = NULL
SELECT COUNT(*) FROM `your-gcp-project.your_dataset.ta_p_discount_rr`
WHERE CONTRACT_ID IS NULL;
-- Pass if COUNT(*) = 0

-- Example: Verify NULL data columns propagate
-- Setup: Insert a record into ta_cntrct_crs with CONTRACT_ID = 'VALID_ID' and CONTRACT_NUMBER = NULL
SELECT CONTRACT_NUMBER FROM `your-gcp-project.your_dataset.ta_p_discount_rr`
WHERE CONTRACT_ID = 'VALID_ID';
-- Pass if CONTRACT_NUMBER is NULL for this record.
```

---

## 5. Transformation Correctness - Empty Source Tables

**Purpose:** To verify that the job handles scenarios where one or more source tables are empty gracefully, without errors, and produces the expected output (typically an empty target table).

**Setup:**
1.  **Case A: All primary source tables empty:**
    *   Truncate `your_dataset.ta_discount_rr`, `your_dataset.ta_disc_zusgf`, `your_dataset.ta_cntrct_crs`, `your_dataset.ta_cntrct_templ`.
    *   `your_dataset.dwtk_meldungen` can be populated or empty (Stichtag should default to '1900-01-01').
2.  **Case B: Only `ta_discount_rr` (or `ta_disc_zusgf`) empty:**
    *   Truncate `your_dataset.ta_discount_rr` (for `_rr` job) or `your_dataset.ta_disc_zusgf` (for non-`_rr` job).
    *   Populate other join tables (`ta_cntrct_crs`, `ta_cntrct_templ`) with data.
    *   Ensure `dwtk_meldungen` has data.
3.  **Case C: Only join tables (`ta_cntrct_crs`, `ta_cntrct_templ`) empty:**
    *   Truncate `your_dataset.ta_cntrct_crs` and `your_dataset.ta_cntrct_templ`.
    *   Populate `your_dataset.ta_discount_rr` (or `ta_disc_zusgf`) with data.
    *   Ensure `dwtk_meldungen` has data.

**Action:**
1.  For each case (A, B, C), trigger both `dw_bert_ausd_v_ta_p_discount_rr` and `dw_bert_ausd_v_ta_p_discount` Airflow DAGs.

**Pass/Fail Criterion:**
*   For all cases (A, B, C), both Airflow DAGs must complete successfully (status 'success').
*   The target tables (`your_dataset.ta_p_discount_rr` and `your_dataset.ta_p_discount`) must be empty (row count = 0) after each run (assuming `INNER JOIN` behavior).

**Test Code (SQL Assertion Example):**

```sql
-- After running the DAGs for each case:
SELECT COUNT(*) FROM `your-gcp-project.your_dataset.ta_p_discount_rr`;
-- Pass if COUNT(*) = 0

SELECT COUNT(*) FROM `your-gcp-project.your_dataset.ta_p_discount`;
-- Pass if COUNT(*) = 0
```

---

## 6. Data Quality - Schema and Data Types

**Purpose:** To verify that the schema (column names, data types, nullability) of the target BigQuery tables matches the expected schema, reflecting correct translation from Oracle types and ensuring data integrity.

**Setup:**
1.  Ensure the target tables `your_dataset.ta_p_discount_rr` and `your_dataset.ta_p_discount` have been created (e.g., by running a successful job or via DDL).
2.  Have the expected schema definitions readily available (e.g., from the legacy Oracle DDLs or a design document).

**Action:**
1.  Inspect the schema of `your_dataset.ta_p_discount_rr` and `your_dataset.ta_p_discount` using BigQuery's information schema or `bq show` command.

**Pass/Fail Criterion:**
*   **Column Names:** All expected column names (e.g., `DISCOUNT_ID`, `DISCOUNT_NAME`, `CONTRACT_ID`, `CONTRACT_NUMBER`, `CONTRACT_TEMPLATE`, `PROCESSING_DATE`) are present.
*   **Data Types:** Data types of columns (e.g., `INT64`, `STRING`, `DATE`, `TIMESTAMP`, `NUMERIC`) match the BigQuery-translated types and are appropriate for the data.
*   **Nullability:** Nullability constraints (e.g., `NOT NULL` for primary keys or required fields) are correctly applied.

**Test Code (SQL Assertion Example):**

```sql
-- Example for ta_p_discount_rr
SELECT
    column_name,
    data_type,
    is_nullable
FROM
    `your-gcp-project.your_dataset.INFORMATION_SCHEMA.COLUMNS`
WHERE
    table_name = 'ta_p_discount_rr'
ORDER BY
    ordinal_position;

-- Expected output (example, adjust based on actual schema):
-- column_name      data_type   is_nullable
-- DISCOUNT_ID      INT64       NO
-- DISCOUNT_NAME    STRING      YES
-- CONTRACT_ID      STRING      NO
-- CONTRACT_NUMBER  STRING      YES
-- CONTRACT_TEMPLATE STRING     YES
-- PROCESSING_DATE  DATE        NO

-- Assertions can be made programmatically in Python (e.g., using pytest-bigquery)
# Example pytest assertion (conceptual)
# def test_ta_p_discount_rr_schema(bigquery_client):
#     table = bigquery_client.get_table("your-gcp-project.your_dataset.ta_p_discount_rr")
#     schema = {field.name: (field.field_type, field.mode) for field in table.schema}
#     expected_schema = {
#         "DISCOUNT_ID": ("INT64", "REQUIRED"),
#         "DISCOUNT_NAME": ("STRING", "NULLABLE"),
#         "CONTRACT_ID": ("STRING", "REQUIRED"),
#         "CONTRACT_NUMBER": ("STRING", "NULLABLE"),
#         "CONTRACT_TEMPLATE": ("STRING", "NULLABLE"),
#         "PROCESSING_DATE": ("DATE", "REQUIRED"),
#     }
#     assert schema == expected_schema
```

---

## 7. Orchestration and Logging - Successful Run

**Purpose:** To verify that the Airflow DAG successfully triggers the BigQuery stored procedures and that the `job_control`, `job_log`, and `job_error_log` tables are correctly populated upon successful completion, reflecting the job's lifecycle.

**Setup:**
1.  Populate BigQuery source tables with valid data to ensure a successful job execution.
2.  Ensure `your_dataset.job_control`, `your_dataset.job_log`, and `your_dataset.job_error_log` tables are created with the expected schema.
3.  Clear any existing entries in the logging tables to ensure a clean test.

**Action:**
1.  Trigger both `dw_bert_ausd_v_ta_p_discount_rr` and `dw_bert_ausd_v_ta_p_discount` Airflow DAGs.
    *   Note the `execution_date` for each DAG run.

**Pass/Fail Criterion:**
*   Both Airflow DAGs must complete with a 'success' status in the Airflow UI.
*   **`job_control` table:**
    *   Two new entries must exist, one for each job (`DW.BERT_V_TA_P_DISCOUNT_RR` and `DW.BERT_V_TA_P_DISCOUNT`).
    *   For each entry: `status` must be 'COMPLETED', `start_time` and `end_time` must be populated, and `processing_date` must match the Airflow `execution_date` (e.g., `{{ ds }}`).
*   **`job_log` table:**
    *   Multiple 'INFO' messages must be present for each `job_id`, indicating the start and end of `sp_r_` and `sp_k_` procedures.
*   **`job_error_log` table:**
    *   This table must be empty.

**Test Code (SQL Assertion Example):**

```sql
-- Verify job_control entries
SELECT
    job_name,
    status,
    processing_date,
    start_time IS NOT NULL AS start_time_populated,
    end_time IS NOT NULL AS end_time_populated
FROM
    `your-gcp-project.your_dataset.job_control`
WHERE
    processing_date = DATE('{{ ds }}') -- Replace with actual execution date
    AND job_name IN ('DW.BERT_V_TA_P_DISCOUNT_RR', 'DW.BERT_V_TA_P_DISCOUNT');
-- Pass if 2 rows are returned, both with status 'COMPLETED' and start/end times populated.

-- Verify job_log entries (example for one job)
SELECT
    log_level,
    message
FROM
    `your-gcp-project.your_dataset.job_log`
WHERE
    job_id = (SELECT job_id FROM `your-gcp-project.your_dataset.job_control` WHERE job_name = 'DW.BERT_V_TA_P_DISCOUNT_RR' AND processing_date = DATE('{{ ds }}'))
ORDER BY timestamp;
-- Pass if messages like "Starting sp_r...", "Starting sp_k...", "Finished sp_k...", "sp_r_... completed successfully." are present.

-- Verify job_error_log is empty
SELECT COUNT(*) FROM `your-gcp-project.your_dataset.job_error_log`
WHERE job_id IN (SELECT job_id FROM `your-gcp-project.your_dataset.job_control` WHERE processing_date = DATE('{{ ds }}'));
-- Pass if COUNT(*) = 0
```

---

## 8. Orchestration and Logging - Error Handling

**Purpose:** To verify that the job gracefully handles errors during the SQL transformation (e.g., within the `sp_d_` procedures) and correctly logs these errors in the `job_control`, `job_log`, and `job_error_log` tables.

**Setup:**
1.  **Introduce an intentional error:**
    *   Modify one of the `sp_d_` procedures (e.g., `sp_d_ausd_v_ta_p_discount_rr`) to intentionally raise an error (e.g., `RAISE 'Simulated Test Error';` or a SQL operation that will fail, like `SELECT 1/0;`).
2.  Populate source tables with minimal valid data.
3.  Clear any existing entries in the logging tables.

**Action:**
1.  Trigger the `dw_bert_ausd_v_ta_p_discount_rr` Airflow DAG.
2.  (Optional) Trigger the `dw_bert_ausd_v_ta_p_discount` Airflow DAG (which should succeed if not modified).

**Pass/Fail Criterion:**
*   The `dw_bert_ausd_v_ta_p_discount_rr` Airflow DAG must complete with a 'failed' status.
*   **`job_control` table:**
    *   An entry for `DW.BERT_V_TA_P_DISCOUNT_RR` must exist with `status = 'FAILED'`, and `start_time`/`end_time` populated.
*   **`job_log` table:**
    *   An 'ERROR' message must be present for the failed `job_id`, indicating the failure and ideally including the error message.
*   **`job_error_log` table:**
    *   An entry must exist for the failed `job_id` with the `error_message` (e.g., 'Simulated Test Error' or the BigQuery error message) and `script_name` (e.g., `sp_k_ausd_v_ta_p_discount_rr` or the actual `sp_d_` procedure if the error is caught there and passed up).

**Test Code (SQL Assertion Example):**

```sql
-- Verify job_control entry for the failed job
SELECT
    job_name,
    status,
    start_time IS NOT NULL AS start_time_populated,
    end_time IS NOT NULL AS end_time_populated
FROM
    `your-gcp-project.your_dataset.job_control`
WHERE
    job_name = 'DW.BERT_V_TA_P_DISCOUNT_RR'
    AND processing_date = DATE('{{ ds }}'); -- Replace with actual execution date
-- Pass if 1 row is returned with status 'FAILED' and start/end times populated.

-- Verify job_log entry for the failed job
SELECT
    log_level,
    message
FROM
    `your-gcp-project.your_dataset.job_log`
WHERE
    job_id = (SELECT job_id FROM `your-gcp-project.your_dataset.job_control` WHERE job_name = 'DW.BERT_V_TA_P_DISCOUNT_RR' AND processing_date = DATE('{{ ds }}'))
    AND log_level = 'ERROR';
-- Pass if at least one 'ERROR' message is found, containing the expected error text.

-- Verify job_error_log entry for the failed job
SELECT
    error_message,
    script_name
FROM
    `your-gcp-project.your_dataset.job_error_log`
WHERE
    job_id = (SELECT job_id FROM `your-gcp-project.your_dataset.job_control` WHERE job_name = 'DW.BERT_V_TA_P_DISCOUNT_RR' AND processing_date = DATE('{{ ds }}'));
-- Pass if 1 row is returned with the expected error_message and script_name.
```