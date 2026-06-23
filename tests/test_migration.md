As a senior data-migration QA engineer, I have analyzed the provided migration design document and the legacy/migrated code for `k_ausd_bp_ta_bpr_opt_text.ksh`. The following test cases are designed to ensure the migrated BigQuery solution is behaviorally equivalent to the legacy Oracle/KornShell job, covering output parity, transformation correctness, external system replacements, and data quality assertions.

**General Pre-requisite for all tests:**
Before executing any tests, ensure that the BigQuery source tables (`isbert_dataset.dwtk_meldungen`, `isbert_dataset.sof_ta_bpr_optionen`, `isbert_dataset.sof_ta_bpr_beschr`) are populated with data that is a faithful representation (identical schema and data, or type-compatible) of their Oracle counterparts. For output parity tests, this data should be identical to what was present in Oracle during a reference legacy run.

---

## 1. Test Case: End-to-End Output Parity (Happy Path)

*   **Purpose:** To verify that the migrated BigQuery Stored Procedure, when executed with valid and representative input data, produces an identical final dataset in the target table (`isbert_dataset.sof_ta_bpr_opt_text`) as the legacy Oracle job. This covers overall output parity and basic transformation correctness.
*   **Setup:**
    1.  Populate BigQuery source tables (`isbert_dataset.dwtk_meldungen`, `isbert_dataset.sof_ta_bpr_optionen`, `isbert_dataset.sof_ta_bpr_beschr`) with a comprehensive and representative dataset. This dataset should be identical to what was used in a successful legacy Oracle run.
    2.  Execute the legacy Oracle job (`k_ausd_bp_ta_bpr_opt_text.ksh`) with the corresponding source data and parameters. Extract the final data from `sof$ta_bpr_opt_text` into a temporary BigQuery table (e.g., `your_temp_dataset.legacy_sof_ta_bpr_opt_text`) for comparison.
    3.  Ensure `isbert_dataset.sof_ta_bpr_opt_text` is empty or contains old data (the procedure will truncate it).
    4.  Define valid input parameters for the BigQuery Stored Procedure, e.g., `p_job_kennung='TEST_JOB_001'`, `p_eintrags_nr='123'`, `p_stichtag='20231026'`.
*   **Action:** Execute the BigQuery Stored Procedure:
    ```sql
    CALL `isbert_dataset.k_ausd_bp_ta_bpr_opt_text_sp`(
        'TEST_JOB_001',
        '123',
        '20231026'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  The stored procedure completes successfully without raising any errors.
    2.  The row count in `isbert_dataset.sof_ta_bpr_opt_text` matches the row count from the `your_temp_dataset.legacy_sof_ta_bpr_opt_text`.
    3.  A deep comparison of the data in `isbert_dataset.sof_ta_bpr_opt_text` with `your_temp_dataset.legacy_sof_ta_bpr_opt_text` shows no differences.

    ```sql
    -- SQL Assertion for data parity
    -- This query should return 0 rows if the data is identical.
    SELECT 'Only in BigQuery' AS source, t.* FROM `isbert_dataset.sof_ta_bpr_opt_text` t
    EXCEPT DISTINCT
    SELECT 'Only in Legacy' AS source, t.* FROM `your_temp_dataset.legacy_sof_ta_bpr_opt_text` t

    UNION ALL

    SELECT 'Only in Legacy' AS source, t.* FROM `your_temp_dataset.legacy_sof_ta_bpr_opt_text` t
    EXCEPT DISTINCT
    SELECT 'Only in BigQuery' AS source, t.* FROM `isbert_dataset.sof_ta_bpr_opt_text` t;
    ```

---

## 2. Test Case: Parameter Validation - Missing Required Parameters

*   **Purpose:** To verify that the BigQuery Stored Procedure correctly validates the presence of required input parameters (`p_job_kennung`, `p_eintrags_nr`, `p_stichtag`) and raises an error if they are missing or empty, mimicking the legacy `pruefeParameterGesetzt` behavior.
*   **Setup:** Ensure `isbert_dataset.sof_ta_bpr_opt_text` is empty before each call.
*   **Action:** Attempt to execute the stored procedure with one or more required parameters as `NULL` or empty strings:
    1.  Missing `p_job_kennung`:
        ```sql
        CALL `isbert_dataset.k_ausd_bp_ta_bpr_opt_text_sp`(NULL, '123', '20231026');
        ```
    2.  Empty `p_eintrags_nr`:
        ```sql
        CALL `isbert_dataset.k_ausd_bp_ta_bpr_opt_text_sp`('TEST_JOB_002', '', '20231026');
        ```
    3.  Missing `p_stichtag`:
        ```sql
        CALL `isbert_dataset.k_ausd_bp_ta_bpr_opt_text_sp`('TEST_JOB_003', '123', NULL);
        ```
*   **Pass/Fail Criterion:**
    1.  Each call fails with a `RAISE` error message indicating the specific missing parameter (e.g., "Parameter p_job_kennung must be provided.").
    2.  No data is inserted into `isbert_dataset.sof_ta_bpr_opt_text` for any of the failed calls.
    3.  No entry is made into `isbert_dataset.job_run_control` with a 'SUCCESS' status for any of the failed calls.

---

## 3. Test Case: Parameter Validation - Invalid Stichtag Format

*   **Purpose:** To verify that the BigQuery Stored Procedure correctly validates the `p_stichtag` format (YYYYMMDD) and raises an error for invalid formats, mimicking the legacy `DWDate_Datum_Check` behavior.
*   **Setup:** Ensure `isbert_dataset.sof_ta_bpr_opt_text` is empty before each call.
*   **Action:** Attempt to execute the stored procedure with an invalid `p_stichtag` format:
    1.  Wrong date separator:
        ```sql
        CALL `isbert_dataset.k_ausd_bp_ta_bpr_opt_text_sp`('TEST_JOB_004', '123', '2023-10-26');
        ```
    2.  Non-date string:
        ```sql
        CALL `isbert_dataset.k_ausd_bp_ta_bpr_opt_text_sp`('TEST_JOB_005', '123', 'INVALIDDATE');
        ```
    3.  Incorrect length:
        ```sql
        CALL `isbert_dataset.k_ausd_bp_ta_bpr_opt_text_sp`('TEST_JOB_006', '123', '231026');
        ```
*   **Pass/Fail Criterion:**
    1.  Each call fails with a `RAISE` error message indicating an invalid date format (e.g., "Invalid format for p_stichtag. Expected YYYYMMDD, got 2023-10-26").
    2.  No data is inserted into `isbert_dataset.sof_ta_bpr_opt_text` for any of the failed calls.
    3.  No entry is made into `isbert_dataset.job_run_control` with a 'SUCCESS' status for any of the failed calls.

---

## 4. Test Case: Date Derivation (`v_datum_heute`, `v_datum_gestern`)

*   **Purpose:** To verify that the BigQuery Stored Procedure correctly derives `v_datum_heute` and `v_datum_gestern` using BigQuery's native date functions, replacing the `gestern.ksh` script.
*   **Setup:** Ensure `isbert_dataset.job_run_control` is empty or cleared of previous test data for the specific `job_id`.
*   **Action:** Execute the stored procedure with valid parameters:
    ```sql
    CALL `isbert_dataset.k_ausd_bp_ta_bpr_opt_text_sp`(
        'TEST_JOB_007',
        '123',
        '20231026'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  The stored procedure completes successfully.
    2.  Query the `isbert_dataset.job_run_control` table for the latest entry for `job_id = 'TEST_JOB_007'`.
    3.  The `run_date` column in the `job_run_control` entry should be equal to `CURRENT_DATE()` at the time of execution.
    4.  The `stichtag` column in the `job_run_control` entry should be equal to `PARSE_DATE('%Y%m%d', '20231026')`.
    *(Note: `v_datum_gestern` is derived but not explicitly logged or used in the provided SP code, so its correctness is implicitly covered by the correct usage of `DATE_SUB` if it were to be used.)*

---

## 5. Test Case: `v_datum` Derivation from `dwtk_meldungen`

*   **Purpose:** To verify that the `v_datum` variable is correctly populated based on `MAX(m.timecreated)` from `isbert_dataset.dwtk_meldungen` for `job_kennung = 'BERT_DROP_TEMP_TABLE'`, including the `COALESCE` fallback to `'19000101'`. This validates a specific external system replacement (Oracle `dwtk_meldungen` to BigQuery `dwtk_meldungen`) and transformation correctness.
*   **Setup:**
    *   **Scenario A (Normal):** Populate `isbert_dataset.dwtk_meldungen` with multiple entries, ensuring one has the maximum `timecreated` for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    *   **Scenario B (No matching `job_kennung`):** Populate `isbert_dataset.dwtk_meldungen` with entries, but none for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    *   **Scenario C (Empty table):** Ensure `isbert_dataset.dwtk_meldungen` is empty.
    *(Note: Since `v_datum` is not used in the `INSERT` statement and not logged in the provided SP, a temporary modification to the SP to log `v_datum` to `job_run_control` or a debug table is required for direct assertion.)*
*   **Action:** Execute the stored procedure for each scenario.
    ```sql
    -- Example for Scenario A Setup:
    TRUNCATE TABLE `isbert_dataset.dwtk_meldungen`;
    INSERT INTO `isbert_dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES
    ('OTHER_JOB', TIMESTAMP('2023-01-01')),
    ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-10-25 10:00:00')),
    ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-10-26 15:30:00')); -- Max timecreated
    CALL `isbert_dataset.k_ausd_bp_ta_bpr_opt_text_sp`('TEST_JOB_008A', '123', '20231026');

    -- Example for Scenario B Setup:
    TRUNCATE TABLE `isbert_dataset.dwtk_meldungen`;
    INSERT INTO `isbert_dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES
    ('OTHER_JOB', TIMESTAMP('2023-01-01'));
    CALL `isbert_dataset.k_ausd_bp_ta_bpr_opt_text_sp`('TEST_JOB_008B', '123', '20231026');

    -- Example for Scenario C Setup:
    TRUNCATE TABLE `isbert_dataset.dwtk_meldungen`;
    CALL `isbert_dataset.k_ausd_bp_ta_bpr_opt_text_sp`('TEST_JOB_008C', '123', '20231026');
    ```
*   **Pass/Fail Criterion:**
    1.  The stored procedure completes successfully for all scenarios.
    2.  **Scenario A:** The logged `v_datum` (e.g., in `job_run_control.additional_info`) should be `'20231026'`.
    3.  **Scenario B & C:** The logged `v_datum` should be `'19000101'`.

---

## 6. Test Case: Truncate Table Behavior

*   **Purpose:** To verify that the `TRUNCATE TABLE` statement correctly empties the target table (`isbert_dataset.sof_ta_bpr_opt_text`) before new data insertion, matching the Oracle `DWPA_UTIL_SKRIPT.runstatement` behavior.
*   **Setup:**
    1.  Populate `isbert_dataset.sof_ta_bpr_opt_text` with some dummy data (e.g., `(1, 100, 'Dummy Text')`).
    2.  Populate source tables (`sof_ta_bpr_optionen`, `sof_ta_bpr_beschr`) with data that would result in a *different* set of records than the dummy data (e.g., `(2, 200, 'New Text')`).
*   **Action:** Execute the stored procedure with valid parameters:
    ```sql
    CALL `isbert_dataset.k_ausd_bp_ta_bpr_opt_text_sp`(
        'TEST_JOB_009',
        '123',
        '20231026'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  The stored procedure completes successfully.
    2.  The final data in `isbert_dataset.sof_ta_bpr_opt_text` should only contain the records inserted by the current run (e.g., `(2, 200, 'New Text')`), and none of the initial dummy data.
    3.  The row count in `isbert_dataset.sof_ta_bpr_opt_text` matches the number of records inserted by the `INSERT` statement, not the sum of dummy data + new data.

---

## 7. Test Case: Core Transformation - Join Logic, Data Types, and NULL Handling

*   **Purpose:** To verify the correctness of the `JOIN` condition (`bp.bpr_id = bs.bpr_id`), the mapping of columns, and that data types and `NULL` values are handled correctly during insertion. This is a critical transformation correctness test.
*   **Setup:**
    1.  Populate `isbert_dataset.sof_ta_bpr_optionen` and `isbert_dataset.sof_ta_bpr_beschr` with diverse data, including:
        *   Records with matching `bpr_id` values (expected to be inserted).
        *   Records with `bpr_id` values present in `sof_ta_bpr_optionen` but not in `sof_ta_bpr_beschr` (expected to be excluded).
        *   Records with `bpr_id` values present in `sof_ta_bpr_beschr` but not in `sof_ta_bpr_optionen` (expected to be excluded).
        *   `NULL` values in `cntrct_id` and `pds_description` (if allowed by schema and present in source).
        *   Edge cases for `PDS_DESCRIPTION` (e.g., very long strings, special characters, empty strings).
    2.  Ensure data types in source tables are compatible with the target table DDL.
*   **Action:** Execute the stored procedure with valid parameters:
    ```sql
    CALL `isbert_dataset.k_ausd_bp_ta_bpr_opt_text_sp`(
        'TEST_JOB_010',
        '123',
        '20231026'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  The stored procedure completes successfully.
    2.  Only records where `bp.bpr_id = bs.bpr_id` are inserted into `isbert_dataset.sof_ta_bpr_opt_text`.
    3.  The `CNTRCT_ID`, `BPR_ID`, and `PDS_DESCRIPTION` values in the target table exactly match the joined values from the source tables, including correct handling of `NULL`s and special characters.
    4.  No data truncation or type conversion errors occur.

    ```sql
    -- SQL Assertion for join logic and data correctness
    -- This query should return 0 rows if the data is identical to the expected join result.
    SELECT
        bp.cntrct_id,
        bp.bpr_id,
        bs.pds_description
    FROM
        `isbert_dataset.sof_ta_bpr_optionen` AS bp
    JOIN
        `isbert_dataset.sof_ta_bpr_beschr` AS bs
    ON
        bp.bpr_id = bs.bpr_id
    EXCEPT DISTINCT
    SELECT
        CNTRCT_ID,
        BPR_ID,
        PDS_DESCRIPTION
    FROM
        `isbert_dataset.sof_ta_bpr_opt_text`;
    ```

---

## 8. Test Case: Empty Source Tables

*   **Purpose:** To verify that the job handles cases where the primary source tables (`sof_ta_bpr_optionen`, `sof_ta_bpr_beschr`) are empty gracefully, resulting in an empty target table and correct record count.
*   **Setup:**
    1.  Ensure `isbert_dataset.sof_ta_bpr_optionen` and `isbert_dataset.sof_ta_bpr_beschr` are empty.
    2.  `isbert_dataset.dwtk_meldungen` can be empty or populated (it should not affect the `INSERT` logic).
*   **Action:** Execute the stored procedure with valid parameters:
    ```sql
    CALL `isbert_dataset.k_ausd_bp_ta_bpr_opt_text_sp`(
        'TEST_JOB_011',
        '123',
        '20231026'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  The stored procedure completes successfully.
    2.  `isbert_dataset.sof_ta_bpr_opt_text` is empty (0 rows).
    3.  The `records_processed` column in the `isbert_dataset.job_run_control` entry for `TEST_JOB_011` is 0.

---

## 9. Test Case: Record Count and Logging

*   **Purpose:** To verify that the number of processed records (`v_records_processed`) is correctly captured using `@@row_count` and accurately logged to `isbert_dataset.job_run_control`, replacing the legacy temporary file mechanism.
*   **Setup:**
    1.  Populate source tables (`sof_ta_bpr_optionen`, `sof_ta_bpr_beschr`) with a known number of records that will result in a specific, verifiable number of joined records (e.g., 100 records after the join).
    2.  Ensure `isbert_dataset.job_run_control` is ready to receive entries.
*   **Action:** Execute the stored procedure with valid parameters:
    ```sql
    CALL `isbert_dataset.k_ausd_bp_ta_bpr_opt_text_sp`(
        'TEST_JOB_012',
        '123',
        '20231026'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  The stored procedure completes successfully.
    2.  Query `isbert_dataset.job_run_control` for the latest entry for `job_id = 'TEST_JOB_012'`.
    3.  The `records_processed` column in `job_run_control` matches the actual `COUNT(*)` from `isbert_dataset.sof_ta_bpr_opt_text` after the run.
    4.  The `status` column in `job_run_control` is 'SUCCESS'.
    5.  `start_timestamp` and `end_timestamp` are populated, and `end_timestamp` is after `start_timestamp`.

    ```sql
    -- SQL Assertion (after SP execution)
    SELECT
        (SELECT COUNT(*) FROM `isbert_dataset.sof_ta_bpr_opt_text`) = jrc.records_processed
    FROM
        `isbert_dataset.job_run_control` jrc
    WHERE
        jrc.job_id = 'TEST_JOB_012'
    ORDER BY
        jrc.start_timestamp DESC
    LIMIT 1;
    -- Pass if the query returns TRUE.
    ```

---

## 10. Test Case: Schema Assertions for Target Table

*   **Purpose:** To verify that the DDL for the target table `isbert_dataset.sof_ta_bpr_opt_text` matches the expected schema, including column names, data types, and nullability, based on the Oracle source and migration design.
*   **Setup:** The `isbert_dataset.sof_ta_bpr_opt_text` table must exist in BigQuery.
*   **Action:** Query BigQuery's `INFORMATION_SCHEMA` for the table schema.
*   **Pass/Fail Criterion:**
    1.  The table `isbert_dataset.sof_ta_bpr_opt_text` exists.
    2.  It has exactly the following columns with the specified BigQuery data types:
        *   `CNTRCT_ID` (INT64)
        *   `BPR_ID` (INT64)
        *   `PDS_DESCRIPTION` (STRING)
    3.  All columns are nullable, as per the provided BigQuery DDL.

    ```python
    # Pytest example using BigQuery client
    from google.cloud import bigquery
    import pytest

    @pytest.fixture(scope="module")
    def bq_client():
        return bigquery.Client()

    def test_target_table_schema(bq_client):
        project_id = "your_gcp_project_id" # Replace with your GCP project ID
        dataset_id = "isbert_dataset"
        table_id = "sof_ta_bpr_opt_text"
        full_table_id = f"{project_id}.{dataset_id}.{table_id}"

        try:
            table = bq_client.get_table(full_table_id)
        except Exception as e:
            pytest.fail(f"Failed to retrieve table schema for {full_table_id}: {e}")

        expected_schema = {
            "CNTRCT_ID": "INT64",
            "BPR_ID": "INT64",
            "PDS_DESCRIPTION": "STRING",
        }

        actual_schema = {field.name: field.field_type for field in table.schema}

        assert actual_schema == expected_schema, \
            f"Schema mismatch for {full_table_id}. Expected: {expected_schema}, Actual: {actual_schema}"

        # Verify nullability (BigQuery default is NULLABLE if not specified as REQUIRED)
        for field in table.schema:
            assert field.mode == "NULLABLE", \
                f"Column {field.name} in {full_table_id} should be NULLABLE, but is {field.mode}"
    ```

---

## 11. Test Case: External System Replacement - Oracle DB Link

*   **Purpose:** To confirm that the BigQuery solution correctly accesses the migrated source tables within BigQuery, effectively replacing the legacy Oracle DB link (`@pcrs1`) usage. This is a verification of the "External-system replacements" requirement.
*   **Setup:**
    1.  Ensure `isbert_dataset.dwtk_meldungen`, `isbert_dataset.sof_ta_bpr_optionen`, and `isbert_dataset.sof_ta_bpr_beschr` are populated with data.
    2.  Crucially, ensure there are *no* active connections or configurations in the BigQuery environment that would attempt to connect to an external Oracle database for these specific tables.
*   **Action:** Execute the stored procedure with valid parameters:
    ```sql
    CALL `isbert_dataset.k_ausd_bp_ta_bpr_opt_text_sp`(
        'TEST_JOB_013',
        '123',
        '20231026'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  The stored procedure completes successfully.
    2.  The data processed and inserted into `isbert_dataset.sof_ta_bpr_opt_text` is derived *solely* from the BigQuery source tables (`isbert_dataset.dwtk_meldungen`, `isbert_dataset.sof_ta_bpr_optionen`, `isbert_dataset.sof_ta_bpr_beschr`), confirming that the BigQuery tables are being used as the data source, not any external Oracle connection. This is implicitly covered by the success of Test 1 (Output Parity) and Test 7 (Core Transformation) when external Oracle links are explicitly absent.

---

## 12. Test Case: Error Handling - Unexpected SQL Error

*   **Purpose:** To verify that the stored procedure handles unexpected SQL errors during the core transformation logic (e.g., due to schema mismatch, data type issues, or constraint violations) and logs an appropriate status, even if it doesn't have an explicit `EXCEPTION` block.
*   **Setup:**
    1.  Introduce a controlled error condition in the target table. For example, temporarily alter `isbert_dataset.sof_ta_bpr_opt_text` to have a `NOT NULL` constraint on `PDS_DESCRIPTION` and then populate `isbert_dataset.sof_ta_bpr_beschr` with a `NULL` value for `pds_description` for a matching `bpr_id`.
    2.  Ensure source tables are populated to trigger the `INSERT`.
*   **Action:** Execute the stored procedure with valid parameters:
    ```sql
    -- Example of introducing an error (run this BEFORE the SP call)
    ALTER TABLE `isbert_dataset.sof_ta_bpr_opt_text` ALTER COLUMN PDS_DESCRIPTION SET NOT NULL;
    -- Then, ensure source data has a NULL PDS_DESCRIPTION for a matching BPR_ID
    INSERT INTO `isbert_dataset.sof_ta_bpr_optionen` (cntrct_id, bpr_id) VALUES (999, 999);
    INSERT INTO `isbert_dataset.sof_ta_bpr_beschr` (bpr_id, pds_description) VALUES (999, NULL);

    CALL `isbert_dataset.k_ausd_bp_ta_bpr_opt_text_sp`(
        'TEST_JOB_014',
        '123',
        '20231026'
    );
    ```
*   **Pass/Fail Criterion:**
    1.  The stored procedure execution fails with a BigQuery error (e.g., "Cannot insert NULL value into non-nullable column PDS_DESCRIPTION").
    2.  No data is inserted into `isbert_dataset.sof_ta_bpr_opt_text` (or the transaction is rolled back, leaving the table in its pre-execution state).
    3.  No entry is made into `isbert_dataset.job_run_control` with a 'SUCCESS' status. (If an `EXCEPTION` block were implemented, an 'ERROR' or 'FAILED' status would be expected).