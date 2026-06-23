The migration of `r_ausd_v_ta_barrier.ksh` to Google Cloud BigQuery involves a complete re-implementation of its orchestration and data transformation logic. The following tests are designed to ensure behavioral equivalence, data integrity, and correct functionality of the migrated BigQuery Stored Procedures.

---

## Migration Validation Tests for `r_ausd_v_ta_barrier.ksh`

**Target BigQuery Dataset:** `isrpt_isbert_data_processing`

### General Setup for All Tests

Before running any tests, ensure the following:

1.  **BigQuery Environment:** A Google Cloud project with the `isrpt_isbert_data_processing` dataset created.
2.  **Source Tables:** All source tables (`dwtk_meldungen`, `cds_ta_barrier`, `cds_ta_barrier_class`, `cds_ta_barrier_kind`, `cds_ta_care_description`) are created in `isrpt_isbert_data_processing` with schemas matching their Oracle counterparts.
3.  **Target Table:** The `sof_ta_barrier` table is created in `isrpt_isbert_data_processing` with a schema matching the Oracle `sof$ta_barrier`.
4.  **Logging/Control Tables:** The following tables are created in `isrpt_isbert_data_processing` with appropriate schemas (as defined in the build plan):
    *   `job_table` (columns: `job_kennung` STRING, `status` STRING, `start_timestamp` TIMESTAMP, `end_timestamp` TIMESTAMP, `last_modified` TIMESTAMP)
    *   `job_log` (columns: `job_id` STRING, `job_name` STRING, `log_message` STRING, `log_timestamp` TIMESTAMP, `log_level` STRING)
    *   `job_error_log` (columns: `job_id` STRING, `job_name` STRING, `script_name` STRING, `error_message` STRING, `error_timestamp` TIMESTAMP)
    *   `sql_execution_results` (columns: `job_id` STRING, `job_name` STRING, `script_name` STRING, `records_processed` INT64, `execution_timestamp` TIMESTAMP, `status` STRING, `error_message` STRING)
5.  **BigQuery Stored Procedures:** All migrated procedures (`d_ausd_v_ta_barrier_etl`, `starteSQLSkript`, `k_ausd_v_ta_barrier_control`, `r_ausd_v_ta_barrier_wrapper`) are deployed in `isrpt_isbert_data_processing`.
6.  **Data Loading Mechanism:** A reliable method to load identical test data into both the legacy Oracle source tables and the BigQuery source tables.

---

### Test Case 1: End-to-End Output Parity (Happy Path)

*   **Purpose:** To verify that the final output table (`sof_ta_barrier`) in BigQuery is identical to the legacy Oracle `sof$ta_barrier` table when processed with the same input data under normal conditions. This covers overall output parity and transformation correctness.
*   **Setup:**
    1.  Populate all Oracle source tables (`cds$ta_barrier`, `cds$ta_barrier_class`, `cds$ta_barrier_kind`, `cds$ta_care_description`, `dwtk_meldungen`) with a comprehensive, representative dataset.
    2.  Load an *identical* dataset into the corresponding BigQuery source tables (`isrpt_isbert_data_processing.cds_ta_barrier`, etc.).
    3.  Ensure `isrpt_isbert_data_processing.job_table` is empty or has no active jobs for `BERT_V_TA_BARRIER`.
    4.  Clear the target tables: `TRUNCATE TABLE sof$ta_barrier` (Oracle) and `TRUNCATE TABLE isrpt_isbert_data_processing.sof_ta_barrier` (BigQuery).
*   **Action:**
    1.  Execute the legacy KornShell job: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier.ksh -j BERT_V_TA_BARRIER -f 12345`.
    2.  Execute the migrated BigQuery wrapper procedure:
        ```sql
        CALL `isrpt_isbert_data_processing.r_ausd_v_ta_barrier_wrapper`(
            'Vertragsdatenabgleich',
            'BERT_V_TA_BARRIER',
            '12345'
        );
        ```
*   **Pass/Fail Criterion:**
    *   The row count of `isrpt_isbert_data_processing.sof_ta_barrier` must be exactly equal to the row count of Oracle's `sof$ta_barrier`.
    *   A deep comparison of all columns in both tables (after ordering by a unique key or all columns) must show no differences.

    ```sql
    -- Example BigQuery assertion for row count
    SELECT
        (SELECT COUNT(*) FROM `isrpt_isbert_data_processing.sof_ta_barrier`) = (SELECT <oracle_row_count_from_legacy_run>) AS row_count_match;

    -- Example BigQuery assertion for data parity (assuming a unique key, e.g., cntrct_id, barrier_kind_id, sperr_beginn)
    -- This requires the Oracle data to be loaded into a temporary BQ table for comparison.
    -- Let's assume 'legacy_oracle_sof_ta_barrier' is a BQ table containing the exact output from Oracle.
    SELECT
        (SELECT COUNT(*) FROM `isrpt_isbert_data_processing.sof_ta_barrier` EXCEPT DISTINCT SELECT * FROM `isrpt_isbert_data_processing.legacy_oracle_sof_ta_barrier`) = 0
        AND
        (SELECT COUNT(*) FROM `isrpt_isbert_data_processing.legacy_oracle_sof_ta_barrier` EXCEPT DISTINCT SELECT * FROM `isrpt_isbert_data_processing.sof_ta_barrier`) = 0
    AS data_parity_match;
    ```

---

### Test Case 2: `v_datum` Derivation Correctness

*   **Purpose:** To ensure the `v_datum` variable, which drives date-based filtering, is correctly derived from `dwtk_meldungen`, including edge cases like an empty or non-matching source.
*   **Setup:**
    1.  **Scenario A (Happy Path):** Populate `isrpt_isbert_data_processing.dwtk_meldungen` with multiple rows, including one with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and `timecreated` set to a specific date (e.g., `2023-01-15 10:00:00 UTC`).
    2.  **Scenario B (No Matching Job Kennung):** Populate `dwtk_meldungen` with rows, but none matching `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    3.  **Scenario C (Empty Table):** Ensure `dwtk_meldungen` is empty.
*   **Action:**
    1.  For each scenario, execute a BigQuery query to manually derive `v_datum` as per the logic in `d_ausd_v_ta_barrier_etl`.
    2.  (Ideally, a test procedure would expose `v_datum` for direct assertion, but for now, we rely on the `INSERT` statement's outcome). Execute `d_ausd_v_ta_barrier_etl` with minimal data in other source tables to observe the effect of `v_datum` on filtering.
*   **Pass/Fail Criterion:**
    *   **Scenario A:** The derived `v_datum` (e.g., '20230115') matches the `FORMAT_DATE('%Y%m%d', MAX(timecreated))` for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    *   **Scenario B & C:** The derived `v_datum` defaults to `'19000101'` as per the `COALESCE` logic.
    *   The filtering in `sof_ta_barrier` based on `v_datum` reflects the correctly derived value.

    ```sql
    -- Manual derivation query for v_datum
    SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
    FROM `isrpt_isbert_data_processing.dwtk_meldungen` AS m
    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
    ```

---

### Test Case 3: Transformation Logic: `SPERRGRUND` DECODE/CASE Equivalence

*   **Purpose:** To verify that the BigQuery `CASE` expression for `SPERRGRUND` correctly replicates the Oracle `DECODE` function's behavior, including all specified values, `NULL`, and the `ELSE` condition.
*   **Setup:**
    1.  Populate `isrpt_isbert_data_processing.cds_ta_barrier_class` with rows covering all `barrier_reason_cv` values explicitly listed in the `CASE` statement (1, 2, 3, 4, 7, 9, 10, 11, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32).
    2.  Include a row with `barrier_reason_cv = NULL`.
    3.  Include a row with a `barrier_reason_cv` not explicitly listed (e.g., 99) to test the `ELSE` clause.
    4.  Populate other source tables minimally to allow the `d_ausd_v_ta_barrier_etl` procedure to run.
*   **Action:**
    1.  Execute `isrpt_isbert_data_processing.d_ausd_v_ta_barrier_etl`.
    2.  Query `isrpt_isbert_data_processing.sof_ta_barrier` to inspect the `SPERRGRUND` column.
*   **Pass/Fail Criterion:**
    *   For each `barrier_reason_cv` value, the `SPERRGRUND` in `sof_ta_barrier` must match the expected string from the `CASE` statement.
    *   `barrier_reason_cv = NULL` must result in `SPERRGRUND = ''`.
    *   `barrier_reason_cv = 99` (or any other unlisted value) must result in `SPERRGRUND = 'Betreiberinterne Sperre'`.

    ```sql
    -- Example BigQuery assertion
    SELECT
        COUNTIF(SPERRGRUND = 'Kartenverlust' AND barrier_reason_cv = 1) = (SELECT COUNT(*) FROM `isrpt_isbert_data_processing.cds_ta_barrier_class` WHERE barrier_reason_cv = 1)
        AND COUNTIF(SPERRGRUND = 'Kundenwunsch' AND barrier_reason_cv = 2) = (SELECT COUNT(*) FROM `isrpt_isbert_data_processing.cds_ta_barrier_class` WHERE barrier_reason_cv = 2)
        -- ... and so on for all specific WHEN clauses
        AND COUNTIF(SPERRGRUND = '' AND barrier_reason_cv IS NULL) = (SELECT COUNT(*) FROM `isrpt_isbert_data_processing.cds_ta_barrier_class` WHERE barrier_reason_cv IS NULL)
        AND COUNTIF(SPERRGRUND = 'Betreiberinterne Sperre' AND barrier_reason_cv = 99) = (SELECT COUNT(*) FROM `isrpt_isbert_data_processing.cds_ta_barrier_class` WHERE barrier_reason_cv = 99)
    FROM `isrpt_isbert_data_processing.sof_ta_barrier`;
    ```

---

### Test Case 4: Transformation Logic: `COALESCE` for Dates (`SPERR_BEGINN`, `SPERR_ENDE`)

*   **Purpose:** To verify that `SPERR_BEGINN` and `SPERR_ENDE` correctly handle `NULL` values using `COALESCE`, falling back to `valid_from` or `valid_to` respectively.
*   **Setup:**
    1.  Populate `isrpt_isbert_data_processing.cds_ta_barrier` with rows exhibiting different combinations for `net_barr_on_date`/`valid_from` and `net_barr_off_date`/`valid_to`:
        *   Both values present.
        *   `net_barr_on_date`/`off_date` is `NULL`, `valid_from`/`to` is present.
        *   `net_barr_on_date`/`off_date` is present, `valid_from`/`to` is `NULL`.
        *   Both values are `NULL` (should result in `NULL` in target).
    2.  Populate other source tables minimally.
*   **Action:**
    1.  Execute `isrpt_isbert_data_processing.d_ausd_v_ta_barrier_etl`.
    2.  Query `isrpt_isbert_data_processing.sof_ta_barrier` to inspect `SPERR_BEGINN` and `SPERR_ENDE`.
*   **Pass/Fail Criterion:**
    *   If `net_barr_on_date` is not `NULL`, `SPERR_BEGINN` must equal `net_barr_on_date`.
    *   If `net_barr_on_date` is `NULL` but `valid_from` is not `NULL`, `SPERR_BEGINN` must equal `valid_from`.
    *   If both `net_barr_on_date` and `valid_from` are `NULL`, `SPERR_BEGINN` must be `NULL`.
    *   The same logic applies to `SPERR_ENDE` with `net_barr_off_date` and `valid_to`.

    ```sql
    -- Example BigQuery assertion
    SELECT
        COUNTIF(SPERR_BEGINN = b.net_barr_on_date AND b.net_barr_on_date IS NOT NULL) = (SELECT COUNT(*) FROM `isrpt_isbert_data_processing.cds_ta_barrier` WHERE net_barr_on_date IS NOT NULL)
        AND COUNTIF(SPERR_BEGINN = b.valid_from AND b.net_barr_on_date IS NULL AND b.valid_from IS NOT NULL) = (SELECT COUNT(*) FROM `isrpt_isbert_data_processing.cds_ta_barrier` WHERE net_barr_on_date IS NULL AND valid_from IS NOT NULL)
        AND COUNTIF(SPERR_BEGINN IS NULL AND b.net_barr_on_date IS NULL AND b.valid_from IS NULL) = (SELECT COUNT(*) FROM `isrpt_isbert_data_processing.cds_ta_barrier` WHERE net_barr_on_date IS NULL AND valid_from IS NULL)
        -- Add similar checks for SPERR_ENDE
    FROM `isrpt_isbert_data_processing.sof_ta_barrier` AS s
    JOIN `isrpt_isbert_data_processing.cds_ta_barrier` AS b ON s.cntrct_id = b.cntrct_id; -- Assuming cntrct_id is sufficient for joining back
    ```

---

### Test Case 5: Filtering Logic Correctness

*   **Purpose:** To verify that all `WHERE` clause conditions in `d_ausd_v_ta_barrier_etl`, especially date-based filters and `NULL` handling, are correctly applied.
*   **Setup:**
    1.  Populate source tables (`cds_ta_barrier`, `cds_ta_barrier_class`, `cds_ta_barrier_kind`) with data specifically designed to test each `WHERE` clause condition:
        *   Rows that should pass all filters.
        *   Rows that should be excluded by `b.insert_at <= v_datum`.
        *   Rows that should be excluded by `b.modified_at > v_datum` (when `modified_at` is not `NULL`).
        *   Rows that should be excluded by `b.valid_from <= v_datum`.
        *   Rows that should be excluded by `b.valid_to > v_datum` (when `valid_to` is not `NULL`).
        *   Rows with `b.is_production = 0`.
        *   Rows that should be excluded by `bk.insert_at <= v_datum`.
        *   Rows that should be excluded by `bk.modified_at > v_datum` (when `modified_at` is not `NULL`).
    2.  Ensure `v_datum` is set to a known value (e.g., by populating `dwtk_meldungen` appropriately).
*   **Action:**
    1.  Execute `isrpt_isbert_data_processing.d_ausd_v_ta_barrier_etl`.
    2.  Query `isrpt_isbert_data_processing.sof_ta_barrier`.
*   **Pass/Fail Criterion:**
    *   Only rows that satisfy *all* `WHERE` conditions are present in `sof_ta_barrier`.
    *   The count of rows in `sof_ta_barrier` matches the expected count based on the test data and filter logic.

    ```sql
    -- Example BigQuery assertion (conceptual, requires complex setup to verify each filter)
    -- This would typically involve a separate query that applies the WHERE clause to the source data
    -- and compares its row count to the target table's row count.
    DECLARE expected_row_count INT64;
    SET expected_row_count = (
        SELECT COUNT(*)
        FROM `isrpt_isbert_data_processing.cds_ta_barrier` AS b
        INNER JOIN `isrpt_isbert_data_processing.cds_ta_barrier_class` AS bc ON b.barrier_class_id = bc.barrier_class_id
        INNER JOIN `isrpt_isbert_data_processing.cds_ta_barrier_kind` AS bk ON bk.barrier_kind_id = bc.barrier_kind_id
        INNER JOIN `isrpt_isbert_data_processing.cds_ta_care_description` AS dk ON dk.cds_description_id = bk.cds_description_id
        WHERE
                b.insert_at <= PARSE_DATE('%Y%m%d', (SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101') FROM `isrpt_isbert_data_processing.dwtk_meldungen` AS m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'))
        AND     (b.modified_at IS NULL OR b.modified_at > PARSE_DATE('%Y%m%d', (SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101') FROM `isrpt_isbert_data_processing.dwtk_meldungen` AS m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'))))
        AND     b.valid_from <= PARSE_DATE('%Y%m%d', (SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101') FROM `isrpt_isbert_data_processing.dwtk_meldungen` AS m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE')))
        AND     (b.valid_to IS NULL OR b.valid_to > PARSE_DATE('%Y%m%d', (SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101') FROM `isrpt_isbert_data_processing.dwtk_meldungen` AS m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'))))
        AND     b.is_production = 1
        AND     bk.insert_at <= PARSE_DATE('%Y%m%d', (SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101') FROM `isrpt_isbert_data_processing.dwtk_meldungen` AS m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE')))
        AND     (bk.modified_at IS NULL OR bk.modified_at > PARSE_DATE('%Y%m%d', (SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101') FROM `isrpt_isbert_data_processing.dwtk_meldungen` AS m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'))))
    );

    SELECT (SELECT COUNT(*) FROM `isrpt_isbert_data_processing.sof_ta_barrier`) = expected_row_count AS filter_match;
    ```

---

### Test Case 6: Job Control Logic: Active Job Handling

*   **Purpose:** To verify that `k_ausd_v_ta_barrier_control` correctly identifies an already active job and logs a warning without re-executing the ETL.
*   **Setup:**
    1.  Insert a row into `isrpt_isbert_data_processing.job_table` with `job_kennung = 'BERT_V_TA_BARRIER'` and `status = 'ACTIVE'`.
    2.  Populate `isrpt_isbert_data_processing.sof_ta_barrier` with some dummy data (e.g., one row) that should *not* be truncated if the job is ignored.
    3.  Clear `isrpt_isbert_data_processing.job_log` and `job_error_log`.
*   **Action:**
    1.  Execute the BigQuery wrapper procedure:
        ```sql
        CALL `isrpt_isbert_data_processing.r_ausd_v_ta_barrier_wrapper`(
            'Vertragsdatenabgleich',
            'BERT_V_TA_BARRIER',
            '12345'
        );
        ```
*   **Pass/Fail Criterion:**
    *   The `r_ausd_v_ta_barrier_wrapper` procedure completes without raising an error.
    *   `isrpt_isbert_data_processing.job_log` contains an entry with `log_level = 'WARNING'` and a message indicating the job is already active (e.g., "Job BERT_V_TA_BARRIER is already active. Ignoring this run.").
    *   `isrpt_isbert_data_processing.sof_ta_barrier` remains unchanged (the dummy data is still present, not truncated).
    *   No new entries are added to `isrpt_isbert_data_processing.sql_execution_results` for `d_ausd_v_ta_barrier_etl`.

    ```sql
    -- Check for warning log
    SELECT COUNT(*) FROM `isrpt_isbert_data_processing.job_log`
    WHERE job_id = 'BERT_V_TA_BARRIER' AND log_level = 'WARNING'
    AND log_message LIKE '%already active. Ignoring this run%';

    -- Check if target table was untouched
    SELECT COUNT(*) FROM `isrpt_isbert_data_processing.sof_ta_barrier`; -- Should be 1 (the dummy row)
    ```

---

### Test Case 7: Job Control Logic: Deactivation and Status Updates

*   **Purpose:** To verify that `k_ausd_v_ta_barrier_control` correctly activates a new job, deactivates any older active jobs for the same `job_kennung`, and updates the job status (`ACTIVE`, `SUCCESS`, `FAILED`) in `job_table`.
*   **Setup:**
    1.  **Scenario A (Success):** Ensure `isrpt_isbert_data_processing.job_table` is empty. Populate source tables for a successful ETL run.
    2.  **Scenario B (Failure):** Insert an older `ACTIVE` job for `BERT_V_TA_BARRIER` into `job_table`. Introduce a deliberate error in `d_ausd_v_ta_barrier_etl` (e.g., `SELECT 1/0;` or reference a non-existent column) to force a failure.
    3.  Clear `isrpt_isbert_data_processing.job_log`, `job_error_log`, `sql_execution_results` for both scenarios.
*   **Action:**
    1.  For Scenario A, execute:
        ```sql
        CALL `isrpt_isbert_data_processing.r_ausd_v_ta_barrier_wrapper`(
            'Vertragsdatenabgleich',
            'BERT_V_TA_BARRIER',
            '12345'
        );
        ```
    2.  For Scenario B, execute the same call.
*   **Pass/Fail Criterion:**
    *   **Scenario A (Success):**
        *   `isrpt_isbert_data_processing.job_table` contains one entry for `BERT_V_TA_BARRIER` with `status = 'SUCCESS'`.
        *   `job_log` contains `INFO` entries for start and successful completion.
        *   `sql_execution_results` contains a `SUCCESS` entry for `d_ausd_v_ta_barrier_etl`.
    *   **Scenario B (Failure):**
        *   The older `ACTIVE` job in `job_table` is updated to `DEACTIVATED`.
        *   The new job entry in `job_table` for `BERT_V_TA_BARRIER` has `status = 'FAILED'`.
        *   `job_log` contains `ERROR` entries indicating the failure.
        *   `job_error_log` contains a detailed error message.
        *   `sql_execution_results` contains a `FAILED` entry for `d_ausd_v_ta_barrier_etl`.
        *   The `r_ausd_v_ta_barrier_wrapper` procedure `RAISE`s an error, indicating failure to any orchestrator.

    ```sql
    -- Scenario A (Success) checks
    SELECT status FROM `isrpt_isbert_data_processing.job_table` WHERE job_kennung = 'BERT_V_TA_BARRIER'; -- Should be 'SUCCESS'
    SELECT COUNT(*) FROM `isrpt_isbert_data_processing.job_log` WHERE job_id = 'BERT_V_TA_BARRIER' AND log_level = 'INFO' AND log_message LIKE '%completed successfully%'; -- Should be > 0
    SELECT status FROM `isrpt_isbert_data_processing.sql_execution_results` WHERE job_id = 'BERT_V_TA_BARRIER' AND script_name = 'd_ausd_v_ta_barrier_etl'; -- Should be 'SUCCESS'

    -- Scenario B (Failure) checks
    SELECT status FROM `isrpt_isbert_data_processing.job_table` WHERE job_kennung = 'BERT_V_TA_BARRIER' ORDER BY start_timestamp DESC LIMIT 1; -- Should be 'FAILED'
    SELECT COUNT(*) FROM `isrpt_isbert_data_processing.job_log` WHERE job_id = 'BERT_V_TA_BARRIER' AND log_level = 'ERROR'; -- Should be > 0
    SELECT COUNT(*) FROM `isrpt_isbert_data_processing.job_error_log` WHERE job_id = 'BERT_V_TA_BARRIER'; -- Should be > 0
    SELECT status FROM `isrpt_isbert_data_processing.sql_execution_results` WHERE job_id = 'BERT_V_TA_BARRIER' AND script_name = 'd_ausd_v_ta_barrier_etl'; -- Should be 'FAILED'
    ```

---

### Test Case 8: Logging Parity and Completeness

*   **Purpose:** To verify that the BigQuery logging tables (`job_log`, `job_error_log`, `sql_execution_results`) capture equivalent information to the legacy KornShell log files.
*   **Setup:**
    1.  Perform a successful run of the legacy KornShell job, capturing its log file output.
    2.  Perform a successful run of the migrated BigQuery job.
    3.  Perform a failed run of the legacy KornShell job (e.g., by introducing an error in the SQL script), capturing its log file output.
    4.  Perform a failed run of the migrated BigQuery job.
*   **Action:**
    1.  Compare the content of the legacy log files with queries against the BigQuery logging tables.
*   **Pass/Fail Criterion:**
    *   All critical events (job start, script calls, success messages, error messages, warnings) present in the legacy log files have a corresponding entry in the BigQuery logging tables.
    *   The `log_level` (INFO, WARNING, ERROR) in BigQuery logs accurately reflects the severity.
    *   Error messages in `job_error_log` are descriptive and capture the root cause.
    *   `sql_execution_results` accurately records the `records_processed` (if available from the ETL) and `status` for the ETL step.

    ```sql
    -- Example BigQuery query to review logs
    SELECT log_timestamp, log_level, log_message
    FROM `isrpt_isbert_data_processing.job_log`
    WHERE job_id = 'BERT_V_TA_BARRIER'
    ORDER BY log_timestamp;

    SELECT error_timestamp, script_name, error_message
    FROM `isrpt_isbert_data_processing.job_error_log`
    WHERE job_id = 'BERT_V_TA_BARRIER'
    ORDER BY error_timestamp;

    SELECT execution_timestamp, script_name, records_processed, status, error_message
    FROM `isrpt_isbert_data_processing.sql_execution_results`
    WHERE job_id = 'BERT_V_TA_BARRIER'
    ORDER BY execution_timestamp;
    ```

---

### Test Case 9: Schema and Data Type Parity

*   **Purpose:** To ensure the target `sof_ta_barrier` table in BigQuery has the correct schema (column names, data types, nullability) matching the legacy Oracle `sof$ta_barrier`.
*   **Setup:**
    1.  Obtain the schema definition for Oracle's `sof$ta_barrier`.
    2.  Ensure `isrpt_isbert_data_processing.sof_ta_barrier` is created in BigQuery.
*   **Action:**
    1.  Inspect the schema of `isrpt_isbert_data_processing.sof_ta_barrier` in BigQuery using `INFORMATION_SCHEMA`.
*   **Pass/Fail Criterion:**
    *   All column names in BigQuery match Oracle's.
    *   BigQuery data types are appropriate equivalents for Oracle data types (e.g., `NUMBER` to `INT64` or `NUMERIC`, `VARCHAR2` to `STRING`, `DATE` to `DATE` or `TIMESTAMP`).
    *   Nullability constraints are correctly applied.

    ```sql
    -- BigQuery query to inspect schema
    SELECT column_name, data_type, is_nullable
    FROM `isrpt_isbert_data_processing`.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'sof_ta_barrier'
    ORDER BY ordinal_position;

    -- Compare this output with the Oracle schema definition.
    ```

---

### Test Case 10: Empty Source Tables / No Data Scenario

*   **Purpose:** To ensure the job handles scenarios where source tables are empty or yield no data after filtering, completing gracefully without errors.
*   **Setup:**
    1.  Ensure all source tables (`cds_ta_barrier`, `cds_ta_barrier_class`, `cds_ta_barrier_kind`, `cds_ta_care_description`, `dwtk_meldungen`) in `isrpt_isbert_data_processing` are empty.
    2.  Clear `isrpt_isbert_data_processing.sof_ta_barrier`.
    3.  Ensure `isrpt_isbert_data_processing.job_table` is clean.
*   **Action:**
    1.  Execute the BigQuery wrapper procedure:
        ```sql
        CALL `isrpt_isbert_data_processing.r_ausd_v_ta_barrier_wrapper`(
            'Vertragsdatenabgleich',
            'BERT_V_TA_BARRIER',
            '12345'
        );
        ```
*   **Pass/Fail Criterion:**
    *   The `r_ausd_v_ta_barrier_wrapper` procedure completes successfully without raising any errors.
    *   `isrpt_isbert_data_processing.sof_ta_barrier` remains empty (row count = 0).
    *   `isrpt_isbert_data_processing.job_table` shows a `SUCCESS` status for the run.
    *   `isrpt_isbert_data_processing.sql_execution_results` shows a `SUCCESS` status for `d_ausd_v_ta_barrier_etl` with `records_processed = 0`.

    ```sql
    -- Check for successful completion
    SELECT status FROM `isrpt_isbert_data_processing.job_table` WHERE job_kennung = 'BERT_V_TA_BARRIER'; -- Should be 'SUCCESS'

    -- Check target table is empty
    SELECT COUNT(*) FROM `isrpt_isbert_data_processing.sof_ta_barrier`; -- Should be 0

    -- Check records processed
    SELECT records_processed FROM `isrpt_isbert_data_processing.sql_execution_results`
    WHERE job_id = 'BERT_V_TA_BARRIER' AND script_name = 'd_ausd_v_ta_barrier_etl'; -- Should be 0
    ```