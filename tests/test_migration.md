As a senior data-migration QA engineer, I understand the critical importance of thoroughly validating migrated ETL jobs. The `k_ausd_bp_ta_cntrct_evn.ksh` job, which orchestrates an Oracle SQL script to aggregate and transform contract event data, requires robust testing to ensure behavioral equivalence in the new environment.

Given that the migration build was skipped and the design phase failed, these tests are designed to be comprehensive, assuming a hypothetical migration to a modern data platform (e.g., BigQuery for data, Python/Airflow for orchestration). The focus is on the *behavior* and *outcomes* of the job as described in the legacy design, rather than specific implementation details of the migrated code.

We will establish a "golden record" approach where the output of the legacy job with controlled input data serves as the ground truth for comparison with the migrated job's output.

---

## Migration Validation Tests for `k_ausd_bp_ta_cntrct_evn.ksh`

### **Assumptions for Testing:**
*   **Legacy Environment:** Oracle Database (tables `sof$ta_bpr_evn`, `dwtk_meldungen`, `sof$ta_cntrct_evn`), KornShell script execution.
*   **Migrated Environment:** A modern data warehouse (e.g., BigQuery, Snowflake) for data storage, and an orchestration tool (e.g., Airflow, Python script) for job execution. The legacy Oracle tables are assumed to be replicated or migrated to this new platform (e.g., `migrated_sof_ta_bpr_evn`, `migrated_dwtk_meldungen`, `migrated_sof_ta_cntrct_evn`).
*   **Target Table Structure (Hypothetical based on aggregation purpose):**
    *   `sof$ta_cntrct_evn` (and `migrated_sof_ta_cntrct_evn`) is assumed to have columns like:
        *   `CONTRACT_BPR_ID` (Primary Key, derived from `bpr_id`)
        *   `TOTAL_EVENT_VALUE` (e.g., SUM of an `event_value` column from `sof$ta_bpr_evn`)
        *   `EVENT_COUNT` (COUNT of events for a `bpr_id`)
        *   `LAST_EVENT_DATE` (MAX of an `event_date` column from `sof$ta_bpr_evn`)
        *   `FIRST_MESSAGE_CODE` (e.g., MIN of a `message_code` column from `dwtk_meldungen`)
        *   `PROCESSING_DATE` (derived from `p_Stichtag` or `p_datum_heute` parameter)
        *   `INSERT_TIMESTAMP` (Audit column, typically `SYSDATE` or equivalent)
*   **Source Table Structure (Hypothetical):**
    *   `sof$ta_bpr_evn`: `bpr_id`, `event_type`, `event_value` (NUMBER), `event_date` (DATE), `status_code`
    *   `dwtk_meldungen`: `bpr_id`, `message_code` (VARCHAR), `message_date` (DATE), `severity`

---

### 1. End-to-End Output Parity (Golden Record Comparison)

*   **Purpose:** To verify that the migrated job produces an identical final dataset in the target table (`sof$ta_cntrct_evn` equivalent) compared to the legacy job, given the same input data and parameters. This is the most critical test for behavioral equivalence.
*   **Setup:**
    1.  Prepare a comprehensive set of test data for `sof$ta_bpr_evn` and `dwtk_meldungen` in the **legacy Oracle environment**. This dataset should include:
        *   Various `bpr_id`s with different numbers of associated events/messages.
        *   Records with `NULL` values in columns that are aggregated or used in joins.
        *   Records that would be filtered out by date parameters (`p_Stichtag`, `p_datum_heute`, `p_datum_gestern`).
        *   Edge cases like `bpr_id`s present in one source but not the other (if a join is implied).
        *   Data types that might require implicit conversion.
    2.  Ensure the **migrated environment** has an exact replica of this test data in `migrated_sof_ta_bpr_evn` and `migrated_dwtk_meldungen`.
    3.  Define a specific set of command-line parameters for the job, e.g., `p_JobKennung='TEST_JOB'`, `p_EintragsNr='123'`, `p_Stichtag='20231026'`, `p_wiederanlaufWert='0'`.
*   **Action:**
    1.  **Run Legacy Job:** Execute `k_ausd_bp_ta_cntrct_evn.ksh` with the defined parameters in the legacy environment.
        ```bash
        # Example legacy execution
        ./k_ausd_bp_ta_cntrct_evn.ksh -j TEST_JOB -f 123 -s 20231026 -l 0
        ```
    2.  **Extract Legacy Output:** After successful execution, extract all data from the legacy `sof$ta_cntrct_evn` table into a canonical format (e.g., CSV, JSON, or a temporary table in the migrated environment). This is our "golden record."
    3.  **Run Migrated Job:** Execute the migrated job (e.g., Airflow DAG, Python script) with the *exact same logical input data and parameters* in the migrated environment.
    4.  **Extract Migrated Output:** Extract all data from `migrated_sof_ta_cntrct_evn`.
*   **Pass/Fail Criterion:** The data extracted from `sof$ta_cntrct_evn` (legacy) must be *identical* to the data extracted from `migrated_sof_ta_cntrct_evn` (migrated) when compared row-by-row and column-by-column, ignoring audit columns like `INSERT_TIMESTAMP` if they are expected to differ.

    ```sql
    -- Example SQL for comparison (assuming both tables are in the same migrated DB for comparison)
    -- This query identifies rows present in legacy but not in migrated, or vice-versa.
    -- Adjust column names based on actual schema.
    SELECT 'Legacy Only' AS diff_type, l.*
    FROM legacy_sof_ta_cntrct_evn l
    MINUS
    SELECT 'Legacy Only' AS diff_type, m.*
    FROM migrated_sof_ta_cntrct_evn m
    UNION ALL
    SELECT 'Migrated Only' AS diff_type, m.*
    FROM migrated_sof_ta_cntrct_evn m
    MINUS
    SELECT 'Migrated Only' AS diff_type, l.*
    FROM legacy_sof_ta_cntrct_evn l;

    -- Pass if the above query returns 0 rows.
    ```
    ```python
    # Example Python (pytest) for comparison using pandas
    import pandas as pd
    from pandas.testing import assert_frame_equal

    def test_output_parity(legacy_data_df: pd.DataFrame, migrated_data_df: pd.DataFrame):
        # Sort dataframes by primary key(s) to ensure consistent comparison
        # Adjust 'CONTRACT_BPR_ID' to actual PK column(s)
        legacy_data_df = legacy_data_df.sort_values(by=['CONTRACT_BPR_ID']).reset_index(drop=True)
        migrated_data_df = migrated_data_df.sort_values(by=['CONTRACT_BPR_ID']).reset_index(drop=True)

        # Drop audit columns if they are expected to differ
        legacy_data_df = legacy_data_df.drop(columns=['INSERT_TIMESTAMP'], errors='ignore')
        migrated_data_df = migrated_data_df.drop(columns=['INSERT_TIMESTAMP'], errors='ignore')

        assert_frame_equal(legacy_data_df, migrated_data_df, check_dtype=True, check_exact=False, rtol=1e-5)
        # check_exact=False and rtol for floating point comparisons if applicable
    ```

### 2. Transformation Correctness - Aggregation and Join Logic

*   **Purpose:** To specifically validate that the core data aggregation and join logic (from `sof$ta_bpr_evn` and `dwtk_meldungen` grouped by `bpr_id`) is correctly translated and executed in the migrated job.
*   **Setup:**
    1.  Prepare a focused dataset in both legacy and migrated source tables (`sof$ta_bpr_evn`, `dwtk_meldungen`) that highlights various aggregation scenarios:
        *   Multiple records for a single `bpr_id`.
        *   `bpr_id`s with no matching records in the other source table (to test outer/inner join behavior).
        *   `bpr_id`s where some aggregated columns are `NULL` for some records.
        *   `bpr_id`s with only one event/message.
    2.  Use the same job parameters as in Test 1.
*   **Action:**
    1.  Run both legacy and migrated jobs as described in Test 1.
    2.  Query the target tables (`sof$ta_cntrct_evn` and `migrated_sof_ta_cntrct_evn`) to inspect specific aggregated values for selected `bpr_id`s.
*   **Pass/Fail Criterion:**
    *   For each selected `bpr_id`, the `TOTAL_EVENT_VALUE`, `EVENT_COUNT`, `LAST_EVENT_DATE`, `FIRST_MESSAGE_CODE`, and `PROCESSING_DATE` in the migrated target table must exactly match those in the legacy target table.
    *   The overall comparison from Test 1 should already cover this, but this test focuses on *specific* aggregation results for deeper inspection if Test 1 fails.

    ```sql
    -- Example SQL to verify specific aggregated values for a BPR_ID
    -- Replace with actual column names and a representative BPR_ID
    SELECT
        'Legacy' AS source,
        CONTRACT_BPR_ID,
        TOTAL_EVENT_VALUE,
        EVENT_COUNT,
        LAST_EVENT_DATE,
        FIRST_MESSAGE_CODE
    FROM legacy_sof_ta_cntrct_evn
    WHERE CONTRACT_BPR_ID = 'BPR123'
    UNION ALL
    SELECT
        'Migrated' AS source,
        CONTRACT_BPR_ID,
        TOTAL_EVENT_VALUE,
        EVENT_COUNT,
        LAST_EVENT_DATE,
        FIRST_MESSAGE_CODE
    FROM migrated_sof_ta_cntrct_evn
    WHERE CONTRACT_BPR_ID = 'BPR123';

    -- Pass if the two rows returned (one from legacy, one from migrated) are identical for all selected columns.
    ```

### 3. Transformation Correctness - Date Parameter Handling

*   **Purpose:** To ensure that the job correctly interprets and applies the date parameters (`p_Stichtag`, `p_datum_heute`, `p_datum_gestern`) for filtering source data or populating target columns. The `gestern.ksh` script's logic for deriving `p_datum_heute` and `p_datum_gestern` must be preserved.
*   **Setup:**
    1.  Prepare source data in `sof$ta_bpr_evn` and `dwtk_meldungen` that includes records with `event_date`s:
        *   On `p_Stichtag`.
        *   On `p_datum_heute` (derived from `p_Stichtag` by `gestern.ksh`).
        *   On `p_datum_gestern` (derived from `p_Stichtag` by `gestern.ksh`).
        *   Before `p_datum_gestern`.
        *   After `p_datum_heute`.
    2.  Set `p_Stichtag` to a specific date (e.g., `20231026`).
    3.  Ensure the `gestern.ksh` script is available and its output can be captured for the legacy run.
*   **Action:**
    1.  **Determine `p_datum_heute` and `p_datum_gestern`:** Manually run `gestern.ksh` for the chosen `p_Stichtag` to get the exact values.
        ```bash
        # Example:
        # Assume `gestern.ksh` outputs "YYYYMMDD_TODAY YYYYMMDD_YESTERDAY"
        # For p_Stichtag=20231026, if today is 20231027, gestern.ksh might output "20231027 20231026"
        # The ksh script uses `set` to assign these.
        # The actual logic of `gestern.ksh` needs to be understood and replicated.
        ```
    2.  Run both legacy and migrated jobs with the chosen `p_Stichtag` and other parameters.
    3.  Inspect the `PROCESSING_DATE` column (or any other date-derived column) in the target tables.
*   **Pass/Fail Criterion:**
    *   The `PROCESSING_DATE` column in `migrated_sof_ta_cntrct_evn` must match the corresponding column in `sof$ta_cntrct_evn` for all records.
    *   Records that should be included/excluded based on date filters must be consistently processed by both jobs.

    ```sql
    -- Example SQL to check processing_date
    SELECT
        'Legacy' AS source,
        CONTRACT_BPR_ID,
        PROCESSING_DATE
    FROM legacy_sof_ta_cntrct_evn
    WHERE CONTRACT_BPR_ID = 'BPR_DATE_TEST'
    UNION ALL
    SELECT
        'Migrated' AS source,
        CONTRACT_BPR_ID,
        PROCESSING_DATE
    FROM migrated_sof_ta_cntrct_evn
    WHERE CONTRACT_BPR_ID = 'BPR_DATE_TEST';

    -- Pass if PROCESSING_DATE values match for the same BPR_ID.
    ```

### 4. External System Replacements - Oracle Reads

*   **Purpose:** To confirm that the migrated job correctly accesses and reads data from the new data platform's equivalent of the Oracle source tables (`sof$ta_bpr_evn`, `dwtk_meldungen`). This is implicitly covered by output parity, but a dedicated check ensures the *source data access* itself is sound.
*   **Setup:**
    1.  Ensure the migrated source tables (`migrated_sof_ta_bpr_evn`, `migrated_dwtk_meldungen`) contain a representative dataset.
    2.  Introduce a specific, easily identifiable record (e.g., `bpr_id = 'EXT_SYS_TEST'`) into both legacy Oracle and migrated source tables.
    3.  Use standard job parameters.
*   **Action:**
    1.  Run both legacy and migrated jobs.
    2.  Query the target tables for the `EXT_SYS_TEST` `bpr_id`.
*   **Pass/Fail Criterion:**
    *   The record for `bpr_id = 'EXT_SYS_TEST'` must be present in both `sof$ta_cntrct_evn` and `migrated_sof_ta_cntrct_evn`, and its aggregated values must match.
    *   This test primarily relies on the success of Test 1, but serves as a conceptual check for the source system integration. If Test 1 passes, this implicitly passes.

### 5. Data Quality / Row Count / Schema Assertions

*   **Purpose:** To verify that the migrated job maintains data integrity, produces the expected number of rows, and adheres to the defined schema for the target table.
*   **Setup:**
    1.  Use the same comprehensive test data and parameters as in Test 1.
*   **Action:**
    1.  Run both legacy and migrated jobs.
    2.  After execution, perform the following checks on both `sof$ta_cntrct_evn` and `migrated_sof_ta_cntrct_evn`:
        *   Count rows.
        *   Check schema (column names, data types, nullability).
        *   Perform data quality checks (e.g., no `NULL`s in `CONTRACT_BPR_ID`, `TOTAL_EVENT_VALUE` is non-negative, `EVENT_COUNT` is non-negative).
*   **Pass/Fail Criterion:**
    1.  **Row Count Parity:** The total number of rows in `migrated_sof_ta_cntrct_evn` must be identical to the total number of rows in `sof$ta_cntrct_evn`.
        ```sql
        -- SQL for row count comparison
        SELECT COUNT(*) FROM legacy_sof_ta_cntrct_evn;
        SELECT COUNT(*) FROM migrated_sof_ta_cntrct_evn;
        -- Pass if counts are equal.
        ```
    2.  **Schema Parity:** The column names, data types, and nullability constraints of `migrated_sof_ta_cntrct_evn` must match `sof$ta_cntrct_evn`.
        ```sql
        -- Example SQL for schema comparison (database-specific, e.g., Oracle vs. BigQuery)
        -- Oracle:
        SELECT COLUMN_NAME, DATA_TYPE, NULLABLE FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = 'SOF$TA_CNTRCT_EVN' ORDER BY COLUMN_ID;
        -- BigQuery:
        SELECT column_name, data_type, is_nullable FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'migrated_sof_ta_cntrct_evn' ORDER BY ordinal_position;
        -- Pass if all attributes match.
        ```
    3.  **Data Quality Assertions:**
        ```sql
        -- Example SQL for data quality checks on migrated table
        -- Check for NULLs in primary key
        SELECT COUNT(*) FROM migrated_sof_ta_cntrct_evn WHERE CONTRACT_BPR_ID IS NULL; -- Should be 0
        -- Check for non-negative aggregated values
        SELECT COUNT(*) FROM migrated_sof_ta_cntrct_evn WHERE TOTAL_EVENT_VALUE < 0; -- Should be 0
        SELECT COUNT(*) FROM migrated_sof_ta_cntrct_evn WHERE EVENT_COUNT < 0; -- Should be 0
        -- Pass if all data quality checks return expected results (e.g., 0 for error conditions).
        ```

### 6. Edge Case: Empty Source Tables

*   **Purpose:** To ensure the migrated job handles scenarios where one or both source tables are empty gracefully, producing an empty target table without errors.
*   **Setup:**
    1.  **Scenario A:** `sof$ta_bpr_evn` is empty, `dwtk_meldungen` has data.
    2.  **Scenario B:** `dwtk_meldungen` is empty, `sof$ta_bpr_evn` has data.
    3.  **Scenario C:** Both `sof$ta_bpr_evn` and `dwtk_meldungen` are empty.
    4.  Ensure these scenarios are replicated in both legacy Oracle and migrated source tables.
    5.  Use standard job parameters.
*   **Action:**
    1.  For each scenario (A, B, C), run both legacy and migrated jobs.
    2.  Check the row count of the target tables.
*   **Pass/Fail Criterion:**
    *   For all scenarios, the `migrated_sof_ta_cntrct_evn` table must have the same number of rows as the `sof$ta_cntrct_evn` table (expected to be 0 in most empty source scenarios, depending on join type).
    *   Both jobs must complete successfully without errors.

    ```sql
    -- SQL for row count check
    SELECT COUNT(*) FROM legacy_sof_ta_cntrct_evn;
    SELECT COUNT(*) FROM migrated_sof_ta_cntrct_evn;
    -- Pass if counts are equal (and typically 0 for these scenarios).
    ```

### 7. Edge Case: NULL Value Handling in Aggregations

*   **Purpose:** To verify that `NULL` values in source columns used for aggregation or filtering are handled identically by the migrated job as in the legacy Oracle SQL. This includes `COUNT(column)` vs `COUNT(*)`, `SUM(column)` ignoring NULLs, `MAX(column)` ignoring NULLs, and `MIN(column)` ignoring NULLs.
*   **Setup:**
    1.  Prepare source data with `bpr_id`s where:
        *   `event_value` is `NULL` for some records within a `bpr_id` group.
        *   `event_date` is `NULL` for some records within a `bpr_id` group.
        *   `message_code` is `NULL` for some records within a `bpr_id` group.
        *   A `bpr_id` has all `NULL`s for an aggregated column.
    2.  Replicate this data in both legacy and migrated source tables.
    3.  Use standard job parameters.
*   **Action:**
    1.  Run both legacy and migrated jobs.
    2.  Inspect the aggregated values (`TOTAL_EVENT_VALUE`, `EVENT_COUNT`, `LAST_EVENT_DATE`, `FIRST_MESSAGE_CODE`) for the `bpr_id`s designed to test `NULL` handling.
*   **Pass/Fail Criterion:** The aggregated values in `migrated_sof_ta_cntrct_evn` must exactly match those in `sof$ta_cntrct_evn` for all test `bpr_id`s.

    ```sql
    -- Example SQL to check NULL handling for a specific BPR_ID
    SELECT
        'Legacy' AS source,
        CONTRACT_BPR_ID,
        TOTAL_EVENT_VALUE,
        EVENT_COUNT,
        LAST_EVENT_DATE,
        FIRST_MESSAGE_CODE
    FROM legacy_sof_ta_cntrct_evn
    WHERE CONTRACT_BPR_ID = 'BPR_NULL_TEST'
    UNION ALL
    SELECT
        'Migrated' AS source,
        CONTRACT_BPR_ID,
        TOTAL_EVENT_VALUE,
        EVENT_COUNT,
        LAST_EVENT_DATE,
        FIRST_MESSAGE_CODE
    FROM migrated_sof_ta_cntrct_evn
    WHERE CONTRACT_BPR_ID = 'BPR_NULL_TEST';

    -- Pass if the aggregated values match exactly.
    ```

### 8. Idempotency Test (Running the Job Twice)

*   **Purpose:** To ensure that running the migrated job multiple times with the same inputs produces the same result, without accumulating duplicate data or causing errors. This verifies the `TRUNCATE` and `INSERT` logic.
*   **Setup:**
    1.  Prepare a standard, representative dataset in both legacy and migrated source tables.
    2.  Use standard job parameters.
*   **Action:**
    1.  **Legacy Job:** Run `k_ausd_bp_ta_cntrct_evn.ksh` once. Then run it a second time immediately after the first.
    2.  **Migrated Job:** Run the migrated job once. Then run it a second time immediately after the first.
    3.  After both runs, extract the final state of `sof$ta_cntrct_evn` and `migrated_sof_ta_cntrct_evn`.
*   **Pass/Fail Criterion:**
    *   The final state of `sof$ta_cntrct_evn` after two runs must be identical to its state after one run.
    *   The final state of `migrated_sof_ta_cntrct_evn` after two runs must be identical to its state after one run.
    *   Crucially, the final state of `migrated_sof_ta_cntrct_evn` must be identical to the final state of `sof$ta_cntrct_evn` (as per Test 1).
    *   Both jobs must complete successfully on both runs.

    ```sql
    -- SQL to check for duplicate rows (should be 0 if PK is enforced or logic is correct)
    SELECT CONTRACT_BPR_ID, COUNT(*)
    FROM migrated_sof_ta_cntrct_evn
    GROUP BY CONTRACT_BPR_ID
    HAVING COUNT(*) > 1;
    -- Pass if this query returns 0 rows.
    ```

---

These tests provide a comprehensive framework for validating the migration of `k_ausd_bp_ta_cntrct_evn.ksh`. By focusing on behavioral equivalence and covering various data scenarios and edge cases, we can ensure the migrated job performs as expected and maintains data integrity.