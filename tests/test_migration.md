As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the migration of `k_ausd_v_ta_barrier.ksh` to a BigQuery Stored Procedure. These tests cover output parity, transformation correctness, external system replacements, and data quality assertions, ensuring the migrated solution is behaviourally equivalent to the legacy system.

---

## Migration Validation Tests: `k_ausd_v_ta_barrier.ksh` to BigQuery

### Test Environment Setup (Pre-requisites for all tests)

Before running any tests, ensure the following:
1.  **BigQuery Project and Datasets:**
    *   `my-project.data_warehouse` (for target table `sof_ta_barrier` and control table `job_control_log`)
    *   `my-project.oracle_raw` (for ingested source tables: `dwtk_meldungen`, `cds_ta_barrier`, `cds_ta_barrier_class`, `cds_ta_barrier_kind`, `cds_ta_care_description`)
2.  **BigQuery Stored Procedure:** The `my-project.data_warehouse.r_ausd_vertrag_control` procedure is deployed.
3.  **Source Data Ingestion:** The `oracle_raw` tables are populated with representative data from the Oracle source system. For parity tests, this data must be identical to what the legacy Oracle job would process.
4.  **Legacy Environment:** Access to the legacy Oracle database and the ability to execute the `k_ausd_v_ta_barrier.ksh` script and query `sof$ta_barrier`.

---

### 1. Output Parity Tests

#### Test Case 1.1: Full Data Equivalence (Happy Path)

*   **Purpose:** To verify that for a given set of identical source data, the BigQuery stored procedure produces an output in `sof_ta_barrier` that is byte-for-byte identical to the output of the legacy Oracle job in `sof$ta_barrier`.
*   **Setup:**
    1.  Ensure `my-project.oracle_raw` tables (`dwtk_meldungen`, `cds_ta_barrier`, `cds_ta_barrier_class`, `cds_ta_barrier_kind`, `cds_ta_care_description`) are populated with a known, representative dataset.
    2.  Ensure the legacy Oracle source tables (`isbert_schema.dwtk_meldungen`, `cds$ta_barrier`, etc.) contain *exactly* the same data as their BigQuery `oracle_raw` counterparts.
    3.  Clear the target tables in both environments:
        *   Oracle: `TRUNCATE TABLE sof$ta_barrier;`
        *   BigQuery: `TRUNCATE TABLE \`my-project.data_warehouse.sof_ta_barrier\`;`
*   **Action:**
    1.  Execute the legacy KornShell script:
        ```bash
        ./k_ausd_v_ta_barrier.ksh -j "TEST_JOB" -f "12345"
        ```
    2.  Execute the BigQuery Stored Procedure:
        ```sql
        CALL `my-project.data_warehouse.r_ausd_vertrag_control`('TEST_JOB', '12345');
        ```
    3.  Export the contents of `sof$ta_barrier` from Oracle and `sof_ta_barrier` from BigQuery to CSV files.
*   **Pass/Fail Criterion:**
    *   The exported CSV files from Oracle and BigQuery are identical (e.g., using `diff` command or a data comparison tool).
    *   Alternatively, a SQL query comparing the two datasets (after linking Oracle to BigQuery or vice-versa, or by loading Oracle data into a temporary BigQuery table) returns zero differences.

    ```sql
    -- Example BigQuery SQL for comparison (assuming Oracle data is loaded into a temp BQ table 'oracle_sof_ta_barrier_temp')
    SELECT 'Only in BigQuery' AS source, A.* FROM `my-project.data_warehouse.sof_ta_barrier` AS A
    EXCEPT DISTINCT
    SELECT 'Only in Oracle' AS source, B.* FROM `my-project.temp_dataset.oracle_sof_ta_barrier_temp` AS B

    UNION ALL

    SELECT 'Only in Oracle' AS source, B.* FROM `my-project.temp_dataset.oracle_sof_ta_barrier_temp` AS B
    EXCEPT DISTINCT
    SELECT 'Only in BigQuery' AS source, A.* FROM `my-project.data_warehouse.sof_ta_barrier` AS A;

    -- Pass if the query returns 0 rows.
    ```

---

### 2. Transformation Correctness Tests

#### Test Case 2.1: `v_datum` Calculation

*   **Purpose:** To verify that the `v_datum` parameter, derived from `dwtk_meldungen`, is calculated correctly, including the `NVL` to '19000101' for edge cases.
*   **Setup:**
    1.  Clear `my-project.oracle_raw.dwtk_meldungen`.
    2.  Insert specific test data into `my-project.oracle_raw.dwtk_meldungen`:
        *   Scenario A: Multiple `timecreated` entries for `BERT_DROP_TEMP_TABLE`.
        *   Scenario B: No entries for `BERT_DROP_TEMP_TABLE`.
        *   Scenario C: `timecreated` is `NULL` for `BERT_DROP_TEMP_TABLE`.
*   **Action:**
    1.  For each scenario, execute the BigQuery Stored Procedure:
        ```sql
        CALL `my-project.data_warehouse.r_ausd_vertrag_control`('TEST_JOB_VDATUM', '1');
        ```
    2.  Query the `job_control_log` table to inspect the `message` field for the `v_datum` value.
*   **Pass/Fail Criterion:**
    *   **Scenario A:** `v_datum` in the log message matches `FORMAT_DATE('%Y%m%d', MAX(timecreated))` for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    *   **Scenario B & C:** `v_datum` in the log message is '19000101'.

    ```sql
    -- Example Setup for Scenario A
    TRUNCATE TABLE `my-project.oracle_raw.dwtk_meldungen`;
    INSERT INTO `my-project.oracle_raw.dwtk_meldungen` (job_kennung, timecreated) VALUES
    ('BERT_DROP_TEMP_TABLE', '2023-01-15 10:00:00 UTC'),
    ('BERT_DROP_TEMP_TABLE', '2023-01-20 11:30:00 UTC'), -- Max timecreated
    ('OTHER_JOB', '2023-01-25 12:00:00 UTC');

    -- Example Setup for Scenario B
    TRUNCATE TABLE `my-project.oracle_raw.dwtk_meldungen`;
    INSERT INTO `my-project.oracle_raw.dwtk_meldungen` (job_kennung, timecreated) VALUES
    ('OTHER_JOB', '2023-01-25 12:00:00 UTC');

    -- Example Setup for Scenario C
    TRUNCATE TABLE `my-project.oracle_raw.dwtk_meldungen`;
    INSERT INTO `my-project.oracle_raw.dwtk_meldungen` (job_kennung, timecreated) VALUES
    ('BERT_DROP_TEMP_TABLE', NULL);

    -- After calling the SP, check the log:
    SELECT message FROM `my-project.data_warehouse.job_control_log`
    WHERE job_name = 'r_ausd_vertrag_control' AND job_kennung = 'TEST_JOB_VDATUM'
    ORDER BY start_time DESC LIMIT 1;
    -- Expected for A: "Job started. v_datum determined: 20230120"
    -- Expected for B/C: "Job started. v_datum determined: 19000101"
    ```

#### Test Case 2.2: `TRUNCATE` Behavior

*   **Purpose:** To confirm that the target table `sof_ta_barrier` is effectively truncated before new data is inserted.
*   **Setup:**
    1.  Populate `my-project.data_warehouse.sof_ta_barrier` with at least 5 dummy rows.
    2.  Populate `my-project.oracle_raw` source tables with data that would result in 3 new rows being inserted.
*   **Action:**
    1.  Execute the BigQuery Stored Procedure:
        ```sql
        CALL `my-project.data_warehouse.r_ausd_vertrag_control`('TEST_JOB_TRUNCATE', '2');
        ```
    2.  Query the row count of `my-project.data_warehouse.sof_ta_barrier`.
*   **Pass/Fail Criterion:** The final row count of `sof_ta_barrier` is 3 (matching the new data, not 5+3=8), indicating the previous 5 dummy rows were removed.

    ```sql
    -- Setup: Insert dummy data into sof_ta_barrier
    INSERT INTO `my-project.data_warehouse.sof_ta_barrier` (cntrct_id, barrier_kind_id, sperrart) VALUES
    ('DUMMY1', 'BK1', 'Art1'), ('DUMMY2', 'BK2', 'Art2'), ('DUMMY3', 'BK3', 'Art3'), ('DUMMY4', 'BK4', 'Art4'), ('DUMMY5', 'BK5', 'Art5');

    -- Setup: Populate oracle_raw tables to produce 3 rows
    -- (Detailed inserts for all source tables would go here)

    -- Action:
    CALL `my-project.data_warehouse.r_ausd_vertrag_control`('TEST_JOB_TRUNCATE', '2');

    -- Pass/Fail:
    SELECT COUNT(*) FROM `my-project.data_warehouse.sof_ta_barrier`;
    -- Expected result: 3
    ```

#### Test Case 2.3: `COALESCE` (NVL) for `sperr_beginn` and `sperr_ende`

*   **Purpose:** To verify the correct translation of Oracle's `NVL` to BigQuery's `COALESCE` for date fields, ensuring `net_barr_on_date`/`off_date` takes precedence over `valid_from`/`to`.
*   **Setup:**
    1.  Clear `my-project.oracle_raw.cds_ta_barrier`.
    2.  Insert test data into `my-project.oracle_raw.cds_ta_barrier` covering these scenarios:
        *   `net_barr_on_date` is present, `valid_from` is present.
        *   `net_barr_on_date` is `NULL`, `valid_from` is present.
        *   `net_barr_on_date` is present, `valid_from` is `NULL`.
        *   Both `net_barr_on_date` and `valid_from` are `NULL`.
        *   Repeat for `net_barr_off_date` and `valid_to`.
    3.  Ensure other source tables are populated to allow joins.
*   **Action:**
    1.  Execute the BigQuery Stored Procedure.
    2.  Query `my-project.data_warehouse.sof_ta_barrier` for `sperr_beginn` and `sperr_ende`.
*   **Pass/Fail Criterion:**
    *   `sperr_beginn` matches `net_barr_on_date` when not `NULL`, otherwise `valid_from`. If both are `NULL`, `sperr_beginn` should be `NULL`.
    *   `sperr_ende` matches `net_barr_off_date` when not `NULL`, otherwise `valid_to`. If both are `NULL`, `sperr_ende` should be `NULL`.

    ```sql
    -- Example Setup for cds_ta_barrier (simplified for brevity, assume other join tables exist)
    TRUNCATE TABLE `my-project.oracle_raw.cds_ta_barrier`;
    INSERT INTO `my-project.oracle_raw.cds_ta_barrier` (cntrct_id, barrier_class_id, barrier_kind_id, is_production, valid_from, valid_to, net_barr_on_date, net_barr_off_date, insert_at) VALUES
    ('C1', 'BC1', 'BK1', 1, '2023-01-01', '2023-12-31', '2023-01-15', '2023-11-30', '2023-01-01 00:00:00 UTC'), -- Both present
    ('C2', 'BC1', 'BK1', 1, '2023-02-01', '2023-10-31', NULL, '2023-10-31', '2023-01-01 00:00:00 UTC'),      -- net_barr_on_date NULL
    ('C3', 'BC1', 'BK1', 1, NULL, '2023-09-30', '2023-03-01', NULL, '2023-01-01 00:00:00 UTC'),      -- valid_from NULL, net_barr_off_date NULL
    ('C4', 'BC1', 'BK1', 1, NULL, NULL, NULL, NULL, '2023-01-01 00:00:00 UTC');                      -- All NULL

    -- Action:
    CALL `my-project.data_warehouse.r_ausd_vertrag_control`('TEST_JOB_COALESCE', '3');

    -- Pass/Fail:
    SELECT
        cntrct_id,
        sperr_beginn,
        sperr_ende
    FROM `my-project.data_warehouse.sof_ta_barrier`
    ORDER BY cntrct_id;
    -- Expected:
    -- C1: sperr_beginn=2023-01-15, sperr_ende=2023-11-30
    -- C2: sperr_beginn=2023-02-01, sperr_ende=2023-10-31
    -- C3: sperr_beginn=2023-03-01, sperr_ende=NULL
    -- C4: sperr_beginn=NULL, sperr_ende=NULL
    ```

#### Test Case 2.4: `GREATEST` for `bfc_age`

*   **Purpose:** To verify the `GREATEST` function correctly determines the later of `b.insert_at` and `bc.insert_at` for `bfc_age`.
*   **Setup:**
    1.  Clear `my-project.oracle_raw.cds_ta_barrier` and `my-project.oracle_raw.cds_ta_barrier_class`.
    2.  Insert test data with varying `insert_at` values:
        *   `b.insert_at` > `bc.insert_at`
        *   `b.insert_at` < `bc.insert_at`
        *   `b.insert_at` = `bc.insert_at`
        *   One or both `insert_at` values are `NULL`.
    3.  Ensure other source tables are populated to allow joins.
*   **Action:**
    1.  Execute the BigQuery Stored Procedure.
    2.  Query `my-project.data_warehouse.sof_ta_barrier` for `bfc_age`.
*   **Pass/Fail Criterion:** `bfc_age` matches the later of `b.insert_at` and `bc.insert_at`. If one is `NULL`, it should return the non-`NULL` value. If both are `NULL`, `bfc_age` should be `NULL`.

    ```sql
    -- Example Setup (simplified)
    TRUNCATE TABLE `my-project.oracle_raw.cds_ta_barrier`;
    TRUNCATE TABLE `my-project.oracle_raw.cds_ta_barrier_class`;
    INSERT INTO `my-project.oracle_raw.cds_ta_barrier` (cntrct_id, barrier_class_id, is_production, valid_from, insert_at) VALUES
    ('C1', 'BC1', 1, '2023-01-01', '2023-01-10 10:00:00 UTC'),
    ('C2', 'BC2', 1, '2023-01-01', '2023-01-05 10:00:00 UTC'),
    ('C3', 'BC3', 1, '2023-01-01', '2023-01-10 10:00:00 UTC'),
    ('C4', 'BC4', 1, '2023-01-01', NULL),
    ('C5', 'BC5', 1, '2023-01-01', '2023-01-10 10:00:00 UTC'),
    ('C6', 'BC6', 1, '2023-01-01', NULL);

    INSERT INTO `my-project.oracle_raw.cds_ta_barrier_class` (barrier_class_id, barrier_kind_id, barrier_reason_cv, closure, insert_at) VALUES
    ('BC1', 'BK1', '1', 0, '2023-01-05 10:00:00 UTC'), -- b.insert_at > bc.insert_at
    ('BC2', 'BK1', '1', 0, '2023-01-10 10:00:00 UTC'), -- b.insert_at < bc.insert_at
    ('BC3', 'BK1', '1', 0, '2023-01-10 10:00:00 UTC'), -- b.insert_at = bc.insert_at
    ('BC4', 'BK1', '1', 0, '2023-01-10 10:00:00 UTC'), -- b.insert_at NULL
    ('BC5', 'BK1', '1', 0, NULL),                     -- bc.insert_at NULL
    ('BC6', 'BK1', '1', 0, NULL);                     -- Both NULL

    -- Action:
    CALL `my-project.data_warehouse.r_ausd_vertrag_control`('TEST_JOB_GREATEST', '4');

    -- Pass/Fail:
    SELECT
        cntrct_id,
        bfc_age
    FROM `my-project.data_warehouse.sof_ta_barrier`
    ORDER BY cntrct_id;
    -- Expected:
    -- C1: 2023-01-10 10:00:00 UTC
    -- C2: 2023-01-10 10:00:00 UTC
    -- C3: 2023-01-10 10:00:00 UTC
    -- C4: 2023-01-10 10:00:00 UTC
    -- C5: 2023-01-10 10:00:00 UTC
    -- C6: NULL
    ```

#### Test Case 2.5: `CASE` (DECODE) for `sperrgrund`

*   **Purpose:** To verify the complex `DECODE` logic for `sperrgrund` is correctly translated to a BigQuery `CASE` expression, including the default 'Unbekannter Sperrgrund'.
*   **Setup:**
    1.  Clear `my-project.oracle_raw.cds_ta_barrier_class`.
    2.  Insert test data into `my-project.oracle_raw.cds_ta_barrier_class` with `barrier_reason_cv` values covering:
        *   All defined numeric codes (1-18).
        *   A numeric code outside the defined range (e.g., '99').
        *   `NULL` for `barrier_reason_cv`.
    3.  Ensure other source tables are populated to allow joins.
*   **Action:**
    1.  Execute the BigQuery Stored Procedure.
    2.  Query `my-project.data_warehouse.sof_ta_barrier` for `sperrgrund`.
*   **Pass/Fail Criterion:** `sperrgrund` values match the expected descriptive strings for codes 1-18, 'Unbekannter Sperrgrund' for undefined codes, and 'Unbekannter Sperrgrund' for `NULL`.

    ```sql
    -- Example Setup (simplified)
    TRUNCATE TABLE `my-project.oracle_raw.cds_ta_barrier_class`;
    INSERT INTO `my-project.oracle_raw.cds_ta_barrier_class` (barrier_class_id, barrier_kind_id, barrier_reason_cv, closure, insert_at) VALUES
    ('BC1', 'BK1', '1', 0, '2023-01-01 00:00:00 UTC'),  -- Kartenverlust
    ('BC2', 'BK1', '18', 0, '2023-01-01 00:00:00 UTC'), -- Sperre wegen Datenbereinigung
    ('BC3', 'BK1', '99', 0, '2023-01-01 00:00:00 UTC'), -- Undefined code
    ('BC4', 'BK1', NULL, 0, '2023-01-01 00:00:00 UTC'); -- NULL code

    -- Action:
    CALL `my-project.data_warehouse.r_ausd_vertrag_control`('TEST_JOB_DECODE', '5');

    -- Pass/Fail:
    SELECT
        (SELECT cntrct_id FROM `my-project.oracle_raw.cds_ta_barrier` WHERE barrier_class_id = T.barrier_class_id) AS cntrct_id,
        sperrgrund
    FROM `my-project.data_warehouse.sof_ta_barrier` AS T
    ORDER BY cntrct_id;
    -- Expected:
    -- C1: 'Kartenverlust'
    -- C2: 'Sperre wegen Datenbereinigung'
    -- C3: 'Unbekannter Sperrgrund'
    -- C4: 'Unbekannter Sperrgrund'
    ```

#### Test Case 2.6: `CASE WHEN` for `ist_stillegung`

*   **Purpose:** To verify the `CASE WHEN` logic for converting `bc.closure` (numeric) to `ist_stillegung` (BOOLEAN).
*   **Setup:**
    1.  Clear `my-project.oracle_raw.cds_ta_barrier_class`.
    2.  Insert test data with `closure` values:
        *   `1`
        *   `0`
        *   `NULL`
    3.  Ensure other source tables are populated to allow joins.
*   **Action:**
    1.  Execute the BigQuery Stored Procedure.
    2.  Query `my-project.data_warehouse.sof_ta_barrier` for `ist_stillegung`.
*   **Pass/Fail Criterion:** `ist_stillegung` is `TRUE` when `closure = 1`, and `FALSE` when `closure = 0` or `NULL`.

    ```sql
    -- Example Setup (simplified)
    TRUNCATE TABLE `my-project.oracle_raw.cds_ta_barrier_class`;
    INSERT INTO `my-project.oracle_raw.cds_ta_barrier_class` (barrier_class_id, barrier_kind_id, barrier_reason_cv, closure, insert_at) VALUES
    ('BC1', 'BK1', '1', 1, '2023-01-01 00:00:00 UTC'),
    ('BC2', 'BK1', '1', 0, '2023-01-01 00:00:00 UTC'),
    ('BC3', 'BK1', '1', NULL, '2023-01-01 00:00:00 UTC');

    -- Action:
    CALL `my-project.data_warehouse.r_ausd_vertrag_control`('TEST_JOB_CLOSURE', '6');

    -- Pass/Fail:
    SELECT
        (SELECT cntrct_id FROM `my-project.oracle_raw.cds_ta_barrier` WHERE barrier_class_id = T.barrier_class_id) AS cntrct_id,
        ist_stillegung
    FROM `my-project.data_warehouse.sof_ta_barrier` AS T
    ORDER BY cntrct_id;
    -- Expected:
    -- C1: TRUE
    -- C2: FALSE
    -- C3: FALSE
    ```

#### Test Case 2.7: Join Conditions and `LEFT JOIN` Behavior

*   **Purpose:** To verify all join conditions are correctly applied, especially the `LEFT JOIN` to `cds_ta_care_description`, ensuring `sperrart` is `NULL` when no match is found.
*   **Setup:**
    1.  Clear all `my-project.oracle_raw` source tables.
    2.  Insert data to cover:
        *   All tables have matching join keys.
        *   `cds_ta_care_description` has no matching `cds_description_id` for a `cds_ta_barrier_kind` entry.
        *   `cds_ta_barrier_class` has no matching `barrier_kind_id` for a `cds_ta_barrier_kind` entry (should filter out).
    3.  Ensure `v_datum` and filter conditions (`is_production`) allow all test rows to be processed.
*   **Action:**
    1.  Execute the BigQuery Stored Procedure.
    2.  Query `my-project.data_warehouse.sof_ta_barrier` for `cntrct_id` and `sperrart`.
*   **Pass/Fail Criterion:**
    *   Rows with full matches across all `INNER JOIN` tables are present.
    *   Rows where `cds_ta_care_description` has no match (due to `LEFT JOIN`) are present, but their `sperrart` is `NULL`.
    *   Rows that would fail an `INNER JOIN` (e.g., `cds_ta_barrier_class` not matching `cds_ta_barrier_kind`) are *not* present.

    ```sql
    -- Example Setup (simplified, assume other required columns are populated)
    TRUNCATE TABLE `my-project.oracle_raw.cds_ta_barrier`;
    TRUNCATE TABLE `my-project.oracle_raw.cds_ta_barrier_class`;
    TRUNCATE TABLE `my-project.oracle_raw.cds_ta_barrier_kind`;
    TRUNCATE TABLE `my-project.oracle_raw.cds_ta_care_description`;
    TRUNCATE TABLE `my-project.oracle_raw.dwtk_meldungen`;
    INSERT INTO `my-project.oracle_raw.dwtk_meldungen` (job_kennung, timecreated) VALUES ('BERT_DROP_TEMP_TABLE', '2000-01-01 00:00:00 UTC'); -- Set v_datum early

    INSERT INTO `my-project.oracle_raw.cds_ta_barrier` (cntrct_id, barrier_class_id, is_production, valid_from, insert_at) VALUES
    ('C_FULL_MATCH', 'BC1', 1, '1999-01-01', '2023-01-01 00:00:00 UTC'),
    ('C_LEFT_JOIN_NULL', 'BC2', 1, '1999-01-01', '2023-01-01 00:00:00 UTC'),
    ('C_INNER_JOIN_FAIL', 'BC3', 1, '1999-01-01', '2023-01-01 00:00:00 UTC'); -- This row should be filtered out by inner join

    INSERT INTO `my-project.oracle_raw.cds_ta_barrier_class` (barrier_class_id, barrier_kind_id, barrier_reason_cv, closure, insert_at) VALUES
    ('BC1', 'BK1', '1', 0, '2023-01-01 00:00:00 UTC'),
    ('BC2', 'BK2', '1', 0, '2023-01-01 00:00:00 UTC'),
    ('BC3', 'BK_NO_MATCH', '1', 0, '2023-01-01 00:00:00 UTC'); -- No match in cds_ta_barrier_kind

    INSERT INTO `my-project.oracle_raw.cds_ta_barrier_kind` (barrier_kind_id, cds_description_id) VALUES
    ('BK1', 'CD1'),
    ('BK2', 'CD_NO_MATCH'); -- No match in cds_ta_care_description

    INSERT INTO `my-project.oracle_raw.cds_ta_care_description` (cds_description_id, cds_description) VALUES
    ('CD1', 'Sperrart A');

    -- Action:
    CALL `my-project.data_warehouse.r_ausd_vertrag_control`('TEST_JOB_JOINS', '7');

    -- Pass/Fail:
    SELECT cntrct_id, sperrart FROM `my-project.data_warehouse.sof_ta_barrier` ORDER BY cntrct_id;
    -- Expected:
    -- C_FULL_MATCH: 'Sperrart A'
    -- C_LEFT_JOIN_NULL: NULL
    -- C_INNER_JOIN_FAIL: (should not appear)
    ```

#### Test Case 2.8: Filtering Conditions (`valid_from` and `is_production`)

*   **Purpose:** To verify that rows are correctly filtered based on `b.valid_from >= PARSE_DATE('%Y%m%d', v_datum)` and `b.is_production = 1`.
*   **Setup:**
    1.  Clear `my-project.oracle_raw.cds_ta_barrier`.
    2.  Set `v_datum` to a specific date (e.g., '20230101') by inserting into `dwtk_meldungen`.
    3.  Insert test data into `my-project.oracle_raw.cds_ta_barrier` covering:
        *   `valid_from` >= `v_datum` AND `is_production = 1` (should be included).
        *   `valid_from` < `v_datum` AND `is_production = 1` (should be excluded).
        *   `valid_from` >= `v_datum` AND `is_production = 0` (should be excluded).
        *   `valid_from` >= `v_datum` AND `is_production = NULL` (should be excluded, as `NULL = 1` is false).
        *   `valid_from` is `NULL` AND `is_production = 1` (should be excluded, as `NULL >= date` is false).
    4.  Ensure other source tables are populated to allow joins.
*   **Action:**
    1.  Execute the BigQuery Stored Procedure.
    2.  Query `my-project.data_warehouse.sof_ta_barrier` for `cntrct_id`.
*   **Pass/Fail Criterion:** Only rows satisfying both `valid_from >= v_datum` and `is_production = 1` are present in the target table.

    ```sql
    -- Example Setup (simplified)
    TRUNCATE TABLE `my-project.oracle_raw.dwtk_meldungen`;
    INSERT INTO `my-project.oracle_raw.dwtk_meldungen` (job_kennung, timecreated) VALUES ('BERT_DROP_TEMP_TABLE', '2023-01-01 00:00:00 UTC'); -- Sets v_datum to '20230101'

    TRUNCATE TABLE `my-project.oracle_raw.cds_ta_barrier`;
    INSERT INTO `my-project.oracle_raw.cds_ta_barrier` (cntrct_id, barrier_class_id, is_production, valid_from, insert_at) VALUES
    ('C_INCL_1', 'BC1', 1, '2023-01-01', '2023-01-01 00:00:00 UTC'), -- Included
    ('C_INCL_2', 'BC2', 1, '2023-01-02', '2023-01-01 00:00:00 UTC'), -- Included
    ('C_EXCL_DATE', 'BC3', 1, '2022-12-31', '2023-01-01 00:00:00 UTC'), -- Excluded (date)
    ('C_EXCL_PROD', 'BC4', 0, '2023-01-01', '2023-01-01 00:00:00 UTC'), -- Excluded (is_production)
    ('C_EXCL_PROD_NULL', 'BC5', NULL, '2023-01-01', '2023-01-01 00:00:00 UTC'), -- Excluded (is_production NULL)
    ('C_EXCL_DATE_NULL', 'BC6', 1, NULL, '2023-01-01 00:00:00 UTC'); -- Excluded (valid_from NULL)

    -- Action:
    CALL `my-project.data_warehouse.r_ausd_vertrag_control`('TEST_JOB_FILTERS', '8');

    -- Pass/Fail:
    SELECT cntrct_id FROM `my-project.data_warehouse.sof_ta_barrier` ORDER BY cntrct_id;
    -- Expected:
    -- C_INCL_1
    -- C_INCL_2
    ```

---

### 3. External-System Replacements Tests

#### Test Case 3.1: Job Control - Active Job Skipping

*   **Purpose:** To verify that the BigQuery Stored Procedure correctly identifies and skips execution if another instance with the same `job_name` and `p_job_kennung` is already marked as 'RUNNING'.
*   **Setup:**
    1.  Clear `my-project.data_warehouse.job_control_log`.
    2.  Manually insert a 'RUNNING' entry into `job_control_log` for `job_name = 'r_ausd_vertrag_control'` and `job_kennung = 'ACTIVE_JOB_TEST'`, with a `start_time` in the past.
    3.  Ensure `my-project.data_warehouse.sof_ta_barrier` is empty.
*   **Action:**
    1.  Execute the BigQuery Stored Procedure with the same `job_kennung`:
        ```sql
        CALL `my-project.data_warehouse.r_ausd_vertrag_control`('ACTIVE_JOB_TEST', '9');
        ```
    2.  Query `job_control_log` for entries related to `ACTIVE_JOB_TEST`.
    3.  Query `sof_ta_barrier` for any inserted data.
*   **Pass/Fail Criterion:**
    *   A new entry exists in `job_control_log` for the second call, with `status = 'SKIPPED'` and a message indicating it was skipped.
    *   The `sof_ta_barrier` table remains empty (no data was inserted by the skipped run).
    *   The original 'RUNNING' entry remains unchanged (or is eventually updated by its own completion, but not by the skipped job).

    ```sql
    -- Setup:
    TRUNCATE TABLE `my-project.data_warehouse.job_control_log`;
    INSERT INTO `my-project.data_warehouse.job_control_log` (job_name, job_kennung, entry_nr, start_time, status, message) VALUES
    ('r_ausd_vertrag_control', 'ACTIVE_JOB_TEST', '999', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE), 'RUNNING', 'First instance running.');

    TRUNCATE TABLE `my-project.data_warehouse.sof_ta_barrier`;

    -- Action:
    CALL `my-project.data_warehouse.r_ausd_vertrag_control`('ACTIVE_JOB_TEST', '9');

    -- Pass/Fail:
    SELECT job_name, job_kennung, status, message, records_processed FROM `my-project.data_warehouse.job_control_log` WHERE job_kennung = 'ACTIVE_JOB_TEST' ORDER BY start_time DESC;
    -- Expected: Two rows. The latest one should have status 'SKIPPED'.
    -- Example:
    -- job_name                 | job_kennung       | status    | message                                                              | records_processed
    -- -------------------------|-------------------|-----------|----------------------------------------------------------------------|------------------
    -- r_ausd_vertrag_control   | ACTIVE_JOB_TEST   | SKIPPED   | Skipping execution, another instance of this job is already running. | NULL
    -- r_ausd_vertrag_control   | ACTIVE_JOB_TEST   | RUNNING   | First instance running.                                              | NULL

    SELECT COUNT(*) FROM `my-project.data_warehouse.sof_ta_barrier`;
    -- Expected: 0
    ```

#### Test Case 3.2: Job Control - Successful Execution Logging

*   **Purpose:** To verify that `job_control_log` is updated correctly with 'SUCCESS' status, `start_time`, `end_time`, and `records_processed` upon successful completion.
*   **Setup:**
    1.  Clear `my-project.data_warehouse.job_control_log`.
    2.  Populate `my-project.oracle_raw` source tables with data that will result in a known number of rows (e.g., 10 rows) being inserted.
    3.  Clear `my-project.data_warehouse.sof_ta_barrier`.
*   **Action:**
    1.  Execute the BigQuery Stored Procedure:
        ```sql
        CALL `my-project.data_warehouse.r_ausd_vertrag_control`('SUCCESS_LOG_TEST', '10');
        ```
    2.  Query `job_control_log` for the `SUCCESS_LOG_TEST` entry.
*   **Pass/Fail Criterion:**
    *   A single entry exists for `SUCCESS_LOG_TEST` with `status = 'SUCCESS'`.
    *   `start_time` and `end_time` are populated, and `end_time` is after `start_time`.
    *   `records_processed` matches the actual number of rows inserted into `sof_ta_barrier` (e.g., 10).
    *   The `message` field indicates successful completion.

    ```sql
    -- Action:
    CALL `my-project.data_warehouse.r_ausd_vertrag_control`('SUCCESS_LOG_TEST', '10');

    -- Pass/Fail:
    SELECT job_name, job_kennung, status, start_time, end_time, records_processed, message
    FROM `my-project.data_warehouse.job_control_log`
    WHERE job_kennung = 'SUCCESS_LOG_TEST';
    -- Expected:
    -- job_name                 | job_kennung       | status    | start_time                | end_time                  | records_processed | message
    -- -------------------------|-------------------|-----------|---------------------------|---------------------------|-------------------|-----------------------------------------------------
    -- r_ausd_vertrag_control   | SUCCESS_LOG_TEST  | SUCCESS   | (timestamp)               | (timestamp)               | 10                | Job completed successfully. Records processed: 10
    ```

#### Test Case 3.3: Job Control - Failed Execution Logging and Rollback

*   **Purpose:** To verify that `job_control_log` is updated correctly with 'FAILED' status and an error message upon failure, and that the transaction is rolled back, leaving the target table in its pre-execution state.
*   **Setup:**
    1.  Clear `my-project.data_warehouse.job_control_log`.
    2.  Populate `my-project.data_warehouse.sof_ta_barrier` with 5 dummy rows.
    3.  Modify the BigQuery Stored Procedure *temporarily* to introduce a deliberate error (e.g., attempt to insert a string into a DATE column, or reference a non-existent table in the `INSERT` statement).
    4.  Populate `my-project.oracle_raw` source tables with data that would result in 10 new rows being inserted.
*   **Action:**
    1.  Execute the BigQuery Stored Procedure (which will now fail):
        ```sql
        CALL `my-project.data_warehouse.r_ausd_vertrag_control`('FAILED_LOG_TEST', '11');
        ```
    2.  Query `job_control_log` for the `FAILED_LOG_TEST` entry.
    3.  Query `sof_ta_barrier` for its row count.
*   **Pass/Fail Criterion:**
    *   A single entry exists for `FAILED_LOG_TEST` with `status = 'FAILED'`.
    *   `start_time` and `end_time` are populated.
    *   The `message` field contains an error description.
    *   `records_processed` is `NULL` or `0` (as the transaction should have rolled back).
    *   The `sof_ta_barrier` table still contains the original 5 dummy rows (it was not truncated, or the truncation was rolled back).

    ```sql
    -- Setup: (Example of introducing an error in the SP for testing)
    -- Temporarily modify the SP to cause an error, e.g., change a column type in the INSERT SELECT:
    -- INSERT INTO `my-project.data_warehouse.sof_ta_barrier` (..., sperr_beginn, ...)
    -- SELECT ..., 'INVALID_DATE_STRING', ... FROM ...
    -- (Remember to revert this change after the test)

    TRUNCATE TABLE `my-project.data_warehouse.job_control_log`;
    INSERT INTO `my-project.data_warehouse.sof_ta_barrier` (cntrct_id, barrier_kind_id, sperrart) VALUES
    ('DUMMY1', 'BK1', 'Art1'), ('DUMMY2', 'BK2', 'Art2'), ('DUMMY3', 'BK3', 'Art3'), ('DUMMY4', 'BK4', 'Art4'), ('DUMMY5', 'BK5', 'Art5');

    -- Action:
    CALL `my-project.data_warehouse.r_ausd_vertrag_control`('FAILED_LOG_TEST', '11');

    -- Pass/Fail:
    SELECT job_name, job_kennung, status, start_time, end_time, records_processed, message
    FROM `my-project.data_warehouse.job_control_log`
    WHERE job_kennung = 'FAILED_LOG_TEST';
    -- Expected: status 'FAILED', message with error details, records_processed = 0 or NULL.

    SELECT COUNT(*) FROM `my-project.data_warehouse.sof_ta_barrier`;
    -- Expected: 5 (original dummy rows remain due to rollback)
    ```

---

### 4. Data Quality / Row Count / Schema Assertions

#### Test Case 4.1: Row Count Parity

*   **Purpose:** To ensure the total number of rows processed and inserted into `sof_ta_barrier` matches the legacy job's output.
*   **Setup:**
    1.  Ensure `my-project.oracle_raw` tables are populated with a known dataset.
    2.  Ensure the legacy Oracle source tables contain *exactly* the same data.
    3.  Clear target tables in both environments.
*   **Action:**
    1.  Execute the legacy KornShell script and record the final row count reported by the script or by querying `sof$ta_barrier`.
    2.  Execute the BigQuery Stored Procedure.
    3.  Query `my-project.data_warehouse.sof_ta_barrier` for its row count, and check `records_processed` in `job_control_log`.
*   **Pass/Fail Criterion:**
    *   The row count from the legacy Oracle job matches `records_processed` in the BigQuery `job_control_log`.
    *   The row count from the legacy Oracle job matches `COUNT(*)` from `my-project.data_warehouse.sof_ta_barrier`.

    ```sql
    -- After running both legacy and migrated jobs:
    SELECT records_processed FROM `my-project.data_warehouse.job_control_log`
    WHERE job_name = 'r_ausd_vertrag_control' AND status = 'SUCCESS'
    ORDER BY start_time DESC LIMIT 1;
    -- Expected: (e.g., 12345)

    SELECT COUNT(*) FROM `my-project.data_warehouse.sof_ta_barrier`;
    -- Expected: (e.g., 12345)

    -- Compare these to the row count from the legacy Oracle job.
    ```

#### Test Case 4.2: Schema Conformance

*   **Purpose:** To verify that the `sof_ta_barrier` table schema (column names, data types, nullability) in BigQuery matches the expected DDL and is compatible with the transformed data.
*   **Setup:** N/A (Schema is defined by DDL).
*   **Action:** Query BigQuery's `INFORMATION_SCHEMA.COLUMNS` for `sof_ta_barrier`.
*   **Pass/Fail Criterion:** The schema of `my-project.data_warehouse.sof_ta_barrier` matches the provided DDL in `data_warehouse/ddl/sof_ta_barrier.sql` for column names, data types, and nullability.

    ```sql
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM
        `my-project.data_warehouse.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = 'sof_ta_barrier'
    ORDER BY
        ordinal_position;
    -- Expected output should match the DDL:
    -- cntrct_id STRING, is_nullable=YES
    -- barrier_kind_id STRING, is_nullable=YES
    -- barrier_init_cv STRING, is_nullable=YES
    -- barrier_reason_cv STRING, is_nullable=YES
    -- bfc_age TIMESTAMP, is_nullable=YES
    -- sperrart STRING, is_nullable=YES
    -- sperr_beginn DATE, is_nullable=YES
    -- sperr_ende DATE, is_nullable=YES
    -- sperrgrund STRING, is_nullable=YES
    -- ist_stillegung BOOL, is_nullable=YES
    ```

#### Test Case 4.3: Data Type Integrity and Range Checks

*   **Purpose:** To ensure that data types are correctly handled during transformation, and that values fall within expected ranges (e.g., dates are valid, strings are not truncated).
*   **Setup:**
    1.  Populate `my-project.oracle_raw` tables with data that includes:
        *   Minimum and maximum valid dates/timestamps.
        *   Strings that are at or near the maximum expected length.
        *   Numeric values that might implicitly convert (though not many in this specific transformation).
        *   Edge cases for `GREATEST` and `COALESCE` (e.g., `NULL` values).
    2.  Execute the BigQuery Stored Procedure.
*   **Action:**
    1.  Query `my-project.data_warehouse.sof_ta_barrier` and perform checks on specific columns.
*   **Pass/Fail Criterion:**
    *   All `DATE` columns (`sperr_beginn`, `sperr_ende`) contain valid date values or `NULL`.
    *   `bfc_age` contains valid `TIMESTAMP` values or `NULL`.
    *   `ist_stillegung` contains only `TRUE` or `FALSE`.
    *   No string truncation is observed for `sperrart`, `sperrgrund`, etc. (can be checked by comparing `LENGTH()` of source vs. target for long strings).

    ```sql
    -- Example checks for data type integrity and ranges
    SELECT
        COUNTIF(sperr_beginn IS NOT NULL AND NOT SAFE.PARSE_DATE('%Y-%m-%d', CAST(sperr_beginn AS STRING))) AS invalid_sperr_beginn_dates,
        COUNTIF(sperr_ende IS NOT NULL AND NOT SAFE.PARSE_DATE('%Y-%m-%d', CAST(sperr_ende AS STRING))) AS invalid_sperr_ende_dates,
        COUNTIF(ist_stillegung IS NOT TRUE AND ist_stillegung IS NOT FALSE AND ist_stillegung IS NOT NULL) AS invalid_ist_stillegung_booleans,
        MAX(LENGTH(sperrart)) AS max_sperrart_length,
        MAX(LENGTH(sperrgrund)) AS max_sperrgrund_length
    FROM `my-project.data_warehouse.sof_ta_barrier`;

    -- Pass if:
    -- invalid_sperr_beginn_dates = 0
    -- invalid_sperr_ende_dates = 0
    -- invalid_ist_stillegung_booleans = 0
    -- max_sperrart_length and max_sperrgrund_length are within expected limits (e.g., less than 255 or 500 characters, depending on original Oracle column sizes)
    ```