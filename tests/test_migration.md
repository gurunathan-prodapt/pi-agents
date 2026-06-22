As a senior data-migration QA engineer, I've designed a suite of validation tests for the migrated `k_ausd_geschaeftspartner.ksh` job. These tests aim to ensure behavioral equivalence, data integrity, and correctness across all aspects of the migration to BigQuery.

---

## Migration Validation Tests for `k_ausd_geschaeftspartner.ksh`

### General Assumptions for All Tests:
*   **Legacy Oracle Access:** The legacy Oracle environment is accessible for extracting baseline data and verifying behavior.
*   **Staging Data Parity:** BigQuery staging tables (`your_project.your_dataset_staging.*`) are populated with data that is *identical* to the corresponding Oracle source tables for the test run. This is a critical prerequisite for accurate comparison.
*   **BigQuery Project/Dataset Names:** `your_project`, `your_dataset_procs`, `your_dataset_staging`, `your_dataset_target`, `your_dataset_logging` are placeholders and should be replaced with actual BigQuery project and dataset IDs.
*   **Test Data:** Representative test data (including edge cases like NULLs, empty strings, boundary dates) has been prepared and loaded into the staging tables.
*   **Oracle Baseline:** For output parity tests, a mechanism exists to extract data from Oracle target tables into a comparable format (e.g., CSV, or loaded into a BigQuery "Oracle_Mirror" dataset).

---

### Test Case 1: End-to-End Output Parity

*   **Purpose:** To verify that the final output tables in BigQuery are identical to those produced by the legacy Oracle job when given the same input data. This is the primary behavioral equivalence test.
*   **Setup:**
    1.  Ensure BigQuery staging tables (`bpd_ta_bp_valueseg_assoc`, `pds_ta_bpri_com`, `dwtk_meldungen`, `sof_ta_e_reach_gp`, `sof_ta_e_business_gp`, `sof_ta_e_reach_dn`, `sof_ta_e_business_dn`, `sof_ta_e_reach_ev`, `sof_ta_e_business_ev`) are populated with a representative dataset that is *identical* to the corresponding Oracle source tables.
    2.  Run the legacy `k_ausd_geschaeftspartner.ksh` job with a specific, well-defined set of parameters (e.g., `p_JobKennung='TEST_GP'`, `p_EintragsNr='T001'`, `p_Stichtag='01012023'`, `p_wiederanlaufWert=0`).
    3.  Extract the full content of the final target tables from Oracle: `sof$ta_p_geschaeftspartner`, `sof$ta_p_dienstenutzer`, `sof$ta_p_evn_empf`. Store these as baseline files or load them into a BigQuery "Oracle_Mirror" dataset (e.g., `oracle_mirror_dataset.sof_ta_p_geschaeftspartner_baseline`).
    4.  Ensure the BigQuery target tables (`sof_ta_p_gesch_part`, `sof_ta_p_dn_nutzer`, `sof_ta_p_evn_empf`) are empty before the test run.
*   **Action:**
    1.  Execute the migrated BigQuery orchestration stored procedure:
        ```sql
        CALL your_project.your_dataset_procs.k_ausd_geschaeftspartner_main(
          'TEST_GP', 'T001', '01012023', 0
        );
        ```
    2.  Extract the full content of the BigQuery target tables: `sof_ta_p_gesch_part`, `sof_ta_p_dn_nutzer`, `sof_ta_p_evn_empf`.
*   **Pass/Fail Criterion:**
    *   The row counts for each target table in BigQuery must exactly match the row counts from the corresponding Oracle baseline tables.
    *   A deep comparison (e.g., using `EXCEPT DISTINCT` in SQL) of the content of each BigQuery target table against its Oracle baseline must yield zero differences. This confirms exact data parity, including column order, data types, and values.

    ```sql
    -- Example SQL Assertion for sof_ta_p_gesch_part
    -- Check row count parity
    SELECT
        (SELECT COUNT(*) FROM `your_project.your_dataset_target.sof_ta_p_gesch_part`) AS bq_row_count,
        (SELECT COUNT(*) FROM `oracle_mirror_dataset.sof_ta_p_geschaeftspartner_baseline`) AS oracle_row_count
    HAVING bq_row_count = oracle_row_count;

    -- Check data parity (no extra rows in BQ)
    SELECT COUNT(*) FROM (
        SELECT * FROM `your_project.your_dataset_target.sof_ta_p_gesch_part`
        EXCEPT DISTINCT
        SELECT * FROM `oracle_mirror_dataset.sof_ta_p_geschaeftspartner_baseline`
    )
    HAVING COUNT(*) = 0;

    -- Check data parity (no missing rows in BQ)
    SELECT COUNT(*) FROM (
        SELECT * FROM `oracle_mirror_dataset.sof_ta_p_geschaeftspartner_baseline`
        EXCEPT DISTINCT
        SELECT * FROM `your_project.your_dataset_target.sof_ta_p_gesch_part`
    )
    HAVING COUNT(*) = 0;

    -- Repeat similar queries for sof_ta_p_dn_nutzer and sof_ta_p_evn_empf.
    ```

---

### Test Case 2: Parameter Validation and Error Handling

*   **Purpose:** To verify that the `k_ausd_geschaeftspartner_main` stored procedure correctly validates input parameters and logs errors as specified in the design.
*   **Setup:**
    1.  Ensure the `job_log` table (`your_project.your_dataset_logging.job_log`) is empty before each sub-test.
*   **Action:**
    1.  Attempt to call `k_ausd_geschaeftspartner_main` with a missing `p_JobKennung`:
        ```sql
        CALL your_project.your_dataset_procs.k_ausd_geschaeftspartner_main(
          NULL, 'T001', '01012023', 0
        );
        ```
    2.  Attempt to call `k_ausd_geschaeftspartner_main` with an invalid `p_Stichtag` format:
        ```sql
        CALL your_project.your_dataset_procs.k_ausd_geschaeftspartner_main(
          'TEST_GP', 'T001', '2023-01-01', 0
        );
        ```
    3.  Attempt to call `k_ausd_geschaeftspartner_main` with a valid `p_Stichtag` but `p_wiederanlaufWert` missing (should default to 0):
        ```sql
        CALL your_project.your_dataset_procs.k_ausd_geschaeftspartner_main(
          'TEST_GP', 'T001', '01012023', NULL
        );
        ```
*   **Pass/Fail Criterion:**
    *   For actions 1 and 2, the `CALL` statement must fail with an `ASSERT` error message matching the expected error (e.g., "JobKennung parameter is missing." or "Stichtag has invalid date format.").
    *   For actions 1 and 2, an `ERROR` level entry must be recorded in `job_log` containing the error message.
    *   For action 3, the `CALL` must succeed, and the `job_log` entry for parameters (level `DEBUG`) must show `p_wiederanlaufWert` as `0`.

    ```sql
    -- After attempting call with missing p_JobKennung (Action 1)
    SELECT message FROM `your_project.your_dataset_logging.job_log`
    WHERE job_name = 'k_ausd_geschaeftspartner' AND level = 'ERROR'
    ORDER BY timestamp DESC LIMIT 1
    HAVING message LIKE '%JobKennung parameter is missing%';

    -- After attempting call with invalid p_Stichtag (Action 2)
    SELECT message FROM `your_project.your_dataset_logging.job_log`
    WHERE job_name = 'k_ausd_geschaeftspartner' AND level = 'ERROR'
    ORDER BY timestamp DESC LIMIT 1
    HAVING message LIKE '%Stichtag has invalid date format%';

    -- After successful call with NULL p_wiederanlaufWert (Action 3)
    SELECT message FROM `your_project.your_dataset_logging.job_log`
    WHERE job_name = 'k_ausd_geschaeftspartner' AND level = 'DEBUG'
    ORDER BY timestamp DESC LIMIT 1
    HAVING message LIKE '%WiederanlaufWert: 0%';
    ```

---

### Test Case 3: Date Derivation and `dwtk_meldungen` Logic

*   **Purpose:** To verify that `v_datum_heute`, `v_datum_gestern` are correctly derived using BigQuery's native date functions, and that `v_datum_str` (from `dwtk_meldungen`) is correctly calculated and used in filtering.
*   **Setup:**
    1.  Populate `your_project.your_dataset_staging.dwtk_meldungen` with specific test data, including a `timecreated` value for `job_kennung = 'BERT_DROP_TEMP_TABLE'`. For example:
        ```sql
        INSERT INTO `your_project.your_dataset_staging.dwtk_meldungen` (job_kennung, timecreated)
        VALUES ('BERT_DROP_TEMP_TABLE', '2023-01-15 10:00:00 UTC');
        ```
    2.  Ensure target tables are empty.
*   **Action:**
    1.  Call `k_ausd_geschaeftspartner_main` with valid parameters:
        ```sql
        CALL your_project.your_dataset_procs.k_ausd_geschaeftspartner_main(
          'TEST_DATE', 'T002', '15012023', 0
        );
        ```
*   **Pass/Fail Criterion:**
    *   The `job_log` table must contain a `DEBUG` entry showing the correctly derived `v_datum_heute` (current date of execution), `v_datum_gestern` (previous day), and `v_stichtag_date`.
    *   The `d_ausd_geschaeftspartner_proc` must correctly use the `v_datum_str` derived from `dwtk_meldungen` in its `WHERE` clauses (e.g., `bp.insert_at <= PARSE_DATE('%Y%m%d', v_datum_str)`). This can be verified by inspecting the data in `sof_ta_bpr_dn_evn_his` for records that should be filtered in/out based on this date.

    ```sql
    -- Check derived dates in log
    SELECT message FROM `your_project.your_dataset_logging.job_log`
    WHERE job_name = 'k_ausd_geschaeftspartner' AND level = 'DEBUG'
    ORDER BY timestamp DESC LIMIT 1
    HAVING
        message LIKE CONCAT('%Heute: ', FORMAT_DATE('%Y-%m-%d', CURRENT_DATE()), '%') AND
        message LIKE CONCAT('%Gestern: ', FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)), '%') AND
        message LIKE '%Stichtag_DATE: 2023-01-15%';

    -- Verify filtering based on v_datum_str in sof_ta_bpr_dn_evn_his
    -- This assumes '20230115' was derived from dwtk_meldungen.
    -- Insert test data into pds_ta_bpri_com where some records should be filtered out by this date.
    SELECT COUNT(*) FROM `your_project.your_dataset_target.sof_ta_bpr_dn_evn_his`
    WHERE insert_at > '2023-01-15'; -- Should be 0 if the filter worked correctly
    ```

---

### Test Case 4: Truncation of Target Tables

*   **Purpose:** To verify that the `d_ausd_geschaeftspartner_proc` correctly truncates all specified target tables at the beginning of its execution, ensuring idempotency and a clean state for each run.
*   **Setup:**
    1.  Populate all target tables (`sof_ta_segm_prem`, `sof_ta_bpr_dn_evn`, `sof_ta_bpr_dn_evn_his`, `sof_ta_p_gesch_part`, `sof_ta_p_dn_nutzer`, `sof_ta_p_evn_empf`) with some dummy data.
    2.  Populate staging tables with minimal data to allow the job to complete without errors.
*   **Action:**
    1.  Call `k_ausd_geschaeftspartner_main` with valid parameters:
        ```sql
        CALL your_project.your_dataset_procs.k_ausd_geschaeftspartner_main(
          'TEST_TRUNC', 'T003', '01012023', 0
        );
        ```
*   **Pass/Fail Criterion:**
    *   After the procedure completes, the final row counts in all specified target tables must reflect only the data inserted by the current run, implying that previous data was truncated. If the job inserts 0 rows, the tables should be empty.

    ```sql
    -- This test is best verified by checking the final state.
    -- If Test Case 1 (Output Parity) passes, it implicitly confirms truncation.
    -- To explicitly test truncation in isolation, one would need to call
    -- d_ausd_geschaeftspartner_proc and check table counts *after* the truncation block,
    -- but *before* any inserts. This typically requires modifying the SP for testing or
    -- using a debugger. For an end-to-end test, the following is sufficient:
    SELECT
        (SELECT COUNT(*) FROM `your_project.your_dataset_target.sof_ta_segm_prem`) AS segm_prem_count,
        (SELECT COUNT(*) FROM `your_project.your_dataset_target.sof_ta_bpr_dn_evn`) AS bpr_dn_evn_count,
        (SELECT COUNT(*) FROM `your_project.your_dataset_target.sof_ta_bpr_dn_evn_his`) AS bpr_dn_evn_his_count,
        (SELECT COUNT(*) FROM `your_project.your_dataset_target.sof_ta_p_gesch_part`) AS p_gesch_part_count,
        (SELECT COUNT(*) FROM `your_project.your_dataset_target.sof_ta_p_dn_nutzer`) AS p_dn_nutzer_count,
        (SELECT COUNT(*) FROM `your_project.your_dataset_target.sof_ta_p_evn_empf`) AS p_evn_empf_count
    HAVING
        segm_prem_count = <expected_count_for_this_run> AND
        bpr_dn_evn_count = <expected_count_for_this_run> AND
        bpr_dn_evn_his_count = <expected_count_for_this_run> AND
        p_gesch_part_count = <expected_count_for_this_run> AND
        p_dn_nutzer_count = <expected_count_for_this_run> AND
        p_evn_empf_count = <expected_count_for_this_run>;
    ```

---

### Test Case 5: Transformation Correctness - `sof_ta_p_gesch_part` (Step 04)

*   **Purpose:** To validate the complex transformation logic for `sof_ta_p_gesch_part`, including `COALESCE`, `CASE` statements, `CONCAT`, and join conditions, specifically focusing on `FIRMENNAME`, `AKAD_TITEL`, `STRASSE`, and `KUNDE_SEGMENT_ID`.
*   **Setup:**
    1.  Populate `sof_ta_e_reach_gp`, `sof_ta_e_business_gp`, and `sof_ta_segm_prem` staging tables with specific test data covering various scenarios:
        *   `rg.corp_unit` vs `bp.organisation_name` for `FIRMENNAME` (COALESCE, including both NULL, one NULL, both non-NULL).
        *   `rg.surname_s` vs `bp.title` for `AKAD_TITEL` (CASE, including `rg.surname_s` being NULL).
        *   `rg.street` vs `rg.pobox` for `STRASSE` (nested CASE/CONCAT, including `street` NULL, `pobox` NULL, both NULL, both non-NULL).
        *   Different `pr.segment_id` values (11, 12, 13, 14, 15, 16, other, NULL) for `KUNDE_SEGMENT_ID` (CASE).
        *   Records with matching `rg.bp_id = pr.bp_id` (LEFT JOIN) and records with no match in `pr` to test `LEFT JOIN` behavior.
    2.  Ensure `sof_ta_p_gesch_part` is empty.
*   **Action:**
    1.  Call `k_ausd_geschaeftspartner_main` with valid parameters:
        ```sql
        CALL your_project.your_dataset_procs.k_ausd_geschaeftspartner_main(
          'TEST_GP_TRANS', 'T004', '01012023', 0
        );
        ```
*   **Pass/Fail Criterion:**
    *   Query `sof_ta_p_gesch_part` and verify that the transformed columns (`FIRMENNAME`, `AKAD_TITEL`, `NACHNAME`, `VORNAME`, `STRASSE`, `KUNDE_SEGMENT_ID`) match the expected values based on the input test data and the specified logic.

    ```sql
    -- Example: Test FIRMENNAME COALESCE logic
    SELECT COUNT(*)
    FROM `your_project.your_dataset_target.sof_ta_p_gesch_part` t
    JOIN `your_project.your_dataset_staging.sof_ta_e_reach_gp` rg ON t.CNTRCT_ID = rg.cntrct_cp2_id
    JOIN `your_project.your_dataset_staging.sof_ta_e_business_gp` bp ON rg.bp_id = bp.bp_id
    WHERE t.FIRMENNAME != COALESCE(rg.corp_unit, bp.organisation_name);
    -- This query should return 0 rows for a pass.

    -- Example: Test STRASSE logic
    SELECT COUNT(*)
    FROM `your_project.your_dataset_target.sof_ta_p_gesch_part` t
    JOIN `your_project.your_dataset_staging.sof_ta_e_reach_gp` rg ON t.CNTRCT_ID = rg.cntrct_cp2_id
    WHERE t.STRASSE !=
        CASE
            WHEN rg.street IS NULL THEN
                CASE
                    WHEN rg.pobox IS NULL THEN ''
                    ELSE CONCAT('Postfach ', rg.pobox)
                END
            ELSE CONCAT(rg.street, ' ', rg.house_nr)
        END;
    -- This query should return 0 rows for a pass.

    -- Example: Test KUNDE_SEGMENT_ID CASE logic
    SELECT COUNT(*)
    FROM `your_project.your_dataset_target.sof_ta_p_gesch_part` t
    LEFT JOIN `your_project.your_dataset_staging.sof_ta_e_reach_gp` rg ON t.CNTRCT_ID = rg.cntrct_cp2_id
    LEFT JOIN `your_project.your_dataset_target.sof_ta_segm_prem` pr ON rg.bp_id = pr.BP_ID
    WHERE t.KUNDE_SEGMENT_ID !=
        CASE pr.segment_id
            WHEN 11 THEN 'SP'
            WHEN 12 THEN 'RV'
            WHEN 13 THEN 'MA'
            WHEN 14 THEN 'SO'
            WHEN 15 THEN 'VJ'
            WHEN 16 THEN 'IN'
            ELSE CAST(pr.segment_id AS STRING)
        END;
    -- This query should return 0 rows for a pass.
    ```

---

### Test Case 6: Transformation Correctness - `sof_ta_bpr_dn_evn_his` and `sof_ta_bpr_dn_evn` (Step 05)

*   **Purpose:** To validate the logic for populating these intermediate tables, specifically the date filtering, `bpr_id` filtering, and the window function for `sof_ta_bpr_dn_evn`.
*   **Setup:**
    1.  Populate `pds_ta_bpri_com` staging table with diverse test data:
        *   Records with `bpr_id` in the specified list (31, 2759, etc.) and outside.
        *   Records with `insert_at` before, on, and after a chosen `v_datum_str`.
        *   Records with `modified_at` NULL, before, and after a chosen `v_datum_str`.
        *   Records with `valid_from` before, on, and after a chosen `v_datum_str`.
        *   Records with `is_production` = 0 and 1.
        *   Records with varying `valid_to` values for the same `cntrct_id`, `bpr_id` to thoroughly test the `MAX(COALESCE(valid_to, '47121231')) OVER (PARTITION BY ...)` window function.
    2.  Populate `dwtk_meldungen` to ensure a specific `v_datum_str` is derived (e.g., '20230115').
    3.  Ensure `sof_ta_bpr_dn_evn_his` and `sof_ta_bpr_dn_evn` are empty.
*   **Action:**
    1.  Call `k_ausd_geschaeftspartner_main` with valid parameters:
        ```sql
        CALL your_project.your_dataset_procs.k_ausd_geschaeftspartner_main(
          'TEST_BPR_TRANS', 'T005', '01012023', 0
        );
        ```
*   **Pass/Fail Criterion:**
    *   Query `sof_ta_bpr_dn_evn_his` and verify that only records matching all `WHERE` clause conditions (bpr_id, insert_at, modified_at, valid_from, is_production) are present.
    *   Query `sof_ta_bpr_dn_evn` and verify that for each `(cntrct_id, bpr_id)` group, only the record with the maximum `COALESCE(valid_to, '47121231')` is selected. Also, verify `COLUMN_5VALID_TO` correctly applies `COALESCE`.

    ```sql
    -- Verify filtering in sof_ta_bpr_dn_evn_his (assuming v_datum_str was '20230115')
    SELECT COUNT(*) FROM `your_project.your_dataset_target.sof_ta_bpr_dn_evn_his`
    WHERE
        bpr_id NOT IN (31, 2759, 2800, 2835, 2836, 2837, 2839, 2840, 3056) OR
        insert_at > '2023-01-15' OR
        (modified_at IS NOT NULL AND modified_at <= '2023-01-15') OR
        valid_from > '2023-01-15' OR
        is_production = 0;
    -- This query should return 0 rows for a pass.

    -- Verify window function and COALESCE in sof_ta_bpr_dn_evn
    WITH ExpectedMaxValidTo AS (
        SELECT
            bp1.cntrct_id,
            bp1.bpr_id,
            MAX(COALESCE(bp1.valid_to, PARSE_DATE('%Y%m%d', '47121231'))) AS expected_max_valid_to
        FROM `your_project.your_dataset_target.sof_ta_bpr_dn_evn_his` AS bp1
        GROUP BY bp1.cntrct_id, bp1.bpr_id
    )
    SELECT COUNT(*)
    FROM `your_project.your_dataset_target.sof_ta_bpr_dn_evn` AS actual
    JOIN ExpectedMaxValidTo AS expected
        ON actual.cntrct_id = expected.cntrct_id
        AND actual.bpr_id = expected.bpr_id
    WHERE
        COALESCE(actual.COLUMN_5VALID_TO, PARSE_DATE('%Y%m%d', '47121231')) != expected.expected_max_valid_to;
    -- This query should return 0 rows for a pass.
    ```

---

### Test Case 7: External System Replacements (Staging Data Integrity)

*   **Purpose:** To verify that the data ingested into BigQuery staging tables from external Oracle sources is complete and accurate, matching the Oracle source data. This is a crucial pre-requisite for the ETL job itself.
*   **Setup:**
    1.  Identify a specific point-in-time snapshot of the Oracle source tables: `bpd$ta_bp_valueseg_assoc`, `pds$ta_bpri_com`, `isbert_schema.dwtk_meldungen`, `sof$ta_e_reach_gp`, `sof$ta_e_business_gp`, `sof$ta_e_reach_dn`, `sof$ta_e_business_dn`, `sof$ta_e_reach_ev`, `sof$ta_e_business_ev`.
    2.  Extract this data from Oracle into a format suitable for comparison (e.g., CSV, or load into a temporary BigQuery dataset as "Oracle_Mirror" tables).
*   **Action:**
    1.  Trigger the data ingestion pipeline for these tables.
    2.  Once ingestion is complete, query the BigQuery staging tables (`your_project.your_dataset_staging.*`).
*   **Pass/Fail Criterion:**
    *   For each staging table, the row count must match the corresponding Oracle source table.
    *   A deep comparison of the content of each BigQuery staging table against its Oracle baseline/mirror must yield zero differences. This includes checking data types, nullability, and exact values for all columns.

    ```sql
    -- Example for bpd_ta_bp_valueseg_assoc
    SELECT
        (SELECT COUNT(*) FROM `your_project.your_dataset_staging.bpd_ta_bp_valueseg_assoc`) AS bq_staging_count,
        (SELECT COUNT(*) FROM `oracle_mirror_dataset.bpd_ta_bp_valueseg_assoc_mirror`) AS oracle_source_count
    HAVING bq_staging_count = oracle_source_count;

    SELECT COUNT(*) FROM (
        SELECT * FROM `your_project.your_dataset_staging.bpd_ta_bp_valueseg_assoc`
        EXCEPT DISTINCT
        SELECT * FROM `oracle_mirror_dataset.bpd_ta_bp_valueseg_assoc_mirror`
    )
    HAVING COUNT(*) = 0;

    SELECT COUNT(*) FROM (
        SELECT * FROM `oracle_mirror_dataset.bpd_ta_bp_valueseg_assoc_mirror`
        EXCEPT DISTINCT
        SELECT * FROM `your_project.your_dataset_staging.bpd_ta_bp_valueseg_assoc`
    )
    HAVING COUNT(*) = 0;
    -- Repeat similar queries for all other staging tables.
    ```
    *Note: This test is typically performed by the ingestion team, but as a migration QA, it's crucial to ensure this foundation is solid.*

---

### Test Case 8: Data Quality - Null Handling and Schema Assertions

*   **Purpose:** To verify that NULL values are handled correctly (e.g., `COALESCE`, `IFNULL` translations) and that the schema of the target tables matches expectations (data types, nullability where applicable).
*   **Setup:**
    1.  Populate staging tables with data specifically designed to test NULL scenarios for columns involved in `COALESCE`, `CASE`, and `CONCAT` operations. For example, `sof_ta_e_reach_gp` with `corp_unit` as NULL, `street` as NULL, `pobox` as NULL, etc.
    2.  Ensure target tables are empty.
*   **Action:**
    1.  Call `k_ausd_geschaeftspartner_main` with valid parameters:
        ```sql
        CALL your_project.your_dataset_procs.k_ausd_geschaeftspartner_main(
          'TEST_NULLS', 'T006', '01012023', 0
        );
        ```
*   **Pass/Fail Criterion:**
    *   For columns where `COALESCE` was used (e.g., `FIRMENNAME`, `NACHNAME`, `VORNAME`, `COLUMN_5VALID_TO`), verify that the output correctly reflects the non-NULL value or the default if all inputs are NULL.
    *   For `STRASSE` column, verify that `NULL` values in `street` and `pobox` correctly result in an empty string, or `Postfach ` + `pobox` if `street` is NULL and `pobox` is not.
    *   Verify that the data types of the columns in the BigQuery target tables match the expected types (e.g., `DATE` for dates, `STRING` for text, `INT64` for integers).

    ```sql
    -- Check FIRMENNAME COALESCE (similar to Test Case 5, but focused on NULLs)
    SELECT COUNT(*)
    FROM `your_project.your_dataset_target.sof_ta_p_gesch_part` t
    JOIN `your_project.your_dataset_staging.sof_ta_e_reach_gp` rg ON t.CNTRCT_ID = rg.cntrct_cp2_id
    JOIN `your_project.your_dataset_staging.sof_ta_e_business_gp` bp ON rg.bp_id = bp.bp_id
    WHERE t.FIRMENNAME != COALESCE(rg.corp_unit, bp.organisation_name);
    -- This query should return 0 rows for a pass.

    -- Check COLUMN_5VALID_TO COALESCE (should never be NULL)
    SELECT COUNT(*)
    FROM `your_project.your_dataset_target.sof_ta_bpr_dn_evn`
    WHERE COLUMN_5VALID_TO IS NULL;
    -- This query should return 0 rows for a pass.

    -- Schema assertion (using BigQuery INFORMATION_SCHEMA)
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM `your_project.your_dataset_target.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'sof_ta_p_gesch_part'
    ORDER BY ordinal_position;
    -- Manually compare this output with the expected schema, ensuring data types and nullability match Oracle's behavior or the migration design's explicit changes.
    ```

---

### Test Case 9: Record Count Parity (Intermediate and Final)

*   **Purpose:** To verify that the number of records inserted into each target table (intermediate and final) matches the legacy job's behavior. Also, verify the `v_records_processed` output parameter.
*   **Setup:**
    1.  Ensure BigQuery staging tables are populated with a representative dataset.
    2.  Run the legacy job and capture the record counts for each `INSERT` statement into `sof$ta_segm_prem`, `sof$ta_bpr_dn_evn_his`, `sof$ta_bpr_dn_evn`, `sof$ta_p_geschaeftspartner`, `sof$ta_p_dienstenutzer`, `sof$ta_p_evn_empf`. Also capture the final total record count reported by the legacy job.
    3.  Ensure BigQuery target tables are empty.
*   **Action:**
    1.  Call `k_ausd_geschaeftspartner_main` with valid parameters:
        ```sql
        CALL your_project.your_dataset_procs.k_ausd_geschaeftspartner_main(
          'TEST_COUNTS', 'T007', '01012023', 0
        );
        ```
*   **Pass/Fail Criterion:**
    *   The final `v_records_processed` value returned by `k_ausd_geschaeftspartner_main` (and logged in `job_log`) must match the total record count reported by the legacy job.
    *   The row count for each individual target table in BigQuery (`sof_ta_segm_prem`, `sof_ta_bpr_dn_evn_his`, `sof_ta_bpr_dn_evn`, `sof_ta_p_gesch_part`, `sof_ta_p_dn_nutzer`, `sof_ta_p_evn_empf`) must exactly match the corresponding record counts from the legacy Oracle job for that specific step.

    ```sql
    -- Check final record count in log
    SELECT records_processed FROM `your_project.your_dataset_logging.job_log`
    WHERE job_name = 'k_ausd_geschaeftspartner' AND level = 'INFO' AND message LIKE 'Job completed successfully.'
    ORDER BY timestamp DESC LIMIT 1
    HAVING records_processed = <expected_total_records_from_oracle>;

    -- Check individual table row counts (example for one table)
    SELECT
        (SELECT COUNT(*) FROM `your_project.your_dataset_target.sof_ta_segm_prem`) AS bq_segm_prem_count
    HAVING bq_segm_prem_count = <expected_segm_prem_count_from_oracle>;

    -- Repeat for all 6 target tables.
    ```

---

### Test Case 10: Idempotency (Restartability)

*   **Purpose:** To verify that running the job multiple times with the same inputs produces the same final output, and that the `TRUNCATE` statements ensure a clean state for each run.
*   **Setup:**
    1.  Populate BigQuery staging tables with a representative dataset.
    2.  Ensure target tables are empty.
*   **Action:**
    1.  Call `k_ausd_geschaeftspartner_main` with valid parameters (Run 1):
        ```sql
        CALL your_project.your_dataset_procs.k_ausd_geschaeftspartner_main(
          'TEST_IDEMPOTENT', 'T008', '01012023', 0
        );
        ```
    2.  Capture the content of the final target tables (e.g., by copying them to a temporary dataset: `CREATE TABLE your_project.temp_test.sof_ta_p_gesch_part_run1 AS SELECT * FROM your_project.your_dataset_target.sof_ta_p_gesch_part;`).
    3.  Call `k_ausd_geschaeftspartner_main` again with the *exact same* parameters (Run 2):
        ```sql
        CALL your_project.your_dataset_procs.k_ausd_geschaeftspartner_main(
          'TEST_IDEMPOTENT', 'T008', '01012023', 0
        );
        ```
*   **Pass/Fail Criterion:**
    *   The content of the target tables after Run 1 must be identical to the content after Run 2. This confirms that the `TRUNCATE` statements correctly clear previous data and the subsequent `INSERT`s are deterministic.

    ```sql
    -- Example for sof_ta_p_gesch_part
    SELECT COUNT(*) FROM (
        SELECT * FROM `your_project.your_dataset_target.sof_ta_p_gesch_part` -- After Run 2
        EXCEPT DISTINCT
        SELECT * FROM `your_project.temp_test.sof_ta_p_gesch_part_run1`
    )
    HAVING COUNT(*) = 0;

    SELECT COUNT(*) FROM (
        SELECT * FROM `your_project.temp_test.sof_ta_p_gesch_part_run1`
        EXCEPT DISTINCT
        SELECT * FROM `your_project.your_dataset_target.sof_ta_p_gesch_part` -- After Run 2
    )
    HAVING COUNT(*) = 0;
    -- Repeat for all 3 final target tables.
    ```

---

### Test Case 11: Airflow DAG Execution

*   **Purpose:** To verify that the Cloud Composer (Airflow) DAG correctly triggers the BigQuery stored procedure and passes parameters as expected, ensuring the orchestration layer functions as designed.
*   **Setup:**
    1.  Deploy the `k_ausd_geschaeftspartner_dag.py` to Cloud Composer.
    2.  Ensure BigQuery staging tables are populated with representative data.
    3.  Ensure target tables are empty.
    4.  Configure Airflow variables or connections as needed (e.g., `google_cloud_default` connection).
*   **Action:**
    1.  Manually trigger the `k_ausd_geschaeftspartner_dag` in Airflow.
    2.  Observe the Airflow task logs in the Cloud Composer UI.
    3.  Query the BigQuery `job_log` table.
*   **Pass/Fail Criterion:**
    *   The Airflow task `execute_k_ausd_geschaeftspartner_main` must complete successfully (green status in Airflow UI).
    *   A `DEBUG` entry in `your_project.your_dataset_logging.job_log` must show the parameters passed from Airflow, including the correctly formatted `p_Stichtag` (e.g., `01012023` if `ds_nodash` was `20230101`).
    *   A `INFO` entry in `job_log` must indicate "Job completed successfully." with a non-NULL `records_processed` count.
    *   The target tables in BigQuery must be populated with data, matching the expected output for the given input date.

    ```sql
    -- Check for successful job completion and parameter logging
    SELECT message, records_processed FROM `your_project.your_dataset_logging.job_log`
    WHERE job_name = 'k_ausd_geschaeftspartner'
    ORDER BY timestamp DESC
    LIMIT 2;
    -- Expected output:
    -- Row 1 (INFO): message = 'Job completed successfully.', records_processed = <some_positive_number>
    -- Row 2 (DEBUG): message = 'Parameters - JobKennung: ISBERT_GP_DAILY, EintragsNr: GP_001, Stichtag: 01012023, WiederanlaufWert: 0, Stichtag_DATE: 2023-01-01, Heute: <current_date>, Gestern: <yesterday_date>'
    ```