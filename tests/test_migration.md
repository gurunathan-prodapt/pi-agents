As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the `DW.BERT_AUSD_V_TA_CNTRCT_CRS3` job migration. These tests aim to ensure the migrated BigQuery/Airflow solution is behaviourally equivalent to the legacy Oracle/KornShell system.

The tests are categorised by the four required areas: Output Parity, Transformation Correctness, External System Replacements, and Data Quality/Row Count/Schema Assertions.

---

## Migration Validation Tests: DW.BERT_AUSD_V_TA_CNTRCT_CRS3

### 1. Output Parity Tests

#### Test Case 1.1: End-to-End Data Comparison (Golden Dataset)

*   **Purpose:** To verify that the final output table in BigQuery (`my-gcp-project.sof_schema.sof_ta_cntrct_crs3`) is identical to the legacy Oracle output (`sof$ta_cntrct_crs3`) when provided with the same input data. This is the most critical test for behavioural equivalence.
*   **Setup:**
    1.  **Golden Dataset Creation:** Prepare a comprehensive "golden dataset" for the source tables (`sof$ta_cntrct_crs2` and `isbert_schema.dwtk_meldungen`). This dataset should include:
        *   Contracts with `cntrct_ty` not in (10, 20) and no twin-bill child.
        *   Contracts with `cntrct_ty` not in (10, 20) and one or more twin-bill children (`cntrct_ty = 20`).
        *   Contracts with `cntrct_ty = 20` that are twin-bills, and their parents (`cntrct_ty` not in (10, 20)).
        *   Contracts with `cntrct_ty = 10` or `20` that should be excluded as parents.
        *   Contracts with `NULL` values in relevant join/filter columns (`cntrct_parent`, `cntrct_ty`).
        *   Entries in `dwtk_meldungen` for `BERT_DROP_TEMP_TABLE` with various `timecreated` values, and entries for other `job_kennung`.
        *   Edge cases like empty source tables.
    2.  **Legacy Execution:** Load the golden dataset into the Oracle source tables. Execute the legacy `DW.BERT_AUSD_V_TA_CNTRCT_CRS3` job. Extract the full content of `sof$ta_cntrct_crs3` into a canonical format (e.g., CSV, JSON, or a temporary table).
    3.  **Migrated Execution:** Load the *exact same* golden dataset into the BigQuery source tables (`my-gcp-project.sof_schema.sof_ta_cntrct_crs2` and `my-gcp-project.isbert_schema.dwtk_meldungen`). Execute the migrated Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs3`.
*   **Action:**
    1.  After both jobs have completed, extract the full content of `my-gcp-project.sof_schema.sof_ta_cntrct_crs3` from BigQuery.
    2.  Perform a row-by-row, column-by-column comparison between the legacy Oracle output and the migrated BigQuery output.
*   **Pass/Fail Criterion:** The content of `sof$ta_cntrct_crs3` (Oracle) and `my-gcp-project.sof_schema.sof_ta_cntrct_crs3` (BigQuery) must be identical in terms of row count, column values, and data types (allowing for BigQuery's internal type representations).
*   **Example SQL for comparison (after extraction to a common format or temporary tables):**
    ```sql
    -- Assuming Oracle output is loaded into a BigQuery temp table `legacy_output_temp`
    -- and BigQuery output is `my-gcp-project.sof_schema.sof_ta_cntrct_crs3`

    -- Check for rows in BigQuery output not in Legacy output
    SELECT 'Only in BigQuery' AS source, * FROM `my-gcp-project.sof_schema.sof_ta_cntrct_crs3`
    EXCEPT DISTINCT
    SELECT 'Only in BigQuery' AS source, * FROM `legacy_output_temp`;

    -- Check for rows in Legacy output not in BigQuery output
    SELECT 'Only in Legacy' AS source, * FROM `legacy_output_temp`
    EXCEPT DISTINCT
    SELECT 'Only in Legacy' AS source, * FROM `my-gcp-project.sof_schema.sof_ta_cntrct_crs3`;

    -- If both queries return 0 rows, the datasets are identical.
    ```

### 2. Transformation Correctness Tests

#### Test Case 2.1: `v_datum` Variable Calculation

*   **Purpose:** To verify that the `v_datum` variable, derived from `isbert_schema.dwtk_meldungen`, is calculated correctly according to the specified logic, even if it's not directly used in the main `INSERT` statement. This addresses the "Unresolved / Risks" point.
*   **Setup:**
    1.  Populate `my-gcp-project.isbert_schema.dwtk_meldungen` with the following data:
        *   `('2023-01-15 10:00:00', 'OTHER_JOB')`
        *   `('2023-01-10 11:00:00', 'BERT_DROP_TEMP_TABLE')`
        *   `('2023-01-20 12:00:00', 'BERT_DROP_TEMP_TABLE')`
        *   `('2023-01-05 09:00:00', 'BERT_DROP_TEMP_TABLE')`
        *   `('2023-02-01 13:00:00', 'ANOTHER_JOB')`
    2.  Also, prepare a scenario where `BERT_DROP_TEMP_TABLE` has no entries, or `dwtk_meldungen` is empty.
*   **Action:** Execute only the `DECLARE v_datum ... SET v_datum = (...)` part of the `d_ausd_v_ta_cntrct_crs3_bq.sql` script.
*   **Pass/Fail Criterion:**
    *   For the given setup, `v_datum` must be `'20230120'`.
    *   If no `BERT_DROP_TEMP_TABLE` entries exist, `v_datum` must be `'19000101'`.
*   **Example BigQuery SQL (for assertion):**
    ```sql
    -- Setup: Insert data into `my-gcp-project.isbert_schema.dwtk_meldungen`
    INSERT INTO `my-gcp-project.isbert_schema.dwtk_meldungen` (timecreated, job_kennung) VALUES
    ('2023-01-15 10:00:00', 'OTHER_JOB'),
    ('2023-01-10 11:00:00', 'BERT_DROP_TEMP_TABLE'),
    ('2023-01-20 12:00:00', 'BERT_DROP_TEMP_TABLE'),
    ('2023-01-05 09:00:00', 'BERT_DROP_TEMP_TABLE'),
    ('2023-02-01 13:00:00', 'ANOTHER_JOB');

    -- Action & Assertion:
    DECLARE v_datum STRING;
    SET v_datum = (
      SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(m.timecreated))), '19000101')
      FROM `my-gcp-project.isbert_schema.dwtk_meldungen` m
      WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    );
    SELECT v_datum AS actual_v_datum, '20230120' AS expected_v_datum,
           CASE WHEN v_datum = '20230120' THEN 'PASS' ELSE 'FAIL' END AS test_result;

    -- Scenario 2: No 'BERT_DROP_TEMP_TABLE' entries
    TRUNCATE TABLE `my-gcp-project.isbert_schema.dwtk_meldungen`;
    INSERT INTO `my-gcp-project.isbert_schema.dwtk_meldungen` (timecreated, job_kennung) VALUES
    ('2023-01-15 10:00:00', 'OTHER_JOB');

    DECLARE v_datum STRING;
    SET v_datum = (
      SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(m.timecreated))), '19000101')
      FROM `my-gcp-project.isbert_schema.dwtk_meldungen` m
      WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    );
    SELECT v_datum AS actual_v_datum, '19000101' AS expected_v_datum,
           CASE WHEN v_datum = '19000101' THEN 'PASS' ELSE 'FAIL' END AS test_result;
    ```

#### Test Case 2.2: `TRUNCATE` Behavior

*   **Purpose:** To ensure that the target table (`my-gcp-project.sof_schema.sof_ta_cntrct_crs3`) is correctly truncated before new data is inserted, preventing data accumulation from previous runs.
*   **Setup:**
    1.  Populate `my-gcp-project.sof_schema.sof_ta_cntrct_crs3` with some dummy data (e.g., 5 rows).
    2.  Populate `my-gcp-project.sof_schema.sof_ta_cntrct_crs2` with a small, known set of data that should result in 3 rows being inserted.
    3.  Populate `my-gcp-project.isbert_schema.dwtk_meldungen` as needed for `v_datum` (e.g., one entry for `BERT_DROP_TEMP_TABLE`).
*   **Action:** Execute the full `d_ausd_v_ta_cntrct_crs3_bq.sql` script (or run the Airflow DAG).
*   **Pass/Fail Criterion:** After execution, the row count of `my-gcp-project.sof_schema.sof_ta_cntrct_crs3` must be exactly 3 (the number of rows expected from the `INSERT` statement), not 5+3=8.
*   **Example BigQuery SQL (for assertion):**
    ```sql
    -- Setup:
    TRUNCATE TABLE `my-gcp-project.sof_schema.sof_ta_cntrct_crs3`;
    INSERT INTO `my-gcp-project.sof_schema.sof_ta_cntrct_crs3` (cntrct_id, obj_version, contract_number) VALUES
    ('DUMMY1', 1, 'DUMMY_CONTRACT_1'),
    ('DUMMY2', 1, 'DUMMY_CONTRACT_2'),
    ('DUMMY3', 1, 'DUMMY_CONTRACT_3'),
    ('DUMMY4', 1, 'DUMMY_CONTRACT_4'),
    ('DUMMY5', 1, 'DUMMY_CONTRACT_5');

    -- (Assume sof_ta_cntrct_crs2 and dwtk_meldungen are populated to yield 3 rows)

    -- Action: Run the full d_ausd_v_ta_cntrct_crs3_bq.sql script

    -- Assertion:
    SELECT COUNT(*) AS actual_row_count, 3 AS expected_row_count,
           CASE WHEN COUNT(*) = 3 THEN 'PASS' ELSE 'FAIL' END AS test_result
    FROM `my-gcp-project.sof_schema.sof_ta_cntrct_crs3`;
    ```

#### Test Case 2.3: `cntrct_ty` Filtering (Exclusion)

*   **Purpose:** To verify that contracts with `cntrct_ty` values of `10` or `20` are correctly excluded from being processed as *parent* contracts in both `UNION ALL` branches.
*   **Setup:** Populate `my-gcp-project.sof_schema.sof_ta_cntrct_crs2` with the following contracts:
    *   `C1`: `cntrct_id='C1'`, `cntrct_ty=1`, `cntrct_parent=NULL` (should be included as parent)
    *   `C10`: `cntrct_id='C10'`, `cntrct_ty=10`, `cntrct_parent=NULL` (should be excluded as parent)
    *   `C20`: `cntrct_id='C20'`, `cntrct_ty=20`, `cntrct_parent=NULL` (should be excluded as parent, but can be a twin-bill child)
    *   `C_CHILD_OF_C1`: `cntrct_id='CC1'`, `cntrct_ty=20`, `cntrct_parent='C1'` (should be included as twin-bill child)
    *   `C_CHILD_OF_C10`: `cntrct_id='CC10'`, `cntrct_ty=20`, `cntrct_parent='C10'` (should be excluded as twin-bill child because parent `C10` has `cntrct_ty=10`)
    *   `C_CHILD_OF_C20`: `cntrct_id='CC20'`, `cntrct_ty=20`, `cntrct_parent='C20'` (should be excluded as twin-bill child because parent `C20` has `cntrct_ty=20`)
*   **Action:** Run the migrated job.
*   **Pass/Fail Criterion:**
    *   `C1` should be present in `sof_schema.sof_ta_cntrct_crs3` (from first `SELECT`).
    *   `CC1` should be present in `sof_schema.sof_ta_cntrct_crs3` (from second `SELECT`).
    *   `C10`, `C20`, `CC10`, `CC20` should *not* be present in `sof_schema.sof_ta_cntrct_crs3` as primary records.
*   **Example BigQuery SQL (for assertion):**
    ```sql
    -- Setup: Insert data into `my-gcp-project.sof_schema.sof_ta_cntrct_crs2`
    TRUNCATE TABLE `my-gcp-project.sof_schema.sof_ta_cntrct_crs2`;
    INSERT INTO `my-gcp-project.sof_schema.sof_ta_cntrct_crs2` (cntrct_id, obj_version, contract_number, cntrct_ty, cntrct_parent) VALUES
    ('C1', 1, 'Contract_1', 1, NULL),
    ('C10', 1, 'Contract_10', 10, NULL),
    ('C20', 1, 'Contract_20', 20, NULL),
    ('CC1', 1, 'Child_C1', 20, 'C1'),
    ('CC10', 1, 'Child_C10', 20, 'C10'),
    ('CC20', 1, 'Child_C20', 20, 'C20');

    -- Action: Run the full d_ausd_v_ta_cntrct_crs3_bq.sql script

    -- Assertion:
    SELECT
        COUNTIF(cntrct_id = 'C1') AS C1_count,
        COUNTIF(cntrct_id = 'CC1') AS CC1_count,
        COUNTIF(cntrct_id = 'C10') AS C10_count,
        COUNTIF(cntrct_id = 'C20') AS C20_count,
        COUNTIF(cntrct_id = 'CC10') AS CC10_count,
        COUNTIF(cntrct_id = 'CC20') AS CC20_count,
        CASE
            WHEN COUNTIF(cntrct_id = 'C1') = 1
             AND COUNTIF(cntrct_id = 'CC1') = 1
             AND COUNTIF(cntrct_id = 'C10') = 0
             AND COUNTIF(cntrct_id = 'C20') = 0
             AND COUNTIF(cntrct_id = 'CC10') = 0
             AND COUNTIF(cntrct_id = 'CC20') = 0
            THEN 'PASS' ELSE 'FAIL'
        END AS test_result
    FROM `my-gcp-project.sof_schema.sof_ta_cntrct_crs3`;
    ```

#### Test Case 2.4: Twin-Bill Logic (First `SELECT` - `LEFT JOIN`)

*   **Purpose:** To verify that `twinbill` and `twin_vertrag_id` are correctly populated for parent contracts (where `c.cntrct_ty NOT IN (10, 20)`) based on the presence or absence of a `cntrct_ty = 20` child.
*   **Setup:** Populate `my-gcp-project.sof_schema.sof_ta_cntrct_crs2` with:
    *   `P_NO_TB`: `cntrct_id='P_NO_TB'`, `cntrct_ty=1`, `cntrct_parent=NULL` (Parent with no twin-bill child)
    *   `P_HAS_TB`: `cntrct_id='P_HAS_TB'`, `cntrct_ty=2`, `cntrct_parent=NULL` (Parent with a twin-bill child)
    *   `TB_CHILD_OF_P_HAS_TB`: `cntrct_id='TB_CHILD_P_HAS_TB'`, `cntrct_ty=20`, `cntrct_parent='P_HAS_TB'`
    *   `P_HAS_NON_TB_CHILD`: `cntrct_id='P_HAS_NON_TB_CHILD'`, `cntrct_ty=3`, `cntrct_parent=NULL` (Parent with a non-twin-bill child)
    *   `NON_TB_CHILD_OF_P_HAS_NON_TB_CHILD`: `cntrct_id='NON_TB_CHILD'`, `cntrct_ty=1`, `cntrct_parent='P_HAS_NON_TB_CHILD'`
*   **Action:** Run the migrated job.
*   **Pass/Fail Criterion:**
    *   For `P_NO_TB`: `twinbill` should be `NULL`, `twin_vertrag_id` should be `NULL`.
    *   For `P_HAS_TB`: `twinbill` should be `'TB'`, `twin_vertrag_id` should be `'TB_CHILD_P_HAS_TB'`.
    *   For `P_HAS_NON_TB_CHILD`: `twinbill` should be `NULL`, `twin_vertrag_id` should be `NULL`.
*   **Example BigQuery SQL (for assertion):**
    ```sql
    -- Setup: Insert data into `my-gcp-project.sof_schema.sof_ta_cntrct_crs2`
    TRUNCATE TABLE `my-gcp-project.sof_schema.sof_ta_cntrct_crs2`;
    INSERT INTO `my-gcp-project.sof_schema.sof_ta_cntrct_crs2` (cntrct_id, obj_version, contract_number, cntrct_ty, cntrct_parent) VALUES
    ('P_NO_TB', 1, 'Parent_NoTB', 1, NULL),
    ('P_HAS_TB', 1, 'Parent_HasTB', 2, NULL),
    ('TB_CHILD_P_HAS_TB', 1, 'TB_Child', 20, 'P_HAS_TB'),
    ('P_HAS_NON_TB_CHILD', 1, 'Parent_NonTBChild', 3, NULL),
    ('NON_TB_CHILD', 1, 'NonTB_Child', 1, 'P_HAS_NON_TB_CHILD');

    -- Action: Run the full d_ausd_v_ta_cntrct_crs3_bq.sql script

    -- Assertion:
    SELECT
        cntrct_id,
        twinbill,
        twin_vertrag_id,
        CASE
            WHEN cntrct_id = 'P_NO_TB' AND twinbill IS NULL AND twin_vertrag_id IS NULL THEN 'PASS'
            WHEN cntrct_id = 'P_HAS_TB' AND twinbill = 'TB' AND twin_vertrag_id = 'TB_CHILD_P_HAS_TB' THEN 'PASS'
            WHEN cntrct_id = 'P_HAS_NON_TB_CHILD' AND twinbill IS NULL AND twin_vertrag_id IS NULL THEN 'PASS'
            ELSE 'FAIL'
        END AS test_result
    FROM `my-gcp-project.sof_schema.sof_ta_cntrct_crs3`
    WHERE cntrct_id IN ('P_NO_TB', 'P_HAS_TB', 'P_HAS_NON_TB_CHILD');
    ```

#### Test Case 2.5: Twin-Bill Logic (Second `SELECT` - `JOIN`)

*   **Purpose:** To verify that twin-bill contracts (`ctb.cntrct_ty = 20`) are correctly inserted, with `twinbill` as `'TB'` and `twin_vertrag_id` pointing to their parent, and that the parent's `cntrct_ty` filter (`c.cntrct_ty NOT IN (10, 20)`) is applied.
*   **Setup:** Populate `my-gcp-project.sof_schema.sof_ta_cntrct_crs2` with:
    *   `PARENT_VALID`: `cntrct_id='PARENT_VALID'`, `cntrct_ty=1`, `cntrct_parent=NULL`
    *   `TB_CHILD_VALID`: `cntrct_id='TB_CHILD_VALID'`, `cntrct_ty=20`, `cntrct_parent='PARENT_VALID'`
    *   `PARENT_INVALID_10`: `cntrct_id='PARENT_INVALID_10'`, `cntrct_ty=10`, `cntrct_parent=NULL`
    *   `TB_CHILD_INVALID_10`: `cntrct_id='TB_CHILD_INVALID_10'`, `cntrct_ty=20`, `cntrct_parent='PARENT_INVALID_10'`
    *   `PARENT_INVALID_20`: `cntrct_id='PARENT_INVALID_20'`, `cntrct_ty=20`, `cntrct_parent=NULL`
    *   `TB_CHILD_INVALID_20`: `cntrct_id='TB_CHILD_INVALID_20'`, `cntrct_ty=20`, `cntrct_parent='PARENT_INVALID_20'`
*   **Action:** Run the migrated job.
*   **Pass/Fail Criterion:**
    *   `TB_CHILD_VALID` should be present in `sof_schema.sof_ta_cntrct_crs3`, with `twinbill='TB'` and `twin_vertrag_id='PARENT_VALID'`.
    *   `TB_CHILD_INVALID_10` and `TB_CHILD_INVALID_20` should *not* be present in `sof_schema.sof_ta_cntrct_crs3`.
*   **Example BigQuery SQL (for assertion):**
    ```sql
    -- Setup: Insert data into `my-gcp-project.sof_schema.sof_ta_cntrct_crs2`
    TRUNCATE TABLE `my-gcp-project.sof_schema.sof_ta_cntrct_crs2`;
    INSERT INTO `my-gcp-project.sof_schema.sof_ta_cntrct_crs2` (cntrct_id, obj_version, contract_number, cntrct_ty, cntrct_parent) VALUES
    ('PARENT_VALID', 1, 'Parent_Valid', 1, NULL),
    ('TB_CHILD_VALID', 1, 'TB_Child_Valid', 20, 'PARENT_VALID'),
    ('PARENT_INVALID_10', 1, 'Parent_Invalid_10', 10, NULL),
    ('TB_CHILD_INVALID_10', 1, 'TB_Child_Invalid_10', 20, 'PARENT_INVALID_10'),
    ('PARENT_INVALID_20', 1, 'Parent_Invalid_20', 20, NULL),
    ('TB_CHILD_INVALID_20', 1, 'TB_Child_Invalid_20', 20, 'PARENT_INVALID_20');

    -- Action: Run the full d_ausd_v_ta_cntrct_crs3_bq.sql script

    -- Assertion:
    SELECT
        COUNTIF(cntrct_id = 'TB_CHILD_VALID' AND twinbill = 'TB' AND twin_vertrag_id = 'PARENT_VALID') AS valid_tb_child_count,
        COUNTIF(cntrct_id = 'TB_CHILD_INVALID_10') AS invalid_tb_child_10_count,
        COUNTIF(cntrct_id = 'TB_CHILD_INVALID_20') AS invalid_tb_child_20_count,
        CASE
            WHEN COUNTIF(cntrct_id = 'TB_CHILD_VALID' AND twinbill = 'TB' AND twin_vertrag_id = 'PARENT_VALID') = 1
             AND COUNTIF(cntrct_id = 'TB_CHILD_INVALID_10') = 0
             AND COUNTIF(cntrct_id = 'TB_CHILD_INVALID_20') = 0
            THEN 'PASS' ELSE 'FAIL'
        END AS test_result
    FROM `my-gcp-project.sof_schema.sof_ta_cntrct_crs3`;
    ```

#### Test Case 2.6: `UNION ALL` Behavior

*   **Purpose:** To ensure that `UNION ALL` correctly combines the results of both `SELECT` statements, including any potential duplicates if a record could theoretically be generated by both branches (though unlikely with current logic, it confirms `UNION ALL` vs `UNION DISTINCT`).
*   **Setup:** Populate `my-gcp-project.sof_schema.sof_ta_cntrct_crs2` with:
    *   `P1`: `cntrct_id='P1'`, `obj_version=1`, `contract_number='P1_CN'`, `cntrct_ty=1`, `cntrct_parent=NULL`
    *   `T1`: `cntrct_id='T1'`, `obj_version=1`, `contract_number='T1_CN'`, `cntrct_ty=20`, `cntrct_parent='P1'`
*   **Action:** Run the migrated job.
*   **Pass/Fail Criterion:** `my-gcp-project.sof_schema.sof_ta_cntrct_crs3` must contain exactly two rows:
    *   One row for `P1` (from the first `SELECT` branch), with `twinbill='TB'` and `twin_vertrag_id='T1'`.
    *   One row for `T1` (from the second `SELECT` branch), with `twinbill='TB'` and `twin_vertrag_id='P1'`.
*   **Example BigQuery SQL (for assertion):**
    ```sql
    -- Setup: Insert data into `my-gcp-project.sof_schema.sof_ta_cntrct_crs2`
    TRUNCATE TABLE `my-gcp-project.sof_schema.sof_ta_cntrct_crs2`;
    INSERT INTO `my-gcp-project.sof_schema.sof_ta_cntrct_crs2` (cntrct_id, obj_version, contract_number, cntrct_ty, cntrct_parent) VALUES
    ('P1', 1, 'P1_CN', 1, NULL),
    ('T1', 1, 'T1_CN', 20, 'P1');

    -- Action: Run the full d_ausd_v_ta_cntrct_crs3_bq.sql script

    -- Assertion:
    SELECT
        COUNT(*) AS total_rows,
        COUNTIF(cntrct_id = 'P1' AND twinbill = 'TB' AND twin_vertrag_id = 'T1') AS P1_expected_row,
        COUNTIF(cntrct_id = 'T1' AND twinbill = 'TB' AND twin_vertrag_id = 'P1') AS T1_expected_row,
        CASE
            WHEN COUNT(*) = 2
             AND COUNTIF(cntrct_id = 'P1' AND twinbill = 'TB' AND twin_vertrag_id = 'T1') = 1
             AND COUNTIF(cntrct_id = 'T1' AND twinbill = 'TB' AND twin_vertrag_id = 'P1') = 1
            THEN 'PASS' ELSE 'FAIL'
        END AS test_result
    FROM `my-gcp-project.sof_schema.sof_ta_cntrct_crs3`;
    ```

#### Test Case 2.7: NULL Handling in Source Columns

*   **Purpose:** To verify that `NULL` values in source columns are correctly propagated to the target table, and that derived columns handle `NULL` inputs as expected.
*   **Setup:** Populate `my-gcp-project.sof_schema.sof_ta_cntrct_crs2` with a contract where several non-key columns are `NULL`.
    *   `C_NULLS`: `cntrct_id='C_NULLS'`, `obj_version=1`, `contract_number=NULL`, `cntrct_template_id=NULL`, `valid_from=NULL`, `cntrct_ty=1`, `cntrct_parent=NULL`, etc.
*   **Action:** Run the migrated job.
*   **Pass/Fail Criterion:** The row for `C_NULLS` in `my-gcp-project.sof_schema.sof_ta_cntrct_crs3` must have `NULL` in all corresponding columns that were `NULL` in the source, and `twinbill` and `twin_vertrag_id` should also be `NULL` (as `cntrct_parent` is `NULL`).
*   **Example BigQuery SQL (for assertion):**
    ```sql
    -- Setup: Insert data into `my-gcp-project.sof_schema.sof_ta_cntrct_crs2`
    TRUNCATE TABLE `my-gcp-project.sof_schema.sof_ta_cntrct_crs2`;
    INSERT INTO `my-gcp-project.sof_schema.sof_ta_cntrct_crs2` (
        cntrct_id, obj_version, contract_number, cntrct_template_id, cntrct_validity_id, valid_from,
        com_per_ext_rea_cv, billcycle_id, vo_code, cntrct_start_date, cntrct_st, cntrct_parent,
        cntrct_ty, cost_centre, cost_centre_user, commitment_reference_date, order_number, rv_num
    ) VALUES (
        'C_NULLS', 1, NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, NULL,
        1, NULL, NULL, NULL, NULL, NULL
    );

    -- Action: Run the full d_ausd_v_ta_cntrct_crs3_bq.sql script

    -- Assertion:
    SELECT
        cntrct_id,
        contract_number IS NULL AS contract_number_is_null,
        valid_from IS NULL AS valid_from_is_null,
        cntrct_parent IS NULL AS cntrct_parent_is_null,
        twinbill IS NULL AS twinbill_is_null,
        twin_vertrag_id IS NULL AS twin_vertrag_id_is_null,
        CASE
            WHEN cntrct_id = 'C_NULLS'
             AND contract_number IS NULL
             AND valid_from IS NULL
             AND cntrct_parent IS NULL
             AND twinbill IS NULL
             AND twin_vertrag_id IS NULL
            THEN 'PASS' ELSE 'FAIL'
        END AS test_result
    FROM `my-gcp-project.sof_schema.sof_ta_cntrct_crs3`
    WHERE cntrct_id = 'C_NULLS';
    ```

### 3. External System Replacements Tests

#### Test Case 3.1: Airflow DAG Execution & Logging

*   **Purpose:** To verify that the Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs3` successfully triggers the Python script (`contract_data_updater.py`) and that the script's logging is correctly captured in Cloud Logging. This replaces the UC4 scheduler and KornShell logging.
*   **Setup:**
    1.  Deploy the `dw_bert_ausd_v_ta_cntrct_crs3_dag.py` DAG, `contract_data_updater.py` script, and `d_ausd_v_ta_cntrct_crs3_bq.sql` to a Cloud Composer environment.
    2.  Ensure the Airflow service account has necessary BigQuery permissions.
    3.  Ensure source tables (`sof_schema.sof_ta_cntrct_crs2`, `isbert_schema.dwtk_meldungen`) have some data for a successful run.
*   **Action:**
    1.  Manually trigger the `dw_bert_ausd_v_ta_cntrct_crs3` DAG from the Airflow UI.
    2.  Monitor the DAG run in the Airflow UI.
    3.  Check Cloud Logging for logs generated by the `contract_data_updater.py` script.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG run must complete successfully with all tasks marked as `success`.
    *   Cloud Logging must contain INFO messages from the `contract_data_updater.py` script, including:
        *   `Starting contract data update process.`
        *   `Starting BigQuery job for SQL from: scripts/dw_bert_ausd_v_ta_cntrct_crs3/d_ausd_v_ta_cntrct_crs3_bq.sql` (or similar path)
        *   `BigQuery job <job_id> completed successfully.`
        *   `Contract data update process finished successfully.`

#### Test Case 3.2: Python Script Parameter Handling & BigQuery Execution

*   **Purpose:** To verify that the `contract_data_updater.py` Python script correctly parses command-line arguments (`--project_id`, `--sql_file`) and successfully executes the BigQuery SQL file. This replaces the KornShell parameter handling and `sqlplus` execution.
*   **Setup:**
    1.  Ensure `contract_data_updater.py` and `d_ausd_v_ta_cntrct_crs3_bq.sql` are accessible in a test environment (e.g., a VM with `google-cloud-bigquery` installed).
    2.  Ensure the environment has `gcloud` configured and authenticated to a GCP project with BigQuery access.
    3.  Populate BigQuery source tables with minimal data to allow a successful SQL execution.
*   **Action:**
    1.  **Valid Execution:** Execute the Python script with valid parameters:
        ```bash
        python contract_data_updater.py --project_id my-gcp-project --sql_file scripts/dw_bert_ausd_v_ta_cntrct_crs3/d_ausd_v_ta_cntrct_crs3_bq.sql
        ```
    2.  **Missing Parameter:** Execute without a required parameter:
        ```bash
        python contract_data_updater.py --project_id my-gcp-project
        ```
    3.  **Non-existent SQL File:** Execute with a path to a non-existent SQL file:
        ```bash
        python contract_data_updater.py --project_id my-gcp-project --sql_file non_existent_sql_file.sql
        ```
*   **Pass/Fail Criterion:**
    *   **Valid Execution:** The script must execute successfully, print INFO logs, and the BigQuery target table (`my-gcp-project.sof_schema.sof_ta_cntrct_crs3`) should be updated.
    *   **Missing Parameter:** The script must exit with an error message indicating a missing argument (e.g., `argument --sql_file is required`).
    *   **Non-existent SQL File:** The script must log a `FileNotFoundError` and exit with a non-zero status code.

### 4. Data Quality / Row Count / Schema Assertions

#### Test Case 4.1: Schema Parity

*   **Purpose:** To verify that the BigQuery target table schema (`my-gcp-project.sof_schema.sof_ta_cntrct_crs3`) accurately reflects the Oracle legacy table schema (`sof$ta_cntrct_crs3`), including column names, data types, and nullability. This addresses potential data type mismatches.
*   **Setup:**
    1.  Obtain the DDL for `sof$ta_cntrct_crs3` from the Oracle legacy environment.
    2.  Obtain the DDL for `my-gcp-project.sof_schema.sof_ta_cntrct_crs3` from BigQuery.
*   **Action:** Compare the column names, their order, and their data types between the Oracle and BigQuery DDLs. Pay close attention to Oracle-specific types (e.g., `NUMBER`, `VARCHAR2`, `DATE`) and their BigQuery equivalents (`INT64`/`BIGNUMERIC`, `STRING`, `DATE`/`DATETIME`).
*   **Pass/Fail Criterion:**
    *   All column names must match exactly.
    *   The order of columns should ideally match, or at least be consistent with the `INSERT` statement.
    *   Data types must be functionally equivalent (e.g., `VARCHAR2(X)` -> `STRING`, `NUMBER` -> `INT64` or `BIGNUMERIC` depending on precision/scale, `DATE` -> `DATE`).
    *   Nullability constraints should be consistent where applicable (BigQuery allows `NULL` by default, so explicit `NOT NULL` in Oracle needs careful consideration if it's critical).

#### Test Case 4.2: Row Count Parity

*   **Purpose:** To verify that the total number of rows in the target table is identical in BigQuery as in Oracle for the same input data.
*   **Setup:**
    1.  Use the same golden dataset and execution steps as in Test Case 1.1 (End-to-End Data Comparison).
    2.  Ensure both legacy and migrated jobs have completed.
*   **Action:** Count the rows in `sof$ta_cntrct_crs3` (Oracle) and `my-gcp-project.sof_schema.sof_ta_cntrct_crs3` (BigQuery).
*   **Pass/Fail Criterion:** The row counts from both target tables must be identical.
*   **Example SQL:**
    ```sql
    -- Oracle (example syntax)
    SELECT COUNT(*) FROM sof$ta_cntrct_crs3;

    -- BigQuery
    SELECT COUNT(*) FROM `my-gcp-project.sof_schema.sof_ta_cntrct_crs3`;
    ```

#### Test Case 4.3: Data Integrity - No Unexpected Duplicates

*   **Purpose:** To ensure that the migration process does not introduce unintended duplicate rows in the target table, especially given the `UNION ALL` operation.
*   **Setup:** Run the migrated job with a representative dataset (e.g., the golden dataset from Test Case 1.1).
*   **Action:** Query `my-gcp-project.sof_schema.sof_ta_cntrct_crs3` to identify any duplicate `cntrct_id` values.
*   **Pass/Fail Criterion:** There should be no duplicate `cntrct_id` values in the `my-gcp-project.sof_schema.sof_ta_cntrct_crs3` table. (Based on the logic, `cntrct_id` from the first `SELECT` is `c.cntrct_id`, and from the second `SELECT` is `ctb.cntrct_id`. These are distinct sets of IDs, so the final `cntrct_id` should be unique).
*   **Example BigQuery SQL:**
    ```sql
    SELECT
        cntrct_id,
        COUNT(*) AS num_occurrences
    FROM `my-gcp-project.sof_schema.sof_ta_cntrct_crs3`
    GROUP BY cntrct_id
    HAVING COUNT(*) > 1;

    -- Pass if this query returns 0 rows.
    ```

#### Test Case 4.4: Data Integrity - Expected `NULL` vs. Not `NULL` for Derived Columns

*   **Purpose:** To verify that the `twinbill` and `twin_vertrag_id` columns are populated correctly, specifically checking for expected `NULL` values when no twin-bill relationship exists, and non-`NULL` values when it does.
*   **Setup:** Use a dataset that includes contracts representing all scenarios:
    *   Parent contracts with no twin-bill children.
    *   Parent contracts with twin-bill children.
    *   Twin-bill children contracts.
*   **Action:** Run the migrated job. Query `my-gcp-project.sof_schema.sof_ta_cntrct_crs3` and inspect the `twinbill` and `twin_vertrag_id` columns for the different contract types.
*   **Pass/Fail Criterion:**
    *   For contracts that are parents without a `cntrct_ty = 20` child (from the first `SELECT` branch), `twinbill` and `twin_vertrag_id` must be `NULL`.
    *   For contracts that are parents with a `cntrct_ty = 20` child (from the first `SELECT` branch), `twinbill` must be `'TB'` and `twin_vertrag_id` must be `NOT NULL`.
    *   For contracts that are twin-bill children (`cntrct_ty = 20` from the second `SELECT` branch), `twinbill` must be `'TB'` and `twin_vertrag_id` must be `NOT NULL`.
*   **Example BigQuery SQL (building on Test Cases 2.4 and 2.5 setup):**
    ```sql
    -- Assuming setup from Test Cases 2.4 and 2.5 is applied
    SELECT
        cntrct_id,
        twinbill,
        twin_vertrag_id,
        CASE
            WHEN cntrct_id = 'P_NO_TB' AND twinbill IS NULL AND twin_vertrag_id IS NULL THEN 'PASS' -- Parent, no TB child
            WHEN cntrct_id = 'P_HAS_TB' AND twinbill = 'TB' AND twin_vertrag_id = 'TB_CHILD_P_HAS_TB' THEN 'PASS' -- Parent, has TB child
            WHEN cntrct_id = 'P_HAS_NON_TB_CHILD' AND twinbill IS NULL AND twin_vertrag_id IS NULL THEN 'PASS' -- Parent, has non-TB child
            WHEN cntrct_id = 'TB_CHILD_VALID' AND twinbill = 'TB' AND twin_vertrag_id = 'PARENT_VALID' THEN 'PASS' -- TB child
            ELSE 'FAIL'
        END AS test_result
    FROM `my-gcp-project.sof_schema.sof_ta_cntrct_crs3`
    WHERE cntrct_id IN ('P_NO_TB', 'P_HAS_TB', 'P_HAS_NON_TB_CHILD', 'TB_CHILD_VALID');
    ```