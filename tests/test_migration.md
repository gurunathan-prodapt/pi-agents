As a senior data-migration QA engineer, I've analyzed the provided migration design and generated code for `r_ausd_v_ta_vertrag_tmp.ksh`. The migration involves transforming a KornShell-orchestrated Oracle SQL process into BigQuery stored procedures.

The following test cases are designed to ensure behavioral equivalence, covering output parity, transformation correctness, external system replacements (logging), and data quality.

---

## Migration Validation Tests for `r_ausd_v_ta_vertrag_tmp`

### I. End-to-End Output Parity

*   **Purpose**: To verify that the migrated BigQuery job produces an identical dataset in the `ta_vertrag_tmp` target table as the legacy KornShell/Oracle job, given the same input data. This is the primary validation for behavioral equivalence.
*   **Setup**:
    1.  **Source Data Replication**: Ensure all source tables (`sof$ta_cntrct_crs3`, `sof$ta_bp_ref`, `sof$ta_inv_acc`, `sof$ta_notice`, `sof$ta_barrier_zusgf`, `sof$ta_cntrct_templ`, `sof$ta_cntrct_valid`, `sof$ta_period`, `sof$ta_vvl_upgrade`, `sof$ta_apn_ve`, `dwh$vi_s_rd_segment`, `sof$ta_action_assoc`, `sof$vi_c_bfc`) in BigQuery (`my_project.source_dataset`) are populated with a comprehensive, representative dataset that precisely mirrors the Oracle source data used for the legacy run. This dataset should include various scenarios for all `CASE` statements, `JOIN` conditions, and `NULL` values.
    2.  **Legacy Run Execution**: Execute the legacy `r_ausd_v_ta_vertrag_tmp.ksh` job on the Oracle environment with a specific execution date (e.g., `20231026`).
    3.  **Golden Data Extraction**: After the legacy job completes, extract the final content of the `ta_vertrag_tmp` table from the Oracle database. Load this data into a BigQuery "golden" reference table (e.g., `my_project.test_dataset.ta_vertrag_tmp_legacy_golden`). Ensure data types in the golden table match the target BigQuery table.
    4.  **Target Table Preparation**: Ensure the `my_project.my_target_dataset.ta_vertrag_tmp` table is empty before the migrated job run (the `TRUNCATE` in `k_ausd_v_ta_vertrag_tmp` handles this, but an initial check is good).
*   **Action**:
    1.  Execute the migrated BigQuery stored procedure, passing the same execution date used for the legacy run:
        ```sql
        CALL `my_project.my_target_dataset.r_ausd_v_ta_vertrag_tmp`('20231026');
        ```
    2.  After execution, query the `my_project.my_target_dataset.ta_vertrag_tmp` table.
*   **Pass/Fail Criterion**:
    1.  **Row Count Parity**: The number of rows in `my_project.my_target_dataset.ta_vertrag_tmp` must be exactly equal to the number of rows in `my_project.test_dataset.ta_vertrag_tmp_legacy_golden`.
        ```sql
        SELECT
          (SELECT COUNT(*) FROM `my_project.my_target_dataset.ta_vertrag_tmp`) =
          (SELECT COUNT(*) FROM `my_project.test_dataset.ta_vertrag_tmp_legacy_golden`) AS row_count_match;
        ```
    2.  **Data Parity**: All columns and rows in `my_project.my_target_dataset.ta_vertrag_tmp` must exactly match the corresponding columns and rows in `my_project.test_dataset.ta_vertrag_tmp_legacy_golden`.
        ```sql
        -- Find rows present in target but not in golden
        SELECT 'Only in Target' AS source, * FROM `my_project.my_target_dataset.ta_vertrag_tmp`
        EXCEPT DISTINCT
        SELECT 'Only in Target' AS source, * FROM `my_project.test_dataset.ta_vertrag_tmp_legacy_golden`;

        -- Find rows present in golden but not in target
        SELECT 'Only in Golden' AS source, * FROM `my_project.test_dataset.ta_vertrag_tmp_legacy_golden`
        EXCEPT DISTINCT
        SELECT 'Only in Golden' AS source, * FROM `my_project.my_target_dataset.ta_vertrag_tmp`;
        ```
        *Pass if both `EXCEPT DISTINCT` queries return 0 rows.*

### II. Logging and Orchestration Verification (Success Scenario)

*   **Purpose**: To verify that the migrated BigQuery stored procedure `r_ausd_v_ta_vertrag_tmp` correctly orchestrates the job, including logging start/end times and status for a successful run, mirroring the KornShell wrapper's behavior.
*   **Setup**:
    1.  Ensure the `my_project.my_utils_dataset.job_log` table is empty.
    2.  Ensure source data is valid and will lead to a successful execution of the core transformation.
*   **Action**:
    1.  Execute the migrated job with a valid input date:
        ```sql
        CALL `my_project.my_target_dataset.r_ausd_v_ta_vertrag_tmp`('20231026');
        ```
    2.  Query the `job_log` table.
*   **Pass/Fail Criterion**:
    *   The `job_log` table must contain at least three entries:
        *   One entry with `job_name = 'r_ausd_v_ta_vertrag_tmp'`, `status = 'STARTED'`, `message` indicating job start, `start_time` populated, `end_time` and `exit_code` NULL.
        *   One entry with `job_name = 'r_ausd_v_ta_vertrag_tmp'`, `status = 'SUCCESS'`, `message` indicating successful completion, `start_time` matching the 'STARTED' entry, `end_time` populated, `exit_code = 0`.
        *   One entry with `job_name = 'FINAL_STATUS_LOG'`, `status = 'SUCCESS'`, `message = 'Processing finished successfully.'`, `exit_code = 0`.
        *   The `start_time` of the 'SUCCESS' entry should be close to the `start_time` of the 'STARTED' entry. The `end_time` of the 'SUCCESS' entry should be after its `start_time`.
        ```sql
        SELECT
          COUNTIF(status = 'STARTED' AND job_name = 'r_ausd_v_ta_vertrag_tmp') = 1 AND
          COUNTIF(status = 'SUCCESS' AND job_name = 'r_ausd_v_ta_vertrag_tmp' AND exit_code = 0) = 1 AND
          COUNTIF(status = 'SUCCESS' AND job_name = 'FINAL_STATUS_LOG' AND exit_code = 0) = 1
        FROM `my_project.my_utils_dataset.job_log`
        WHERE job_name IN ('r_ausd_v_ta_vertrag_tmp', 'FINAL_STATUS_LOG');
        ```

### III. Logging and Orchestration Verification (Failure Scenario)

*   **Purpose**: To verify that the migrated BigQuery stored procedure `r_ausd_v_ta_vertrag_tmp` correctly handles and logs errors, including rolling back transactions, mirroring the KornShell wrapper's error trapping.
*   **Setup**:
    1.  Ensure the `my_project.my_utils_dataset.job_log` table is empty.
    2.  Introduce a deliberate error in one of the source tables (e.g., change a column's data type to cause a `CAST` error, or make a non-nullable column `NULL` in a `JOIN` condition) that will cause the `k_ausd_v_ta_vertrag_tmp` procedure to fail.
    3.  Optionally, populate `my_project.my_target_dataset.ta_vertrag_tmp` with some initial data to verify the `ROLLBACK TRANSACTION` functionality.
*   **Action**:
    1.  Execute the migrated job:
        ```sql
        CALL `my_project.my_target_dataset.r_ausd_v_ta_vertrag_tmp`('20231026');
        ```
    2.  Query the `job_log` table and the `ta_vertrag_tmp` table.
*   **Pass/Fail Criterion**:
    *   The `job_log` table must contain entries indicating failure:
        *   One entry with `job_name = 'r_ausd_v_ta_vertrag_tmp'`, `status = 'STARTED'`.
        *   One entry with `job_name = 'r_ausd_v_ta_vertrag_tmp'`, `status = 'FAILED'`, `message` containing error details (e.g., "Error during execution...", SQL error message), `end_time` populated, `exit_code = 1`.
        *   One entry with `job_name = 'FINAL_STATUS_LOG'`, `status = 'FAILED'`, `message = 'Processing failed due to an error.'`, `exit_code = 1`.
        ```sql
        SELECT
          COUNTIF(status = 'STARTED' AND job_name = 'r_ausd_v_ta_vertrag_tmp') = 1 AND
          COUNTIF(status = 'FAILED' AND job_name = 'r_ausd_v_ta_vertrag_tmp' AND exit_code = 1) = 1 AND
          COUNTIF(status = 'FAILED' AND job_name = 'FINAL_STATUS_LOG' AND exit_code = 1) = 1
        FROM `my_project.my_utils_dataset.job_log`
        WHERE job_name IN ('r_ausd_v_ta_vertrag_tmp', 'FINAL_STATUS_LOG');
        ```
    *   The `ta_vertrag_tmp` table should be empty or contain the data it had *before* the failed execution (due to `TRUNCATE` followed by `ROLLBACK TRANSACTION`).
        ```sql
        SELECT COUNT(*) FROM `my_project.my_target_dataset.ta_vertrag_tmp`; -- Should be 0 if initially empty, or original count if pre-populated
        ```

### IV. Data Type and NULL Handling

*   **Purpose**: To specifically test the `CAST` operations and `COALESCE` functions, ensuring data types are correctly converted and `NULL`s are handled as expected, especially for columns that might have different representations in Oracle vs. BigQuery (e.g., numbers as strings, dates).
*   **Setup**:
    1.  Populate source tables with specific test cases:
        *   Records where `cntrct_id`, `bp_id`, `inv_definition_id`, `sales_tax_freed`, `rv_num`, `billcycle_id`, `twinbill`, `bindefrist`, `number_time_measurement`, `twin_vertrag_id`, `cntrct_template_id`, `cntrct_ty`, `segment_id`, `rv_action_id`, `cntrct_validity_id` have values requiring `CAST` to STRING or INT64. Include `NULL` values for these where applicable.
        *   Records where `valid_from`, `entry_date_of_notice`, `cntrct_start_date`, `upgradedatum`, `commitment_reference_date` have various date formats (if source allows) and `NULL`s to test `DATE()` conversion.
        *   Records where `c.commitment_reference_date` is `NULL` to test `COALESCE(DATE(c.commitment_reference_date), DATE(c.cntrct_start_date))` within the `upgradeberechtigt` logic.
    2.  Run the legacy job with these specific test cases and extract results into a golden table for comparison.
*   **Action**:
    1.  Execute `CALL my_project.my_target_dataset.r_ausd_v_ta_vertrag_tmp('20231026');`
    2.  Query `my_project.my_target_dataset.ta_vertrag_tmp` for the specific test cases.
*   **Pass/Fail Criterion**:
    *   For each test case, the values in the migrated `ta_vertrag_tmp` table must match the golden reference for the affected columns.
    *   Example check for `COALESCE` and `DATE` casting:
        ```sql
        SELECT
          vertrag_id_carmen,
          geplant_kuend,
          eingang_kuend,
          vertragsbeginn,
          letztes_upgrade,
          commitment_reference_date,
          upgradeberechtigt -- This column heavily relies on date calculations and COALESCE
        FROM `my_project.my_target_dataset.ta_vertrag_tmp`
        WHERE vertrag_id_carmen IN ('test_id_with_null_commit_date', 'test_id_with_valid_commit_date');
        ```
        *Pass if all selected values match the golden reference.*

### V. Conditional Logic (CASE statements)

*   **Purpose**: To verify the correctness of all `CASE` statements, especially the complex `upgradeberechtigt` logic, under various input conditions.
*   **Setup**:
    1.  Populate source tables with data that covers all branches of each `CASE` statement:
        *   `c.cntrct_st`: Values 5, 6, and other values (e.g., 1, 2, NULL).
        *   `ia.inv_pay_ty_cv`: Values 1, 2, 3, 4, and other values (e.g., 0, 5, NULL).
        *   `ia.inv_media_cv`: Values 1, 2, 3, 4, 5, 6, and other values (e.g., 0, 7, NULL).
        *   `ct.cntrct_template_id`: Values 5104, 5105, 5106, values between 5155 and 5161 (e.g., 5155, 5158, 5161), and other values (e.g., 1000, 6000, NULL).
        *   For `upgradeberechtigt`: Create specific records to trigger each `WHEN` clause and the `ELSE` clause. This includes combinations of `p.number_time_measurement` (NULL, 0, 12, 24, other), `b.sperrart_alle` (NULL, NOT NULL), `b.sperrgrund_zusgf` (2, other), and dates (`v_datum`, `c.commitment_reference_date`, `c.cntrct_start_date`) that result in `DATE_DIFF > 9` or `DATE_DIFF > 23`.
    2.  Run the legacy job with these specific test cases and extract results into a golden table.
*   **Action**:
    1.  Execute `CALL my_project.my_target_dataset.r_ausd_v_ta_vertrag_tmp('20231026');` (use an execution date that allows `DATE_DIFF` conditions to be met for your test data).
    2.  Query `my_project.my_target_dataset.ta_vertrag_tmp` for the specific test cases.
*   **Pass/Fail Criterion**:
    *   For each test case, the values in the migrated `ta_vertrag_tmp` table for `vertragsstatus`, `rechnungszahlart`, `rechnungsmedium`, `upgradeberechtigt`, and `VDA` must match the golden reference.
    *   Example for `upgradeberechtigt` (requires careful setup of source data):
        ```sql
        SELECT
          vertrag_id_carmen,
          vertragsbindung, -- p.number_time_measurement
          sperrart,        -- b.sperrart_alle
          -- sperrgrund_zusgf is not in target, but its effect is in upgradeberechtigt
          vertragsbeginn,
          commitment_reference_date,
          upgradeberechtigt
        FROM `my_project.my_target_dataset.ta_vertrag_tmp`
        WHERE vertrag_id_carmen IN (
          'contract_upgrade_J_case1_null_bind',
          'contract_upgrade_J_case2_bind_12_months',
          'contract_upgrade_J_case3_bind_24_months',
          'contract_upgrade_N_case_no_match'
        );
        ```
        *Pass if all selected values match the golden reference.*

### VI. Join Integrity and Filtering Logic

*   **Purpose**: To ensure all `JOIN` and `LEFT JOIN` conditions correctly link records and handle missing matches, and that the `WHERE` clauses for `c.cntrct_ty` correctly partition data for the `UNION ALL`.
*   **Setup**:
    1.  Populate source tables with data to test:
        *   Records that have matches in all `JOIN`s (e.g., `sof$ta_bp_ref`, `sof$ta_inv_acc`, `sof$ta_cntrct_templ`).
        *   Records that have no matches in `LEFT JOIN`s (e.g., `sof$ta_notice`, `sof$ta_barrier_zusgf`, `sof$ta_period`, `sof$ta_vvl_upgrade`, `sof$ta_apn_ve`, `dwh$vi_s_rd_segment`, `sof$ta_action_assoc`, `sof$vi_c_bfc`) to ensure `NULL`s are correctly propagated.
        *   Records in `sof$ta_cntrct_crs3` where `cntrct_ty` is 20, not 20, and `NULL`.
        *   Ensure `cntrct_parent` and `cntrct_id` are distinct for these test cases to clearly differentiate the two `UNION ALL` branches.
    2.  Run the legacy job with these specific test cases and extract results into a golden table.
*   **Action**:
    1.  Execute `CALL my_project.my_target_dataset.r_ausd_v_ta_vertrag_tmp('20231026');`
    2.  Query `my_project.my_target_dataset.ta_vertrag_tmp` for the specific test cases.
*   **Pass/Fail Criterion**:
    *   The output for the test cases must match the golden reference. Specifically, check that `NULL` values appear correctly for `LEFT JOIN`s where no match is found.
    *   **Filtering**:
        *   All records with `cntrct_ty = 20` should have joined `bp.cntrct_cp2_id = c.cntrct_parent`.
        *   All records with `cntrct_ty <> 20` should have joined `bp.cntrct_cp2_id = c.cntrct_id`.
        *   No records with `cntrct_ty IS NULL` should be present in the output.
    ```sql
    -- Verify cntrct_ty = 20 records use cntrct_parent for bp_ref join
    SELECT COUNT(*) FROM `my_project.my_target_dataset.ta_vertrag_tmp` AS t
    WHERE t.cntrct_ty = 20
      AND NOT EXISTS (
        SELECT 1 FROM `my_project.source_dataset.sof$ta_cntrct_crs3` c_src
        JOIN `my_project.source_dataset.sof$ta_bp_ref` bp_src ON bp_src.cntrct_cp2_id = c_src.cntrct_parent
        WHERE CAST(c_src.cntrct_id AS STRING) = t.vertrag_id_carmen
          AND c_src.cntrct_ty = 20
          AND CAST(bp_src.bp_id AS STRING) = t.partner_id_carmen
      ); -- Should return 0

    -- Verify cntrct_ty <> 20 records use cntrct_id for bp_ref join
    SELECT COUNT(*) FROM `my_project.my_target_dataset.ta_vertrag_tmp` AS t
    WHERE t.cntrct_ty <> 20
      AND NOT EXISTS (
        SELECT 1 FROM `my_project.source_dataset.sof$ta_cntrct_crs3` c_src
        JOIN `my_project.source_dataset.sof$ta_bp_ref` bp_src ON bp_src.cntrct_cp2_id = c_src.cntrct_id
        WHERE CAST(c_src.cntrct_id AS STRING) = t.vertrag_id_carmen
          AND c_src.cntrct_ty <> 20
          AND CAST(bp_src.bp_id AS STRING) = t.partner_id_carmen
      ); -- Should return 0

    -- Verify no NULL cntrct_ty
    SELECT COUNT(*) FROM `my_project.my_target_dataset.ta_vertrag_tmp` WHERE cntrct_ty IS NULL; -- Should return 0
    ```

### VII. Data Quality and Schema Assertions

*   **Purpose**: To ensure the target table `ta_vertrag_tmp` adheres to expected data quality standards and schema definitions after migration.
*   **Setup**:
    1.  Run the migrated job with a full, representative dataset.
*   **Action**:
    1.  Query the schema of `my_project.my_target_dataset.ta_vertrag_tmp`.
    2.  Perform data quality checks on the populated table.
*   **Pass/Fail Criterion**:
    1.  **Schema Match**: The schema (column names, data types) of `my_project.my_target_dataset.ta_vertrag_tmp` must match the expected DDL provided in the migration design.
        ```python
        # Example pytest code for schema validation
        from google.cloud import bigquery

        def test_ta_vertrag_tmp_schema(bigquery_client):
            table_ref = bigquery_client.dataset('my_target_dataset').table('ta_vertrag_tmp')
            table = bigquery_client.get_table(table_ref)
            expected_schema = [
                bigquery.SchemaField("vertrag_id_carmen", "STRING"),
                bigquery.SchemaField("partner_id_carmen", "STRING"),
                bigquery.SchemaField("rechdef_id_carmen", "STRING"),
                bigquery.SchemaField("kundenkonto", "STRING"),
                bigquery.SchemaField("mwst_kennzeichen", "STRING"),
                bigquery.SchemaField("rahmenvertrag_id", "STRING"),
                bigquery.SchemaField("rechnungslauf", "STRING"),
                bigquery.SchemaField("vo_kenn", "STRING"),
                bigquery.SchemaField("order_number", "STRING"),
                bigquery.SchemaField("geplant_kuend", "DATE"),
                bigquery.SchemaField("eingang_kuend", "DATE"),
                bigquery.SchemaField("vertragsbeginn", "DATE"),
                bigquery.SchemaField("vertragsstatus", "STRING"),
                bigquery.SchemaField("sperrart", "STRING"),
                bigquery.SchemaField("sperrgrund", "STRING"),
                bigquery.SchemaField("stillegungszeitraum", "STRING"),
                bigquery.SchemaField("twincard", "STRING"),
                bigquery.SchemaField("dwh_tarifgr_text", "STRING"),
                bigquery.SchemaField("bindefrist", "INT64"),
                bigquery.SchemaField("letztes_upgrade", "DATE"),
                bigquery.SchemaField("vertragsbindung", "INT64"),
                bigquery.SchemaField("vertragsbindungseinheit", "STRING"),
                bigquery.SchemaField("rechnungszahlart", "STRING"),
                bigquery.SchemaField("rechnungsmedium", "STRING"),
                bigquery.SchemaField("twin_vertrag_id", "STRING"),
                bigquery.SchemaField("upgradeberechtigt", "STRING"),
                bigquery.SchemaField("apn", "STRING"),
                bigquery.SchemaField("upgradegrund", "STRING"),
                bigquery.SchemaField("SV_Id", "INT64"),
                bigquery.SchemaField("VDA", "STRING"),
                bigquery.SchemaField("cost_centre", "STRING"),
                bigquery.SchemaField("cost_centre_user", "STRING"),
                bigquery.SchemaField("cntrct_ty", "INT64"),
                bigquery.SchemaField("segment_id", "STRING"),
                bigquery.SchemaField("rv_action_id", "STRING"),
                bigquery.SchemaField("rechn_inh_konfig_text", "STRING"),
                bigquery.SchemaField("commitment_reference_date", "DATE"),
                bigquery.SchemaField("cntrct_validity_id", "STRING"),
            ]
            assert table.schema == expected_schema
        ```
    2.  **Non-Nullability of Key Fields**: `vertrag_id_carmen` should not be NULL.
        ```sql
        SELECT COUNT(*) FROM `my_project.my_target_dataset.ta_vertrag_tmp` WHERE vertrag_id_carmen IS NULL; -- Should be 0
        ```
    3.  **Uniqueness of Key Fields**: `vertrag_id_carmen` should be unique.
        ```sql
        SELECT vertrag_id_carmen FROM `my_project.my_target_dataset.ta_vertrag_tmp`
        GROUP BY vertrag_id_carmen HAVING COUNT(*) > 1; -- Should return 0 rows
        ```
    4.  **Date Format Consistency**: All date columns should contain valid dates or NULLs.
        ```sql
        SELECT COUNT(*) FROM `my_project.my_target_dataset.ta_vertrag_tmp`
        WHERE NOT (
            (geplant_kuend IS NULL OR SAFE.PARSE_DATE('%Y-%m-%d', CAST(geplant_kuend AS STRING)) IS NOT NULL) AND
            (eingang_kuend IS NULL OR SAFE.PARSE_DATE('%Y-%m-%d', CAST(eingang_kuend AS STRING)) IS NOT NULL) AND
            (vertragsbeginn IS NULL OR SAFE.PARSE_DATE('%Y-%m-%d', CAST(vertragsbeginn AS STRING)) IS NOT NULL) AND
            (letztes_upgrade IS NULL OR SAFE.PARSE_DATE('%Y-%m-%d', CAST(letztes_upgrade AS STRING)) IS NOT NULL) AND
            (commitment_reference_date IS NULL OR SAFE.PARSE_DATE('%Y-%m-%d', CAST(commitment_reference_date AS STRING)) IS NOT NULL)
        ); -- Should be 0
        ```
    5.  **Enum Value Checks**: For columns derived from `CASE` statements, ensure values are within the expected set.
        ```sql
        SELECT DISTINCT vertragsstatus FROM `my_project.my_target_dataset.ta_vertrag_tmp`
        WHERE vertragsstatus NOT IN ('A', 'L', ''); -- Should return 0 rows
        ```
        ```sql
        SELECT DISTINCT rechnungszahlart FROM `my_project.my_target_dataset.ta_vertrag_tmp`
        WHERE rechnungszahlart NOT IN ('U', 'E', 'K', 'B', ''); -- Should return 0 rows
        ```
        ```sql
        SELECT DISTINCT rechnungsmedium FROM `my_project.my_target_dataset.ta_vertrag_tmp`
        WHERE rechnungsmedium NOT IN ('Papier', 'ELMO', 'E-Mail', 'Fax', 'Inline/Papier', 'ELMO/Papier', ''); -- Should return 0 rows
        ```
        ```sql
        SELECT DISTINCT upgradeberechtigt FROM `my_project.my_target_dataset.ta_vertrag_tmp`
        WHERE upgradeberechtigt NOT IN ('J', 'N'); -- Should return 0 rows
        ```