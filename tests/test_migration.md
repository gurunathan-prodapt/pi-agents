As a senior data-migration QA engineer, I've developed a comprehensive suite of validation tests for the migration of `k_ausd_v_ta_discount_rr.ksh` and its dependent Oracle SQL script to Google Cloud BigQuery. These tests aim to ensure behavioral equivalence across output parity, transformation correctness, external system replacements, and data quality.

The tests are structured with a clear purpose, setup, action, and concrete pass/fail criteria. Where applicable, runnable SQL or conceptual Python (pytest) code snippets are provided.

**Assumptions for Testing:**
*   `your_gcp_project` and `isrpt_isbert_stage` are placeholders for actual GCP Project ID and BigQuery Dataset ID. These should be replaced with the correct values in all code snippets.
*   All Oracle source tables have been successfully ingested into their corresponding BigQuery tables with identical data for parity testing.
*   The BigQuery DDLs for `job_table`, `job_log`, and `sof_ta_discount_rr` (including the `processing_timestamp` column) have been executed.
*   The BigQuery Stored Procedure `sp_ausd_v_ta_discount_rr` has been deployed.
*   The Cloud Composer DAG `k_ausd_v_ta_discount_rr_orchestrator` has been deployed and configured with the correct BigQuery connection.

---

## 1. Output Parity Tests

### Test Case 1.1: End-to-End Data Parity (Main Output Table)

*   **Purpose:** Verify that the migrated BigQuery Stored Procedure produces the exact same data in the target table (`sof_ta_discount_rr`) as the legacy Oracle job. This covers all transformation logic (joins, filters, column mappings, type conversions, NULL handling).
*   **Setup:**
    1.  Ensure all source tables in Oracle (`dwtk_meldungen`, `cds$ta_discount_bc_assoc`, `cds$ta_discount`, `cds$ta_care_description`, `cds$ta_disc_vector`, `cds$ta_disc_invoice_item`) are populated with a representative, identical dataset.
    2.  Load the exact same data into their corresponding BigQuery tables (`your_gcp_project.isrpt_isbert_stage.dwtk_meldungen`, etc.).
    3.  Clear the target tables in both systems: `TRUNCATE TABLE SOF$TA_DISCOUNT_RR;` (Oracle) and `TRUNCATE TABLE your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr;` (BigQuery).
    4.  Define specific `p_JobKennung` (e.g., `'TEST_JOB_1'`) and `p_EintragsNr` (e.g., `'20231027_01'`) values.
*   **Action:**
    1.  **Legacy:** Execute the KornShell script with the defined parameters.
        ```bash
        k_ausd_v_ta_discount_rr.ksh -j TEST_JOB_1 -f 20231027_01
        ```
    2.  **Migrated:** Call the BigQuery Stored Procedure with the same parameters.
        ```sql
        CALL `your_gcp_project.isrpt_isbert_stage.sp_ausd_v_ta_discount_rr`('TEST_JOB_1', '20231027_01');
        ```
    3.  After both executions, extract the data from `SOF$TA_DISCOUNT_RR` (Oracle) and `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr` (BigQuery).
*   **Pass/Fail Criterion:**
    *   The row count in `SOF$TA_DISCOUNT_RR` (Oracle) must be identical to the row count in `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr` (BigQuery), filtering by `processing_timestamp` for the current run.
    *   A full data comparison (e.g., using `MINUS` in Oracle and `EXCEPT DISTINCT` in BigQuery, or hashing) must show no differences.
    *   **Example Pytest Assertion (conceptual):**
        ```python
        import pandas as pd
        from google.cloud import bigquery
        # import cx_Oracle # Uncomment if direct Oracle connection is used for comparison

        def test_sof_ta_discount_rr_data_parity():
            job_kennung = 'TEST_JOB_1'
            eintrags_nr = '20231027_01'
            bq_project = 'your_gcp_project'
            bq_dataset = 'isrpt_isbert_stage'

            # --- Oracle Execution and Data Extraction ---
            # This part assumes the ksh script has been run and its output
            # (or a snapshot of the Oracle table) is available for comparison.
            # For automated testing, you might need a way to trigger the KSH script
            # and extract data programmatically, or use a pre-migrated "golden" dataset.
            # Example: oracle_df = pd.read_sql("SELECT ... FROM SOF$TA_DISCOUNT_RR ORDER BY ...", oracle_conn)
            # For this example, let's assume `oracle_golden_df` is loaded from a known good state.
            oracle_golden_df = pd.DataFrame({ # Example structure, load from actual Oracle data
                'CNTRCT_ID': ['C1', 'C2'],
                'DISCOUNT_ID': ['D1', 'D2'],
                'DISC_VECTOR_TY': ['T1', 'T2'],
                'CNTRCT_OBJ_VERSION': [1, 2],
                'CNTRCT_TEMPLATE_ID': ['CT1', 'CT2'],
                'DISC_INVOICE_ITEM_ID': ['II1', 'II2'],
                'RABATT': ['Rabatt A', 'Rabatt B'],
                'RABATTHOEHE': [10.5, 20.0],
                'RABATTIERTE_RECH_POS': ['Pos A', 'Pos B']
            })
            oracle_golden_df = oracle_golden_df.sort_values(by=['CNTRCT_ID', 'DISCOUNT_ID']).reset_index(drop=True)


            # --- BigQuery Execution and Data Extraction ---
            bq_client = bigquery.Client(project=bq_project)
            sp_call_query = f"CALL `{bq_project}.{bq_dataset}.sp_ausd_v_ta_discount_rr`('{job_kennung}', '{eintrags_nr}');"
            bq_client.query(sp_call_query).result() # Execute SP

            bq_query = f"""
            SELECT
                cntrct_id, discount_id, disc_vector_ty, cntrct_obj_version, cntrct_template_id,
                disc_invoice_item_id, rabatt, rabatthoehe, rabattierte_rech_pos
            FROM
                `{bq_project}.{bq_dataset}.sof_ta_discount_rr`
            WHERE
                DATE(processing_timestamp) = CURRENT_DATE() -- Filter for records from this run
            ORDER BY cntrct_id, discount_id, disc_vector_ty
            """
            bq_df = bq_client.query(bq_query).to_dataframe()

            # --- Comparison ---
            assert len(oracle_golden_df) == len(bq_df), \
                f"Row count mismatch: Oracle={len(oracle_golden_df)}, BigQuery={len(bq_df)}"
            
            # Ensure column names match for comparison (BQ uses lowercase by default)
            bq_df.columns = [col.upper() for col in bq_df.columns]

            pd.testing.assert_frame_equal(
                oracle_golden_df, bq_df, 
                check_dtype=False, # Data types might differ (e.g., Oracle NUMBER vs BQ FLOAT64)
                check_exact=False, # Allow for floating point differences
                rtol=1e-6 # Relative tolerance for float comparison
            )
        ```

### Test Case 1.2: Job Control Table Parity (job_table and job_log)

*   **Purpose:** Verify that the `job_table` and `job_log` entries created/updated by the BigQuery Stored Procedure are behaviorally equivalent to the implicit job control logic of the legacy `starteSQLSkript` function.
*   **Setup:**
    1.  Ensure both Oracle (if `starteSQLSkript` logs there) and BigQuery `job_table` and `job_log` are empty or in a known, clean state.
    2.  Define `p_JobKennung = 'JOB_CONTROL_TEST'` and `p_EintragsNr = '20231027_02'`.
*   **Action:**
    1.  **Legacy:** Execute the KornShell script with the defined parameters.
        ```bash
        k_ausd_v_ta_discount_rr.ksh -j JOB_CONTROL_TEST -f 20231027_02
        ```
    2.  **Migrated:** Call the BigQuery Stored Procedure with the same parameters.
        ```sql
        CALL `your_gcp_project.isrpt_isbert_stage.sp_ausd_v_ta_discount_rr`('JOB_CONTROL_TEST', '20231027_02');
        ```
    3.  Query the `job_table` and `job_log` in BigQuery.
*   **Pass/Fail Criterion:**
    *   The `job_table` in BigQuery should contain an entry for `JOB_CONTROL_TEST`/`20231027_02` with `active_flag = FALSE` (after successful completion) and appropriate `created_at`/`updated_at` timestamps.
    *   The `job_log` in BigQuery should contain entries reflecting the job's lifecycle (start, success, record count), matching the expected behavior of the legacy `starteSQLSkript` and the KSH script's `print` statements.
    *   **Example SQL Assertion (BigQuery):**
        ```sql
        -- Check job_table entry
        SELECT
            job_name, job_kennung, eintrags_nr, active_flag
        FROM
            `your_gcp_project.isrpt_isbert_stage.job_table`
        WHERE
            job_kennung = 'JOB_CONTROL_TEST' AND eintrags_nr = '20231027_02';
        -- Expected result: A single row with ('ta_discount_rr', 'JOB_CONTROL_TEST', '20231027_02', FALSE)

        -- Check job_log entries for success
        SELECT
            status, message, records_processed
        FROM
            `your_gcp_project.isrpt_isbert_stage.job_log`
        WHERE
            job_kennung = 'JOB_CONTROL_TEST' AND eintrags_nr = '20231027_02'
            AND status = 'SUCCESS';
        -- Expected result: A single row with ('SUCCESS', '---------- ENDE Datenverarbeitung ----------', <expected_record_count>)
        ```

---

## 2. Transformation Correctness Tests

### Test Case 2.1: `v_process_date` Calculation Equivalence

*   **Purpose:** Verify that the BigQuery Stored Procedure calculates `v_process_date` identically to how `v_datum` is derived in the Oracle SQL script. This is crucial for date-based filtering.
*   **Setup:**
    1.  Populate `isbert_schema.dwtk_meldungen` (Oracle) and `your_gcp_project.isrpt_isbert_stage.dwtk_meldungen` (BigQuery) with identical test data, including various `timecreated` values and `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    2.  Include cases with `NULL` `timecreated` or no matching `job_kennung` to test the `NVL` / `IFNULL` and default '19000101' behavior.
*   **Action:**
    1.  **Legacy:** Manually execute the Oracle SQL snippet to get `v_datum`.
        ```sql
        COLUMN s_datum new_value v_datum noprint;
        SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum
        FROM isbert_schema.dwtk_meldungen m
        WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
        -- Then check &v_datum
        ```
    2.  **Migrated:** Execute the BigQuery SQL snippet for `v_process_date`.
        ```sql
        SELECT
          IFNULL(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101') AS v_process_date_bq
        FROM
          `your_gcp_project.isrpt_isbert_stage.dwtk_meldungen` AS m
        WHERE
          m.job_kennung = 'BERT_DROP_TEMP_TABLE';
        ```
*   **Pass/Fail Criterion:** The resulting date string (`v_process_date_bq`) from the BigQuery query must be identical to the `v_datum` obtained from Oracle for all test data scenarios.
    *   **Example SQL Assertion (BigQuery):**
        ```sql
        -- Test case 1: Normal data (assuming MAX(timecreated) for 'BERT_DROP_TEMP_TABLE' is '2023-10-26 10:00:00')
        SELECT
          (SELECT IFNULL(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
           FROM `your_gcp_project.isrpt_isbert_stage.dwtk_meldungen` AS m
           WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE') = '20231026' AS result;
        -- Expected: TRUE

        -- Test case 2: No matching job_kennung (assuming no 'NON_EXISTENT_JOB' in dwtk_meldungen)
        SELECT
          (SELECT IFNULL(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
           FROM `your_gcp_project.isrpt_isbert_stage.dwtk_meldungen` AS m
           WHERE m.job_kennung = 'NON_EXISTENT_JOB') = '19000101' AS result;
        -- Expected: TRUE
        ```

### Test Case 2.2: Join Logic and Filtering (Specific Scenarios)

*   **Purpose:** Verify that complex join conditions and `WHERE` clause filters (e.g., `LANGUAGE = 1`, `is_production = 1`, date range comparisons) are correctly translated and applied in BigQuery.
*   **Setup:**
    1.  Create a minimal dataset in Oracle and BigQuery source tables (`cds_ta_discount_bc_assoc`, `cds_ta_discount`, `cds_ta_care_description`, `cds_ta_disc_vector`, `cds_ta_disc_invoice_item`, `dwtk_meldungen`) that specifically tests:
        *   Rows that *should* be included (all join conditions met, all filters pass).
        *   Rows that *should* be excluded due to a failed join (e.g., no matching `discount_id`).
        *   Rows that *should* be excluded due to a `WHERE` clause filter (e.g., `LANGUAGE != 1`, `is_production = 0`, `insert_at` after `v_process_date`).
        *   Rows with `NULL` values in `modified_at` or `valid_to` to test `IS NULL OR ...` logic.
    2.  Ensure `dwtk_meldungen` is set to produce a specific `v_process_date` (e.g., '20231020').
    3.  Clear target tables.
*   **Action:**
    1.  **Legacy:** Execute the KornShell script with appropriate parameters.
    2.  **Migrated:** Call the BigQuery Stored Procedure with the same parameters.
    3.  Query the `SOF$TA_DISCOUNT_RR` (Oracle) and `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr` (BigQuery) tables.
*   **Pass/Fail Criterion:**
    *   The specific test rows should appear in the target table if they meet all conditions, and be absent if they fail any condition, identically in both Oracle and BigQuery.
    *   The full data parity check (as in Test Case 1.1) should pass for this controlled dataset.
    *   **Example SQL Assertion (BigQuery, after SP run, assuming `v_process_date` was '20231020'):**
        ```sql
        -- Setup: A record in source tables where cds_ta_care_description.LANGUAGE = 2
        -- Expected: This record should NOT be in the target table.
        SELECT COUNT(*) FROM `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr`
        WHERE rabatt = 'Rabatt_Lang_2_Value' AND DATE(processing_timestamp) = CURRENT_DATE();
        -- Expected: 0

        -- Setup: A record in source tables where cds_ta_discount.is_production = 0
        -- Expected: This record should NOT be in the target table.
        SELECT COUNT(*) FROM `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr`
        WHERE discount_id = 'NON_PROD_DISCOUNT' AND DATE(processing_timestamp) = CURRENT_DATE();
        -- Expected: 0

        -- Setup: A record where da.insert_at = '2023-10-21' (after v_process_date)
        -- Expected: This record should NOT be in the target table.
        SELECT COUNT(*) FROM `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr`
        WHERE discount_id = 'FUTURE_INSERT_DISCOUNT' AND DATE(processing_timestamp) = CURRENT_DATE();
        -- Expected: 0
        ```

### Test Case 2.3: NULL Handling in Date Filters

*   **Purpose:** Specifically verify the correct translation of Oracle's `(column IS NULL OR column > date_value)` logic to BigQuery.
*   **Setup:**
    1.  Populate source tables with records where `modified_at` and `valid_to` columns have:
        *   Actual date values (both before and after `v_process_date`).
        *   `NULL` values.
    2.  Ensure `dwtk_meldungen` is set to produce a specific `v_process_date` (e.g., '20231020').
    3.  Clear target tables.
*   **Action:**
    1.  **Legacy:** Execute the KornShell script.
    2.  **Migrated:** Call the BigQuery Stored Procedure.
    3.  Query the `SOF$TA_DISCOUNT_RR` (Oracle) and `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr` (BigQuery) tables.
*   **Pass/Fail Criterion:** Records with `NULL` in `modified_at` or `valid_to` should be included in the output if all other conditions are met, as per the `OR` clause. Records with dates *before or equal to* `v_process_date` should be excluded if the condition is `> v_process_date`. Records with dates *after* `v_process_date` should be included. This behavior must be identical in both systems.
    *   **Example SQL Assertion (BigQuery, after SP run, assuming `v_process_date` was '20231020'):**
        ```sql
        -- Setup: A test record with discount_id = 'NULL_MODIFIED_DATE_TEST' where da.modified_at = NULL
        -- Expected: Should be present in output
        SELECT COUNT(*) FROM `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr`
        WHERE discount_id = 'NULL_MODIFIED_DATE_TEST' AND DATE(processing_timestamp) = CURRENT_DATE();
        -- Expected: 1

        -- Setup: A test record with discount_id = 'OLD_VALID_TO_DATE_TEST' where d.valid_to = '2023-10-01'
        -- Expected: Should be excluded (valid_to is NOT NULL AND valid_to <= v_process_date)
        SELECT COUNT(*) FROM `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr`
        WHERE discount_id = 'OLD_VALID_TO_DATE_TEST' AND DATE(processing_timestamp) = CURRENT_DATE();
        -- Expected: 0

        -- Setup: A test record with discount_id = 'FUTURE_VALID_TO_DATE_TEST' where d.valid_to = '2023-11-01'
        -- Expected: Should be included (valid_to is NOT NULL AND valid_to > v_process_date)
        SELECT COUNT(*) FROM `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr`
        WHERE discount_id = 'FUTURE_VALID_TO_DATE_TEST' AND DATE(processing_timestamp) = CURRENT_DATE();
        -- Expected: 1
        ```

### Test Case 2.4: Data Type Conversion and Function Equivalence

*   **Purpose:** Verify that Oracle functions like `NVL`, `TO_CHAR`, `TO_DATE` and implicit type conversions are correctly handled by their BigQuery equivalents (`IFNULL`, `FORMAT_DATE`, `PARSE_DATE`) and that column data types are preserved or correctly cast. Specifically check `rabatthoehe` (FLOAT64).
*   **Setup:**
    1.  Populate source tables with data that exercises various data types and potential conversion issues:
        *   `CALC_RULE_VALUE` (Oracle NUMBER, BigQuery FLOAT64) with integer, decimal, and NULL values.
        *   Date columns with various formats (if applicable, though `timecreated` is likely TIMESTAMP).
        *   String columns that might contain special characters.
    2.  Clear target tables.
*   **Action:**
    1.  **Legacy:** Execute the KornShell script.
    2.  **Migrated:** Call the BigQuery Stored Procedure.
    3.  Query the `SOF$TA_DISCOUNT_RR` (Oracle) and `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr` (BigQuery) tables, focusing on specific columns like `rabatthoehe`, `rabatt`, `rabattierte_rech_pos`.
*   **Pass/Fail Criterion:**
    *   Numeric values (e.g., `rabatthoehe`) must match exactly (within floating-point precision).
    *   String values must match character-for-character.
    *   NULL values must be handled identically.
    *   **Example SQL Assertion (BigQuery, after SP run):**
        ```sql
        -- Setup: A test record with discount_id = 'TYPE_CONV_TEST_DECIMAL' where dv.CALC_RULE_VALUE = 123.45
        SELECT rabatthoehe FROM `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr`
        WHERE discount_id = 'TYPE_CONV_TEST_DECIMAL' AND DATE(processing_timestamp) = CURRENT_DATE();
        -- Expected: 123.45 (FLOAT64)

        -- Setup: A test record with discount_id = 'TYPE_CONV_TEST_INTEGER' where dv.CALC_RULE_VALUE = 50
        SELECT rabatthoehe FROM `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr`
        WHERE discount_id = 'TYPE_CONV_TEST_INTEGER' AND DATE(processing_timestamp) = CURRENT_DATE();
        -- Expected: 50.0 (FLOAT64, as it's a FLOAT64 column)

        -- Setup: A test record with discount_id = 'NULL_CALC_TEST' where dv.CALC_RULE_VALUE = NULL
        SELECT rabatthoehe IS NULL FROM `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr`
        WHERE discount_id = 'NULL_CALC_TEST' AND DATE(processing_timestamp) = CURRENT_DATE();
        -- Expected: TRUE
        ```

---

## 3. External-System Replacements Tests

### Test Case 3.1: `DWPA_UTIL_SKRIPT` Replacement (`TRUNCATE TABLE`)

*   **Purpose:** Verify that the BigQuery `TRUNCATE TABLE` command within the SP correctly clears the target table `sof_ta_discount_rr` before insertion, mimicking the `DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_discount_rr');` behavior.
*   **Setup:**
    1.  Populate `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr` with some dummy data (e.g., `cntrct_id = 'DUMMY_OLD_DATA'`).
    2.  Ensure source tables are populated such that the subsequent `INSERT` will produce at least one row.
    3.  Define `p_JobKennung = 'TRUNCATE_TEST'` and `p_EintragsNr = '20231027_03'`.
*   **Action:**
    1.  Call the BigQuery Stored Procedure:
        ```sql
        CALL `your_gcp_project.isrpt_isbert_stage.sp_ausd_v_ta_discount_rr`('TRUNCATE_TEST', '20231027_03');
        ```
    2.  Immediately after the call, query the `sof_ta_discount_rr` table.
*   **Pass/Fail Criterion:**
    *   The `sof_ta_discount_rr` table should contain only the records inserted by the current run of the stored procedure, and no prior dummy data.
    *   The count of records in `sof_ta_discount_rr` (filtered by `processing_timestamp`) should match the `v_records` reported in `job_log` for this run.
    *   **Example SQL Assertion (BigQuery):**
        ```sql
        -- Before SP call:
        -- INSERT INTO `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr` (cntrct_id, discount_id, processing_timestamp) VALUES ('DUMMY_OLD_DATA', 'D1', '2023-10-26 00:00:00');
        -- SELECT COUNT(*) FROM `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr`; -- Should be > 0

        CALL `your_gcp_project.isrpt_isbert_stage.sp_ausd_v_ta_discount_rr`('TRUNCATE_TEST', '20231027_03');

        -- After SP call:
        SELECT COUNT(*) FROM `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr`
        WHERE cntrct_id = 'DUMMY_OLD_DATA';
        -- Expected: 0 (old data should be truncated)

        SELECT COUNT(*) FROM `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr`
        WHERE DATE(processing_timestamp) = CURRENT_DATE();
        -- Expected: > 0 (new data should be inserted)
        ```

---

## 4. Data Quality / Row Count / Schema Assertions

### Test Case 4.1: Row Count Validation (Main Output Table)

*   **Purpose:** Verify that the total number of rows processed and inserted into `sof_ta_discount_rr` by the BigQuery Stored Procedure matches the count from the legacy Oracle job. This is a high-level check for data completeness.
*   **Setup:**
    1.  Identical source data in Oracle and BigQuery (as in Test Case 1.1).
    2.  Clear target tables.
    3.  Define specific `p_JobKennung` and `p_EintragsNr` values.
*   **Action:**
    1.  **Legacy:** Execute the KornShell script. Capture the `v_records` value printed or from the temporary file.
    2.  **Migrated:** Call the BigQuery Stored Procedure. The procedure returns `records_processed`.
    3.  Query `SOF$TA_DISCOUNT_RR` (Oracle) and `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr` (BigQuery) to get actual row counts.
*   **Pass/Fail Criterion:**
    *   The `v_records` reported by the legacy KSH script must match the `records_processed` returned by the BigQuery Stored Procedure.
    *   The `COUNT(*)` from `SOF$TA_DISCOUNT_RR` (Oracle) must match `COUNT(*)` from `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr` (BigQuery, filtered by `processing_timestamp`).
    *   **Example Pytest Assertion (conceptual):**
        ```python
        def test_output_row_count_parity():
            job_kennung = 'COUNT_TEST'
            eintrags_nr = '20231027_06'

            # Assume legacy_record_count is obtained from running the KSH script
            # and parsing its output or temp file.
            legacy_record_count = 12345 # Replace with actual count from legacy run

            bq_client = bigquery.Client(project='your_gcp_project')
            sp_call_query = f"CALL `your_gcp_project.isrpt_isbert_stage.sp_ausd_v_ta_discount_rr`('{job_kennung}', '{eintrags_nr}');"
            query_job = bq_client.query(sp_call_query)
            results = query_job.result()
            bq_record_count = next(results).records_processed # Get the returned value

            assert bq_record_count == legacy_record_count, \
                f"Record count mismatch from SP return: Legacy={legacy_record_count}, BigQuery={bq_record_count}"

            # Also verify against actual table count
            bq_table_count_query = f"""
            SELECT COUNT(*) FROM `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr`
            WHERE DATE(processing_timestamp) = CURRENT_DATE();
            """
            bq_table_count = bq_client.query(bq_table_count_query).to_dataframe().iloc[0,0]
            assert bq_table_count == legacy_record_count, \
                f"Table count mismatch: Expected={legacy_record_count}, Actual={bq_table_count}"
        ```

### Test Case 4.2: Schema Validation (Main Output Table)

*   **Purpose:** Verify that the schema of the target BigQuery table `sof_ta_discount_rr` matches the expected schema derived from the Oracle `SOF$TA_DISCOUNT_RR` table, including column names, data types, and nullability.
*   **Setup:**
    1.  Ensure the BigQuery table `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr` is created using the provided DDL.
*   **Action:**
    1.  Retrieve the schema of `SOF$TA_DISCOUNT_RR` from Oracle.
    2.  Retrieve the schema of `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr` from BigQuery.
*   **Pass/Fail Criterion:**
    *   All column names must match (BigQuery column names are typically lowercase in DDL, but the query will use them as defined).
    *   Data types must be compatible and correctly mapped (e.g., Oracle NUMBER to BigQuery INT64/FLOAT64, Oracle VARCHAR2 to BigQuery STRING, Oracle DATE/TIMESTAMP to BigQuery DATE/TIMESTAMP).
    *   Nullability constraints should be consistent.
    *   **Example Pytest Assertion (conceptual):**
        ```python
        from google.cloud import bigquery

        def test_sof_ta_discount_rr_schema():
            bq_client = bigquery.Client(project='your_gcp_project')
            table_id = "your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr"
            table = bq_client.get_table(table_id)

            # Expected schema based on Oracle and migration design
            expected_schema = {
                'cntrct_id': ('STRING', 'NULLABLE'),
                'discount_id': ('STRING', 'NULLABLE'),
                'disc_vector_ty': ('STRING', 'NULLABLE'),
                'cntrct_obj_version': ('INT64', 'NULLABLE'),
                'cntrct_template_id': ('STRING', 'NULLABLE'),
                'disc_invoice_item_id': ('STRING', 'NULLABLE'),
                'rabatt': ('STRING', 'NULLABLE'),
                'rabatthoehe': ('FLOAT64', 'NULLABLE'),
                'rabattierte_rech_pos': ('STRING', 'NULLABLE'),
                'processing_timestamp': ('TIMESTAMP', 'NULLABLE') # Added for partitioning/tracking
            }

            actual_schema = {field.name: (field.field_type, field.mode) for field in table.schema}

            assert len(actual_schema) == len(expected_schema), \
                f"Schema length mismatch. Expected {len(expected_schema)}, Got {len(actual_schema)}"

            for col_name, (expected_type, expected_mode) in expected_schema.items():
                assert col_name in actual_schema, f"Column '{col_name}' missing in BigQuery schema."
                actual_type, actual_mode = actual_schema[col_name]
                assert actual_type == expected_type, \
                    f"Type mismatch for column '{col_name}': Expected {expected_type}, Got {actual_type}"
                assert actual_mode == expected_mode, \
                    f"Nullability mismatch for column '{col_name}': Expected {expected_mode}, Got {actual_mode}"
        ```

### Test Case 4.3: Data Integrity (NULLs, Ranges, Uniqueness)

*   **Purpose:** Verify that the migrated data maintains expected data integrity constraints, such as non-nullability for critical fields, reasonable value ranges, and uniqueness where implied by business logic.
*   **Setup:**
    1.  Run the BigQuery Stored Procedure with a full dataset (as in Test Case 1.1).
*   **Action:**
    1.  Query the `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr` table to check for:
        *   `NULL` values in columns that are implicitly or explicitly NOT NULL in Oracle.
        *   Values outside expected ranges (e.g., `rabatthoehe` should be positive).
        *   Duplicate primary keys (if `(cntrct_id, discount_id, disc_vector_ty)` is a logical primary key).
*   **Pass/Fail Criterion:**
    *   No `NULL` values in columns expected to be NOT NULL.
    *   All numeric values are within business-defined valid ranges.
    *   No duplicate rows based on logical primary key.
    *   **Example SQL Assertion (BigQuery, after SP run):**
        ```sql
        -- Check for NULLs in critical fields (assuming cntrct_id, discount_id are NOT NULL based on joins)
        SELECT COUNT(*) FROM `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr`
        WHERE cntrct_id IS NULL OR discount_id IS NULL OR disc_vector_ty IS NULL
        AND DATE(processing_timestamp) = CURRENT_DATE();
        -- Expected: 0

        -- Check for rabatthoehe being non-negative (assuming business rule)
        SELECT COUNT(*) FROM `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr`
        WHERE rabatthoehe < 0 AND DATE(processing_timestamp) = CURRENT_DATE();
        -- Expected: 0

        -- Check for uniqueness of (cntrct_id, discount_id, disc_vector_ty) if it's a logical key
        SELECT
            cntrct_id, discount_id, disc_vector_ty, COUNT(*)
        FROM
            `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr`
        WHERE DATE(processing_timestamp) = CURRENT_DATE()
        GROUP BY
            cntrct_id, discount_id, disc_vector_ty
        HAVING
            COUNT(*) > 1;
        -- Expected: 0 rows
        ```

### Test Case 4.4: Parameter Validation and Error Handling

*   **Purpose:** Verify that the BigQuery Stored Procedure correctly validates input parameters (`p_JobKennung`, `p_EintragsNr`) and handles errors by raising an exception and logging to `job_log`, mimicking the KSH script's `pruefeParameterGesetzt` and `DWMSG_MeldeFehler` behavior.
*   **Setup:**
    1.  Ensure `job_log` table is empty or in a clean state.
*   **Action:**
    1.  **Missing `p_JobKennung`:** Attempt to call SP with `NULL` or empty `p_JobKennung`.
        ```sql
        -- This call is expected to fail and raise an error
        CALL `your_gcp_project.isrpt_isbert_stage.sp_ausd_v_ta_discount_rr`(NULL, '20231027_04_A');
        -- or
        -- CALL `your_gcp_project.isrpt_isbert_stage.sp_ausd_v_ta_discount_rr`('', '20231027_04_B');
        ```
    2.  **Missing `p_EintragsNr`:** Attempt to call SP with `NULL` or empty `p_EintragsNr`.
        ```sql
        -- This call is expected to fail and raise an error
        CALL `your_gcp_project.isrpt_isbert_stage.sp_ausd_v_ta_discount_rr`('ERROR_TEST', NULL);
        ```
*   **Pass/Fail Criterion:**
    *   Each call should result in a BigQuery error/exception being raised, preventing further execution of the SP.
    *   An entry should be present in `your_gcp_project.isrpt_isbert_stage.job_log` with `status = 'ERROR'` and a descriptive `message` (e.g., 'Fehler: Jobkennung fehlt. JobKennung: NULL').
    *   No records should be inserted into `sof_ta_discount_rr` by these failed calls.
    *   **Example SQL Assertion (BigQuery, after attempting SP calls):**
        ```sql
        -- After attempting CALL `sp_ausd_v_ta_discount_rr`(NULL, '20231027_04_A');
        SELECT
            status, message
        FROM
            `your_gcp_project.isrpt_isbert_stage.job_log`
        WHERE
            eintrags_nr = '20231027_04_A';
        -- Expected: A single row with ('ERROR', 'Fehler: Jobkennung fehlt. JobKennung: NULL')

        -- After attempting CALL `sp_ausd_v_ta_discount_rr`('ERROR_TEST', NULL);
        SELECT
            status, message
        FROM
            `your_gcp_project.isrpt_isbert_stage.job_log`
        WHERE
            job_kennung = 'ERROR_TEST';
        -- Expected: A single row with ('ERROR', 'Fehler: EintragsNr fehlt. EintragsNr: NULL')

        -- Verify no data was inserted by these error calls
        SELECT COUNT(*) FROM `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr`
        WHERE DATE(processing_timestamp) = CURRENT_DATE();
        -- Expected: 0 (assuming no other successful runs today)
        ```

### Test Case 4.5: Job Active/Deactivation Logic

*   **Purpose:** Verify that the BigQuery Stored Procedure correctly handles the job control logic: deactivating older active jobs and skipping execution if the current job is already active.
*   **Setup:**
    1.  Ensure `job_table` is empty or in a clean state.
    2.  Define `p_JobKennung = 'ACTIVE_JOB_TEST'` and `p_EintragsNr = '20231027_05'`.
*   **Action:**
    1.  **Scenario A: Deactivate older active jobs.**
        *   Manually insert an active job entry into `job_table` for `job_name = 'ta_discount_rr'` but with a *different* `eintrags_nr` (e.g., '20231027_00').
            ```sql
            INSERT INTO `your_gcp_project.isrpt_isbert_stage.job_table` (job_name, job_kennung, eintrags_nr, active_flag, created_at)
            VALUES ('ta_discount_rr', 'OLD_ACTIVE_JOB', '20231027_00', TRUE, CURRENT_TIMESTAMP());
            ```
        *   Call the SP with `p_EintragsNr = '20231027_05'`.
            ```sql
            CALL `your_gcp_project.isrpt_isbert_stage.sp_ausd_v_ta_discount_rr`('ACTIVE_JOB_TEST', '20231027_05');
            ```
    2.  **Scenario B: Skip if current job is active.**
        *   Manually insert an active job entry into `job_table` for `job_name = 'ta_discount_rr'` with the *same* `eintrags_nr` as the upcoming call.
            ```sql
            INSERT INTO `your_gcp_project.isrpt_isbert_stage.job_table` (job_name, job_kennung, eintrags_nr, active_flag, created_at)
            VALUES ('ta_discount_rr', 'ACTIVE_JOB_TEST', '20231027_05', TRUE, CURRENT_TIMESTAMP());
            ```
        *   Call the SP with `p_EintragsNr = '20231027_05'`.
            ```sql
            CALL `your_gcp_project.isrpt_isbert_stage.sp_ausd_v_ta_discount_rr`('ACTIVE_JOB_TEST', '20231027_05');
            ```
*   **Pass/Fail Criterion:**
    *   **Scenario A:** The `job_table` entry for `20231027_00` should be updated to `active_flag = FALSE`. The SP should execute successfully, and a new entry for `20231027_05` should be created (and eventually set to `FALSE`).
    *   **Scenario B:** The SP call should complete without error, but no data transformation should occur. The `job_log` should contain an entry with `status = 'SKIPPED'` and `message = 'Aktiver Job ignoriert, da bereits aktiv'`. The `job_table` entry for `20231027_05` should remain `active_flag = TRUE` (or whatever its state was before the call, as the SP should exit early).
    *   **Example SQL Assertion (BigQuery):**
        ```sql
        -- After Scenario A:
        SELECT active_flag FROM `your_gcp_project.isrpt_isbert_stage.job_table`
        WHERE eintrags_nr = '20231027_00';
        -- Expected: FALSE

        SELECT active_flag FROM `your_gcp_project.isrpt_isbert_stage.job_table`
        WHERE eintrags_nr = '20231027_05' AND job_kennung = 'ACTIVE_JOB_TEST';
        -- Expected: FALSE (after successful run)

        -- After Scenario B:
        SELECT status, message FROM `your_gcp_project.isrpt_isbert_stage.job_log`
        WHERE job_kennung = 'ACTIVE_JOB_TEST' AND eintrags_nr = '20231027_05'
        ORDER BY created_at DESC LIMIT 1;
        -- Expected: ('SKIPPED', 'Aktiver Job ignoriert, da bereits aktiv')

        -- Verify no data was inserted by the skipped run
        SELECT COUNT(*) FROM `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr`
        WHERE DATE(processing_timestamp) = CURRENT_DATE();
        -- Expected: 0 (assuming no other successful runs today)
        ```

### Test Case 4.6: Cloud Composer Orchestration

*   **Purpose:** Verify that the Cloud Composer DAG correctly triggers the BigQuery Stored Procedure with the expected parameters and handles success/failure.
*   **Setup:**
    1.  Deploy the `k_ausd_v_ta_discount_rr_orchestrator` DAG to Cloud Composer.
    2.  Ensure BigQuery connection `google_cloud_default` is configured in Airflow.
    3.  Ensure source data is available in BigQuery.
    4.  Clear target tables and job logs.
*   **Action:**
    1.  Manually trigger the `k_ausd_v_ta_discount_rr_orchestrator` DAG in Airflow.
    2.  Observe the DAG run in Airflow UI for success/failure.
    3.  Query `job_log` and `sof_ta_discount_rr` in BigQuery.
*   **Pass/Fail Criterion:**
    *   The DAG run in Airflow should succeed.
    *   The `BigQueryExecuteStoredProcedureOperator` task should complete successfully.
    *   The `job_log` table in BigQuery should contain entries for the run, including a 'SUCCESS' status.
    *   The `sof_ta_discount_rr` table should be populated with data, matching expected row counts.
    *   The `p_JobKennung` and `p_EintragsNr` passed to the SP (e.g., `TA_DISCOUNT_RR`, `{{ ds_nodash }}`) should be correctly reflected in the `job_log` entries.
    *   **Example SQL Assertion (BigQuery, after DAG run):**
        ```sql
        SELECT
            status, message, records_processed
        FROM
            `your_gcp_project.isrpt_isbert_stage.job_log`
        WHERE
            job_kennung = 'TA_DISCOUNT_RR' AND eintrags_nr = FORMAT_DATE('%Y%m%d', CURRENT_DATE())
        ORDER BY created_at DESC LIMIT 1;
        -- Expected: ('SUCCESS', '---------- ENDE Datenverarbeitung ----------', <expected_record_count>)

        SELECT COUNT(*) FROM `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr`
        WHERE DATE(processing_timestamp) = CURRENT_DATE();
        -- Expected: <expected_record_count>
        ```