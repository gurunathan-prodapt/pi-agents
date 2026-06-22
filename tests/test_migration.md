As a senior data-migration QA engineer, I've analyzed the migration design document and the provided BigQuery SQL and Airflow DAG for `DW.BERT_AUSD_V_TA_CNTRCT_CRS3`. The tests below are designed to ensure the migrated solution is functionally equivalent and robust.

---

## Migration Validation Tests: DW.BERT_AUSD_V_TA_CNTRCT_CRS3

### Test Case 1: Output Parity - Full Data Comparison

*   **Purpose:** To verify that the migrated BigQuery job produces an identical final dataset in `sof_dataset_target.ta_cntrct_crs3` compared to the legacy Oracle job's output in `sof$ta_cntrct_crs3`, given the same input data. This is the ultimate behavioral equivalence test.
*   **Setup:**
    1.  **Baseline Data:** Capture a snapshot of the source tables (`isbert_schema.dwtk_meldungen`, `sof$ta_cntrct_crs2`) from the legacy Oracle system.
    2.  **Legacy Run:** Execute the legacy Oracle job `DW.BERT_AUSD_V_TA_CNTRCT_CRS3` with the baseline data.
    3.  **Legacy Output:** Extract the full content of the resulting `sof$ta_cntrct_crs3` table from Oracle into a "golden" CSV or Parquet file.
    4.  **BigQuery Input:** Load the baseline data into the corresponding BigQuery source tables (`isbert_schema_target.dwtk_meldungen`, `sof_dataset_target.ta_cntrct_crs2`).
    5.  **BigQuery Target Schema:** Ensure `sof_dataset_target.ta_cntrct_crs3` exists with the correct schema in BigQuery.
*   **Action:**
    1.  Execute the Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs3` in the BigQuery environment.
    2.  After successful completion, extract the full content of `sof_dataset_target.ta_cntrct_crs3` from BigQuery.
*   **Pass/Fail Criterion:**
    *   The row count of the BigQuery output table must exactly match the row count of the legacy Oracle output table.
    *   A deep comparison (e.g., using checksums, row-by-row comparison after sorting) of the extracted BigQuery data and the "golden" legacy data must show no differences. All columns, including `twinbill` and `twin_vertrag_id`, must match exactly, accounting for potential NULL vs. empty string differences if applicable (though BigQuery `NULL` and Oracle `NULL` should map directly).

    ```python
    import pandas as pd
    from google.cloud import bigquery
    import os

    def test_output_parity_full_data_comparison():
        bq_client = bigquery.Client()

        # --- Setup: Load Golden Data (assuming it's pre-generated) ---
        # This golden_data_path should point to the extracted legacy output
        golden_data_path = "path/to/legacy_sof_ta_cntrct_crs3_golden.csv"
        assert os.path.exists(golden_data_path), "Golden data file not found."
        golden_df = pd.read_csv(golden_data_path, keep_default_na=True) # keep_default_na handles 'NA' as NaN

        # --- Action: Query BigQuery Output ---
        bq_table_id = "`your-gcp-project.sof_dataset_target.ta_cntrct_crs3`"
        query = f"SELECT * FROM {bq_table_id} ORDER BY cntrct_id, twin_vertrag_id" # Order for consistent comparison
        bq_df = bq_client.query(query).to_dataframe()

        # --- Pass/Fail Criterion ---
        # 1. Row Count Check
        assert len(bq_df) == len(golden_df), \
            f"Row count mismatch: BigQuery has {len(bq_df)} rows, Legacy has {len(golden_df)} rows."

        # 2. Column Name Check (ensure schemas are aligned)
        assert set(bq_df.columns) == set(golden_df.columns), \
            f"Column mismatch: BigQuery columns {bq_df.columns}, Legacy columns {golden_df.columns}"

        # 3. Deep Data Comparison
        # Sort both DataFrames for reliable row-by-row comparison
        golden_df_sorted = golden_df.sort_values(by=['cntrct_id', 'twin_vertrag_id']).reset_index(drop=True)
        bq_df_sorted = bq_df.sort_values(by=['cntrct_id', 'twin_vertrag_id']).reset_index(drop=True)

        # Convert object columns to string to handle potential mixed types or NaN representation differences
        for col in bq_df_sorted.select_dtypes(include=['object']).columns:
            bq_df_sorted[col] = bq_df_sorted[col].astype(str)
        for col in golden_df_sorted.select_dtypes(include=['object']).columns:
            golden_df_sorted[col] = golden_df_sorted[col].astype(str)

        # Compare DataFrames
        pd.testing.assert_frame_equal(
            bq_df_sorted,
            golden_df_sorted,
            check_dtype=True, # Ensure data types are consistent
            check_exact=False, # Allow for floating point differences if applicable (not expected here)
            atol=1e-9 # Absolute tolerance for numerical comparisons
        )
        print("Full data comparison passed: BigQuery output matches legacy golden data.")

    # To run this test:
    # 1. Ensure you have a 'legacy_sof_ta_cntrct_crs3_golden.csv' file.
    # 2. Replace 'path/to/legacy_sof_ta_cntrct_crs3_golden.csv' with the actual path.
    # 3. Replace 'your-gcp-project' with your GCP project ID.
    # 4. Install pandas and google-cloud-bigquery: pip install pandas google-cloud-bigquery
    # 5. Run with pytest: pytest your_test_file.py::test_output_parity_full_data_comparison
    ```

### Test Case 2: Transformation Correctness - `v_datum` Retrieval

*   **Purpose:** To verify that the `v_datum` variable is correctly derived from `isbert_schema_target.dwtk_meldungen` as specified, including the default value for edge cases.
*   **Setup:**
    1.  **Scenario A (Data Exists):** Populate `isbert_schema_target.dwtk_meldungen` with a row where `job_kennung = 'BERT_DROP_TEMP_TABLE'` and `timecreated` is a specific date (e.g., '2023-03-15 10:00:00 UTC').
    2.  **Scenario B (No Data):** Ensure `isbert_schema_target.dwtk_meldungen` is empty or contains no rows with `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
*   **Action:**
    1.  Execute only the `DECLARE` and `SET v_datum` part of the BigQuery SQL for Scenario A and B.
    2.  Retrieve the value of `v_datum` after execution.
*   **Pass/Fail Criterion:**
    *   **Scenario A:** `v_datum` must be '20230315'.
    *   **Scenario B:** `v_datum` must be '19000101'.

    ```sql
    -- Setup for Scenario A:
    -- INSERT INTO `isbert_schema_target.dwtk_meldungen` (job_kennung, timecreated) VALUES ('BERT_DROP_TEMP_TABLE', '2023-03-15 10:00:00 UTC');

    -- Setup for Scenario B:
    -- DELETE FROM `isbert_schema_target.dwtk_meldungen` WHERE job_kennung = 'BERT_DROP_TEMP_TABLE';

    -- Action & Assertion:
    DECLARE v_datum STRING;
    SET v_datum = (
      SELECT
        IFNULL(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
      FROM
        `isbert_schema_target.dwtk_meldungen` AS m
      WHERE
        m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    );
    SELECT v_datum; -- Verify the output
    ```

### Test Case 3: Transformation Correctness - Truncate Behavior

*   **Purpose:** To confirm that the target table `sof_dataset_target.ta_cntrct_crs3` is truncated before new data is inserted.
*   **Setup:**
    1.  Populate `sof_dataset_target.ta_cntrct_crs3` with some dummy data (e.g., 5 rows).
    2.  Populate source tables (`isbert_schema_target.dwtk_meldungen`, `sof_dataset_target.ta_cntrct_crs2`) with data that would result in fewer than 5 rows being inserted.
*   **Action:**
    1.  Execute the Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs3`.
*   **Pass/Fail Criterion:**
    *   The final row count of `sof_dataset_target.ta_cntrct_crs3` must match the expected number of rows generated by the `INSERT` statement, not the sum of initial dummy data and new data. This implicitly confirms truncation.
    *   Alternatively, query the table immediately after the `TRUNCATE` step (if run as separate steps, which it isn't in the provided DAG, but could be for a more granular test) to ensure it's empty.

    ```sql
    -- Pre-check (before running the DAG):
    -- INSERT INTO `sof_dataset_target.ta_cntrct_crs3` (cntrct_id, ...) VALUES (1, ...), (2, ...), ...;
    -- SELECT COUNT(*) FROM `sof_dataset_target.ta_cntrct_crs3`; -- Should be > 0

    -- After running the DAG:
    -- SELECT COUNT(*) FROM `sof_dataset_target.ta_cntrct_crs3`;
    -- This count should match the expected output from the INSERT logic, not the pre-existing count.
    ```

### Test Case 4: Transformation Correctness - `UNION DISTINCT` and Join Logic

*   **Purpose:** To validate the core `UNION DISTINCT` logic, including the `LEFT JOIN` and `JOIN` conditions, filtering, and the correct assignment of `twinbill` and `twin_vertrag_id`.
*   **Setup:** Create a `sof_dataset_target.ta_cntrct_crs2` table with various scenarios:
    *   **Scenario A:** A parent contract `c` (`cntrct_ty` not 10 or 20) with a child `ctb` (`cntrct_ty = 20`).
    *   **Scenario B:** A parent contract `c` (`cntrct_ty` not 10 or 20) with NO child `ctb` (`cntrct_ty = 20`).
    *   **Scenario C:** A parent contract `c` (`cntrct_ty` is 10 or 20) with a child `ctb` (`cntrct_ty = 20`). (Should be filtered out by `c.cntrct_ty NOT IN (10, 20)`).
    *   **Scenario D:** A child contract `ctb` (`cntrct_ty = 20`) whose parent `c` (`cntrct_ty` not 10 or 20) is also present.
    *   **Scenario E:** A child contract `ctb` (`cntrct_ty = 20`) whose parent `c` (`cntrct_ty` is 10 or 20). (Should be filtered out by `c.cntrct_ty NOT IN (10, 20)` in the second `UNION` part).
    *   **Scenario F:** Contracts with `cntrct_ty = 10` or `cntrct_ty = 20` that are not part of a parent-child relationship. (Should be filtered out).
    *   **Scenario G:** Data that would result in identical rows from both `SELECT` statements to verify `UNION DISTINCT`.
*   **Action:**
    1.  Populate `sof_dataset_target.ta_cntrct_crs2` with the test data for all scenarios.
    2.  Execute the Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs3`.
    3.  Query `sof_dataset_target.ta_cntrct_crs3` to inspect the results.
*   **Pass/Fail Criterion:**
    *   **Scenario A:** `c.cntrct_id` should appear in the output with `twinbill = 'TB'` and `twin_vertrag_id = ctb.cntrct_id`.
    *   **Scenario B:** `c.cntrct_id` should appear in the output with `twinbill IS NULL` and `twin_vertrag_id IS NULL`.
    *   **Scenario C:** `c.cntrct_id` should NOT appear in the output from the first `SELECT` part.
    *   **Scenario D:** `ctb.cntrct_id` should appear in the output with `twinbill = 'TB'` and `twin_vertrag_id = c.cntrct_id`.
    *   **Scenario E:** `ctb.cntrct_id` should NOT appear in the output from the second `SELECT` part.
    *   **Scenario F:** These contracts should NOT appear in the output.
    *   **Scenario G:** The final output table should contain only one instance of the identical row, confirming `UNION DISTINCT` functionality.

    ```sql
    -- Example Setup for Scenario A & B:
    -- TRUNCATE TABLE `sof_dataset_target.ta_cntrct_crs2`;
    -- INSERT INTO `sof_dataset_target.ta_cntrct_crs2` (cntrct_id, cntrct_ty, cntrct_parent, ...) VALUES
    -- (100, 30, NULL, ...), -- Parent, no child (Scenario B)
    -- (101, 30, NULL, ...), -- Parent with child (Scenario A)
    -- (102, 20, 101, ...), -- Child of 101 (Scenario A)
    -- (103, 10, NULL, ...), -- Excluded type (Scenario C/F)
    -- (104, 20, NULL, ...), -- Excluded type (Scenario C/F)
    -- (105, 30, NULL, ...), -- Parent of a child with excluded type (Scenario E)
    -- (106, 20, 105, ...); -- Child of 105 (Scenario E)

    -- After running the DAG, verify with:
    -- SELECT cntrct_id, twinbill, twin_vertrag_id, cntrct_ty, cntrct_parent FROM `sof_dataset_target.ta_cntrct_crs3` ORDER BY cntrct_id;

    -- Expected results for the above setup:
    -- cntrct_id | twinbill | twin_vertrag_id | cntrct_ty | cntrct_parent
    -- ----------|----------|-----------------|-----------|--------------
    -- 100       | NULL     | NULL            | 30        | NULL
    -- 101       | TB       | 102             | 30        | NULL
    -- 102       | TB       | 101             | 20        | 101
    -- (103, 104, 105, 106 should not appear in the output based on the WHERE clauses)
    ```

### Test Case 5: Data Type and NULL Handling

*   **Purpose:** To ensure that all column data types are correctly mapped from Oracle to BigQuery and that NULL values are handled consistently, especially for `twinbill` and `twin_vertrag_id`.
*   **Setup:**
    1.  Create `sof_dataset_target.ta_cntrct_crs2` with data that includes NULLs in various columns (e.g., `commitment_reference_date`, `order_number`, `RV_NUM`).
    2.  Ensure `cntrct_id` and `cntrct_parent` are populated to test join conditions.
    3.  Define the BigQuery target table `sof_dataset_target.ta_cntrct_crs3` with the expected BigQuery data types (e.g., `STRING`, `INT64`, `DATE`, `TIMESTAMP`).
*   **Action:**
    1.  Execute the Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs3`.
    2.  Query `sof_dataset_target.ta_cntrct_crs3` and inspect the data types and NULL values.
*   **Pass/Fail Criterion:**
    *   The schema of `sof_dataset_target.ta_cntrct_crs3` must match the defined target schema, with no unexpected type coercions or errors.
    *   Columns that were NULL in `sof_dataset_target.ta_cntrct_crs2` (and are not part of the `twinbill` logic) must remain NULL in `sof_dataset_target.ta_cntrct_crs3`.
    *   `twinbill` must be 'TB' when a twin-bill relationship is found, and `NULL` otherwise (in the first `SELECT` part).
    *   `twin_vertrag_id` must be the `cntrct_id` of the related twin-bill contract, or `NULL` when no twin-bill is found (in the first `SELECT` part).

    ```sql
    -- Example: Check schema
    SELECT column_name, data_type, is_nullable
    FROM `your-gcp-project.sof_dataset_target.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'ta_cntrct_crs3';

    -- Example: Check NULL handling for specific rows
    -- Assuming a row with cntrct_id = 100 (from Test Case 4, Scenario B)
    SELECT cntrct_id, twinbill, twin_vertrag_id, commitment_reference_date, order_number
    FROM `sof_dataset_target.ta_cntrct_crs3`
    WHERE cntrct_id = 100;
    -- Expected: twinbill IS NULL, twin_vertrag_id IS NULL, and other NULLable columns retain their NULL status.
    ```

### Test Case 6: External System Replacements - Airflow DAG Execution

*   **Purpose:** To verify that the Airflow DAG successfully orchestrates the BigQuery transformation, replacing the UC4/KornShell execution.
*   **Setup:**
    1.  Deploy the `dw_bert_ausd_v_ta_cntrct_crs3.py` DAG to a Cloud Composer environment.
    2.  Ensure the Airflow service account has necessary permissions to read from source BigQuery tables and write to the target BigQuery table.
    3.  Ensure source BigQuery tables (`isbert_schema_target.dwtk_meldungen`, `sof_dataset_target.ta_cntrct_crs2`) are populated with valid data.
*   **Action:**
    1.  Manually trigger the `dw_bert_ausd_v_ta_cntrct_crs3` DAG in Airflow.
    2.  Monitor the DAG run in the Airflow UI.
*   **Pass/Fail Criterion:**
    *   The DAG run must complete successfully without any task failures.
    *   The `execute_contract_update` task must show a "success" status.
    *   Cloud Logging should show successful BigQuery job completion for the executed SQL.
    *   The target table `sof_dataset_target.ta_cntrct_crs3` must be populated with data.

    ```bash
    # Example of triggering via Airflow CLI (if direct access)
    # airflow dags trigger dw_bert_ausd_v_ta_cntrct_crs3

    # Or via Airflow UI.

    # After execution, verify in BigQuery:
    # SELECT COUNT(*) FROM `sof_dataset_target.ta_cntrct_crs3`;
    # -- Count should be > 0 and match expected output.
    ```

### Test Case 7: Data Quality - Row Count Assertion

*   **Purpose:** To ensure the number of rows processed and inserted into the target table is within expected bounds, providing a quick check for major data loss or duplication.
*   **Setup:**
    1.  Populate `sof_dataset_target.ta_cntrct_crs2` with a known number of rows that will result in a predictable output row count (e.g., 100 parent contracts, 20 of which have type 20 children).
    2.  Determine the expected output row count based on the transformation logic.
*   **Action:**
    1.  Execute the Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs3`.
    2.  Query the final row count of `sof_dataset_target.ta_cntrct_crs3`.
*   **Pass/Fail Criterion:**
    *   The actual row count of `sof_dataset_target.ta_cntrct_crs3` must exactly match the pre-calculated expected row count.

    ```python
    from google.cloud import bigquery

    def test_row_count_assertion():
        bq_client = bigquery.Client()
        bq_table_id = "`your-gcp-project.sof_dataset_target.ta_cntrct_crs3`"

        # --- Setup: Determine Expected Row Count ---
        # This requires detailed analysis of the source data and transformation logic.
        # For example, if you have 100 contracts in ta_cntrct_crs2,
        # 80 are not type 10/20, and 10 of those 80 have a type 20 child.
        # First SELECT: 80 rows (70 with NULL twinbill, 10 with 'TB')
        # Second SELECT: 10 rows (the type 20 children)
        # UNION DISTINCT: 80 + 10 = 90 distinct rows.
        expected_row_count = 90 # Replace with actual calculated expected count for your test data

        # --- Action: Run DAG and Query BigQuery Output ---
        # Assuming the DAG has just been run or is triggered as part of the test suite
        query = f"SELECT COUNT(*) FROM {bq_table_id}"
        job = bq_client.query(query)
        result = job.result()
        actual_row_count = [row[0] for row in result][0]

        # --- Pass/Fail Criterion ---
        assert actual_row_count == expected_row_count, \
            f"Row count mismatch: Expected {expected_row_count} rows, but found {actual_row_count} rows."
        print(f"Row count assertion passed: Found {actual_row_count} rows as expected.")
    ```

### Test Case 8: Data Quality - Schema Assertion

*   **Purpose:** To ensure the schema (column names, data types, nullability) of the target table `sof_dataset_target.ta_cntrct_crs3` is as expected and consistent with the legacy system's schema (after BigQuery type mapping).
*   **Setup:**
    1.  Define the expected schema for `sof_dataset_target.ta_cntrct_crs3` in BigQuery, including column names, data types, and nullability. This should be derived from the Oracle schema and BigQuery's type mapping rules.
    2.  Ensure the target table is created (or will be created by the job if it's a `CREATE OR REPLACE TABLE AS SELECT` type, though here it's `TRUNCATE` and `INSERT`).
*   **Action:**
    1.  Execute the Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs3`.
    2.  Retrieve the actual schema of `sof_dataset_target.ta_cntrct_crs3` from BigQuery's `INFORMATION_SCHEMA`.
*   **Pass/Fail Criterion:**
    *   The actual schema (column names, data types, and nullability) must exactly match the predefined expected schema.

    ```python
    from google.cloud import bigquery

    def test_schema_assertion():
        bq_client = bigquery.Client()
        bq_table_id = "sof_dataset_target.ta_cntrct_crs3" # No backticks for INFORMATION_SCHEMA query
        project_id = "your-gcp-project" # Replace with your GCP project ID

        # --- Setup: Define Expected Schema ---
        # This should be meticulously derived from Oracle schema + BigQuery type mapping
        expected_schema = {
            "cntrct_id": {"data_type": "INT64", "is_nullable": "NO"},
            "obj_version": {"data_type": "INT64", "is_nullable": "YES"},
            "contract_number": {"data_type": "STRING", "is_nullable": "YES"},
            "cntrct_template_id": {"data_type": "INT64", "is_nullable": "YES"},
            "cntrct_validity_id": {"data_type": "INT64", "is_nullable": "YES"},
            "valid_from": {"data_type": "DATE", "is_nullable": "YES"},
            "com_per_ext_rea_cv": {"data_type": "STRING", "is_nullable": "YES"},
            "billcycle_id": {"data_type": "INT64", "is_nullable": "YES"},
            "vo_code": {"data_type": "STRING", "is_nullable": "YES"},
            "cntrct_start_date": {"data_type": "DATE", "is_nullable": "YES"},
            "cntrct_st": {"data_type": "STRING", "is_nullable": "YES"},
            "cntrct_parent": {"data_type": "INT64", "is_nullable": "YES"},
            "cntrct_ty": {"data_type": "INT64", "is_nullable": "YES"},
            "cost_centre": {"data_type": "STRING", "is_nullable": "YES"},
            "cost_centre_user": {"data_type": "STRING", "is_nullable": "YES"},
            "commitment_reference_date": {"data_type": "DATE", "is_nullable": "YES"},
            "order_number": {"data_type": "STRING", "is_nullable": "YES"},
            "rv_num": {"data_type": "STRING", "is_nullable": "YES"}, # Assuming RV_NUM is string, verify with Oracle
            "twinbill": {"data_type": "STRING", "is_nullable": "YES"}, # CASE WHEN ... END can result in NULL
            "twin_vertrag_id": {"data_type": "INT64", "is_nullable": "YES"}, # Can be NULL from LEFT JOIN
        }

        # --- Action: Retrieve Actual Schema ---
        query = f"""
            SELECT column_name, data_type, is_nullable
            FROM `{project_id}.{bq_table_id.split('.')[0]}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{bq_table_id.split('.')[1]}'
        """
        job = bq_client.query(query)
        actual_schema_rows = job.result()
        actual_schema = {
            row.column_name: {"data_type": row.data_type, "is_nullable": row.is_nullable}
            for row in actual_schema_rows
        }

        # --- Pass/Fail Criterion ---
        assert set(actual_schema.keys()) == set(expected_schema.keys()), \
            f"Column name mismatch. Expected: {set(expected_schema.keys())}, Actual: {set(actual_schema.keys())}"

        for col_name, expected_props in expected_schema.items():
            assert col_name in actual_schema, f"Column {col_name} not found in actual schema."
            actual_props = actual_schema[col_name]
            assert actual_props["data_type"] == expected_props["data_type"], \
                f"Data type mismatch for column {col_name}. Expected: {expected_props['data_type']}, Actual: {actual_props['data_type']}"
            assert actual_props["is_nullable"] == expected_props["is_nullable"], \
                f"Nullability mismatch for column {col_name}. Expected: {expected_props['is_nullable']}, Actual: {actual_props['is_nullable']}"

        print("Schema assertion passed: Actual schema matches expected schema.")
    ```

### Test Case 9: Edge Case - Empty Source Table `ta_cntrct_crs2`

*   **Purpose:** To verify the job handles an empty `sof_dataset_target.ta_cntrct_crs2` table gracefully, resulting in an empty target table.
*   **Setup:**
    1.  Ensure `sof_dataset_target.ta_cntrct_crs2` is completely empty.
    2.  Populate `isbert_schema_target.dwtk_meldungen` with valid data for `v_datum` (or ensure it's empty to test `v_datum` default).
*   **Action:**
    1.  Execute the Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs3`.
    2.  Query the row count of `sof_dataset_target.ta_cntrct_crs3`.
*   **Pass/Fail Criterion:**
    *   The DAG must complete successfully.
    *   The row count of `sof_dataset_target.ta_cntrct_crs3` must be 0.

    ```sql
    -- Setup:
    -- TRUNCATE TABLE `sof_dataset_target.ta_cntrct_crs2`;
    -- (Ensure `dwtk_meldungen` is set up as desired for v_datum)

    -- After running the DAG:
    -- SELECT COUNT(*) FROM `sof_dataset_target.ta_cntrct_crs3`; -- Should be 0
    ```

### Test Case 10: Edge Case - All Contracts Filtered Out

*   **Purpose:** To verify the job handles scenarios where all contracts in `sof_dataset_target.ta_cntrct_crs2` are filtered out by the `WHERE` clauses, resulting in an empty target table.
*   **Setup:**
    1.  Populate `sof_dataset_target.ta_cntrct_crs2` with contracts where `cntrct_ty` is always `10` or `20`.
    2.  Populate `isbert_schema_target.dwtk_meldungen` with valid data for `v_datum`.
*   **Action:**
    1.  Execute the Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs3`.
    2.  Query the row count of `sof_dataset_target.ta_cntrct_crs3`.
*   **Pass/Fail Criterion:**
    *   The DAG must complete successfully.
    *   The row count of `sof_dataset_target.ta_cntrct_crs3` must be 0.

    ```sql
    -- Setup:
    -- TRUNCATE TABLE `sof_dataset_target.ta_cntrct_crs2`;
    -- INSERT INTO `sof_dataset_target.ta_cntrct_crs2` (cntrct_id, cntrct_ty, ...) VALUES
    -- (1, 10, ...),
    -- (2, 20, ...),
    -- (3, 10, ...);

    -- After running the DAG:
    -- SELECT COUNT(*) FROM `sof_dataset_target.ta_cntrct_crs3`; -- Should be 0
    ```

---

**General Considerations for Execution:**

*   **Test Data Management:** For robust testing, consider using a test data generation framework or a dedicated test dataset that can be reset for each test run.
*   **Environment Isolation:** Ensure test runs do not interfere with each other or with production data. Use dedicated test datasets and tables.
*   **Performance Testing:** While not explicitly requested, after functional correctness, performance testing (comparing BigQuery execution time to Oracle execution time) would be a valuable next step.
*   **Monitoring and Alerting:** Verify that the Cloud Monitoring and Cloud Logging configurations are correctly capturing job status, errors, and performance metrics as specified in the design.
*   **Unresolved Risks:** The tests above primarily focus on the data transformation. The "Unresolved / Risks" section of the design document (e.g., `v_carmen` usage, KornShell utility equivalents for error handling/logging) would require separate, more specific tests or further investigation to ensure their behavior is correctly replicated or deemed irrelevant for data migration. For `v_carmen`, if it's truly unused in the provided SQL, then no specific test is needed for its migration, but its non-usage should be confirmed.