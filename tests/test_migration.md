As a senior data-migration QA engineer, I've designed a comprehensive suite of tests to validate the migration of `r_ausd_v_ta_cntrct_crs3.ksh` to Google Cloud BigQuery and Cloud Composer. The tests cover output parity, transformation correctness, external system replacements, and data quality assertions, ensuring the migrated solution is behaviourally equivalent to the legacy system.

---

## Migration Validation Tests for `r_ausd_v_ta_cntrct_crs3.ksh`

**Assumptions:**
*   `YOUR_PROJECT_ID` and `YOUR_DATASET_ID` are placeholders for the actual Google Cloud Project ID and BigQuery Dataset ID.
*   A Python testing framework (e.g., `pytest`) with access to `google-cloud-bigquery` client is available for programmatic assertions.
*   The BigQuery tables (`dw_job_log`, `dw_error_log`, `sof_ta_cntrct_crs3`, `sof_ta_cntrct_crs2`, `dwtk_meldungen`) have been created according to the provided DDL.
*   The BigQuery Stored Procedures (`sp_k_ausd_v_ta_cntrct_crs3`, `sp_vertragsdatenabgleich`) have been deployed.
*   The Airflow DAG (`r_ausd_v_ta_cntrct_crs3_dag`) has been deployed to Cloud Composer.

---

### Test Case 1: Successful Job Execution - Logging and Output Parity

*   **Purpose:** Verify that a successful execution of the migrated job correctly updates the `dw_job_log` table with an 'OK' status and produces the expected data in `sof_ta_cntrct_crs3`. This tests the end-to-end flow for a happy path.
*   **Setup:**
    1.  Clear all data from `dw_job_log`, `dw_error_log`, and `sof_ta_cntrct_crs3`.
    2.  Insert a diverse set of test data into `sof_ta_cntrct_crs2` and `dwtk_meldungen` that covers various contract types, parent-child relationships, and `timecreated` values to ensure all transformation logic paths are exercised.
        *   Example `sof_ta_cntrct_crs2` data:
            *   Parent contract (type not 10 or 20) with children (type 20).
            *   Parent contract (type not 10 or 20) with no children.
            *   Contract type 10 (should be excluded).
            *   Contract type 20 without a parent (should be excluded by the second `UNION ALL` part).
        *   Example `dwtk_meldungen` data:
            *   `INSERT INTO \`YOUR_PROJECT_ID.YOUR_DATASET_ID.dwtk_meldungen\` (timecreated, job_kennung) VALUES ('2023-01-15 10:00:00 UTC', 'BERT_DROP_TEMP_TABLE');`
*   **Action:**
    Trigger the `r_ausd_v_ta_cntrct_crs3_dag` in Cloud Composer. This will execute `sp_vertragsdatenabgleich`, which in turn calls `sp_k_ausd_v_ta_cntrct_crs3`.
*   **Pass/Fail Criterion:**
    1.  The Airflow DAG run completes successfully.
    2.  Query `dw_job_log` to verify one entry exists with `status = 'OK'`, `program_name = 'Vertragsdatenabgleich'`, `program_version = 'V1.0.0'`, and `job_kennung = 'BERT_V_TA_CNTRCT_CRS3'`.
    3.  Query `dw_error_log` to confirm it is empty.
    4.  Compare the data in `sof_ta_cntrct_crs3` with a pre-calculated expected output (derived from the same input data using the transformation logic). The row count and all column values must match exactly.

    ```python
    # Example pytest assertion for job log and error log
    def test_successful_job_execution_logging(bigquery_client):
        dataset_id = "YOUR_DATASET_ID"
        project_id = "YOUR_PROJECT_ID"

        # 1. Check dw_job_log
        query_job_log = f"""
            SELECT job_kennung, program_name, program_version, status, log_message
            FROM `{project_id}.{dataset_id}.dw_job_log`
            WHERE job_kennung = 'BERT_V_TA_CNTRCT_CRS3'
            ORDER BY start_timestamp DESC
            LIMIT 1
        """
        rows = list(bigquery_client.query(query_job_log).result())
        assert len(rows) == 1
        assert rows[0]["status"] == "OK"
        assert rows[0]["program_name"] == "Vertragsdatenabgleich"
        assert rows[0]["program_version"] == "V1.0.0"
        assert rows[0]["job_kennung"] == "BERT_V_TA_CNTRCT_CRS3"
        assert "Job completed successfully." in rows[0]["log_message"]

        # 2. Check dw_error_log
        query_error_log = f"SELECT COUNT(*) FROM `{project_id}.{dataset_id}.dw_error_log`"
        error_count = list(bigquery_client.query(query_error_log).result())[0][0]
        assert error_count == 0

    # Example SQL assertion for data parity (assuming expected_sof_ta_cntrct_crs3 is a table with expected data)
    # This would typically be done by comparing the actual output with a golden dataset.
    def test_successful_job_execution_data_parity(bigquery_client):
        dataset_id = "YOUR_DATASET_ID"
        project_id = "YOUR_PROJECT_ID"

        query_data_parity = f"""
            SELECT
                (SELECT COUNT(*) FROM `{project_id}.{dataset_id}.sof_ta_cntrct_crs3`) AS actual_count,
                (SELECT COUNT(*) FROM `{project_id}.{dataset_id}.expected_sof_ta_cntrct_crs3`) AS expected_count,
                (SELECT COUNT(*) FROM `{project_id}.{dataset_id}.sof_ta_cntrct_crs3` EXCEPT DISTINCT SELECT * FROM `{project_id}.{dataset_id}.expected_sof_ta_cntrct_crs3`) AS diff_actual_expected,
                (SELECT COUNT(*) FROM `{project_id}.{dataset_id}.expected_sof_ta_cntrct_crs3` EXCEPT DISTINCT SELECT * FROM `{project_id}.{dataset_id}.sof_ta_cntrct_crs3`) AS diff_expected_actual
        """
        result = list(bigquery_client.query(query_data_parity).result())[0]
        assert result["actual_count"] == result["expected_count"]
        assert result["diff_actual_expected"] == 0
        assert result["diff_expected_actual"] == 0
    ```

---

### Test Case 2: Error Handling - Core Script Failure

*   **Purpose:** Verify that if the core processing logic (`sp_k_ausd_v_ta_cntrct_crs3`) fails, the wrapper (`sp_vertragsdatenabgleich`) correctly logs the error in `dw_error_log` and updates the job status in `dw_job_log` to 'ERROR'.
*   **Setup:**
    1.  Clear all data from `dw_job_log`, `dw_error_log`, and `sof_ta_cntrct_crs3`.
    2.  **Temporarily modify `sp_k_ausd_v_ta_cntrct_crs3` to force an error.** For example, introduce a syntax error, attempt to divide by zero, or reference a non-existent column/table. A simple `RAISE` statement is effective for testing:
        ```sql
        -- Inside sp_k_ausd_v_ta_cntrct_crs3, e.g., at the beginning of the BEGIN block:
        RAISE USING MESSAGE 'Simulated error for testing purposes.';
        ```
    3.  Insert minimal valid test data into `sof_ta_cntrct_crs2` and `dwtk_meldungen` (the data itself won't be processed, but the tables should exist).
*   **Action:**
    Trigger the `r_ausd_v_ta_cntrct_crs3_dag` in Cloud Composer.
*   **Pass/Fail Criterion:**
    1.  The Airflow DAG run fails, and the `call_sp_vertragsdatenabgleich` task reports a failure.
    2.  Query `dw_job_log` to verify one entry exists with `status = 'ERROR'`, `job_kennung = 'BERT_V_TA_CNTRCT_CRS3'`, and `log_message` indicating a failure.
    3.  Query `dw_error_log` to verify one entry exists with `job_entry_id` matching the `dw_job_log` entry, `job_kennung = 'BERT_V_TA_CNTRCT_CRS3'`, and `error_message` containing the simulated error message (e.g., "Simulated error for testing purposes.").

    ```python
    # Example pytest assertion for error logging
    def test_error_handling_core_script_failure(bigquery_client):
        dataset_id = "YOUR_DATASET_ID"
        project_id = "YOUR_PROJECT_ID"

        # 1. Check dw_job_log
        query_job_log = f"""
            SELECT job_kennung, status, log_message, job_entry_id
            FROM `{project_id}.{dataset_id}.dw_job_log`
            WHERE job_kennung = 'BERT_V_TA_CNTRCT_CRS3'
            ORDER BY start_timestamp DESC
            LIMIT 1
        """
        rows = list(bigquery_client.query(query_job_log).result())
        assert len(rows) == 1
        assert rows[0]["status"] == "ERROR"
        assert "Job failed with error" in rows[0]["log_message"]
        job_entry_id = rows[0]["job_entry_id"]

        # 2. Check dw_error_log
        query_error_log = f"""
            SELECT job_kennung, error_message, job_entry_id
            FROM `{project_id}.{dataset_id}.dw_error_log`
            WHERE job_entry_id = {job_entry_id}
            ORDER BY error_timestamp DESC
            LIMIT 1
        """
        error_rows = list(bigquery_client.query(query_error_log).result())
        assert len(error_rows) == 1
        assert error_rows[0]["job_kennung"] == "BERT_V_TA_CNTRCT_CRS3"
        assert "Simulated error for testing purposes." in error_rows[0]["error_message"]
        assert error_rows[0]["job_entry_id"] == job_entry_id

        # 3. Check target table (should be empty or in its pre-error state due to TRUNCATE)
        query_target_count = f"SELECT COUNT(*) FROM `{project_id}.{dataset_id}.sof_ta_cntrct_crs3`"
        target_count = list(bigquery_client.query(query_target_count).result())[0][0]
        assert target_count == 0 # Assuming TRUNCATE happens before the RAISE
    ```
    *   **Cleanup:** Revert the temporary modification to `sp_k_ausd_v_ta_cntrct_crs3`.

---

### Test Case 3: Parameter Handling (No-op parameters)

*   **Purpose:** Verify that the `p_s` and `p_l` parameters in `sp_vertragsdatenabgleich`, which are placeholders and not used by the original ksh script, are handled without causing errors and do not affect the core logic.
*   **Setup:**
    1.  Clear all data from `dw_job_log`, `dw_error_log`, and `sof_ta_cntrct_crs3`.
    2.  Insert the same diverse test data as in Test Case 1 into `sof_ta_cntrct_crs2` and `dwtk_meldungen`.
*   **Action:**
    Directly call `sp_vertragsdatenabgleich` with non-NULL, arbitrary string values for `p_s` and `p_l`.
    ```sql
    CALL `YOUR_PROJECT_ID.YOUR_DATASET_ID.sp_vertragsdatenabgleich`('test_s_value', 'test_l_value');
    ```
*   **Pass/Fail Criterion:**
    1.  The BigQuery stored procedure call completes successfully.
    2.  `dw_job_log` contains one entry with `status = 'OK'`.
    3.  `dw_error_log` is empty.
    4.  The data in `sof_ta_cntrct_crs3` is identical to the expected output from Test Case 1, confirming the parameters had no functional impact.

    ```python
    # Example pytest assertion
    def test_parameter_handling_no_op(bigquery_client):
        dataset_id = "YOUR_DATASET_ID"
        project_id = "YOUR_PROJECT_ID"

        # Call the SP with non-NULL parameters
        query_call_sp = f"CALL `{project_id}.{dataset_id}.sp_vertragsdatenabgleich`('test_s_value', 'test_l_value');"
        bigquery_client.query(query_call_sp).result() # Should complete without error

        # Assert job log status
        query_job_log = f"""
            SELECT status FROM `{project_id}.{dataset_id}.dw_job_log`
            WHERE job_kennung = 'BERT_V_TA_CNTRCT_CRS3'
            ORDER BY start_timestamp DESC LIMIT 1
        """
        status = list(bigquery_client.query(query_job_log).result())[0]["status"]
        assert status == "OK"

        # Assert error log is empty
        query_error_log = f"SELECT COUNT(*) FROM `{project_id}.{dataset_id}.dw_error_log`"
        error_count = list(bigquery_client.query(query_error_log).result())[0][0]
        assert error_count == 0

        # Assert data parity (same as Test Case 1's data parity check)
        # ... (call the data parity assertion function from Test Case 1)
    ```

---

### Test Case 4: Transformation Correctness - `UNION ALL` and `JOIN` Logic

*   **Purpose:** Specifically test the complex `UNION ALL` and `JOIN` logic, including `CASE WHEN` and `WHERE` clauses within `sp_k_ausd_v_ta_cntrct_crs3`, to ensure correct data transformation.
*   **Setup:**
    1.  Clear `sof_ta_cntrct_crs3`.
    2.  Insert highly specific test data into `sof_ta_cntrct_crs2` to cover all branches of the `UNION ALL` and `WHERE` clauses:
        *   **Scenario 1 (First `SELECT`):**
            *   `cntrct_id = 101`, `cntrct_ty = 50` (not 10 or 20), no child `cntrct_ty = 20`. Expected: Row 101, `TWINBILL`=NULL, `TWIN_VERTRAG_ID`=NULL.
            *   `cntrct_id = 102`, `cntrct_ty = 50`, has child `cntrct_id = 201` (type 20). Expected: Row 102, `TWINBILL`='TB', `TWIN_VERTRAG_ID`=201.
            *   `cntrct_id = 103`, `cntrct_ty = 10` (excluded by `WHERE`). Expected: Not in output.
            *   `cntrct_id = 104`, `cntrct_ty = 20` (excluded by `WHERE`). Expected: Not in output.
        *   **Scenario 2 (Second `SELECT` - `UNION ALL` part):**
            *   `cntrct_id = 105`, `cntrct_ty = 50`, has child `cntrct_id = 205` (type 20). Expected: Row 205, `TWINBILL`='TB', `TWIN_VERTRAG_ID`=105.
            *   `cntrct_id = 106`, `cntrct_ty = 10`, has child `cntrct_id = 206` (type 20). Expected: Not in output (parent `cntrct_ty` is 10).
            *   `cntrct_id = 107`, `cntrct_ty = 50`, no child `cntrct_ty = 20`. Expected: Not in output (no matching child for `JOIN`).
    3.  Insert a `dwtk_meldungen` entry as in Test Case 1.
*   **Action:**
    Call `sp_k_ausd_v_ta_cntrct_crs3` directly (or via `sp_vertragsdatenabgleich`).
    ```sql
    CALL `YOUR_PROJECT_ID.YOUR_DATASET_ID.sp_k_ausd_v_ta_cntrct_crs3`('BERT_V_TA_CNTRCT_CRS3', 1);
    ```
*   **Pass/Fail Criterion:**
    The `sof_ta_cntrct_crs3` table contains exactly the expected rows, with correct `TWINBILL` and `TWIN_VERTRAG_ID` values, and all filtering applied correctly according to the detailed scenarios.

    ```python
    # Example pytest assertion for transformation logic
    def test_transformation_correctness(bigquery_client):
        dataset_id = "YOUR_DATASET_ID"
        project_id = "YOUR_PROJECT_ID"

        # Define expected output based on the specific setup data
        expected_data = [
            # Expected rows from first SELECT part
            {'cntrct_id': 101, 'cntrct_ty': 50, 'TWINBILL': None, 'TWIN_VERTRAG_ID': None, ...},
            {'cntrct_id': 102, 'cntrct_ty': 50, 'TWINBILL': 'TB', 'TWIN_VERTRAG_ID': 201, ...},
            # Expected rows from second SELECT part (children)
            {'cntrct_id': 205, 'cntrct_ty': 20, 'TWINBILL': 'TB', 'TWIN_VERTRAG_ID': 105, ...},
        ]
        # Convert expected_data to a DataFrame or temporary table for comparison

        query_actual_data = f"SELECT cntrct_id, cntrct_ty, TWINBILL, TWIN_VERTRAG_ID FROM `{project_id}.{dataset_id}.sof_ta_cntrct_crs3` ORDER BY cntrct_id"
        actual_rows = [dict(row) for row in bigquery_client.query(query_actual_data).result()]

        assert len(actual_rows) == len(expected_data)
        # Further detailed comparison of each row/column
        for i, expected_row in enumerate(expected_data):
            for key, value in expected_row.items():
                assert actual_rows[i][key] == value, f"Mismatch in row {i}, column {key}: Expected {value}, Got {actual_rows[i][key]}"
    ```

---

### Test Case 5: `v_datum` Derivation and `TRUNCATE` Idempotency

*   **Purpose:** Verify the `v_datum` derivation logic from `dwtk_meldungen` and ensure that running the job multiple times produces the same final state in `sof_ta_cntrct_crs3` due to the `TRUNCATE` operation.
*   **Setup:**
    1.  Clear `dw_job_log`, `dw_error_log`, and `sof_ta_cntrct_crs3`.
    2.  Insert a known set of data into `sof_ta_cntrct_crs2`.
    3.  Insert multiple entries into `dwtk_meldungen` with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and varying `timecreated` values to test `MAX()` and `COALESCE()`:
        ```sql
        INSERT INTO `YOUR_PROJECT_ID.YOUR_DATASET_ID.dwtk_meldungen` (timecreated, job_kennung) VALUES
        ('2023-01-01 00:00:00 UTC', 'BERT_DROP_TEMP_TABLE'),
        ('2023-01-15 10:00:00 UTC', 'BERT_DROP_TEMP_TABLE'), -- This should be MAX
        ('2023-01-10 05:00:00 UTC', 'BERT_DROP_TEMP_TABLE');
        ```
    4.  Also test the `COALESCE` part by having *no* `BERT_DROP_TEMP_TABLE` entries in `dwtk_meldungen` for a separate run.
*   **Action:**
    1.  Call `sp_vertragsdatenabgleich` (First run).
    2.  Call `sp_vertragsdatenabgleich` again (Second run).
    3.  (Optional) Clear `dwtk_meldungen` for `BERT_DROP_TEMP_TABLE` and run `sp_vertragsdatenabgleich` a third time.
*   **Pass/Fail Criterion:**
    1.  After the first run, `sof_ta_cntrct_crs3` contains the expected data.
    2.  After the second run, the data in `sof_ta_cntrct_crs3` is *identical* to the data after the first run (same row count, same content). This confirms idempotency.
    3.  For the optional third run (no `BERT_DROP_TEMP_TABLE` entries), the `v_datum` should default to `'19000101'`, and the final data should still be correct (as `v_datum` is not used in the `WHERE` clause of the `INSERT...SELECT`).

    ```python
    # Example pytest assertion for idempotency
    def test_idempotency_and_v_datum_derivation(bigquery_client):
        dataset_id = "YOUR_DATASET_ID"
        project_id = "YOUR_PROJECT_ID"

        # Run 1
        bigquery_client.query(f"CALL `{project_id}.{dataset_id}.sp_vertragsdatenabgleich`(NULL, NULL);").result()
        query_data_run1 = f"SELECT * FROM `{project_id}.{dataset_id}.sof_ta_cntrct_crs3` ORDER BY cntrct_id"
        data_run1 = [dict(row) for row in bigquery_client.query(query_data_run1).result()]

        # Run 2
        bigquery_client.query(f"CALL `{project_id}.{dataset_id}.sp_vertragsdatenabgleich`(NULL, NULL);").result()
        query_data_run2 = f"SELECT * FROM `{project_id}.{dataset_id}.sof_ta_cntrct_crs3` ORDER BY cntrct_id"
        data_run2 = [dict(row) for row in bigquery_client.query(query_data_run2).result()]

        assert data_run1 == data_run2, "Data in sof_ta_cntrct_crs3 is not identical after two runs (idempotency failure)."

        # Optional: Test COALESCE for v_datum
        # Clear dwtk_meldungen for 'BERT_DROP_TEMP_TABLE'
        bigquery_client.query(f"DELETE FROM `{project_id}.{dataset_id}.dwtk_meldungen` WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'").result()
        bigquery_client.query(f"CALL `{project_id}.{dataset_id}.sp_vertragsdatenabgleich`(NULL, NULL);").result()
        query_data_run3 = f"SELECT * FROM `{project_id}.{dataset_id}.sof_ta_cntrct_crs3` ORDER BY cntrct_id"
        data_run3 = [dict(row) for row in bigquery_client.query(query_data_run3).result()]
        assert data_run1 == data_run3, "Data changed when v_datum defaulted to '19000101' (COALESCE issue)."
    ```

---

### Test Case 6: NULL Handling and Data Types

*   **Purpose:** Verify that NULL values in source columns are handled correctly and that data types are preserved or correctly cast during migration, especially for nullable columns and `CASE WHEN` expressions.
*   **Setup:**
    1.  Clear `sof_ta_cntrct_crs3`.
    2.  Insert data into `sof_ta_cntrct_crs2` with various NULL values for nullable columns (e.g., `contract_number`, `vo_code`, `commitment_reference_date`, `order_number`, `rv_num`, `cntrct_parent`). Include cases where `cntrct_parent` is NULL for a `cntrct_ty = 20` contract (which should not be joined).
    3.  Insert a `dwtk_meldungen` entry.
*   **Action:**
    Call `sp_k_ausd_v_ta_cntrct_crs3`.
*   **Pass/Fail Criterion:**
    1.  The `sof_ta_cntrct_crs3` table contains the expected data, with NULLs correctly propagated or handled as per the transformation logic. For example, `CASE WHEN ctb.cntrct_id IS NOT NULL THEN 'TB' END` should correctly produce NULL if `ctb.cntrct_id` is NULL.
    2.  All columns in `sof_ta_cntrct_crs3` have the correct BigQuery data types as defined in `create_tables.sql`.

    ```python
    # Example pytest assertion for NULL handling and data types
    def test_null_handling_and_data_types(bigquery_client):
        dataset_id = "YOUR_DATASET_ID"
        project_id = "YOUR_PROJECT_ID"

        # Insert test data with NULLs (example)
        # ... (insert into sof_ta_cntrct_crs2)

        bigquery_client.query(f"CALL `{project_id}.{dataset_id}.sp_k_ausd_v_ta_cntrct_crs3`('BERT_V_TA_CNTRCT_CRS3', 1);").result()

        # Check data types
        table_ref = bigquery_client.get_table(f"{project_id}.{dataset_id}.sof_ta_cntrct_crs3")
        schema = {field.name: field.field_type for field in table_ref.schema}
        expected_schema = {
            'cntrct_id': 'INT64', 'obj_version': 'INT64', 'contract_number': 'STRING',
            'cntrct_template_id': 'INT64', 'cntrct_validity_id': 'INT64', 'valid_from': 'DATE',
            'com_per_ext_rea_cv': 'INT64', 'billcycle_id': 'INT64', 'vo_code': 'STRING',
            'cntrct_start_date': 'DATE', 'cntrct_st': 'INT64', 'cntrct_parent': 'INT64',
            'cntrct_ty': 'INT64', 'cost_centre': 'STRING', 'cost_centre_user': 'STRING',
            'commitment_reference_date': 'DATE', 'order_number': 'STRING', 'rv_num': 'STRING',
            'twinbill': 'STRING', 'twin_vertrag_id': 'INT64'
        }
        assert schema == expected_schema, "Schema mismatch in sof_ta_cntrct_crs3"

        # Check NULL propagation for specific rows
        query_null_check = f"""
            SELECT cntrct_id, twinbill, twin_vertrag_id, contract_number
            FROM `{project_id}.{dataset_id}.sof_ta_cntrct_crs3`
            WHERE cntrct_id IN (/* IDs from your NULL test data */)
            ORDER BY cntrct_id
        """
        null_check_results = [dict(row) for row in bigquery_client.query(query_null_check).result()]
        # Assert specific NULL values based on your test data
        # e.g., assert null_check_results[0]['twinbill'] is None
        # e.g., assert null_check_results[1]['contract_number'] is None
    ```

---

### Test Case 7: Row Count and Schema Assertions

*   **Purpose:** Verify that the number of rows processed and inserted is as expected, and that the schema of the target table is correct and stable.
*   **Setup:**
    1.  Clear `sof_ta_cntrct_crs3`.
    2.  Insert a known number of rows into `sof_ta_cntrct_crs2` that will result in a predictable number of output rows after the `UNION ALL` and filtering. For example, 10 parent contracts (not type 10/20), 5 of which have 1 child each (type 20). Expected output: 10 (parents) + 5 (children) = 15 rows.
    3.  Insert a `dwtk_meldungen` entry.
*   **Action:**
    Call `sp_k_ausd_v_ta_cntrct_crs3`.
*   **Pass/Fail Criterion:**
    1.  The row count in `sof_ta_cntrct_crs3` matches the expected count (e.g., 15 rows).
    2.  The schema of `sof_ta_cntrct_crs3` (column names, types, nullability) matches the DDL in `create_tables.sql`.

    ```python
    # Example pytest assertion for row count and schema
    def test_row_count_and_schema_assertions(bigquery_client):
        dataset_id = "YOUR_DATASET_ID"
        project_id = "YOUR_PROJECT_ID"

        # Insert test data to achieve a predictable row count
        # ... (insert into sof_ta_cntrct_crs2)
        expected_row_count = 15 # Based on your specific test data setup

        bigquery_client.query(f"CALL `{project_id}.{dataset_id}.sp_k_ausd_v_ta_cntrct_crs3`('BERT_V_TA_CNTRCT_CRS3', 1);").result()

        # Check row count
        query_row_count = f"SELECT COUNT(*) FROM `{project_id}.{dataset_id}.sof_ta_cntrct_crs3`"
        actual_row_count = list(bigquery_client.query(query_row_count).result())[0][0]
        assert actual_row_count == expected_row_count, f"Row count mismatch: Expected {expected_row_count}, Got {actual_row_count}"

        # Check schema (same as in Test Case 6)
        table_ref = bigquery_client.get_table(f"{project_id}.{dataset_id}.sof_ta_cntrct_crs3")
        schema = {field.name: field.field_type for field in table_ref.schema}
        expected_schema = {
            'cntrct_id': 'INT64', 'obj_version': 'INT64', 'contract_number': 'STRING',
            'cntrct_template_id': 'INT64', 'cntrct_validity_id': 'INT64', 'valid_from': 'DATE',
            'com_per_ext_rea_cv': 'INT64', 'billcycle_id': 'INT64', 'vo_code': 'STRING',
            'cntrct_start_date': 'DATE', 'cntrct_st': 'INT64', 'cntrct_parent': 'INT64',
            'cntrct_ty': 'INT64', 'cost_centre': 'STRING', 'cost_centre_user': 'STRING',
            'commitment_reference_date': 'DATE', 'order_number': 'STRING', 'rv_num': 'STRING',
            'twinbill': 'STRING', 'twin_vertrag_id': 'INT64'
        }
        assert schema == expected_schema, "Schema mismatch in sof_ta_cntrct_crs3"
    ```