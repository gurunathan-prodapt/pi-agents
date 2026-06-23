The following migration validation tests are designed to ensure the `BERT_V_TA_DISC_ZUSGF` job, migrated to Google Cloud Platform (BigQuery and Airflow), is behaviourally equivalent to its legacy Oracle/KornShell counterpart.

---

## 1. Output Parity

Since the legacy source code is unavailable for direct comparison, these tests focus on proving the functional equivalence of the BigQuery transformation logic and the Airflow orchestration based on the design document's description.

### Test Case 1.1: Core Transformation Logic - Concatenation Parity

*   **Purpose:** Verify that the BigQuery `STRING_AGG` logic correctly concatenates discount descriptions, matching the expected behavior of the Oracle pipelined function. This is the most critical part of the transformation.
*   **Setup:**
    1.  Create a test dataset in BigQuery (e.g., `test_isbert_schema`).
    2.  Populate `test_isbert_schema.sof$ta_discount` with diverse test data, including:
        *   Contracts with a single discount.
        *   Contracts with multiple discounts, ensuring `rabatt_text` values that would sort differently (e.g., 'B (5%)', 'A (10%)').
        *   Contracts with NULL `rabatt` or `rabatthoehe` values.
        *   Contracts with no associated discounts (i.e., `cntrct_id`/`cntrct_obj_version` in `discount_base` but no matching `rabatt`/`rabatthoehe` in `sof$ta_discount`).
        *   Contracts with identical `rabatt_text` values (should still be aggregated).
        *   Ensure `cntrct_id`, `cntrct_obj_version`, `disc_vector_ty`, `rabatt`, `rabatthoehe` columns are populated with appropriate types and values.
*   **Action:**
    1.  Modify the `execute_bq_transformation` task in the Airflow DAG to point to the `test_isbert_schema` for source and target tables.
    2.  Trigger the Airflow DAG `dw_bert_ausd_v_ta_disc_zusgf`.
    3.  After the DAG completes, query the `test_isbert_schema.sof$ta_disc_zusgf` table.
*   **Pass/Fail Criterion:**
    *   For each `(cntrct_id, cntrct_obj_version)` pair in the test data, the `rabatt_alle` column in `test_isbert_schema.sof$ta_disc_zusgf` must match the pre-calculated expected concatenated string.
    *   The `rabatt_alle` string must be ordered alphabetically by the `rabatt_text` components and separated by `, `.
    *   The `disc_vector_ty` should be correctly carried over from `discount_base`.

    ```sql
    -- Example assertion for a specific contract
    SELECT
        CASE
            WHEN rabatt_alle = 'Discount A (10%), Discount B (5%)' THEN 'PASS'
            ELSE 'FAIL'
        END AS test_result
    FROM `test_isbert_schema.sof$ta_disc_zusgf`
    WHERE cntrct_id = 101 AND cntrct_obj_version = 1;

    -- Example for a contract with no discounts
    SELECT
        CASE
            WHEN rabatt_alle IS NULL THEN 'PASS'
            ELSE 'FAIL'
        END AS test_result
    FROM `test_isbert_schema.sof$ta_disc_zusgf`
    WHERE cntrct_id = 103 AND cntrct_obj_version = 1;
    ```

### Test Case 1.2: `determine_processing_date` Logic Parity

*   **Purpose:** Verify that the `determine_processing_date` PythonOperator correctly derives `s_datum` as described, matching the Oracle `TO_CHAR(MAX(m.timecreated), 'YYYYMMDD')` logic and handling default values.
*   **Setup:**
    1.  Create `test_isbert_schema.dwtk_meldungen` in BigQuery.
    2.  Populate `test_isbert_schema.dwtk_meldungen` with test data:
        *   Scenario A: Multiple rows for `job_kennung = 'BERT_DROP_TEMP_TABLE'` with varying `timecreated` values (e.g., '2023-01-01 10:00:00 UTC', '2023-01-01 11:00:00 UTC', '2023-01-02 09:00:00 UTC').
        *   Scenario B: No rows matching `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
        *   Scenario C: `timecreated` is NULL for the matching `job_kennung` (if BigQuery allows, though `MAX` typically ignores NULLs).
*   **Action:**
    1.  Modify the `determine_processing_date` task to query `test_isbert_schema.dwtk_meldungen`.
    2.  Trigger the Airflow DAG `dw_bert_ausd_v_ta_disc_zusgf`.
    3.  After the `determine_processing_date` task completes, inspect its XCom value.
*   **Pass/Fail Criterion:**
    *   **Scenario A:** The `s_datum` value pushed to XCom must be `FORMAT_TIMESTAMP('%Y%m%d', MAX(timecreated))` for `job_kennung = 'BERT_DROP_TEMP_TABLE'`. For the example data, it should be '20230102'.
    *   **Scenario B:** The `s_datum` value pushed to XCom must be '19000101'.
    *   **Scenario C:** If `MAX(timecreated)` results in NULL, `s_datum` must be '19000101' due to `COALESCE`.

    ```python
    # Conceptual pytest assertion for Airflow XCom
    def test_determine_processing_date_xcom(mock_task_instance):
        # Simulate Scenario A data in BigQuery
        # ... (code to insert data into test_isbert_schema.dwtk_meldungen) ...

        # Call the Python callable directly or run the Airflow task
        _determine_processing_date(ti=mock_task_instance)
        s_datum = mock_task_instance.xcom_pull(key='s_datum')
        assert s_datum == '20230102' # Expected based on MAX(timecreated)

        # Simulate Scenario B data in BigQuery (clear previous data)
        # ... (code to clear and insert no matching data) ...
        _determine_processing_date(ti=mock_task_instance)
        s_datum = mock_task_instance.xcom_pull(key='s_datum')
        assert s_datum == '19000101' # Expected default
    ```

---

## 2. Transformation Correctness

These tests verify the specific BigQuery SQL transformation logic, including joins, aggregations, filters, type handling, and NULL handling.

### Test Case 2.1: Join Logic (LEFT JOIN) Correctness

*   **Purpose:** Verify that the `LEFT JOIN` between `discount_base` and `discount_agg` correctly handles cases where a contract has no discounts, resulting in `NULL` for `rabatt_alle`.
*   **Setup:**
    1.  Populate `test_isbert_schema.sof$ta_discount` with data such that:
        *   Contract A: Has `cntrct_id`, `cntrct_obj_version`, `disc_vector_ty` but no corresponding `rabatt` or `rabatthoehe` values (or they are all NULL).
        *   Contract B: Has valid discounts.
*   **Action:**
    1.  Run the `execute_bq_transformation` task (pointing to `test_isbert_schema`).
    2.  Query `test_isbert_schema.sof$ta_disc_zusgf`.
*   **Pass/Fail Criterion:**
    *   For Contract A, the `rabatt_alle` column in `test_isbert_schema.sof$ta_disc_zusgf` must be `NULL`.
    *   For Contract B, `rabatt_alle` must be populated correctly.

    ```sql
    SELECT
        CASE
            WHEN rabatt_alle IS NULL THEN 'PASS'
            ELSE 'FAIL'
        END AS test_result
    FROM `test_isbert_schema.sof$ta_disc_zusgf`
    WHERE cntrct_id = <Contract_A_ID> AND cntrct_obj_version = <Contract_A_Version>;
    ```

### Test Case 2.2: Data Type Handling and Casting

*   **Purpose:** Verify that `NUMBER(10)` to `INT64` and `VARCHAR2` to `STRING` conversions, and `CONCAT` operations, handle various input values correctly without errors or unexpected truncation/conversion issues.
*   **Setup:**
    1.  Populate `test_isbert_schema.sof$ta_discount` with:
        *   `cntrct_id` and `cntrct_obj_version` values at the boundaries of `INT64` (e.g., `2147483647`).
        *   `rabatt` and `rabatthoehe` values that are integers, decimals (e.g., `10.5`, `5.25`), and potentially very large numbers (within BigQuery's `STRING` limits).
        *   `disc_vector_ty` values that are long strings (up to BigQuery's `STRING` limits).
*   **Action:**
    1.  Run the `execute_bq_transformation` task (pointing to `test_isbert_schema`).
    2.  Query `test_isbert_schema.sof$ta_disc_zusgf` and inspect the schema and data.
*   **Pass/Fail Criterion:**
    *   `cntrct_id` and `cntrct_obj_version` in `test_isbert_schema.sof$ta_disc_zusgf` must be `INT64` and contain the correct integer values.
    *   `disc_vector_ty` and `rabatt_alle` must be `STRING` type.
    *   The `rabatt_alle` string must correctly represent the concatenated `rabatt` and `rabatthoehe` values, including decimal points if present in the source (e.g., '10.5 (5.25%)').
    *   No data truncation or conversion errors should occur.

    ```sql
    -- Check data types
    SELECT
        column_name,
        data_type
    FROM `test_isbert_schema.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'sof$ta_disc_zusgf'
    AND (
        (column_name = 'cntrct_id' AND data_type = 'INT64') OR
        (column_name = 'cntrct_obj_version' AND data_type = 'INT64') OR
        (column_name = 'disc_vector_ty' AND data_type = 'STRING') OR
        (column_name = 'rabatt_alle' AND data_type = 'STRING')
    );

    -- Check specific concatenated value for decimal handling
    SELECT
        CASE
            WHEN rabatt_alle LIKE '%10.5 (5.25%)%' THEN 'PASS'
            ELSE 'FAIL'
        END AS test_result
    FROM `test_isbert_schema.sof$ta_disc_zusgf`
    WHERE cntrct_id = <Contract_with_decimals_ID>;
    ```

### Test Case 2.3: NULL Handling in Concatenation

*   **Purpose:** Verify how NULL values in source columns (`rabatt`, `rabatthoehe`) are handled during the `CONCAT` and `STRING_AGG` operations.
*   **Setup:**
    1.  Populate `test_isbert_schema.sof$ta_discount` with test data including:
        *   Contract A: `rabatt` is NULL, `rabatthoehe` is '5'.
        *   Contract B: `rabatthoehe` is NULL, `rabatt` is 'Discount X'.
        *   Contract C: Both `rabatt` and `rabatthoehe` are NULL.
        *   Contract D: `disc_vector_ty` is NULL.
*   **Action:**
    1.  Run the `execute_bq_transformation` task (pointing to `test_isbert_schema`).
    2.  Query `test_isbert_schema.sof$ta_disc_zusgf`.
*   **Pass/Fail Criterion:**
    *   For Contracts A, B, and C, the `rabatt_alle` column must be `NULL` (as BigQuery's `CONCAT` returns NULL if any argument is NULL).
    *   For Contract D, `disc_vector_ty` must be `NULL`.

    ```sql
    SELECT
        cntrct_id,
        CASE WHEN rabatt_alle IS NULL THEN 'PASS' ELSE 'FAIL' END AS rabatt_alle_null_check,
        CASE WHEN disc_vector_ty IS NULL THEN 'PASS' ELSE 'FAIL' END AS disc_vector_ty_null_check
    FROM `test_isbert_schema.sof$ta_disc_zusgf`
    WHERE cntrct_id IN (<Contract_A_ID>, <Contract_B_ID>, <Contract_C_ID>, <Contract_D_ID>);
    ```

---

## 3. External-System Replacements

These tests verify the Airflow DAG's interaction with BigQuery and its ability to replace legacy system components.

### Test Case 3.1: Source Table Availability and Permissions

*   **Purpose:** Verify that the Airflow DAG correctly accesses the BigQuery equivalent of `dwtk_meldungen` and `sof$ta_discount` without permission or "table not found" errors.
*   **Setup:**
    1.  Ensure `isbert_schema.dwtk_meldungen` and `sof$ta_discount` tables exist in BigQuery in the expected dataset and project.
    2.  Ensure the Airflow service account has `bigquery.dataViewer` (read) permissions on these tables and `bigquery.dataEditor` (write) permissions on the target `sof$ta_disc_zusgf` table.
*   **Action:**
    1.  Trigger the Airflow DAG `dw_bert_ausd_v_ta_disc_zusgf`.
*   **Pass/Fail Criterion:**
    *   The `determine_processing_date` and `execute_bq_transformation` tasks must complete successfully without permission errors or "table not found" errors. The DAG run status should be "success".

    ```python
    # Conceptual pytest for Airflow DAG run status
    def test_dag_run_success(airflow_client):
        dag_id = 'dw_bert_ausd_v_ta_disc_zusgf'
        # Trigger DAG and wait for completion
        dag_run = airflow_client.trigger_dag(dag_id)
        dag_run.wait_for_completion()
        assert dag_run.state == 'success'
    ```

### Test Case 3.2: Airflow Orchestration and XCom Parameter Passing

*   **Purpose:** Verify that the Airflow DAG correctly orchestrates the tasks and that `s_datum` is passed via XComs from `determine_processing_date`.
*   **Setup:**
    1.  Ensure `isbert_schema.dwtk_meldungen` has data to produce a non-default `s_datum` (e.g., `MAX(timecreated)` is '2023-01-01').
*   **Action:**
    1.  Trigger the Airflow DAG `dw_bert_ausd_v_ta_disc_zusgf`.
    2.  After the DAG completes, inspect the XComs of the `determine_processing_date` task for the latest DAG run.
*   **Pass/Fail Criterion:**
    *   The `determine_processing_date` task must successfully push an `s_datum` value to XCom.
    *   The value of `s_datum` in XCom must be '20230101' (or whatever `FORMAT_TIMESTAMP('%Y%m%d', MAX(timecreated))` evaluates to).
    *   The `execute_bq_transformation` task must execute successfully, demonstrating the correct task dependency.

    ```python
    # Conceptual pytest for Airflow XCom
    def test_s_datum_xcom_value(airflow_client):
        dag_id = 'dw_bert_ausd_v_ta_disc_zusgf'
        # Trigger DAG and wait for completion
        dag_run = airflow_client.trigger_dag(dag_id)
        dag_run.wait_for_completion()

        # Get task instance for 'determine_processing_date'
        ti = airflow_client.get_task_instance(dag_id, 'determine_processing_date', dag_run.run_id)
        s_datum = ti.xcom_pull(key='s_datum')

        assert s_datum == '20230101' # Expected value based on setup data
    ```

---

## 4. Data-Quality / Row-Count / Schema Assertions

These tests ensure the integrity and structure of the output data in BigQuery.

### Test Case 4.1: Target Table Schema and Data Types

*   **Purpose:** Verify that the `sof$ta_disc_zusgf` table is created with the correct schema and data types as per the migration design.
*   **Setup:**
    1.  Ensure `sof$ta_discount` has some data.
*   **Action:**
    1.  Run the `execute_bq_transformation` task (pointing to `test_isbert_schema`).
    2.  Query the schema of `test_isbert_schema.sof$ta_disc_zusgf` in BigQuery.
*   **Pass/Fail Criterion:**
    *   The `test_isbert_schema.sof$ta_disc_zusgf` table must exist.
    *   Its schema must match the following:
        *   `cntrct_id`: `INT64`
        *   `cntrct_obj_version`: `INT64`
        *   `disc_vector_ty`: `STRING`
        *   `rabatt_alle`: `STRING`

    ```sql
    SELECT
        COUNT(*)
    FROM `test_isbert_schema.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'sof$ta_disc_zusgf'
    AND (
        (column_name = 'cntrct_id' AND data_type = 'INT64') OR
        (column_name = 'cntrct_obj_version' AND data_type = 'INT64') OR
        (column_name = 'disc_vector_ty' AND data_type = 'STRING') OR
        (column_name = 'rabatt_alle' AND data_type = 'STRING')
    )
    HAVING COUNT(*) = 4; -- Expecting 4 matching columns
    ```

### Test Case 4.2: Row Count Parity

*   **Purpose:** Verify that the number of rows in the target table `sof$ta_disc_zusgf` is as expected, matching the distinct `(cntrct_id, cntrct_obj_version)` pairs from the source `sof$ta_discount` table.
*   **Setup:**
    1.  Populate `test_isbert_schema.sof$ta_discount` with a known number of distinct `(cntrct_id, cntrct_obj_version)` pairs (e.g., 100 distinct pairs).
*   **Action:**
    1.  Run the `execute_bq_transformation` task (pointing to `test_isbert_schema`).
    2.  Count the rows in `test_isbert_schema.sof$ta_disc_zusgf`.
*   **Pass/Fail Criterion:**
    *   The row count of `test_isbert_schema.sof$ta_disc_zusgf` must be equal to the count of `SELECT COUNT(DISTINCT cntrct_id, cntrct_obj_version) FROM test_isbert_schema.sof$ta_discount`.

    ```sql
    SELECT
        CASE
            WHEN (SELECT COUNT(*) FROM `test_isbert_schema.sof$ta_disc_zusgf`) =
                 (SELECT COUNT(DISTINCT cntrct_id, cntrct_obj_version) FROM `test_isbert_schema.sof$ta_discount`)
            THEN 'PASS'
            ELSE 'FAIL'
        END AS row_count_check;
    ```

### Test Case 4.3: Uniqueness Constraint

*   **Purpose:** Verify that the combination of `(cntrct_id, cntrct_obj_version)` is unique in the target table, as implied by the `GROUP BY` and `DISTINCT` operations in the BigQuery SQL.
*   **Setup:**
    1.  Populate `test_isbert_schema.sof$ta_discount` with data, including multiple discounts for the same contract.
*   **Action:**
    1.  Run the `execute_bq_transformation` task (pointing to `test_isbert_schema`).
    2.  Execute a query to check for duplicate `(cntrct_id, cntrct_obj_version)` pairs in `test_isbert_schema.sof$ta_disc_zusgf`.
*   **Pass/Fail Criterion:**
    *   The query `SELECT cntrct_id, cntrct_obj_version FROM test_isbert_schema.sof$ta_disc_zusgf GROUP BY 1, 2 HAVING COUNT(*) > 1` must return zero rows.

    ```sql
    SELECT
        CASE
            WHEN COUNT(*) = 0 THEN 'PASS'
            ELSE 'FAIL'
        END AS uniqueness_check
    FROM (
        SELECT
            cntrct_id,
            cntrct_obj_version
        FROM `test_isbert_schema.sof$ta_disc_zusgf`
        GROUP BY 1, 2
        HAVING COUNT(*) > 1
    );
    ```

### Test Case 4.4: Data Integrity - No Unexpected Values

*   **Purpose:** Verify that `rabatt_alle` only contains expected concatenated strings or NULLs, and does not contain unexpected characters or malformed data.
*   **Setup:**
    1.  Populate `test_isbert_schema.sof$ta_discount` with valid and some potentially problematic (but still valid) `rabatt` and `rabatthoehe` values (e.g., very long numbers, numbers with many decimal places, special characters if allowed in source).
*   **Action:**
    1.  Run the `execute_bq_transformation` task (pointing to `test_isbert_schema`).
    2.  Query `test_isbert_schema.sof$ta_disc_zusgf` and sample `rabatt_alle` values.
*   **Pass/Fail Criterion:**
    *   All non-NULL `rabatt_alle` values must conform to the pattern `"<rabatt_value> (<rabatthoehe_value>%)"` or `"<rabatt_value> (<rabatthoehe_value>%), <rabatt_value_2> (<rabatthoehe_value_2>%)"`, etc.
    *   No `rabatt_alle` values should be empty strings unless explicitly designed.
    *   No `rabatt_alle` values should contain unparsed or erroneous characters.

    ```sql
    -- Check for malformed strings (e.g., missing parentheses, incorrect delimiter)
    SELECT
        COUNT(*)
    FROM `test_isbert_schema.sof$ta_disc_zusgf`
    WHERE rabatt_alle IS NOT NULL
    AND NOT REGEXP_CONTAINS(rabatt_alle, r'^[^,]+ \([^)]+%\)(, [^,]+ \([^)]+%\))*$');
    -- This regex checks for one or more "text (number%)" patterns, separated by ", "
    -- A count of 0 indicates pass.
    ```