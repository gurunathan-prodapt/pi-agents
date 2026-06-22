The following migration validation tests are designed to ensure the migrated Airflow DAG (`k_ausd_v_ta_p_discount_rr.py`) is behaviorally equivalent to the legacy KornShell job (`k_ausd_v_ta_p_discount_rr.ksh`). These tests cover output parity, transformation correctness, external system replacements, and data quality assertions, with a focus on critical discrepancies identified in the migration design.

---

## Migration Validation Tests for `k_ausd_v_ta_p_discount_rr`

**Assumptions for all tests:**
*   Oracle source tables (`sof$ta_discount_rr`, `sof$ta_cntrct_crs`, `sof$ta_cntrct_templ`, `isbert_schema.dwtk_meldungen`) are populated with a consistent set of test data.
*   BigQuery source tables (`your_project.your_dataset.sof_ta_discount_rr`, `your_project.your_dataset.sof_ta_cntrct_crs`, `your_project.your_dataset.sof_ta_cntrct_templ`, `your_project.your_dataset.dwtk_meldungen`) are populated with data identical to their Oracle counterparts.
*   Access to execute the legacy KornShell script and the migrated Airflow DAG.
*   Tools or scripts are available to extract and compare data from Oracle and BigQuery.

---

### Test Case 1: End-to-End Output Parity (Full Load)

*   **Purpose:** To verify that the final data in the target table `sof_ta_p_discount_rr` is identical between the legacy Oracle job and the migrated BigQuery job, assuming the `TRUNCATE` behavior is replicated in BigQuery. This is the primary test for overall functional correctness.
*   **Setup:**
    1.  Ensure all source tables (Oracle and BigQuery) are populated with an identical, representative dataset.
    2.  **Crucially, modify the generated Airflow DAG to perform a `TRUNCATE` before `INSERT` to match the legacy behavior.** This can be done by changing `write_disposition="WRITE_APPEND"` to `write_disposition="WRITE_TRUNCATE"` in the `BigQueryExecuteQueryOperator`, or by adding an explicit `TRUNCATE TABLE` statement before the `INSERT` in `build_discount_rr_sql()`. For this test, we assume the `WRITE_TRUNCATE` approach.
    3.  Ensure the `v_datum` logic (reading `dwtk_meldungen` for `timecreated`) is incorporated into the BigQuery SQL, and if it's used for filtering, that filter is also applied. (This will be explicitly tested in Test Case 6, but for parity, we need to assume the fix is applied here).
*   **Action:**
    1.  Execute the legacy KornShell job (`k_ausd_v_ta_p_discount_rr.ksh`).
    2.  Extract all data from the Oracle target table `sof$ta_p_discount_rr`.
    3.  Execute the modified Airflow DAG (`k_ausd_v_ta_p_discount_rr.py`).
    4.  Extract all data from the BigQuery target table `your_project.your_dataset.sof_ta_p_discount_rr`.
    5.  Compare the extracted datasets.
*   **Pass/Fail Criterion:** The data extracted from the Oracle `sof$ta_p_discount_rr` table is identical to the data extracted from the BigQuery `your_project.your_dataset.sof_ta_p_discount_rr` table, considering column order and data types.

    ```python
    # Example Python pseudo-code for comparison
    import pandas as pd
    from google.cloud import bigquery
    import cx_Oracle # Assuming cx_Oracle for Oracle connection

    def fetch_oracle_data(query):
        # Establish Oracle connection and fetch data
        conn = cx_Oracle.connect("user/password@host:port/service_name")
        df = pd.read_sql(query, conn)
        conn.close()
        return df

    def fetch_bigquery_data(query, project_id):
        client = bigquery.Client(project=project_id)
        df = client.query(query).to_dataframe()
        return df

    oracle_query = "SELECT cntrct_id, discount_id, disc_vector_ty, cntrct_obj_version, cntrct_template_id, disc_invoice_item_id, rabatt, rabatthoehe, rabattierte_rech_pos, contract_number, std_vertrag FROM sof$ta_p_discount_rr ORDER BY 1,2,3"
    bq_query = "SELECT cntrct_id, discount_id, disc_vector_ty, cntrct_obj_version, cntrct_template_id, disc_invoice_item_id, rabatt, rabatthoehe, rabattierte_rech_pos, contract_number, std_vertrag FROM `your_project.your_dataset.sof_ta_p_discount_rr` ORDER BY 1,2,3"

    oracle_df = fetch_oracle_data(oracle_query)
    bq_df = fetch_bigquery_data(bq_query, "your_project")

    # Ensure column names and types are consistent for comparison
    bq_df.columns = [col.upper() for col in bq_df.columns] # Oracle often returns uppercase
    # Convert BigQuery NUMERIC to float/decimal if Oracle uses NUMBER
    for col in ['RABATT', 'RABATTHOEHE']:
        if col in bq_df.columns:
            bq_df[col] = bq_df[col].astype(float) # Or Decimal if precision is critical

    assert oracle_df.equals(bq_df), "Data in target tables do not match!"
    print("Test Case 1 Passed: Data in target tables are identical.")
    ```

---

### Test Case 2: Transformation - Join Logic

*   **Purpose:** To specifically verify the correctness of the `INNER JOIN` conditions used in the `INSERT ... SELECT` statement.
*   **Setup:**
    1.  Populate source tables (`sof$ta_discount_rr`, `sof$ta_cntrct_crs`, `sof$ta_cntrct_templ`) with specific test data covering:
        *   Rows where all join conditions (`da.cntrct_id = c.cntrct_id AND da.cntrct_obj_version = c.obj_version`, `da.cntrct_template_id = ct.cntrct_template_id`) are met.
        *   Rows where `da.cntrct_id` matches `c.cntrct_id` but `da.cntrct_obj_version` does not match `c.obj_version`.
        *   Rows where `da.cntrct_id` does not match `c.cntrct_id`.
        *   Rows where `da.cntrct_template_id` does not match `ct.cntrct_template_id`.
        *   Rows with NULL values in join keys (these should be excluded by `INNER JOIN`).
    2.  Ensure the DAG is configured for `TRUNCATE` before `INSERT` (as in Test Case 1).
*   **Action:**
    1.  Execute the legacy KornShell job.
    2.  Execute the migrated Airflow DAG.
    3.  Query the target tables (`sof$ta_p_discount_rr` in Oracle and BigQuery) and compare the number of rows and the specific data for the test cases.
*   **Pass/Fail Criterion:**
    *   The number of rows in the target table is identical for both jobs.
    *   For each specific test data scenario, the presence or absence of rows in the target table matches expectations based on `INNER JOIN` logic.

    ```sql
    -- Example BigQuery SQL assertion for a specific join scenario
    -- Assuming a specific 'cntrct_id' and 'discount_id' that should join
    SELECT COUNT(*)
    FROM `your_project.your_dataset.sof_ta_p_discount_rr`
    WHERE cntrct_id = 'TEST_CNTRCT_MATCH' AND discount_id = 'TEST_DISCOUNT_MATCH';
    -- Expected: 1 (if the test data is set up to produce one match)

    -- Assuming a specific 'cntrct_id' that should NOT join due to obj_version mismatch
    SELECT COUNT(*)
    FROM `your_project.your_dataset.sof_ta_p_discount_rr`
    WHERE cntrct_id = 'TEST_CNTRCT_MISMATCH_OBJ_VER';
    -- Expected: 0
    ```

---

### Test Case 3: Transformation - Column Mapping and Data Types

*   **Purpose:** To verify that all source columns are correctly mapped to their target columns, and that data types are handled without loss of precision or unexpected conversion errors.
*   **Setup:**
    1.  Populate source tables with data that covers the full range of expected values for each column, including:
        *   Maximum length strings.
        *   Numbers with maximum precision (e.g., `rabatt`, `rabatthoehe`).
        *   Special characters in strings.
        *   NULL values (covered in Test Case 4, but also relevant here).
    2.  Ensure the DAG is configured for `TRUNCATE` before `INSERT`.
*   **Action:**
    1.  Execute the legacy KornShell job.
    2.  Execute the migrated Airflow DAG.
    3.  Select all columns from both target tables for a sample of rows (or all rows if feasible) and compare values.
*   **Pass/Fail Criterion:**
    *   All column values in the BigQuery target table exactly match their Oracle counterparts.
    *   The data types of the columns in the BigQuery target table (`your_project.your_dataset.sof_ta_p_discount_rr`) are appropriate (e.g., `NUMERIC` for `rabatt`, `STRING` for `cntrct_id`).

    ```sql
    -- Example BigQuery SQL to check data types
    SELECT
      column_name,
      data_type
    FROM
      `your_project.your_dataset.INFORMATION_SCHEMA.COLUMNS`
    WHERE
      table_name = 'sof_ta_p_discount_rr';

    -- Expected output (example):
    -- column_name        data_type
    -- cntrct_id          STRING
    -- discount_id        STRING
    -- disc_vector_ty     STRING
    -- cntrct_obj_version STRING
    -- cntrct_template_id STRING
    -- disc_invoice_item_id STRING
    -- rabatt             NUMERIC
    -- rabatthoehe        NUMERIC
    -- rabattierte_rech_pos STRING
    -- contract_number    STRING
    -- std_vertrag        STRING
    ```

---

### Test Case 4: Transformation - NULL Handling in Joins

*   **Purpose:** To ensure that NULL values in join keys are handled consistently between Oracle and BigQuery, specifically that `INNER JOIN`s correctly exclude rows where join keys are NULL.
*   **Setup:**
    1.  Populate `sof$ta_discount_rr` with rows where `cntrct_id` or `cntrct_obj_version` or `cntrct_template_id` are NULL.
    2.  Populate `sof$ta_cntrct_crs` with rows where `cntrct_id` or `obj_version` are NULL.
    3.  Populate `sof$ta_cntrct_templ` with rows where `cntrct_template_id` is NULL.
    4.  Ensure the DAG is configured for `TRUNCATE` before `INSERT`.
*   **Action:**
    1.  Execute the legacy KornShell job.
    2.  Execute the migrated Airflow DAG.
    3.  Query the target tables and verify that rows with NULLs in any of the join keys are *not* present in the final `sof_ta_p_discount_rr` table, as expected for `INNER JOIN` behavior.
*   **Pass/Fail Criterion:** The count of rows in the target table is identical for both jobs, and no rows resulting from NULL join key matches are present.

    ```sql
    -- Example BigQuery SQL assertion
    SELECT COUNT(*)
    FROM `your_project.your_dataset.sof_ta_p_discount_rr`
    WHERE cntrct_id IS NULL OR discount_id IS NULL OR disc_vector_ty IS NULL;
    -- Expected: 0 (assuming these are not nullable in the target and are derived from non-nullable source columns or join keys)
    ```

---

### Test Case 5: Behavioral Equivalence - Truncate vs. Append (CRITICAL DISCREPANCY)

*   **Purpose:** To explicitly demonstrate and test the difference in load strategy between the legacy job (which `TRUNCATE`s the target table) and the *original generated* Airflow DAG (which `APPEND`s to the target table). This test will highlight a functional mismatch.
*   **Setup:**
    1.  Ensure source tables are populated with an initial set of data (e.g., `data_set_A`).
    2.  **Do NOT modify the generated DAG for this test; keep `write_disposition="WRITE_APPEND"` as it was originally generated.**
*   **Action:**
    1.  **Initial Run:**
        *   Execute the legacy KornShell job.
        *   Record the row count in Oracle's `sof$ta_p_discount_rr` (e.g., `count_A_oracle`).
        *   Execute the *original generated* Airflow DAG.
        *   Record the row count in BigQuery's `your_project.your_dataset.sof_ta_p_discount_rr` (e.g., `count_A_bq`).
    2.  **Second Run (with new data):**
        *   Add a new, distinct set of data (e.g., `data_set_B`) to the source tables.
        *   Execute the legacy KornShell job again.
        *   Record the new row count in Oracle (e.g., `count_B_oracle`).
        *   Execute the *original generated* Airflow DAG again.
        *   Record the new row count in BigQuery (e.g., `count_B_bq`).
*   **Pass/Fail Criterion:**
    *   **FAIL:** `count_B_oracle` will be equal to the number of rows generated by `data_set_B` (because of `TRUNCATE`). `count_B_bq` will be `count_A_bq + number_of_rows_from_data_set_B` (because of `APPEND`).
    *   The test *fails* because `count_B_oracle` != `count_B_bq`. This demonstrates the behavioral difference.
*   **Recommendation for Fix:** The `BigQueryExecuteQueryOperator` should be configured with `write_disposition="WRITE_TRUNCATE"` if the intent is to fully replace the table on each run, or an explicit `DELETE FROM` statement should precede the `INSERT` if conditional deletion is required.

    ```python
    # Python pseudo-code for demonstrating the failure
    # (Assuming functions to run jobs and get counts exist)

    # Initial data set A
    run_legacy_job()
    count_A_oracle = get_oracle_row_count()
    run_migrated_dag_append() # Original generated DAG
    count_A_bq = get_bq_row_count()

    # Add new data set B to source tables
    add_new_source_data()

    # Second run
    run_legacy_job()
    count_B_oracle = get_oracle_row_count()
    run_migrated_dag_append()
    count_B_bq = get_bq_row_count()

    print(f"Oracle after 1st run: {count_A_oracle}")
    print(f"BigQuery after 1st run: {count_A_bq}")
    print(f"Oracle after 2nd run: {count_B_oracle}")
    print(f"BigQuery after 2nd run: {count_B_bq}")

    # This assertion is expected to FAIL with the original DAG
    assert count_B_oracle == count_B_bq, "CRITICAL: Load strategy mismatch (TRUNCATE vs. APPEND)"
    ```

---

### Test Case 6: Behavioral Equivalence - `v_datum` Date Filtering (CRITICAL DISCREPANCY)

*   **Purpose:** To test the absence of the `v_datum` logic (derived from `dwtk_meldungen`) in the *original generated* Airflow DAG. If the original Oracle SQL script used `v_datum` for filtering (which is highly probable for a data warehouse job), the migrated job will produce different results.
*   **Setup:**
    1.  Populate `isbert_schema.dwtk_meldungen` (Oracle) and `your_project.your_dataset.dwtk_meldungen` (BigQuery) with identical data, including a `timecreated` entry for `job_kennung = 'BERT_DROP_TEMP_TABLE'`. For example, set `timecreated` to '2023-01-01 00:00:00'.
    2.  Populate `sof$ta_discount_rr` with data where some rows have a `time_created` (or similar date column) *before* '2023-01-01' and some *after*.
    3.  **Do NOT modify the generated DAG for this test; it currently lacks the `v_datum` logic.**
    4.  **Assume the original Oracle SQL `d_ausd_v_ta_p_discount_rr.sql` contains a `WHERE` clause like `WHERE da.some_date_column >= TO_DATE(v_datum, 'YYYYMMDD')`.**
*   **Action:**
    1.  Execute the legacy KornShell job.
    2.  Record the row count in Oracle's `sof$ta_p_discount_rr`.
    3.  Execute the *original generated* Airflow DAG (ensure it's configured to `TRUNCATE` for a clean comparison, or clear the table manually before running).
    4.  Record the row count in BigQuery's `your_project.your_dataset.sof_ta_p_discount_rr`.
*   **Pass/Fail Criterion:**
    *   **FAIL:** The row count in Oracle will be lower (due to filtering by `v_datum`) than the row count in BigQuery (which processes all data without the filter).
    *   The test *fails* because the row counts differ, demonstrating the missing filtering logic.
*   **Recommendation for Fix:** The `build_discount_rr_sql()` function in the DAG must be updated to include the `v_datum` calculation and apply it as a filter in the `INSERT ... SELECT` statement.

    ```sql
    -- Proposed BigQuery SQL modification to incorporate v_datum
    -- This would be added to build_discount_rr_sql()
    DECLARE v_datum STRING;

    SET v_datum = (
      SELECT
        COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
      FROM
        `your_project.your_dataset.dwtk_meldungen` AS m
      WHERE
        m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    );

    -- Then, the INSERT statement would include a WHERE clause:
    INSERT INTO `your_project.your_dataset.sof_ta_p_discount_rr` (...)
    SELECT ...
    FROM `your_project.your_dataset.sof_ta_discount_rr` AS da
    JOIN ...
    WHERE da.some_date_column >= PARSE_DATE('%Y%m%d', v_datum); -- Assuming 'some_date_column' exists in da
    ```

---

### Test Case 7: Data Quality - Row Count

*   **Purpose:** To verify that the total number of rows processed and loaded into the target table is consistent between the legacy and migrated jobs.
*   **Setup:**
    1.  Ensure identical source data in Oracle and BigQuery.
    2.  Ensure the DAG is configured for `TRUNCATE` before `INSERT` (as in Test Case 1) and includes the `v_datum` logic if applicable.
*   **Action:**
    1.  Execute the legacy KornShell job.
    2.  Get the row count from Oracle's `sof$ta_p_discount_rr`.
    3.  Execute the migrated Airflow DAG.
    4.  Get the row count from BigQuery's `your_project.your_dataset.sof_ta_p_discount_rr`.
*   **Pass/Fail Criterion:** The row count from the Oracle target table is exactly equal to the row count from the BigQuery target table.

    ```sql
    -- BigQuery SQL to get row count
    SELECT COUNT(*) FROM `your_project.your_dataset.sof_ta_p_discount_rr`;
    ```

---

### Test Case 8: Data Quality - Schema Validation

*   **Purpose:** To ensure the BigQuery target table schema (`your_project.your_dataset.sof_ta_p_discount_rr`) correctly reflects the expected schema, including column names, data types, and nullability (if explicitly defined).
*   **Setup:** N/A (this is a metadata check).
*   **Action:**
    1.  Inspect the schema of the Oracle `sof$ta_p_discount_rr` table.
    2.  Inspect the schema of the BigQuery `your_project.your_dataset.sof_ta_p_discount_rr` table.
*   **Pass/Fail Criterion:** The BigQuery table schema matches the Oracle table schema in terms of column names, data types (with appropriate BigQuery equivalents), and nullability constraints.

    ```sql
    -- BigQuery SQL to inspect schema
    SELECT
      column_name,
      data_type,
      is_nullable
    FROM
      `your_project.your_dataset.INFORMATION_SCHEMA.COLUMNS`
    WHERE
      table_name = 'sof_ta_p_discount_rr'
    ORDER BY
      ordinal_position;
    ```

---

### Test Case 9: External System Replacement - Record Count Output

*   **Purpose:** The legacy job captures the number of processed records into a `tmpFile`. This test verifies that the migrated job can also provide this record count, even if the mechanism differs (e.g., via Airflow XComs or logs).
*   **Setup:**
    1.  Ensure identical source data in Oracle and BigQuery.
    2.  Modify the Airflow DAG to capture the row count of the `INSERT` operation. This can be done by using a `BigQueryExecuteQueryOperator` that runs `SELECT COUNT(*) FROM your_project.your_dataset.sof_ta_p_discount_rr` after the main `INSERT`, and pushes the result to an XCom.
*   **Action:**
    1.  Execute the legacy KornShell job.
    2.  Retrieve the record count from `$tmpFile`.
    3.  Execute the modified Airflow DAG.
    4.  Retrieve the record count from the Airflow task's logs or XComs.
*   **Pass/Fail Criterion:** The record count obtained from the migrated Airflow DAG (via XCom or logs) is identical to the record count obtained from the legacy job's `tmpFile`.

    ```python
    # Proposed Airflow DAG modification to capture row count
    # Add a task after process_discount_rr:
    get_row_count = BigQueryExecuteQueryOperator(
        task_id="get_row_count",
        sql="SELECT COUNT(*) FROM `your_project.your_dataset.sof_ta_p_discount_rr`",
        use_legacy_sql=False,
        do_xcom_push=True, # Push the result to XCom
        location="US",
    )

    # Update task dependencies
    start >> process_discount_rr >> get_row_count >> end

    # In a downstream PythonOperator or sensor, you could retrieve it:
    # from airflow.models import XCom
    # row_count = XCom.pull(task_ids='get_row_count', key='return_value')
    # print(f"Processed records: {row_count}")
    ```