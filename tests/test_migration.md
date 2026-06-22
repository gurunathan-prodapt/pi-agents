As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the migration of `r_ausd_v_ta_cntrct_crs3.ksh` to Google Cloud Platform. These tests aim to ensure the migrated BigQuery stored procedure and Airflow DAG are functionally equivalent to the legacy KornShell and Oracle SQL job.

The tests are categorized to cover output parity, transformation correctness, external system replacements, and data quality assertions, as requested.

---

## Migration Validation Tests: `r_ausd_v_ta_cntrct_crs3`

### Common Setup for All Tests

Before executing any specific test case, the following setup steps must be performed to ensure a controlled and comparable environment:

1.  **Environment Preparation**:
    *   Ensure both the legacy Oracle environment and the target BigQuery environment are accessible.
    *   Verify that the Airflow DAG `dag_r_ausd_v_ta_cntrct_crs3` is deployed and accessible in Google Cloud Composer.
    *   Verify the BigQuery stored procedure `my-project.my_dataset.r_ausd_v_ta_cntrct_crs3` exists.
    *   Ensure the `job_audit_log` table exists in BigQuery.

2.  **Source Data Synchronization**:
    *   **Oracle**: Populate `sof$ta_cntrct_crs2` and `isbert_schema.dwtk_meldungen` with a carefully crafted dataset. This dataset should include:
        *   Contracts with `cntrct_ty` values other than 10 or 20 (primary contracts).
        *   Contracts with `cntrct_ty = 10` (RV) and `cntrct_ty = 20` (Mobilfunkzusatzvertrag) to test filtering.
        *   Parent contracts that have `cntrct_ty = 20` children.
        *   `cntrct_ty = 20` children contracts with valid parents.
        *   Contracts with `cntrct_parent` as NULL.
        *   Contracts that are parents but have no `cntrct_ty = 20` children.
        *   Multiple entries in `dwtk_meldungen` for `job_kennung = 'BERT_DROP_TEMP_TABLE'` with varying `timecreated` values to test the `MAX` logic.
        *   Entries in `dwtk_meldungen` for other `job_kennung` values to ensure they are ignored.
    *   **BigQuery**: Ingest the *exact same dataset* into `my-project.my_dataset.sof_ta_cntrct_crs2` and `my-project.my_dataset.dwtk_meldungen`. This is crucial for output parity. Use tools like `bq load` or Dataflow for this.

3.  **Target Table Reset**:
    *   Before each test run, ensure `sof$ta_cntrct_crs3` (Oracle) and `my-project.my_dataset.sof_ta_cntrct_crs3` (BigQuery) are empty.
    *   Clear relevant entries from `job_audit_log` for the specific `p_JobKennung` and `p_EintragsNr` being used, if necessary, to ensure clean audit trails for each test.

---

### Test Case 1.1: Full Data Parity (Golden Record Comparison)

*   **Purpose**: To verify that the migrated job produces an identical final dataset in the target table (`sof_ta_cntrct_crs3`) compared to the legacy job, given the same source data. This is the ultimate end-to-end validation of output parity.
*   **Setup**:
    1.  Follow the "Common Setup for All Tests" above, ensuring `sof_ta_cntrct_crs2` and `dwtk_meldungen` are populated with a comprehensive, identical dataset in both Oracle and BigQuery.
    2.  Ensure both target tables (`sof$ta_cntrct_crs3` and `my-project.my_dataset.sof_ta_cntrct_crs3`) are empty.
*   **Action**:
    1.  Execute the legacy job: `r_ausd_v_ta_cntrct_crs3.ksh`.
    2.  Execute the migrated job by triggering the Airflow DAG `dag_r_ausd_v_ta_cntrct_crs3`.
    3.  Extract all data from `sof$ta_cntrct_crs3` (Oracle) and `my-project.my_dataset.sof_ta_cntrct_crs3` (BigQuery).
*   **Pass/Fail Criterion**: The data extracted from both target tables must be identical in terms of row count, column values, and data types. A row-by-row comparison (after sorting by a unique key like `cntrct_id`) should yield no differences.

    ```python
    # Example Python (pytest) assertion for data parity
    import pandas as pd
    from google.cloud import bigquery
    import cx_Oracle # Assuming cx_Oracle for Oracle connection

    def test_full_data_parity():
        # --- Action: Extract data ---
        # Oracle extraction (example, actual implementation may vary)
        oracle_conn = cx_Oracle.connect("user/password@host:port/service_name")
        oracle_query = "SELECT * FROM sof$ta_cntrct_crs3 ORDER BY cntrct_id"
        df_oracle = pd.read_sql(oracle_query, oracle_conn)
        oracle_conn.close()

        # BigQuery extraction
        bq_client = bigquery.Client(project='my-project')
        bq_query = "SELECT * FROM `my-project.my_dataset.sof_ta_cntrct_crs3` ORDER BY cntrct_id"
        df_bigquery = bq_client.query(bq_query).to_dataframe()

        # --- Pass/Fail Criterion: Compare DataFrames ---
        # Ensure column names and order are consistent for comparison
        df_bigquery.columns = [col.upper() for col in df_bigquery.columns] # Oracle often uses uppercase
        df_oracle = df_oracle.astype(df_bigquery.dtypes.to_dict()) # Align data types if necessary

        pd.testing.assert_frame_equal(df_oracle, df_bigquery, check_dtype=True, check_exact=False, rtol=1e-9)
    ```

### Test Case 2.1: Date Determination Logic

*   **Purpose**: To verify that the `v_datum` variable (processing date) is correctly derived in the BigQuery stored procedure, matching the logic of selecting the maximum `timecreated` for `job_kennung = 'BERT_DROP_TEMP_TABLE'` from `dwtk_meldungen`.
*   **Setup**:
    1.  Follow the "Common Setup for All Tests".
    2.  Populate `dwtk_meldungen` in both Oracle and BigQuery with specific data:
        *   `job_kennung = 'BERT_DROP_TEMP_TABLE'`, `timecreated = '2023-01-15 10:00:00'`
        *   `job_kennung = 'BERT_DROP_TEMP_TABLE'`, `timecreated = '2023-01-10 12:00:00'`
        *   `job_kennung = 'OTHER_JOB'`, `timecreated = '2023-01-20 08:00:00'`
    3.  Ensure `dwtk_meldungen` also contains a row for `BERT_DROP_TEMP_TABLE` with a NULL `timecreated` to test `COALESCE`.
*   **Action**:
    1.  Manually execute the date determination part of the legacy SQL script (or inspect logs if `v_datum` is logged).
    2.  Call the BigQuery stored procedure `r_ausd_v_ta_cntrct_crs3` and then query the `job_audit_log` table for the `process_date` field.
*   **Pass/Fail Criterion**: The `v_datum` (or `process_date` in `job_audit_log`) derived by the BigQuery stored procedure must be `20230115` (or `2023-01-15` as a date type), matching the `MAX(timecreated)` for the specified `job_kennung`. If all `timecreated` for `BERT_DROP_TEMP_TABLE` are NULL, it should default to `19000101`.

    ```sql
    -- BigQuery assertion after SP execution
    SELECT
        FORMAT_DATE('%Y%m%d', process_date) AS derived_date
    FROM
        `my-project.my_dataset.job_audit_log`
    WHERE
        job_kennung = 'BERT_AUSD_V_TA_CNTRCT_CRS3' -- Or the p_JobKennung used
        AND status = 'SUCCESS'
    ORDER BY
        start_timestamp DESC
    LIMIT 1;
    -- Expected result: '20230115' (or '19000101' if all relevant timecreated are NULL)
    ```

### Test Case 2.2: Truncate Behavior

*   **Purpose**: To confirm that the BigQuery stored procedure correctly truncates the target table `sof_ta_cntrct_crs3` before inserting new data, mirroring the Oracle `TRUNCATE TABLE` behavior.
*   **Setup**:
    1.  Follow the "Common Setup for All Tests".
    2.  Pre-populate `my-project.my_dataset.sof_ta_cntrct_crs3` with some dummy data (e.g., 5 rows).
    3.  Populate `sof_ta_cntrct_crs2` with 3 rows of valid data that would be inserted by the job.
*   **Action**:
    1.  Execute the Airflow DAG `dag_r_ausd_v_ta_cntrct_crs3`.
    2.  Query the row count of `my-project.my_dataset.sof_ta_cntrct_crs3` after the job completes.
*   **Pass/Fail Criterion**: The final row count in `my-project.my_dataset.sof_ta_cntrct_crs3` must be equal to the number of rows inserted by the `INSERT ... SELECT` statement (3 in this example), not the sum of initial dummy data and new data.

    ```sql
    -- BigQuery assertion after SP execution
    SELECT COUNT(*) FROM `my-project.my_dataset.sof_ta_cntrct_crs3`;
    -- Expected result: 3 (based on the example setup)
    ```

### Test Case 2.3: Primary Contracts Branch (No Twinbill)

*   **Purpose**: To verify the first `SELECT` branch of the `UNION DISTINCT` correctly identifies primary contracts that are not RV (10) or Mobilfunkzusatzvertrag (20) and have no `cntrct_ty = 20` children, resulting in `twinbill` and `twin_vertrag_id` being NULL.
*   **Setup**:
    1.  Follow the "Common Setup for All Tests".
    2.  Populate `sof_ta_cntrct_crs2` with:
        *   `cntrct_id = 100`, `cntrct_ty = 30` (e.g., "Normal Contract"), `cntrct_parent = NULL`.
        *   `cntrct_id = 101`, `cntrct_ty = 40`, `cntrct_parent = NULL`.
        *   No `cntrct_ty = 20` contracts that have 100 or 101 as parent.
*   **Action**:
    1.  Execute the Airflow DAG `dag_r_ausd_v_ta_cntrct_crs3`.
    2.  Query `my-project.my_dataset.sof_ta_cntrct_crs3` for `cntrct_id` 100 and 101.
*   **Pass/Fail Criterion**: For `cntrct_id` 100 and 101, `twinbill` must be NULL and `twin_vertrag_id` must be NULL.

    ```sql
    -- BigQuery assertion after SP execution
    SELECT
        cntrct_id,
        twinbill,
        twin_vertrag_id
    FROM
        `my-project.my_dataset.sof_ta_cntrct_crs3`
    WHERE
        cntrct_id IN (100, 101);
    -- Expected result:
    -- cntrct_id | twinbill | twin_vertrag_id
    -- ----------|----------|----------------
    -- 100       | NULL     | NULL
    -- 101       | NULL     | NULL
    ```

### Test Case 2.4: Primary Contracts Branch (With Twinbill)

*   **Purpose**: To verify the first `SELECT` branch correctly identifies primary contracts that are not RV (10) or Mobilfunkzusatzvertrag (20) and *do* have `cntrct_ty = 20` children, resulting in `twinbill = 'TB'` and `twin_vertrag_id` being the child's ID.
*   **Setup**:
    1.  Follow the "Common Setup for All Tests".
    2.  Populate `sof_ta_cntrct_crs2` with:
        *   `cntrct_id = 200`, `cntrct_ty = 30` (Parent contract).
        *   `cntrct_id = 201`, `cntrct_ty = 20` (Child contract), `cntrct_parent = 200`.
*   **Action**:
    1.  Execute the Airflow DAG `dag_r_ausd_v_ta_cntrct_crs3`.
    2.  Query `my-project.my_dataset.sof_ta_cntrct_crs3` for `cntrct_id` 200.
*   **Pass/Fail Criterion**: For `cntrct_id` 200, `twinbill` must be 'TB' and `twin_vertrag_id` must be 201.

    ```sql
    -- BigQuery assertion after SP execution
    SELECT
        cntrct_id,
        twinbill,
        twin_vertrag_id
    FROM
        `my-project.my_dataset.sof_ta_cntrct_crs3`
    WHERE
        cntrct_id = 200;
    -- Expected result:
    -- cntrct_id | twinbill | twin_vertrag_id
    -- ----------|----------|----------------
    -- 200       | 'TB'     | 201
    ```

### Test Case 2.5: Mobilfunkzusatzvertrag Branch

*   **Purpose**: To verify the second `SELECT` branch of the `UNION DISTINCT` correctly identifies `cntrct_ty = 20` contracts and assigns `twinbill = 'TB'` and `twin_vertrag_id` as their parent's ID, and that `rv_num` is correctly taken from the parent.
*   **Setup**:
    1.  Follow the "Common Setup for All Tests".
    2.  Populate `sof_ta_cntrct_crs2` with:
        *   `cntrct_id = 300`, `cntrct_ty = 30` (Parent contract), `rv_num = 'RV123'`.
        *   `cntrct_id = 301`, `cntrct_ty = 20` (Child contract), `cntrct_parent = 300`, `rv_num = 'RV456'` (this `rv_num` should be ignored).
*   **Action**:
    1.  Execute the Airflow DAG `dag_r_ausd_v_ta_cntrct_crs3`.
    2.  Query `my-project.my_dataset.sof_ta_cntrct_crs3` for `cntrct_id` 301.
*   **Pass/Fail Criterion**: For `cntrct_id` 301, `twinbill` must be 'TB', `twin_vertrag_id` must be 300, and `rv_num` must be 'RV123' (from the parent).

    ```sql
    -- BigQuery assertion after SP execution
    SELECT
        cntrct_id,
        twinbill,
        twin_vertrag_id,
        rv_num
    FROM
        `my-project.my_dataset.sof_ta_cntrct_crs3`
    WHERE
        cntrct_id = 301;
    -- Expected result:
    -- cntrct_id | twinbill | twin_vertrag_id | rv_num
    -- ----------|----------|-----------------|---------
    -- 301       | 'TB'     | 300             | 'RV123'
    ```

### Test Case 2.6: `cntrct_ty` Filtering

*   **Purpose**: To confirm that contracts with `cntrct_ty = 10` (RV) or `cntrct_ty = 20` (Mobilfunkzusatzvertrag) are correctly excluded from being processed as primary contracts in the first branch.
*   **Setup**:
    1.  Follow the "Common Setup for All Tests".
    2.  Populate `sof_ta_cntrct_crs2` with:
        *   `cntrct_id = 400`, `cntrct_ty = 10`.
        *   `cntrct_id = 401`, `cntrct_ty = 20`.
        *   `cntrct_id = 402`, `cntrct_ty = 30` (control contract).
*   **Action**:
    1.  Execute the Airflow DAG `dag_r_ausd_v_ta_cntrct_crs3`.
    2.  Query `my-project.my_dataset.sof_ta_cntrct_crs3` for `cntrct_id` 400, 401, and 402.
*   **Pass/Fail Criterion**: Only `cntrct_id = 402` should be present in the target table. `cntrct_id` 400 and 401 should not be present (unless 401 is a child of another contract, which would be covered by Test 2.5).

    ```sql
    -- BigQuery assertion after SP execution
    SELECT
        cntrct_id
    FROM
        `my-project.my_dataset.sof_ta_cntrct_crs3`
    WHERE
        cntrct_id IN (400, 401, 402);
    -- Expected result:
    -- cntrct_id
    -- ----------
    -- 402
    ```

### Test Case 2.7: NULL Handling in Joins/Conditions

*   **Purpose**: To ensure the transformation logic correctly handles NULL values in `cntrct_parent` and other relevant columns, especially in join conditions and `CASE` statements.
*   **Setup**:
    1.  Follow the "Common Setup for All Tests".
    2.  Populate `sof_ta_cntrct_crs2` with:
        *   `cntrct_id = 500`, `cntrct_ty = 30`, `cntrct_parent = NULL`.
        *   `cntrct_id = 501`, `cntrct_ty = 20`, `cntrct_parent = NULL` (orphan child, should not be processed by second branch).
        *   `cntrct_id = 502`, `cntrct_ty = 30`, `cntrct_parent = 500`.
*   **Action**:
    1.  Execute the Airflow DAG `dag_r_ausd_v_ta_cntrct_crs3`.
    2.  Query `my-project.my_dataset.sof_ta_cntrct_crs3` for `cntrct_id` 500, 501, and 502.
*   **Pass/Fail Criterion**:
    *   `cntrct_id = 500` should be present, with `twinbill = NULL` and `twin_vertrag_id = NULL`.
    *   `cntrct_id = 501` should *not* be present in the target table (as it's a `cntrct_ty=20` without a parent, and the second branch requires a join).
    *   `cntrct_id = 502` should be present, with `twinbill = NULL` and `twin_vertrag_id = NULL`.

    ```sql
    -- BigQuery assertion after SP execution
    SELECT
        cntrct_id,
        twinbill,
        twin_vertrag_id
    FROM
        `my-project.my_dataset.sof_ta_cntrct_crs3`
    WHERE
        cntrct_id IN (500, 501, 502);
    -- Expected result:
    -- cntrct_id | twinbill | twin_vertrag_id
    -- ----------|----------|----------------
    -- 500       | NULL     | NULL
    -- 502       | NULL     | NULL
    -- (501 should not appear)
    ```

### Test Case 3.1: Airflow Orchestration

*   **Purpose**: To verify that the Airflow DAG successfully triggers the BigQuery stored procedure and handles its execution, including parameter passing.
*   **Setup**:
    1.  Ensure the Airflow DAG `dag_r_ausd_v_ta_cntrct_crs3` is deployed and configured with correct `project_id`, `dataset_id`, and `procedure_id`.
    2.  Ensure `google_cloud_default` connection is configured in Airflow.
    3.  Set `p_JobKennung` and `p_EintragsNr` parameters in the DAG to specific test values (e.g., `p_JobKennung='TEST_AIRFLOW_JOB'`, `p_EintragsNr=999`).
*   **Action**:
    1.  Manually trigger the Airflow DAG `dag_r_ausd_v_ta_cntrct_crs3` from the Airflow UI.
    2.  Monitor the DAG run in the Airflow UI.
    3.  Check the BigQuery `job_audit_log` table.
*   **Pass/Fail Criterion**:
    1.  The Airflow DAG run must complete successfully (green status).
    2.  A "STARTED" and "SUCCESS" entry must be present in `my-project.my_dataset.job_audit_log` with `job_kennung = 'TEST_AIRFLOW_JOB'` and `eintrags_nr = 999`.

    ```sql
    -- BigQuery assertion after DAG run
    SELECT
        job_kennung,
        eintrags_nr,
        status,
        message
    FROM
        `my-project.my_dataset.job_audit_log`
    WHERE
        job_kennung = 'TEST_AIRFLOW_JOB'
        AND eintrags_nr = 999
    ORDER BY
        start_timestamp;
    -- Expected result:
    -- job_kennung        | eintrags_nr | status  | message
    -- -------------------|-------------|---------|------------------------
    -- 'TEST_AIRFLOW_JOB' | 999         | 'STARTED' | 'Job started'
    -- 'TEST_AIRFLOW_JOB' | 999         | 'SUCCESS' | 'Job completed successfully'
    ```

### Test Case 3.2: BigQuery Audit Log (Error Handling)

*   **Purpose**: To verify that the BigQuery stored procedure correctly logs "FAILED" status and error messages to `job_audit_log` when an error occurs during execution.
*   **Setup**:
    1.  Follow the "Common Setup for All Tests".
    2.  Modify the `sof_ta_cntrct_crs2` table in BigQuery to intentionally cause an error (e.g., change a column's data type to be incompatible with the `INSERT` statement, or drop a required column).
    3.  Set `p_JobKennung` and `p_EintragsNr` parameters in the DAG to specific test values (e.g., `p_JobKennung='TEST_ERROR_JOB'`, `p_EintragsNr=888`).
*   **Action**:
    1.  Trigger the Airflow DAG `dag_r_ausd_v_ta_cntrct_crs3`.
    2.  Monitor the DAG run in the Airflow UI.
    3.  Query the BigQuery `job_audit_log` table.
*   **Pass/Fail Criterion**:
    1.  The Airflow DAG run must fail (red status).
    2.  A "STARTED" and "FAILED" entry must be present in `my-project.my_dataset.job_audit_log` with `job_kennung = 'TEST_ERROR_JOB'` and `eintrags_nr = 888`.
    3.  The "FAILED" entry must contain a non-empty `error_message` detailing the BigQuery error.

    ```sql
    -- BigQuery assertion after DAG run
    SELECT
        job_kennung,
        eintrags_nr,
        status,
        message,
        error_message
    FROM
        `my-project.my_dataset.job_audit_log`
    WHERE
        job_kennung = 'TEST_ERROR_JOB'
        AND eintrags_nr = 888
    ORDER BY
        start_timestamp;
    -- Expected result:
    -- job_kennung      | eintrags_nr | status  | message      | error_message
    -- -----------------|-------------|---------|--------------|-------------------------------------------------
    -- 'TEST_ERROR_JOB' | 888         | 'STARTED' | 'Job started'| NULL
    -- 'TEST_ERROR_JOB' | 888         | 'FAILED'  | 'Job failed' | 'BigQuery error message details...' (non-NULL)
    ```

### Test Case 4.1: Row Count Parity

*   **Purpose**: To verify that the total number of rows inserted into `sof_ta_cntrct_crs3` by the migrated job matches the row count from the legacy job.
*   **Setup**:
    1.  Follow the "Common Setup for All Tests", ensuring identical source data.
    2.  Ensure both target tables are empty.
*   **Action**:
    1.  Execute the legacy job: `r_ausd_v_ta_cntrct_crs3.ksh`. Record the row count from `sof$ta_cntrct_crs3`.
    2.  Execute the migrated job by triggering the Airflow DAG `dag_r_ausd_v_ta_cntrct_crs3`.
    3.  Query the row count from `my-project.my_dataset.sof_ta_cntrct_crs3`.
*   **Pass/Fail Criterion**: The row count in `my-project.my_dataset.sof_ta_cntrct_crs3` must be exactly equal to the row count in `sof$ta_cntrct_crs3`.

    ```sql
    -- BigQuery assertion after SP execution
    SELECT COUNT(*) FROM `my-project.my_dataset.sof_ta_cntrct_crs3`;
    -- Compare this count to the count from Oracle's sof$ta_cntrct_crs3.
    ```

### Test Case 4.2: Schema Parity

*   **Purpose**: To confirm that the schema (column names, data types, nullability) of the target table `sof_ta_cntrct_crs3` in BigQuery is identical to its Oracle counterpart.
*   **Setup**:
    1.  Ensure both Oracle and BigQuery target tables exist.
*   **Action**:
    1.  Extract the schema definition for `sof$ta_cntrct_crs3` from Oracle (e.g., using `DESCRIBE` or `ALL_TAB_COLUMNS`).
    2.  Extract the schema definition for `my-project.my_dataset.sof_ta_cntrct_crs3` from BigQuery (e.g., using `INFORMATION_SCHEMA.COLUMNS`).
*   **Pass/Fail Criterion**:
    1.  All column names must match (case-insensitivity might need to be considered if Oracle uses mixed case and BigQuery defaults to lowercase).
    2.  Data types must be functionally equivalent (e.g., `NUMBER` in Oracle maps to `INT64` or `BIGNUMERIC` in BigQuery, `VARCHAR2` to `STRING`, `DATE` to `DATE` or `TIMESTAMP`).
    3.  Nullability constraints should match.

    ```sql
    -- BigQuery schema extraction
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM
        `my-project.my_dataset.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = 'sof_ta_cntrct_crs3'
    ORDER BY
        ordinal_position;

    -- Oracle schema extraction (example)
    SELECT
        column_name,
        data_type,
        nullable
    FROM
        ALL_TAB_COLUMNS
    WHERE
        owner = 'SOF' AND table_name = 'TA_CNTRCT_CRS3'
    ORDER BY
        column_id;
    ```

### Test Case 4.3: Data Integrity (Uniqueness)

*   **Purpose**: To assert that the `cntrct_id` column in the target table remains unique, as expected for a primary key or unique identifier.
*   **Setup**:
    1.  Follow the "Common Setup for All Tests".
    2.  Ensure `sof_ta_cntrct_crs2` contains data that, after transformation, should result in unique `cntrct_id` values in the target.
*   **Action**:
    1.  Execute the Airflow DAG `dag_r_ausd_v_ta_cntrct_crs3`.
    2.  Query `my-project.my_dataset.sof_ta_cntrct_crs3` to check for duplicate `cntrct_id` values.
*   **Pass/Fail Criterion**: The query for duplicate `cntrct_id` values must return zero rows.

    ```sql
    -- BigQuery assertion after SP execution
    SELECT
        cntrct_id,
        COUNT(*) AS num_duplicates
    FROM
        `my-project.my_dataset.sof_ta_cntrct_crs3`
    GROUP BY
        cntrct_id
    HAVING
        COUNT(*) > 1;
    -- Expected result: 0 rows
    ```

### Test Case 4.4: Referential Integrity (Twinbill)

*   **Purpose**: To verify that if `twinbill` is 'TB', then `twin_vertrag_id` correctly references an existing `cntrct_id` in the source table (`sof_ta_cntrct_crs2`), ensuring logical consistency.
*   **Setup**:
    1.  Follow the "Common Setup for All Tests".
    2.  Ensure `sof_ta_cntrct_crs2` contains valid parent-child relationships for twinbill contracts.
*   **Action**:
    1.  Execute the Airflow DAG `dag_r_ausd_v_ta_cntrct_crs3`.
    2.  Query `my-project.my_dataset.sof_ta_cntrct_crs3` to find any `twin_vertrag_id` values that do not exist in `my-project.my_dataset.sof_ta_cntrct_crs2`.
*   **Pass/Fail Criterion**: The query must return zero rows, indicating all `twin_vertrag_id` values (where `twinbill = 'TB'`) have a corresponding `cntrct_id` in the source table.

    ```sql
    -- BigQuery assertion after SP execution
    SELECT
        t.cntrct_id AS target_cntrct_id,
        t.twin_vertrag_id
    FROM
        `my-project.my_dataset.sof_ta_cntrct_crs3` AS t
    LEFT JOIN
        `my-project.my_dataset.sof_ta_cntrct_crs2` AS s
        ON t.twin_vertrag_id = s.cntrct_id
    WHERE
        t.twinbill = 'TB'
        AND t.twin_vertrag_id IS NOT NULL -- Should always be true if twinbill is 'TB'
        AND s.cntrct_id IS NULL; -- twin_vertrag_id not found in source
    -- Expected result: 0 rows
    ```