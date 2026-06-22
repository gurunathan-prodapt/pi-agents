As a senior data-migration QA engineer, I have reviewed the migration design document and the generated code for `DW.BERT_AUSD_V_TA_CNTRCT_CRS3`. Below is a comprehensive suite of migration validation tests, organised by the specified criteria.

---

## Migration Validation Tests for DW.BERT_AUSD_V_TA_CNTRCT_CRS3

### 1. Output Parity — Same inputs produce the same outputs as the legacy job.

#### Test Case 1.1: End-to-End Data Parity

*   **Purpose**: To verify that the final output table `sof.ta_cntrct_crs3` in BigQuery is identical to the legacy `sof$ta_cntrct_crs3` in Oracle after running the migrated job with the same input data. This is the ultimate validation of behavioral equivalence.
*   **Setup**:
    1.  **Legacy Data Snapshot**: Obtain a consistent snapshot of the legacy Oracle tables: `isbert_schema.dwtk_meldungen`, `sof$ta_cntrct_crs2`, and the final output `sof$ta_cntrct_crs3`.
    2.  **BigQuery Input Data**: Ingest the data from the legacy `isbert_schema.dwtk_meldungen` and `sof$ta_cntrct_crs2` into their respective BigQuery tables (`isbert_schema.dwtk_meldungen`, `sof.ta_cntrct_crs2`). This ingestion must be a 1:1, type-compatible migration.
    3.  **BigQuery Target State**: Ensure the BigQuery target table `sof.ta_cntrct_crs3` is empty before running the migrated job.
    4.  **Airflow Configuration**: Configure the Airflow DAG (`dw_bert_ausd_v_ta_cntrct_crs3`) with correct `PROJECT_ID`, `REGION`, `CLUSTER_NAME`, and `GCS_BUCKET_FOR_SCRIPTS`.
*   **Action**:
    1.  Manually trigger the Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs3` in the GCP Composer environment.
    2.  Monitor the Airflow task and Dataproc job for successful completion.
    3.  Once completed, query the `sof.ta_cntrct_crs3` table in BigQuery.
*   **Pass/Fail Criterion**:
    *   **Pass**: The Airflow DAG completes successfully. The number of rows in BigQuery's `sof.ta_cntrct_crs3` is identical to the number of rows in the legacy Oracle `sof$ta_cntrct_crs3`. A deep comparison (e.g., checksum of sorted data, or row-by-row comparison) confirms that all columns and values in BigQuery's `sof.ta_cntrct_crs3` match the legacy Oracle `sof$ta_cntrct_crs3` for the same input data.
    *   **Fail**: The DAG fails, row counts differ, or data content does not match.

    ```sql
    -- Assuming legacy data is loaded into a temporary BigQuery table for comparison,
    -- e.g., `your-gcp-project-id.sof.ta_cntrct_crs3_legacy`.

    -- 1. Compare row counts
    SELECT
        (SELECT COUNT(*) FROM `your-gcp-project-id.sof.ta_cntrct_crs3`) AS new_row_count,
        (SELECT COUNT(*) FROM `your-gcp-project-id.sof.ta_cntrct_crs3_legacy`) AS legacy_row_count;
    -- Pass if new_row_count = legacy_row_count

    -- 2. Identify rows present in new but not in legacy
    SELECT
        COUNT(*) AS rows_in_new_not_in_legacy
    FROM (
        SELECT * FROM `your-gcp-project-id.sof.ta_cntrct_crs3`
        EXCEPT DISTINCT
        SELECT * FROM `your-gcp-project-id.sof.ta_cntrct_crs3_legacy`
    ) AS diff;
    -- Pass if rows_in_new_not_in_legacy = 0

    -- 3. Identify rows present in legacy but not in new
    SELECT
        COUNT(*) AS rows_in_legacy_not_in_new
    FROM (
        SELECT * FROM `your-gcp-project-id.sof.ta_cntrct_crs3_legacy`
        EXCEPT DISTINCT
        SELECT * FROM `your-gcp-project-id.sof.ta_cntrct_crs3`
    ) AS diff;
    -- Pass if rows_in_legacy_not_in_new = 0

    -- Overall Pass: All three queries above meet their respective pass criteria.
    ```

### 2. Transformation Correctness — joins, aggregations, filters, type handling, NULL handling, and any edge cases called out in the design.

#### Test Case 2.1: `v_datum` Variable Calculation

*   **Purpose**: To verify that the `v_datum` variable is correctly calculated as per the BigQuery SQL, even if it's not directly used in the `INSERT` statement. This ensures the BigQuery `DECLARE` logic is sound.
*   **Setup**:
    1.  Populate `isbert_schema.dwtk_meldungen` with various test data:
        *   Rows with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and valid `timecreated` values (e.g., '2023-01-15 10:00:00 UTC').
        *   Rows with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and `NULL` `timecreated`.
        *   Rows with different `job_kennung` (e.g., 'OTHER_JOB').
        *   An empty `dwtk_meldungen` table.
*   **Action**:
    1.  Execute the `DECLARE v_datum ...` statement directly in BigQuery.
    2.  Manually calculate the expected `v_datum` value for the given test data.
*   **Pass/Fail Criterion**:
    *   **Pass**: The `v_datum` variable, as observed in BigQuery's query plan or by extracting its value (if possible), matches the manually calculated expected value for each test scenario. Specifically, it should return the `MAX(FORMAT_DATE('%Y%m%d', DATE(timecreated)))` for matching `job_kennung`, or '19000101' if no matching rows or `timecreated` is NULL.
    *   **Fail**: The calculated `v_datum` does not match the expectation.

    ```sql
    -- Expected v_datum calculation for verification
    SELECT
      COALESCE(MAX(FORMAT_DATE('%Y%m%d', DATE(timecreated))), '19000101') AS expected_v_datum
    FROM
      `your-gcp-project-id.isbert_schema.dwtk_meldungen`
    WHERE
      job_kennung = 'BERT_DROP_TEMP_TABLE';
    -- This query should produce the value that the DECLARE statement assigns to v_datum.
    ```

#### Test Case 2.2: TRUNCATE Behavior

*   **Purpose**: To verify that the target table `sof.ta_cntrct_crs3` is correctly truncated before new data is inserted.
*   **Setup**:
    1.  Populate `sof.ta_cntrct_crs3` with some dummy data (e.g., 100 rows).
    2.  Populate `sof.ta_cntrct_crs2` and `isbert_schema.dwtk_meldungen` with data that would result in a known number of new rows being inserted (e.g., 50 rows).
*   **Action**:
    1.  Query `sof.ta_cntrct_crs3` to get its initial row count.
    2.  Trigger the Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs3`.
    3.  After the DAG completes, query `sof.ta_cntrct_crs3` again to get the final row count.
*   **Pass/Fail Criterion**:
    *   **Pass**: The final row count of `sof.ta_cntrct_crs3` is exactly 50 (the expected number of new rows), not 150 (initial + new). This confirms truncation occurred.
    *   **Fail**: The final row count is greater than the expected new rows, indicating the table was not truncated.

    ```sql
    -- Before job execution
    SELECT COUNT(*) FROM `your-gcp-project-id.sof.ta_cntrct_crs3`; -- Expected: 100 (dummy data)

    -- After job execution
    SELECT COUNT(*) FROM `your-gcp-project-id.sof.ta_cntrct_crs3`; -- Expected: 50 (newly inserted data)
    ```

#### Test Case 2.3: Join and Filter Logic (First SELECT)

*   **Purpose**: To verify the correctness of the `LEFT JOIN` and `WHERE` clause in the first `SELECT` statement, specifically for identifying non-twin-bill contracts and potential parents.
*   **Setup**:
    1.  Populate `sof.ta_cntrct_crs2` with diverse test data, including:
        *   Contracts `c` with `cntrct_ty` not in (10, 20) that have children `ctb` where `ctb.cntrct_ty = 20`.
        *   Contracts `c` with `cntrct_ty` not in (10, 20) that have children `ctb` where `ctb.cntrct_ty` is *not* 20 (should not join).
        *   Contracts `c` with `cntrct_ty` not in (10, 20) that have no children (`cntrct_parent` does not match any `c.cntrct_id`).
        *   Contracts `c` with `cntrct_ty` = 10 or 20 (should be filtered out).
        *   Rows with `NULL` values for `cntrct_parent` or `cntrct_id`.
*   **Action**:
    1.  Execute *only* the first `SELECT` statement (without the `UNION DISTINCT` and `TRUNCATE`/`INSERT`) directly in BigQuery.
    2.  Manually determine the expected output for the given test data based on the logic: `c.cntrct_ty NOT IN (10, 20)` and `LEFT JOIN` on `c.cntrct_id = ctb.cntrct_parent AND ctb.cntrct_ty = 20`.
*   **Pass/Fail Criterion**:
    *   **Pass**: The result set from the executed BigQuery `SELECT` statement matches the manually calculated expected output, including the `twinbill` (correctly 'TB' or `NULL`) and `twin_vertrag_id` columns.
    *   **Fail**: The result set differs from the expectation.

    ```sql
    -- SQL for direct testing of the first SELECT part
    SELECT DISTINCT
      c.cntrct_id,
      c.obj_version,
      c.contract_number,
      c.cntrct_template_id,
      c.cntrct_validity_id,
      c.valid_from,
      c.com_per_ext_rea_cv,
      c.billcycle_id,
      c.vo_code,
      c.cntrct_start_date,
      c.cntrct_st,
      c.cntrct_parent,
      c.cntrct_ty,
      c.cost_centre,
      c.cost_centre_user,
      c.commitment_reference_date,
      c.order_number,
      c.rv_num,
      IF(ctb.cntrct_id IS NOT NULL, 'TB', NULL) AS twinbill,
      ctb.cntrct_id AS twin_vertrag_id
    FROM
      `your-gcp-project-id.sof.ta_cntrct_crs2` c
    LEFT JOIN
      `your-gcp-project-id.sof.ta_cntrct_crs2` ctb
    ON
      c.cntrct_id = ctb.cntrct_parent
      AND ctb.cntrct_ty = 20
    WHERE
      c.cntrct_ty NOT IN (10, 20);
    ```

#### Test Case 2.4: Join and Filter Logic (Second SELECT)

*   **Purpose**: To verify the correctness of the `JOIN` and `WHERE` clause in the second `SELECT` statement, specifically for identifying twin-bill contracts and their parents.
*   **Setup**:
    1.  Populate `sof.ta_cntrct_crs2` with diverse test data, including:
        *   Contracts `c` with `cntrct_ty` not in (10, 20) that are parents of `ctb` contracts.
        *   Contracts `ctb` with `cntrct_ty = 20` whose `cntrct_parent` matches a `c` contract.
        *   Contracts `ctb` with `cntrct_ty = 20` whose `cntrct_parent` does *not* match a `c` contract (should not join).
        *   Contracts `ctb` with `cntrct_ty` *not* 20 (should be filtered out).
        *   Rows with `NULL` values for `cntrct_parent` or `cntrct_id`.
*   **Action**:
    1.  Execute *only* the second `SELECT` statement (without the `UNION DISTINCT` and `TRUNCATE`/`INSERT`) directly in BigQuery.
    2.  Manually determine the expected output for the given test data based on the logic: `ctb.cntrct_ty = 20` and `c.cntrct_ty NOT IN (10, 20)` and `JOIN` on `c.cntrct_id = ctb.cntrct_parent`.
*   **Pass/Fail Criterion**:
    *   **Pass**: The result set from the executed BigQuery `SELECT` statement matches the manually calculated expected output, including the hardcoded `twinbill = 'TB'` and `twin_vertrag_id = c.cntrct_id`.
    *   **Fail**: The result set differs from the expectation.

    ```sql
    -- SQL for direct testing of the second SELECT part
    SELECT DISTINCT
      ctb.cntrct_id,
      ctb.obj_version,
      ctb.contract_number,
      ctb.cntrct_template_id,
      ctb.cntrct_validity_id,
      ctb.valid_from,
      ctb.com_per_ext_rea_cv,
      ctb.billcycle_id,
      ctb.vo_code,
      ctb.cntrct_start_date,
      ctb.cntrct_st,
      ctb.cntrct_parent,
      ctb.cntrct_ty,
      ctb.cost_centre,
      ctb.cost_centre_user,
      ctb.commitment_reference_date,
      ctb.order_number,
      c.rv_num, -- Note: This column comes from 'c'
      'TB' AS twinbill,
      c.cntrct_id AS twin_vertrag_id
    FROM
      `your-gcp-project-id.sof.ta_cntrct_crs2` c
    JOIN
      `your-gcp-project-id.sof.ta_cntrct_crs2` ctb
    ON
      c.cntrct_id = ctb.cntrct_parent
    WHERE
      ctb.cntrct_ty = 20
      AND c.cntrct_ty NOT IN (10, 20);
    ```

#### Test Case 2.5: `UNION DISTINCT` and De-duplication

*   **Purpose**: To verify that the `UNION DISTINCT` operator correctly combines the results of the two `SELECT` statements and removes any duplicate rows that might arise from the join logic.
*   **Setup**:
    1.  Populate `sof.ta_cntrct_crs2` with data that is expected to produce some overlapping rows between the two `SELECT` statements (e.g., a parent contract `c` that is not type 10/20, and its child `ctb` that is type 20).
    2.  Ensure `sof.ta_cntrct_crs3` is empty.
*   **Action**:
    1.  Execute the full `INSERT` statement (excluding `TRUNCATE`) directly in BigQuery.
    2.  Manually calculate the expected final distinct set of rows.
*   **Pass/Fail Criterion**:
    *   **Pass**: The number of rows inserted into `sof.ta_cntrct_crs3` matches the expected distinct count. A comparison of the inserted data against the manually derived distinct set confirms correctness. Additionally, a query for duplicates in the final table returns 0 rows.
    *   **Fail**: The row count is incorrect, or duplicate rows are present when they shouldn't be, or expected distinct rows are missing.

    ```sql
    -- Query to check for duplicates in the final output table
    SELECT
        cntrct_id,
        obj_version,
        contract_number,
        -- ... include all columns ...
        twinbill,
        twin_vertrag_id,
        COUNT(*) AS num_occurrences
    FROM
        `your-gcp-project-id.sof.ta_cntrct_crs3`
    GROUP BY
        cntrct_id, obj_version, contract_number, cntrct_template_id, cntrct_validity_id, valid_from,
        com_per_ext_rea_cv, billcycle_id, vo_code, cntrct_start_date, cntrct_st, cntrct_parent,
        cntrct_ty, cost_centre, cost_centre_user, commitment_reference_date, order_number, rv_num,
        twinbill, twin_vertrag_id
    HAVING
        num_occurrences > 1;
    -- Pass if this query returns 0 rows.
    ```

#### Test Case 2.6: `rv_num` Column Origin in Second SELECT

*   **Purpose**: To verify that the `rv_num` column in the second `SELECT` statement correctly retrieves the value from the parent contract (`c`) and not the twin-bill contract (`ctb`).
*   **Setup**:
    1.  Populate `sof.ta_cntrct_crs2` with test data where:
        *   A parent contract `c` has `cntrct_ty` not in (10, 20) and a distinct `rv_num` (e.g., 'PARENT_RV_123').
        *   A child contract `ctb` has `cntrct_ty = 20`, `cntrct_parent` matching `c.cntrct_id`, and a different `rv_num` (e.g., 'CHILD_RV_456').
*   **Action**:
    1.  Execute the full `INSERT` statement (or the second `SELECT` part directly) in BigQuery.
    2.  Inspect the `rv_num` column for the rows corresponding to the `ctb` contracts that would be generated by the second `SELECT`.
*   **Pass/Fail Criterion**:
    *   **Pass**: For `ctb` contracts generated by the second `SELECT`, the `rv_num` column in `sof.ta_cntrct_crs3` contains the value from the `c` (parent) contract ('PARENT_RV_123'), not the `ctb` (child) contract ('CHILD_RV_456').
    *   **Fail**: The `rv_num` column contains the value from the `ctb` contract or an incorrect value.

    ```sql
    -- Query to verify rv_num in the second SELECT's output
    SELECT
        t3.cntrct_id,
        t3.rv_num AS actual_rv_num,
        c.rv_num AS expected_rv_num_from_parent,
        ctb.rv_num AS rv_num_from_child_if_incorrect
    FROM
        `your-gcp-project-id.sof.ta_cntrct_crs3` t3
    JOIN
        `your-gcp-project-id.sof.ta_cntrct_crs2` c
        ON t3.twin_vertrag_id = c.cntrct_id -- Join back to parent
    JOIN
        `your-gcp-project-id.sof.ta_cntrct_crs2` ctb
        ON t3.cntrct_id = ctb.cntrct_id -- Join back to child
    WHERE
        t3.twinbill = 'TB' -- Focus on rows from the second SELECT
        AND t3.cntrct_ty = 20; -- Ensure it's a twin-bill contract
    -- Pass if actual_rv_num = expected_rv_num_from_parent for all relevant rows.
    ```

### 3. External-system replacements — Oracle reads, SFTP/S3 drops, etc. behave as the design specifies.

#### Test Case 3.1: Airflow Orchestration and Dataproc Execution

*   **Purpose**: To verify that the Airflow DAG correctly triggers the Dataproc job, the Python script executes successfully on Dataproc, and it correctly invokes the BigQuery SQL.
*   **Setup**:
    1.  Deploy the Airflow DAG (`dw_bert_ausd_v_ta_cntrct_crs3.py`) to Cloud Composer.
    2.  Upload the Python script (`r_ausd_v_ta_cntrct_crs3.py`) and the BigQuery SQL file (`d_ausd_v_ta_cntrct_crs3.bqsql`) to the specified GCS bucket (`GCS_BUCKET_FOR_SCRIPTS`).
    3.  Ensure a Dataproc cluster (`CLUSTER_NAME`) is running and accessible by the Airflow service account.
    4.  Ensure BigQuery tables (`isbert_schema.dwtk_meldungen`, `sof.ta_cntrct_crs2`, `sof.ta_cntrct_crs3`) exist and the Airflow/Dataproc service account has necessary IAM permissions (BigQuery Data Editor, Dataproc Worker, Storage Object Viewer/Creator).
*   **Action**:
    1.  Trigger the Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs3` from the Airflow UI.
    2.  Monitor the DAG run in Airflow UI, checking task logs.
    3.  Check Dataproc job history in the GCP Console for the submitted job.
    4.  Check BigQuery job history in the GCP Console for the executed SQL query.
    5.  Verify Cloud Logging for logs generated by the Python script.
*   **Pass/Fail Criterion**:
    *   **Pass**: The Airflow DAG run completes successfully (green status). The Dataproc job is submitted and completes successfully. The BigQuery query is executed and completes successfully. Relevant INFO and DEBUG logs from the Python script are visible in Cloud Logging, indicating successful execution steps.
    *   **Fail**: The Airflow DAG fails, the Dataproc job fails, the BigQuery query fails, or expected logs are missing/contain errors.

    ```python
    # Conceptual Python/pytest assertions for Airflow/Dataproc/BigQuery interaction:
    import pytest
    from airflow.api.client.local_client import Client
    from google.cloud import dataproc_v1 as dataproc
    from google.cloud import bigquery
    from google.cloud import logging_v2 as cloud_logging

    # Assume client setup for Airflow, Dataproc, BigQuery, Cloud Logging

    def test_airflow_dag_triggers_dataproc_job_successfully():
        # 1. Trigger Airflow DAG
        # client.trigger_dag(dag_id='dw_bert_ausd_v_ta_cntrct_crs3')
        # 2. Poll Airflow for DAG run status
        # dag_run_status = client.get_dag_run_status(dag_id='dw_bert_ausd_v_ta_cntrct_crs3', run_id=...)
        # assert dag_run_status == 'success'

        # 3. Check Dataproc job status (extract job_id from Airflow logs)
        # dataproc_client = dataproc.JobControllerClient()
        # job = dataproc_client.get_job(project_id=PROJECT_ID, region=REGION, job_id=dataproc_job_id)
        # assert job.status.state == dataproc.JobStatus.State.DONE

        # 4. Check BigQuery job status (extract job_id from Dataproc/Python logs)
        # bq_client = bigquery.Client(project=PROJECT_ID)
        # bq_job = bq_client.get_job(bigquery_job_id)
        # assert bq_job.state == 'DONE'

        # 5. Check Cloud Logging for specific messages
        # logging_client = cloud_logging.Client(project=PROJECT_ID)
        # filter_string = f'resource.type="cloud_dataproc_cluster" AND textPayload:"Starting BigQuery job DW.BERT_AUSD_V_TA_CNTRCT_CRS3"'
        # entries = list(logging_client.list_entries(filter_=filter_string))
        # assert any("BigQuery job completed successfully." in entry.payload for entry in entries)
        pass # Placeholder for actual implementation
    ```

#### Test Case 3.2: Python Script Error Handling

*   **Purpose**: To verify that the Python script `r_ausd_v_ta_cntrct_crs3.py` correctly handles errors during BigQuery execution (e.g., SQL syntax errors, permission issues) and logs them appropriately to Cloud Logging.
*   **Setup**:
    1.  Deploy the Airflow DAG and Python script.
    2.  **Introduce an error**: Temporarily modify `d_ausd_v_ta_cntrct_crs3.bqsql` to introduce a syntax error (e.g., change `SELECT DISTINCT` to `SELECT DISTINC`). Upload this faulty SQL to GCS.
    3.  Ensure `sof.ta_cntrct_crs3` is empty.
*   **Action**:
    1.  Trigger the Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs3`.
    2.  Monitor the Airflow task logs and Cloud Logging.
*   **Pass/Fail Criterion**:
    *   **Pass**: The Airflow DAG fails gracefully. The Python script logs an error message (e.g., `BigQuery API error: ...`) indicating the specific BigQuery or SQL syntax error. The `sof.ta_cntrct_crs3` table remains empty (or in its pre-execution state if truncation failed). The Python script exits with a non-zero status code, causing the Dataproc job and Airflow task to fail.
    *   **Fail**: The DAG hangs, fails without clear error messages, or the Python script does not log the error correctly, or the table state is unexpectedly modified.

    ```python
    # Conceptual Python/pytest assertion for error logging:
    def test_python_script_handles_bigquery_error_gracefully(caplog):
        # Trigger DAG with faulty SQL (as per setup)
        # Wait for DAG run to complete (expected to fail)

        # Assert DAG run status is 'failed'
        # assert get_dag_run_status(dag_id='dw_bert_ausd_v_ta_cntrct_crs3') == 'failed'

        # Check Cloud Logging for specific error messages from the Python script
        # logging_client = cloud_logging.Client(project=PROJECT_ID)
        # filter_string = f'resource.type="cloud_dataproc_cluster" AND textPayload:"BigQuery API error"'
        # entries = list(logging_client.list_entries(filter_=filter_string))
        # assert any("BigQuery API error" in entry.payload for entry in entries)
        # assert any("400 Bad Request" in entry.payload for entry in entries) # Example BQ error detail
        # assert not any("An unexpected error occurred" in entry.payload for entry in entries) # Should be specific
        pass # Placeholder for actual implementation
    ```

### 4. Data-quality / row-count / schema assertions.

#### Test Case 4.1: Schema Parity and Data Type Handling

*   **Purpose**: To verify that the BigQuery target table `sof.ta_cntrct_crs3` has the correct schema, including column names, data types, and nullability (if specified in the legacy schema), matching the legacy Oracle table. Also, to ensure data types are correctly mapped and values are preserved.
*   **Setup**:
    1.  Ensure the BigQuery DDL for `sof.ta_cntrct_crs3` has been applied.
    2.  Have access to the legacy Oracle schema definition for `sof$ta_cntrct_crs3`.
    3.  Populate `sof.ta_cntrct_crs2` and `isbert_schema.dwtk_meldungen` with diverse data, including edge cases for data types (e.g., max/min values for integers, long strings, various date formats, NULLs).
*   **Action**:
    1.  Query the schema of `sof.ta_cntrct_crs3` in BigQuery.
    2.  Compare it against the legacy Oracle schema.
    3.  After running the migrated job, sample data from `sof.ta_cntrct_crs3` and inspect column values for correctness and type integrity.
*   **Pass/Fail Criterion**:
    *   **Pass**: All column names, their corresponding BigQuery data types (e.g., `STRING`, `INT64`, `DATE`, `TIMESTAMP`), and nullability properties match the legacy Oracle schema. Sampled data values are correctly represented in their BigQuery types (e.g., dates are valid dates, numbers are within range, strings are not truncated). `NULL` values are preserved.
    *   **Fail**: Mismatches in column names, data types, or nullability are found. Data values are incorrect, malformed, or truncated.

    ```sql
    -- SQL to retrieve BigQuery schema for comparison
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM
        `your-gcp-project-id.sof.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = 'ta_cntrct_crs3'
    ORDER BY
        ordinal_position;

    -- Example SQL to check specific data type handling (e.g., date conversion)
    SELECT
        cntrct_id,
        valid_from,
        cntrct_start_date,
        commitment_reference_date
    FROM
        `your-gcp-project-id.sof.ta_cntrct_crs3`
    WHERE
        -- Filter for specific test cases or sample a few rows
        cntrct_id IN ('test_date_contract_1', 'test_date_contract_2');
    -- Manually verify these against expected values from Oracle.
    ```

#### Test Case 4.2: Row Count and Uniqueness Assertions

*   **Purpose**: To verify basic data quality assertions on the final `sof.ta_cntrct_crs3` table, specifically total row count and uniqueness of `cntrct_id` (if it's expected to be a primary key or unique identifier).
*   **Setup**:
    1.  Run the migrated job (Airflow DAG) with a representative dataset.
    2.  Have the expected total row count from the legacy system for the same input data.
    3.  Confirm if `cntrct_id` is expected to be unique in the output.
*   **Action**:
    1.  Query the total row count of `sof.ta_cntrct_crs3`.
    2.  Query for duplicate `cntrct_id` values in `sof.ta_cntrct_crs3`.
*   **Pass/Fail Criterion**:
    *   **Pass**: The total row count matches the expected count from the legacy system. There are no duplicate `cntrct_id` values (if `cntrct_id` is expected to be unique).
    *   **Fail**: Row count mismatch or duplicate `cntrct_id` values are found.

    ```sql
    -- Assert total row count
    SELECT COUNT(*) FROM `your-gcp-project-id.sof.ta_cntrct_crs3`;
    -- Expected: Matches legacy system's output row count (from Test Case 1.1).

    -- Assert uniqueness of cntrct_id (if applicable, based on business rules)
    SELECT
        cntrct_id,
        COUNT(*) AS num_occurrences
    FROM
        `your-gcp-project-id.sof.ta_cntrct_crs3`
    GROUP BY
        cntrct_id
    HAVING
        num_occurrences > 1;
    -- Expected: 0 rows returned if cntrct_id is a unique identifier.
    ```

#### Test Case 4.3: Edge Case - Empty Source Tables

*   **Purpose**: To verify the job handles scenarios where source tables (`sof.ta_cntrct_crs2`, `isbert_schema.dwtk_meldungen`) are completely empty.
*   **Setup**:
    1.  Ensure `sof.ta_cntrct_crs2` and `isbert_schema.dwtk_meldungen` are completely empty.
    2.  Ensure `sof.ta_cntrct_crs3` is empty.
*   **Action**:
    1.  Trigger the Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs3`.
    2.  After completion, query `sof.ta_cntrct_crs3`.
*   **Pass/Fail Criterion**:
    *   **Pass**: The Airflow DAG completes successfully. `sof.ta_cntrct_crs3` remains empty (0 rows). The `v_datum` variable is correctly set to '19000101' (as per Test Case 2.1).
    *   **Fail**: The DAG fails, or `sof.ta_cntrct_crs3` contains unexpected rows.

    ```sql
    -- After job execution
    SELECT COUNT(*) FROM `your-gcp-project-id.sof.ta_cntrct_crs3`;
    -- Expected: 0
    ```

#### Test Case 4.4: Edge Case - No Matching `job_kennung` for `v_datum`

*   **Purpose**: To verify the `v_datum` calculation correctly defaults to '19000101' when no rows match the `job_kennung` filter in `dwtk_meldungen`, and that the job still proceeds correctly.
*   **Setup**:
    1.  Populate `isbert_schema.dwtk_meldungen` with data, but ensure no rows have `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    2.  Populate `sof.ta_cntrct_crs2` with some data that would normally result in output.
    3.  Ensure `sof.ta_cntrct_crs3` is empty.
*   **Action**:
    1.  Trigger the Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs3`.
    2.  After completion, verify the contents of `sof.ta_cntrct_crs3`.
*   **Pass/Fail Criterion**:
    *   **Pass**: The Airflow DAG completes successfully. The `v_datum` variable (if observable) is '19000101'. The data in `sof.ta_cntrct_crs3` is as expected, demonstrating the job ran correctly despite the `v_datum` defaulting.
    *   **Fail**: The DAG fails, or `v_datum` is not '19000101', or the output data is incorrect.

    ```sql
    -- After job execution (assuming sof.ta_cntrct_crs2 had data)
    SELECT COUNT(*) FROM `your-gcp-project-id.sof.ta_cntrct_crs3`;
    -- Expected: > 0 (based on sof.ta_cntrct_crs2 data, unaffected by v_datum)
    ```