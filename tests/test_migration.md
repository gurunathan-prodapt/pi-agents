As a senior data-migration QA engineer, I've reviewed the migration design for `DW.BERT_AUSD_BP_TA_P_BASISPROD`. The core challenge for validation is the lack of the original `r_ausd_bp_ta_p_basisprod.ksh` shell script content. This means the PySpark application (`r_ausd_bp_ta_p_basisprod.py`) is currently a placeholder, and its exact business logic, data sources, and data sinks are unknown.

Therefore, these validation tests are designed in two phases:

1.  **Phase 1: Infrastructure & Orchestration Validation (Current Focus)**: These tests verify that the Airflow DAG correctly triggers the Dataproc job, that the PySpark placeholder script executes successfully on Dataproc, and that basic external system interactions (GCS for script storage, Dataproc for execution) are functional. This validates the "plumbing" of the migration.
2.  **Phase 2: Logic & Data Validation (Post-PySpark Development)**: Once `r_ausd_bp_ta_p_basisprod.py` is fully developed based on a detailed analysis of the original `r_ausd_bp_ta_p_basisprod.ksh` shell script, these tests will focus on comparing its output and behavior with the legacy system. The tests below outline the *methodology* for performing these comparisons once the PySpark logic is known and implemented.

For the purpose of these tests, we will assume that the PySpark application will primarily interact with BigQuery for both source data reads and target data writes, as this is a common pattern in GCP data migrations.

---

### Test Case 1: Airflow DAG Execution and Dataproc Job Submission
*   **Purpose:** To verify that the Airflow DAG successfully triggers and submits the PySpark job to the Dataproc cluster. This validates the orchestration layer.
*   **Setup:**
    *   The Airflow DAG `dw_bert_ausd_bp_ta_p_basisprod` is deployed to a Composer environment.
    *   A Dataproc cluster (`YOUR_DATAPROC_CLUSTER_NAME`) is running and accessible by the Composer service account.
    *   The placeholder PySpark script `r_ausd_bp_ta_p_basisprod.py` is uploaded to the specified GCS bucket (`gs://YOUR_BUCKET_NAME/pyspark_scripts/r_ausd_bp_ta_p_basisprod.py`).
    *   All GCP placeholders in the DAG (`GCP_PROJECT_ID`, `DATAPROC_REGION`, `DATAPROC_CLUSTER_NAME`, `GCS_BUCKET_NAME`) are correctly configured.
*   **Action:**
    1.  Manually trigger the `dw_bert_ausd_bp_ta_p_basisprod` DAG in the Airflow UI.
    2.  Monitor the DAG run in the Airflow UI.
    3.  Check the Dataproc Jobs page in the GCP Console for the submitted job.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG run completes successfully (green status).
    *   The `run_dw_bert_ausd_bp_ta_p_basisprod` task completes successfully.
    *   A Dataproc job with the name derived from the Airflow task (e.g., `airflow-run_dw_bert_ausd_bp_ta_p_basisprod-...`) is visible in the Dataproc Jobs list and shows a "SUCCEEDED" status.
    *   The Dataproc job logs (accessible via GCP Console) show the "Starting PySpark application..." and "PySpark application finished successfully." messages from the placeholder script.

### Test Case 2: PySpark Script Execution and Basic Logging
*   **Purpose:** To confirm that the PySpark script can be executed on Dataproc, access its own code from GCS, and produce expected log output, indicating the basic environment is functional.
*   **Setup:**
    *   Same as Test Case 1.
    *   Ensure the Dataproc cluster has network access to GCS.
*   **Action:**
    1.  Execute Test Case 1 (trigger the Airflow DAG).
    2.  Access the logs for the `run_dw_bert_ausd_bp_ta_p_basisprod` task in Airflow.
    3.  Alternatively, access the Dataproc job logs directly in the GCP Console.
*   **Pass/Fail Criterion:**
    *   The Airflow task logs (or Dataproc job logs) contain the following messages from the PySpark script:
        *   `Starting PySpark application for DW.BERT_AUSD_BP_TA_P_BASISPROD...`
        *   `--- Example Data ---`
        *   `--- Example Transformed Data ---`
        *   `PySpark application finished successfully.`
    *   No critical errors or exceptions related to script execution, GCS access, or Spark environment are present in the logs.

### Test Case 3: Output Parity - End-to-End Data Comparison
*   **Purpose:** To ensure that the migrated PySpark job, when given the same input data as the legacy shell script, produces an identical output dataset. This is the ultimate validation of behavioral equivalence.
*   **Setup:**
    *   **Legacy Output Snapshot:** A snapshot of the final output data produced by the legacy `r_ausd_bp_ta_p_basisprod.ksh` script for a specific run (e.g., a specific date or period) is available. This could be a flat file, a database table export, or a BigQuery table loaded from the legacy system. Let's call this `legacy_output_table`.
    *   **Input Data Replication:** The exact input data used by the legacy script for that specific run is replicated in BigQuery (or other GCP source) for the PySpark job. Let's call this `gcp_source_data`.
    *   The `r_ausd_bp_ta_p_basisprod.py` script is fully developed and configured to read from `gcp_source_data` and write its output to a new BigQuery table, `migrated_output_table`.
*   **Action:**
    1.  Execute the `dw_bert_ausd_bp_ta_p_basisprod` Airflow DAG, ensuring it processes the `gcp_source_data`.
    2.  Once the DAG completes successfully, query both `legacy_output_table` and `migrated_output_table` in BigQuery.
    3.  Perform a full data comparison between the two tables.
*   **Pass/Fail Criterion:**
    *   The `migrated_output_table` contains the exact same data (row count, column values, data types) as the `legacy_output_table`.
    *   A SQL query comparing the two tables yields no differences.

    ```sql
    -- Example SQL for BigQuery comparison
    -- Replace with actual table and column names
    SELECT
        COUNT(*) AS diff_count
    FROM (
        SELECT * FROM `your_gcp_project.your_dataset.legacy_output_table`
        EXCEPT DISTINCT
        SELECT * FROM `your_gcp_project.your_dataset.migrated_output_table`
    )
    UNION ALL
    SELECT
        COUNT(*) AS diff_count
    FROM (
        SELECT * FROM `your_gcp_project.your_dataset.migrated_output_table`
        EXCEPT DISTINCT
        SELECT * FROM `your_gcp_project.your_dataset.legacy_output_table`
    );
    ```
    *   **Pass:** `diff_count` is 0 for both `UNION ALL` queries.
    *   **Fail:** `diff_count` is greater than 0 for either query.

### Test Case 4: Transformation Correctness - Specific Logic Validation
*   **Purpose:** To verify that specific transformation logic (joins, aggregations, filters, type handling, NULL handling) within the PySpark script behaves identically to the legacy shell script. This requires detailed knowledge of the shell script's internal logic.
*   **Setup:**
    *   **Known Legacy Logic:** Detailed understanding of a specific transformation (e.g., "join table A and B on ID, then aggregate by C, filter where D > 100, handle NULLs in E by replacing with 0").
    *   **Mock Input Data:** Create a small, controlled dataset in BigQuery (`mock_input_data`) that specifically targets the transformation logic under test, including edge cases (e.g., NULLs, empty sets, boundary values).
    *   The `r_ausd_bp_ta_p_basisprod.py` script is fully developed and can be configured to process `mock_input_data` and write its intermediate or final output to `migrated_transformed_output`.
    *   **Expected Output:** Manually calculate or obtain the expected output for `mock_input_data` based on the legacy logic. This can be a small BigQuery table (`expected_transformed_output`).
*   **Action:**
    1.  Execute the `dw_bert_ausd_bp_ta_p_basisprod` Airflow DAG, configured to use `mock_input_data`.
    2.  Once the DAG completes, query `migrated_transformed_output` in BigQuery.
    3.  Compare `migrated_transformed_output` with `expected_transformed_output`.
*   **Pass/Fail Criterion:**
    *   The `migrated_transformed_output` table matches `expected_transformed_output` exactly.
    *   Use a SQL comparison similar to Test Case 3.

    ```python
    # Example Pytest assertion for a specific transformation
    # This assumes you can run parts of your PySpark logic locally for unit testing
    import pytest
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    # Assume r_ausd_bp_ta_p_basisprod.py has a function like `process_data(spark, input_df)`
    from pyspark_scripts.r_ausd_bp_ta_p_basisprod import process_data

    @pytest.fixture(scope="module")
    def spark_session():
        spark = SparkSession.builder.appName("TransformationTest").getOrCreate()
        yield spark
        spark.stop()

    def test_aggregation_logic(spark_session):
        # Setup: Mock input data for aggregation
        schema = StructType([
            StructField("category", StringType(), True),
            StructField("value", IntegerType(), True)
        ])
        input_data = [
            ("A", 10), ("A", 20), ("B", 15), ("B", 25), ("A", None), ("C", 5)
        ]
        input_df = spark_session.createDataFrame(input_data, schema)

        # Action: Apply the transformation (assuming process_data handles this)
        # For a specific transformation, you might call a more granular function
        # e.g., `aggregate_by_category(input_df)`
        # For this example, let's assume process_data includes the aggregation
        # and we can extract the relevant part of its output.
        processed_df = process_data(spark_session, input_df) # Placeholder for actual call

        # Expected output based on legacy logic (e.g., sum 'value' by 'category', ignoring NULLs)
        expected_data = [
            ("A", 30), ("B", 40), ("C", 5)
        ]
        expected_schema = StructType([
            StructField("category", StringType(), True),
            StructField("sum_value", IntegerType(), True) # Assuming output column name
        ])
        expected_df = spark_session.createDataFrame(expected_data, expected_schema)

        # Pass/Fail Criterion: Compare the resulting DataFrame
        assert processed_df.count() == expected_df.count()
        assert processed_df.exceptAll(expected_df).count() == 0
        assert expected_df.exceptAll(processed_df).count() == 0

    def test_null_handling_filter(spark_session):
        # Setup: Mock input data with NULLs for filtering
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("status", StringType(), True)
        ])
        input_data = [
            (1, "ACTIVE"), (2, "INACTIVE"), (3, None), (4, "ACTIVE")
        ]
        input_df = spark_session.createDataFrame(input_data, schema)

        # Action: Apply the transformation (e.g., filter out rows where status is NULL)
        filtered_df = input_df.filter(input_df.status.isNotNull())

        # Expected output
        expected_data = [
            (1, "ACTIVE"), (2, "INACTIVE"), (4, "ACTIVE")
        ]
        expected_df = spark_session.createDataFrame(expected_data, schema)

        # Pass/Fail Criterion
        assert filtered_df.count() == expected_df.count()
        assert filtered_df.exceptAll(expected_df).count() == 0
        assert expected_df.exceptAll(filtered_df).count() == 0
    ```

### Test Case 5: External System Replacement - BigQuery Read/Write Permissions
*   **Purpose:** To verify that the Dataproc cluster, using its assigned service account, has the necessary permissions to read from source BigQuery tables and write to target BigQuery tables.
*   **Setup:**
    *   The `r_ausd_bp_ta_p_basisprod.py` script is fully developed and attempts to read from a designated source BigQuery table (`source_bq_table`) and write to a target BigQuery table (`target_bq_table`).
    *   The Dataproc cluster's service account has been granted `BigQuery Data Editor` role (or more granular `bigquery.tables.getData`, `bigquery.tables.create`, `bigquery.tables.updateData`, etc.) on the relevant BigQuery dataset(s).
    *   A dummy source table `source_bq_table` exists with some data.
*   **Action:**
    1.  Execute the `dw_bert_ausd_bp_ta_p_basisprod` Airflow DAG.
    2.  Monitor the Dataproc job logs for any permission-related errors.
    3.  After successful completion, verify the existence and content of `target_bq_table`.
*   **Pass/Fail Criterion:**
    *   The Dataproc job completes successfully without any `PERMISSION_DENIED` errors in the logs.
    *   The `target_bq_table` is created (if it didn't exist) and contains data written by the PySpark job.

### Test Case 6: Parameter Handling (`&DWH_JOB_KENNUNG`)
*   **Purpose:** To verify that parameters or environment variables, such as `&DWH_JOB_KENNUNG` from the legacy UC4 script, are correctly passed to and utilized by the PySpark application.
*   **Setup:**
    *   The Airflow DAG is modified to pass `AUSD_BP_TA_P_BASISPROD` as an argument to the PySpark job (as suggested in the design document's commented-out section).
    *   The `r_ausd_bp_ta_p_basisprod.py` script is updated to accept and log this argument.
    *   The PySpark script's logic is designed to use this parameter (e.g., for logging, filtering, or naming output files/tables).
*   **Action:**
    1.  Execute the `dw_bert_ausd_bp_ta_p_basisprod` Airflow DAG.
    2.  Review the Dataproc job logs.
    3.  If the parameter influences output (e.g., a column value or table name), verify that influence in the output data.
*   **Pass/Fail Criterion:**
    *   The Dataproc job logs explicitly show the `&DWH_JOB_KENNUNG` value being received and processed by the PySpark script (e.g., `Job Kennung: AUSD_BP_TA_P_BASISPROD`).
    *   If the parameter affects the output data or metadata, that effect is correctly observed (e.g., a `job_kennung` column in the output table contains `AUSD_BP_TA_P_BASISPROD`).

    ```python
    # Example PySpark code snippet for argument parsing (from design doc)
    # in r_ausd_bp_ta_p_basisprod.py
    # ...
    # if "--job-kennung" in sys.argv:
    #     job_kennung_index = sys.argv.index("--job-kennung") + 1
    #     job_kennung = sys.argv[job_kennung_index]
    #     print(f"Job Kennung: {job_kennung}")
    # ...

    # Airflow DAG snippet for passing args
    # ...
    # "pyspark_job": {
    #     "main_python_file_uri": PYSPARK_SCRIPT_URI,
    #     "args": ["--job-kennung", "AUSD_BP_TA_P_BASISPROD"]
    # },
    # ...
    ```

### Test Case 7: Data Quality - Row Count Assertion
*   **Purpose:** To verify that the number of rows in the output table produced by the migrated job matches the expected row count from the legacy system, indicating no unexpected data loss or duplication.
*   **Setup:**
    *   The `r_ausd_bp_ta_p_basisprod.py` script is fully developed and writes to `migrated_output_table`.
    *   The expected row count from the legacy job's output for a given input is known (`expected_row_count`). This can be obtained from legacy logs or by querying the `legacy_output_table`.
*   **Action:**
    1.  Execute the `dw_bert_ausd_bp_ta_p_basisprod` Airflow DAG.
    2.  Query the `migrated_output_table` in BigQuery to get its row count.
*   **Pass/Fail Criterion:**
    *   The row count of `migrated_output_table` is exactly equal to `expected_row_count`.

    ```sql
    -- Example SQL assertion
    SELECT
        (SELECT COUNT(*) FROM `your_gcp_project.your_dataset.migrated_output_table`) AS actual_row_count,
        <expected_row_count_from_legacy> AS expected_row_count;
    ```
    *   **Pass:** `actual_row_count` equals `expected_row_count`.
    *   **Fail:** `actual_row_count` does not equal `expected_row_count`.

### Test Case 8: Data Quality - Schema Assertion
*   **Purpose:** To ensure that the schema (column names, data types, nullability) of the output table produced by the migrated job matches the schema of the legacy job's output.
*   **Setup:**
    *   The `r_ausd_bp_ta_p_basisprod.py` script is fully developed and writes to `migrated_output_table`.
    *   The expected schema (column names, data types, nullability) from the legacy job's output is documented or can be extracted from `legacy_output_table`.
*   **Action:**
    1.  Execute the `dw_bert_ausd_bp_ta_p_basisprod` Airflow DAG.
    2.  Retrieve the schema of `migrated_output_table` from BigQuery.
    3.  Compare it against the `legacy_output_table` schema.
*   **Pass/Fail Criterion:**
    *   The `migrated_output_table` schema (column names, data types, and nullability for each column) is identical to the `legacy_output_table` schema.

    ```sql
    -- Example SQL to retrieve schema for comparison (manual or scripted comparison)
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM
        `your_gcp_project.your_dataset.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = 'migrated_output_table'
    ORDER BY
        ordinal_position;

    -- Compare this output with the same query for 'legacy_output_table'
    ```
    *   **Pass:** Schemas match exactly.
    *   **Fail:** Any discrepancy in column names, data types, or nullability.

### Test Case 9: Data Quality - Uniqueness and Referential Integrity (if applicable)
*   **Purpose:** To verify critical data quality constraints, such as uniqueness of primary keys or referential integrity with other tables, are maintained by the migrated job. This is highly dependent on the business logic.
*   **Setup:**
    *   The `r_ausd_bp_ta_p_basisprod.py` script is fully developed and writes to `migrated_output_table`.
    *   Known data quality rules from the legacy system are documented (e.g., `product_id` must be unique, `customer_id` must exist in `dim_customer`).
*   **Action:**
    1.  Execute the `dw_bert_ausd_bp_ta_p_basisprod` Airflow DAG.
    2.  Run SQL queries against `migrated_output_table` to validate these rules.
*   **Pass/Fail Criterion:**
    *   **Uniqueness:** No duplicate primary keys are found.
        ```sql
        SELECT
            primary_key_column,
            COUNT(*)
        FROM
            `your_gcp_project.your_dataset.migrated_output_table`
        GROUP BY
            primary_key_column
        HAVING
            COUNT(*) > 1;
        ```
        *   **Pass:** Query returns 0 rows.
    *   **Referential Integrity:** All foreign keys correctly reference existing primary keys in dimension tables.
        ```sql
        SELECT
            t1.foreign_key_column
        FROM
            `your_gcp_project.your_dataset.migrated_output_table` t1
        LEFT JOIN
            `your_gcp_project.your_dataset.dim_table` t2
        ON
            t1.foreign_key_column = t2.primary_key_column
        WHERE
            t2.primary_key_column IS NULL;
        ```
        *   **Pass:** Query returns 0 rows.

### Test Case 10: Error Handling and Retries
*   **Purpose:** To verify that the migrated job handles errors gracefully and adheres to the defined retry policy (even if it's 0 retries as per the current design).
*   **Setup:**
    *   The Airflow DAG has `retries=0` as per the design.
    *   Modify the `r_ausd_bp_ta_p_basisprod.py` script to intentionally fail under specific conditions (e.g., divide by zero, attempt to write to a non-existent BigQuery dataset, or raise an explicit exception).
*   **Action:**
    1.  Trigger the `dw_bert_ausd_bp_ta_p_basisprod` Airflow DAG with the failing PySpark script.
    2.  Monitor the Airflow UI and Dataproc job logs.
*   **Pass/Fail Criterion:**
    *   The Airflow task `run_dw_bert_ausd_bp_ta_p_basisprod` fails.
    *   The Airflow task does *not* retry (since `retries=0`).
    *   The Dataproc job shows a "FAILED" status.
    *   The logs clearly indicate the cause of the failure (e.g., the intentional exception).
    *   **Pass:** Task fails on the first attempt with no retries.
    *   **Fail:** Task retries, or fails for an unexpected reason, or doesn't fail when expected.