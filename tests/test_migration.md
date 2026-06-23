As a senior data-migration QA engineer, I've reviewed the migration design for `DW.BERT_AUSD_V_TA_VERTRAG_TMP`. The primary challenge is the unavailability of the original KornShell (KSH) script (`r_ausd_v_ta_vertrag_tmp.ksh`) content. This means that tests for direct output parity and transformation correctness can only be fully implemented *after* the KSH script has been reverse-engineered and its logic translated into the PySpark script (`r_ausd_v_ta_vertrag_tmp.py`).

Therefore, the initial set of tests will focus on validating the orchestration framework, argument passing, and the basic functionality of the placeholder PySpark script. Subsequent tests will be conceptual, outlining what needs to be validated once the PySpark script's core logic is developed.

---

## Migration Validation Tests for `DW.BERT_AUSD_V_TA_VERTRAG_TMP`

### Test Case 1: Orchestration Equivalence - Airflow DAG Execution

*   **Purpose:** To verify that the migrated Airflow DAG successfully triggers and completes the Dataproc PySpark job, mimicking the successful execution of the legacy UC4 job. This tests the fundamental re-platforming of the orchestration layer.
*   **Setup:**
    1.  An active Cloud Composer environment with the `dw_bert_ausd_v_ta_vertrag_tmp.py` DAG deployed.
    2.  A running Dataproc cluster (`DATAPROC_CLUSTER_NAME`) accessible by the Composer's service account.
    3.  The PySpark script `r_ausd_v_ta_vertrag_tmp.py` uploaded to the specified GCS bucket (`GCS_PYSPARK_BUCKET/pyspark_scripts/`).
    4.  Ensure the Airflow DAG's `GCP_PROJECT_ID`, `GCP_REGION`, `DATAPROC_CLUSTER_NAME`, and `GCS_PYSPARK_BUCKET` placeholders are correctly configured.
    5.  The Composer service account has `dataproc.jobs.create` and `dataproc.jobs.get` permissions, and the Dataproc cluster's service account has `storage.objects.get` for the PySpark script.
*   **Action:**
    1.  Manually trigger the `dw_bert_ausd_v_ta_vertrag_tmp` DAG from the Airflow UI.
    2.  Monitor the DAG run in the Airflow UI.
    3.  Check the Dataproc Jobs page in the GCP Console for the submitted job.
*   **Pass/Fail Criterion:**
    *   **Pass:** The Airflow DAG run completes successfully (all tasks turn green). The corresponding Dataproc job is submitted, transitions through `RUNNING`, and eventually completes with `SUCCEEDED` status.
    *   **Fail:** The Airflow DAG fails, or the Dataproc job fails or gets stuck in a non-terminal state.

### Test Case 2: Argument Passing from Airflow to PySpark

*   **Purpose:** To verify that the `job_kennung` variable (and any other UC4 variables or parameters that will be passed) is correctly transmitted from the Airflow `DataprocSubmitJobOperator` to the PySpark script.
*   **Setup:**
    1.  Same as Test Case 1.
    2.  The `r_ausd_v_ta_vertrag_tmp.py` script should have its `print(f"Starting PySpark job with JOB_KENNUNG: {job_kennung}")` line active and logging enabled for Dataproc.
*   **Action:**
    1.  Trigger the `dw_bert_ausd_v_ta_vertrag_tmp` DAG.
    2.  Once the Dataproc job starts, navigate to its details in the GCP Console.
    3.  Review the job driver logs (e.g., `stdout` or `stderr`).
*   **Pass/Fail Criterion:**
    *   **Pass:** The Dataproc job logs clearly show the message `Starting PySpark job with JOB_KENNUNG: AUSD_V_TA_VERTRAG_TMP` (or the equivalent for any other arguments passed).
    *   **Fail:** The log message is missing, or the `job_kennung` value is incorrect or `None`.

### Test Case 3: PySpark Script Basic Functionality (Placeholder Logic)

*   **Purpose:** To verify that the PySpark script initializes a Spark session, executes its placeholder logic (e.g., prints messages), and gracefully stops the Spark session without errors. This confirms the basic structure of the PySpark application is sound.
*   **Setup:**
    1.  Same as Test Case 1.
    2.  The `r_ausd_v_ta_vertrag_tmp.py` script should have its `print("PySpark job completed successfully. Please implement the actual processing logic.")` line active.
*   **Action:**
    1.  Trigger the `dw_bert_ausd_v_ta_vertrag_tmp` DAG.
    2.  Review the Dataproc job logs in the GCP Console.
*   **Pass/Fail Criterion:**
    *   **Pass:** The Dataproc job completes successfully, and the logs contain both the "Starting PySpark job..." and "PySpark job completed successfully..." messages. No Spark-related errors (e.g., `SparkException`, `Py4JJavaError`) are present in the logs.
    *   **Fail:** The job fails, or the expected completion message is not found, indicating an issue within the PySpark script's basic execution flow.

### Test Case 4: External System Connectivity (Inferred - GCS/BigQuery)

*   **Purpose:** To verify that the Dataproc cluster and the PySpark script have the necessary permissions and connectivity to interact with inferred external systems like Google Cloud Storage (for input/output) and BigQuery (for output), as suggested by the design document's examples. This tests the environment's capability, not the specific transformation logic.
*   **Setup:**
    1.  Same as Test Case 1.
    2.  **Modify `r_ausd_v_ta_vertrag_tmp.py` (temporarily for this test):** Add minimal code to read a dummy file from GCS, write a dummy DataFrame to GCS, and write a dummy DataFrame to a BigQuery table.
        ```python
        # ... inside main() function ...
        print("Testing GCS read/write and BigQuery write connectivity...")

        # 1. Test GCS Read
        dummy_input_path = "gs://your-gcs-bucket-for-pyspark-scripts/test_data/dummy_input.csv"
        # Create a dummy_input.csv in GCS with some content, e.g., "id,name\n1,test"
        try:
            dummy_df_read = spark.read.csv(dummy_input_path, header=True, inferSchema=True)
            print(f"Successfully read {dummy_df_read.count()} rows from GCS: {dummy_input_path}")
        except Exception as e:
            print(f"ERROR: Failed to read from GCS: {e}")
            sys.exit(1)

        # 2. Test GCS Write
        dummy_output_path = "gs://your-gcs-bucket-for-pyspark-scripts/test_output/dummy_output.parquet"
        dummy_df_read.write.mode("overwrite").parquet(dummy_output_path)
        print(f"Successfully wrote dummy data to GCS: {dummy_output_path}")

        # 3. Test BigQuery Write
        dummy_bq_table = "your_gcp_project_id.your_dataset.test_output_table"
        try:
            dummy_df_read.write.format("bigquery") \
                .option("table", dummy_bq_table) \
                .option("temporaryGcsBucket", GCS_PYSPARK_BUCKET.replace("gs://", "")) \
                .mode("overwrite") \
                .save()
            print(f"Successfully wrote dummy data to BigQuery table: {dummy_bq_table}")
        except Exception as e:
            print(f"ERROR: Failed to write to BigQuery: {e}")
            sys.exit(1)

        print("Connectivity tests completed.")
        # ... rest of the original script ...
        ```
    3.  Ensure the Dataproc cluster's service account has `storage.objects.get`, `storage.objects.create`, `bigquery.tables.create`, `bigquery.tables.updateData`, and `bigquery.datasets.get` permissions.
    4.  A dummy CSV file (`dummy_input.csv`) exists in `gs://your-gcs-bucket-for-pyspark-scripts/test_data/`.
    5.  A BigQuery dataset (`your_dataset`) exists in `your_gcp_project_id`.
*   **Action:**
    1.  Trigger the `dw_bert_ausd_v_ta_vertrag_tmp` DAG.
    2.  Review the Dataproc job logs.
    3.  Verify the existence of `dummy_output.parquet` in GCS and `test_output_table` in BigQuery.
*   **Pass/Fail Criterion:**
    *   **Pass:** The Dataproc job completes successfully. Logs show "Successfully read from GCS", "Successfully wrote dummy data to GCS", and "Successfully wrote dummy data to BigQuery table". The `dummy_output.parquet` file is created in GCS, and the `test_output_table` is created/updated in BigQuery.
    *   **Fail:** The job fails with permission errors, connectivity issues, or the expected output files/tables are not created.

### Test Case 5: Output Parity (Conceptual / Post-Implementation)

*   **Purpose:** To verify that the fully implemented PySpark job produces an output that is bit-for-bit identical to the output of the legacy KSH job, given the same input data. This is the ultimate test of behavioral equivalence.
*   **Setup:**
    1.  **Fully implemented `r_ausd_v_ta_vertrag_tmp.py`:** The PySpark script must accurately replicate all data processing logic from the original `r_ausd_v_ta_vertrag_tmp.ksh`.
    2.  **Identical Input Data:** Prepare a representative set of input data that can be fed to both the legacy KSH job and the migrated PySpark job. This might involve extracting data from legacy sources (e.g., Oracle) and loading it into GCP (e.g., GCS, BigQuery) for the PySpark job.
    3.  **Legacy Output Capture:** Capture the output generated by the legacy KSH job for the given input data. This could be a database table, a flat file, or a report.
    4.  **Migrated Output Destination:** Configure the PySpark script to write its output to a comparable destination (e.g., a BigQuery table, a GCS Parquet file).
*   **Action:**
    1.  Execute the legacy `DW.BERT_AUSD_V_TA_VERTRAG_TMP` UC4 job with the prepared input data.
    2.  Execute the migrated `dw_bert_ausd_v_ta_vertrag_tmp` Airflow DAG with the same prepared input data.
    3.  Compare the output generated by the legacy job with the output generated by the migrated PySpark job.
        *   **For database tables (e.g., BigQuery vs. Oracle/SQL Server):** Use SQL `EXCEPT` or `MINUS` queries to find differences.
        *   **For files (e.g., GCS Parquet vs. legacy flat file):** Convert both to a canonical format (e.g., CSV with sorted columns/rows) and use a file comparison tool (`diff`).
*   **Pass/Fail Criterion:**
    *   **Pass:** The output of the migrated PySpark job is identical to the output of the legacy KSH job. No differences are found in row counts, column values, or data types.
    *   **Fail:** Any discrepancies are found between the legacy and migrated outputs.

    ```python
    # Example Python/SQL assertion for comparing BigQuery tables
    # This assumes both legacy and migrated outputs are in BigQuery for comparison.
    # If legacy is elsewhere, data needs to be staged in BQ for comparison.

    def compare_bigquery_tables(project_id, dataset_id, legacy_table, migrated_table):
        client = bigquery.Client(project=project_id)

        # Query to find rows in legacy_table not in migrated_table
        query_diff_legacy_to_migrated = f"""
        SELECT * FROM `{project_id}.{dataset_id}.{legacy_table}`
        EXCEPT DISTINCT
        SELECT * FROM `{project_id}.{dataset_id}.{migrated_table}`
        """
        diff_legacy_to_migrated = client.query(query_diff_legacy_to_migrated).result()

        # Query to find rows in migrated_table not in legacy_table
        query_diff_migrated_to_legacy = f"""
        SELECT * FROM `{project_id}.{dataset_id}.{migrated_table}`
        EXCEPT DISTINCT
        SELECT * FROM `{project_id}.{dataset_id}.{legacy_table}`
        """
        diff_migrated_to_legacy = client.query(query_diff_migrated_to_legacy).result()

        if diff_legacy_to_migrated.total_rows == 0 and diff_migrated_to_legacy.total_rows == 0:
            print(f"PASS: Tables {legacy_table} and {migrated_table} are identical.")
            return True
        else:
            print(f"FAIL: Differences found between {legacy_table} and {migrated_table}.")
            if diff_legacy_to_migrated.total_rows > 0:
                print(f"Rows in {legacy_table} but not in {migrated_table}:")
                for row in diff_legacy_to_migrated:
                    print(row)
            if diff_migrated_to_legacy.total_rows > 0:
                print(f"Rows in {migrated_table} but not in {legacy_table}:")
                for row in diff_migrated_to_legacy:
                    print(row)
            return False

    # Example usage in a pytest-like scenario:
    # from google.cloud import bigquery
    # def test_output_parity_bigquery():
    #     assert compare_bigquery_tables("your-gcp-project-id", "your_dataset", "legacy_output_table", "migrated_output_table")
    ```

### Test Case 6: Transformation Correctness (Conceptual / Post-Implementation)

*   **Purpose:** To verify that specific transformation logic elements (joins, aggregations, filters, type handling, NULL handling, and any identified edge cases) within the PySpark script are correctly implemented as per the reverse-engineered KSH logic.
*   **Setup:**
    1.  **Fully implemented `r_ausd_v_ta_vertrag_tmp.py`:** The PySpark script must accurately replicate all data processing logic.
    2.  **Unit/Integration Test Data:** Create small, targeted datasets that specifically test individual transformation components. For example:
        *   Data for join conditions (matching, non-matching, multiple matches).
        *   Data for aggregation (empty groups, single-item groups, multiple items, NULLs in aggregated columns).
        *   Data for filters (rows that should pass, rows that should be filtered out).
        *   Data with various data types and NULL values to test type casting and NULL handling.
        *   Specific edge cases identified during KSH script analysis (e.g., division by zero, empty input files, malformed records).
    3.  **Expected Outputs:** Define the precise expected output for each test dataset after each transformation step or for the final output.
*   **Action:**
    1.  Execute the PySpark script (or specific functions/modules within it) with the targeted test datasets.
    2.  Use PySpark's `assert_df_equals` (from libraries like `pyspark-testing`) or collect and compare DataFrames/results against the predefined expected outputs.
*   **Pass/Fail Criterion:**
    *   **Pass:** All assertions for individual transformation logic components (joins, aggregations, filters, type handling, NULL handling, edge cases) pass, confirming the PySpark logic behaves as expected.
    *   **Fail:** Any transformation produces an incorrect result compared to the expected output.

    ```python
    # Example PySpark assertion (requires a testing library like pyspark-testing)
    # pip install pyspark-testing

    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    from pyspark_testing import assert_df_equality
    import pytest

    # Assume r_ausd_v_ta_vertrag_tmp.py has a function like process_contract_data(spark, input_df1, input_df2)
    # For this test, we'd mock or provide dummy input_df1, input_df2

    @pytest.fixture(scope="session")
    def spark_session():
        spark = SparkSession.builder \
            .appName("PySparkTransformationTests") \
            .master("local[*]") \
            .getOrCreate()
        yield spark
        spark.stop()

    def test_contract_data_join(spark_session):
        # Mock input data for a join
        schema1 = StructType([
            StructField("contract_id", IntegerType(), True),
            StructField("customer_id", IntegerType(), True)
        ])
        data1 = [(1, 101), (2, 102), (3, 103)]
        df1 = spark_session.createDataFrame(data1, schema1)

        schema2 = StructType([
            StructField("customer_id", IntegerType(), True),
            StructField("customer_name", StringType(), True)
        ])
        data2 = [(101, "Alice"), (102, "Bob")]
        df2 = spark_session.createDataFrame(data2, schema2)

        # Assume the PySpark script has a function that performs the join
        # For demonstration, let's simulate the join here
        actual_output_df = df1.join(df2, "customer_id", "inner")

        expected_schema = StructType([
            StructField("customer_id", IntegerType(), True),
            StructField("contract_id", IntegerType(), True),
            StructField("customer_name", StringType(), True)
        ])
        expected_data = [(101, 1, "Alice"), (102, 2, "Bob")]
        expected_output_df = spark_session.createDataFrame(expected_data, expected_schema)

        assert_df_equality(actual_output_df, expected_output_df, ignore_row_order=True, ignore_column_order=True)

    def test_contract_data_aggregation_with_nulls(spark_session):
        # Mock input data for aggregation with NULLs
        schema = StructType([
            StructField("category", StringType(), True),
            StructField("value", IntegerType(), True)
        ])
        data = [("A", 10), ("A", 20), ("B", 5), ("B", None), ("C", 15), (None, 30)]
        input_df = spark_session.createDataFrame(data, schema)

        # Simulate aggregation logic (e.g., sum 'value' by 'category')
        from pyspark.sql import functions as F
        actual_output_df = input_df.groupBy("category").agg(F.sum("value").alias("total_value"))

        expected_schema = StructType([
            StructField("category", StringType(), True),
            StructField("total_value", IntegerType(), True)
        ])
        expected_data = [("A", 30), ("B", 5), ("C", 15), (None, 30)]
        expected_output_df = spark_session.createDataFrame(expected_data, expected_schema)

        assert_df_equality(actual_output_df, expected_output_df, ignore_row_order=True)
    ```

### Test Case 7: Data Quality & Schema Assertions (Conceptual / Post-Implementation)

*   **Purpose:** To verify that the output data produced by the migrated PySpark job adheres to expected data quality standards and matches the schema characteristics of the legacy system's output.
*   **Setup:**
    1.  **Fully implemented `r_ausd_v_ta_vertrag_tmp.py`:** The PySpark script must produce its final output.
    2.  **Representative Input Data:** Use a dataset that is representative of production data.
    3.  **Legacy Output Schema/Quality Rules:** Document the schema (column names, data types, nullability) and any known data quality rules (e.g., primary key uniqueness, value ranges, referential integrity) of the legacy job's output.
*   **Action:**
    1.  Execute the migrated `dw_bert_ausd_v_ta_vertrag_tmp` Airflow DAG.
    2.  Inspect the schema of the generated output (e.g., BigQuery table schema, GCS Parquet schema).
    3.  Perform data quality checks on the output:
        *   **Row Count:** Compare the total number of rows with the legacy output (if applicable, for identical inputs).
        *   **Schema Match:** Verify column names, data types, and nullability match the legacy output.
        *   **Primary Key Uniqueness:** Check for duplicate primary keys (if the output has one).
        *   **Nullability:** Verify critical columns do not contain unexpected NULLs.
        *   **Value Ranges/Formats:** Check if values in specific columns fall within expected ranges or adhere to specific formats.
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   Output schema (column names, data types, nullability) matches the legacy output.
        *   Row count matches the legacy output (for identical inputs).
        *   All data quality checks pass (e.g., no duplicate primary keys, no unexpected NULLs, values within expected ranges).
    *   **Fail:** Any deviation from the expected schema or data quality rules is found.

    ```sql
    -- Example SQL assertions for BigQuery output
    -- Assuming 'migrated_output_table' is the output of the PySpark job

    -- 1. Row Count Check
    SELECT COUNT(*) FROM `your-gcp-project-id.your_dataset.migrated_output_table`;
    -- Compare this count to the known legacy row count for the same input.

    -- 2. Primary Key Uniqueness Check (if 'contract_id' is a PK)
    SELECT contract_id, COUNT(*)
    FROM `your-gcp-project-id.your_dataset.migrated_output_table`
    GROUP BY contract_id
    HAVING COUNT(*) > 1;
    -- Expected result: 0 rows

    -- 3. Nullability Check for a critical column (e.g., 'customer_id')
    SELECT COUNT(*) FROM `your-gcp-project-id.your_dataset.migrated_output_table`
    WHERE customer_id IS NULL;
    -- Expected result: 0 rows (if customer_id should never be NULL)

    -- 4. Data Type Check (manual inspection of BigQuery schema or using INFORMATION_SCHEMA)
    SELECT column_name, data_type, is_nullable
    FROM `your-gcp-project-id.your_dataset.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'migrated_output_table';
    -- Compare this against the documented legacy schema.

    -- 5. Value Range Check (e.g., contract_start_date should not be in the future)
    SELECT COUNT(*) FROM `your-gcp-project-id.your_dataset.migrated_output_table`
    WHERE contract_start_date > CURRENT_DATE();
    -- Expected result: 0 rows
    ```