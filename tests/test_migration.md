As a senior data-migration QA engineer, I've reviewed the migration design and the generated code for `DW.BERT_AUSD_BP_TA_BCP_ICCID`.

**Critical Pre-Condition & Disclaimer:**

The migration design explicitly states a significant unresolved risk: "The exact business logic, data sources, transformations, and output targets of the `r_ausd_bp_ta_bcp_iccid.ksh` script are not yet fully understood." and "The original ksh script content was not fully analyzed during migration design."

This is a critical blocker for comprehensive QA. Without understanding the legacy script's internal logic, data sources, and expected outputs, it is impossible to fully validate "Output parity," "Transformation correctness," or detailed "Data-quality / row-count / schema assertions."

The tests below are designed with this limitation in mind. They focus on validating the *infrastructure*, *orchestration*, and *high-level behavior* of the migrated job, and outline the necessary steps for full validation once the legacy script's logic is fully analyzed and implemented in the PySpark code.

---

## Migration Validation Tests for DW.BERT_AUSD_BP_TA_BCP_ICCID

### 1. Orchestration and Basic Execution Validation

#### Test Case 1.1: Airflow DAG Successfully Triggers Dataproc Job

*   **Purpose**: To verify that the Airflow DAG `dw_bert_ausd_bp_ta_bcp_iccid` can successfully submit a PySpark job to a Dataproc cluster. This validates the Airflow-to-Dataproc integration.
*   **Setup**:
    *   The Airflow DAG `dw_bert_ausd_bp_ta_bcp_iccid.py` is deployed to an Airflow environment.
    *   A Dataproc cluster named `YOUR_DATAPROC_CLUSTER_NAME` is running and accessible by the Airflow service account.
    *   The PySpark script `r_ausd_bp_ta_bcp_iccid.py` is uploaded to `gs://YOUR_GCS_BUCKET_NAME/pyspark_scripts/`.
    *   All placeholders (`YOUR_GCP_PROJECT_ID`, `YOUR_GCP_REGION`, `YOUR_DATAPROC_CLUSTER_NAME`, `YOUR_GCS_BUCKET_NAME`) in the DAG are replaced with actual values.
*   **Action**:
    1.  Manually trigger the `dw_bert_ausd_bp_ta_bcp_iccid` DAG in the Airflow UI.
    2.  Monitor the Airflow task `run_dw_bert_ausd_bp_ta_bcp_iccid`.
    3.  Check the Dataproc job list in the GCP Console to confirm a job was submitted and its status.
*   **Pass/Fail Criterion**:
    *   **Pass**: The Airflow task `run_dw_bert_ausd_bp_ta_bcp_iccid` completes successfully (green status). A corresponding Dataproc job is visible in the GCP console and also completes successfully. The PySpark script's `logger.info("PySpark job logic execution completed. (Placeholder)")` message is present in the Dataproc job logs.
    *   **Fail**: The Airflow task fails, or the Dataproc job fails, or no Dataproc job is submitted.

#### Test Case 1.2: PySpark Script Failure Handling

*   **Purpose**: To ensure that if the PySpark script encounters an error, it correctly exits with a non-zero status, causing the Airflow task to fail. This validates the error propagation mechanism.
*   **Setup**:
    *   Same as Test Case 1.1.
    *   Modify the `r_ausd_bp_ta_bcp_iccid.py` script to intentionally raise an unhandled exception or call `sys.exit(1)` within the `main()` function (e.g., `raise ValueError("Simulated error")` before `logger.info("PySpark job logic execution completed.")`).
*   **Action**:
    1.  Deploy the modified PySpark script to GCS.
    2.  Manually trigger the `dw_bert_ausd_bp_ta_bcp_iccid` DAG in the Airflow UI.
    3.  Monitor the Airflow task `run_dw_bert_ausd_bp_ta_bcp_iccid` and the Dataproc job.
*   **Pass/Fail Criterion**:
    *   **Pass**: The Dataproc job fails with an error. The Airflow task `run_dw_bert_ausd_bp_ta_bcp_iccid` fails (red status). The error message from the PySpark script is visible in the Airflow task logs and Dataproc job logs.
    *   **Fail**: The Airflow task or Dataproc job completes successfully despite the injected error, or the error message is not properly logged.

### 2. External-System Replacements Validation

#### Test Case 2.1: GCS Access for PySpark Script

*   **Purpose**: To confirm that the Dataproc cluster can correctly access the GCS bucket where the PySpark script resides. This is a fundamental requirement for the job to run.
*   **Setup**:
    *   Same as Test Case 1.1.
    *   Ensure the Dataproc cluster's service account has `Storage Object Viewer` permissions on `gs://YOUR_GCS_BUCKET_NAME`.
*   **Action**:
    1.  Execute Test Case 1.1 (successful run).
    2.  Review the Dataproc job logs for any GCS access errors.
*   **Pass/Fail Criterion**:
    *   **Pass**: The Dataproc job starts and executes the PySpark script without any "permission denied" or "file not found" errors related to `main_python_file_uri`.
    *   **Fail**: The Dataproc job fails with GCS access errors, indicating incorrect permissions or an incorrect GCS path.

#### Test Case 2.2: Legacy System Interaction (Negative Test)

*   **Purpose**: To confirm that the migrated job does *not* attempt to connect to the legacy host `DWHDWH2P` or use the legacy login `DW.UNIX.ISBERT`. This ensures a clean break from the legacy infrastructure.
*   **Setup**:
    *   The migrated job is running successfully (Test Case 1.1 passes).
    *   Access to network monitoring tools or logs on the legacy `DWHDWH2P` host (if available and feasible).
*   **Action**:
    1.  Run the migrated Airflow DAG.
    2.  Monitor network traffic originating from the Dataproc cluster.
    3.  Check logs on `DWHDWH2P` for any incoming connection attempts from the GCP environment.
    4.  Review Dataproc job logs for any mentions of `DWHDWH2P` or `DW.UNIX.ISBERT`.
*   **Pass/Fail Criterion**:
    *   **Pass**: No network connections are initiated from the Dataproc cluster towards `DWHDWH2P`. No log entries on `DWHDWH2P` indicate connection attempts from the GCP environment. No references to legacy host/login are found in Dataproc logs.
    *   **Fail**: Evidence of connection attempts to `DWHDWH2P` or usage of `DW.UNIX.ISBERT` is found.

#### Test Case 2.3: Data Source/Target Connectivity (Post-KSH Analysis)

*   **Purpose**: Once the `r_ausd_bp_ta_bcp_iccid.ksh` script is analyzed and its data sources (e.g., Oracle, flat files, other databases) and targets are identified and mapped to GCP equivalents (e.g., BigQuery, GCS), this test verifies the PySpark script's ability to connect to and read/write from these new GCP sources/targets.
*   **Setup**:
    *   The `r_ausd_bp_ta_bcp_iccid.py` script has been updated with the actual business logic, including reading from and writing to identified GCP data sources/targets (e.g., BigQuery tables, GCS paths).
    *   The Dataproc cluster's service account has appropriate IAM roles (e.g., `BigQuery Data Editor`, `Storage Object Admin`) for the identified sources/targets.
    *   Test data is available in the identified source systems.
*   **Action**:
    1.  Trigger the Airflow DAG.
    2.  Monitor the Dataproc job logs for successful connection messages and data read/write operations.
    3.  Verify the existence and content of output data in the target systems.
*   **Pass/Fail Criterion**:
    *   **Pass**: The PySpark job successfully connects to all specified GCP data sources, reads data, performs transformations, and writes results to all specified GCP data targets without any connectivity or permission errors.
    *   **Fail**: The job fails due to connection issues, permission errors, or inability to read/write data from/to the configured GCP sources/targets.

### 3. Output Parity & Transformation Correctness (Post-KSH Analysis)

**Note**: These tests are entirely dependent on the detailed analysis of `r_ausd_bp_ta_bcp_iccid.ksh` and its complete translation into `r_ausd_bp_ta_bcp_iccid.py`. They cannot be executed until that work is complete.

#### Test Case 3.1: Output Data Parity with Legacy System

*   **Purpose**: To ensure that for the same input data, the migrated PySpark job produces an identical output dataset to the legacy ksh script. This is the ultimate validation of behavioral equivalence.
*   **Setup**:
    *   The `r_ausd_bp_ta_bcp_iccid.py` script is fully implemented with the legacy logic.
    *   A controlled, representative set of input data is prepared.
    *   The legacy `r_ausd_bp_ta_bcp_iccid.ksh` job is executed with this controlled input data, and its output is captured and stored as a "golden reference" (e.g., in a GCS bucket or BigQuery table).
    *   The migrated Airflow DAG is configured to process the *exact same* input data.
*   **Action**:
    1.  Run the migrated Airflow DAG with the controlled input data.
    2.  Once the job completes, extract the output data from the target system.
    3.  Compare the migrated job's output data with the "golden reference" output from the legacy system. This comparison should be row-by-row, column-by-column.

    ```python
    # Example Pytest assertion (assuming outputs are in BigQuery tables)
    def test_output_data_parity(bigquery_client):
        legacy_output_table = "your_project.your_dataset.legacy_golden_output"
        migrated_output_table = "your_project.your_dataset.migrated_output"

        # Fetch data from legacy output
        query_legacy = f"SELECT * FROM `{legacy_output_table}` ORDER BY 1, 2, 3" # Order for consistent comparison
        df_legacy = bigquery_client.query(query_legacy).to_dataframe()

        # Fetch data from migrated output
        query_migrated = f"SELECT * FROM `{migrated_output_table}` ORDER BY 1, 2, 3"
        df_migrated = bigquery_client.query(query_migrated).to_dataframe()

        # Compare dataframes
        pd.testing.assert_frame_equal(df_legacy, df_migrated, check_dtype=True, check_exact=True)
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**: The output dataset from the migrated job is byte-for-byte identical (or semantically equivalent, considering floating-point precision, etc.) to the "golden reference" output from the legacy job.
    *   **Fail**: Any discrepancies are found between the migrated and legacy outputs.

#### Test Case 3.2: Transformation Logic Correctness (Unit/Integration Tests)

*   **Purpose**: To verify that specific transformations (joins, aggregations, filters, type handling, NULL handling, business rules) identified from the ksh script are correctly implemented in the PySpark code.
*   **Setup**:
    *   The `r_ausd_bp_ta_bcp_iccid.py` script is fully implemented.
    *   For each identified transformation, create small, focused input datasets designed to test specific logic, including:
        *   Standard cases
        *   Edge cases (e.g., empty inputs, all NULLs, boundary values, duplicate keys for joins, division by zero scenarios)
        *   Type conversions (e.g., string to int, date formats)
        *   NULL handling (e.g., NULLs in join keys, NULLs in aggregated columns)
*   **Action**:
    1.  For each specific transformation test case:
        *   Provide the focused input data to the PySpark script (or a mocked version of the relevant PySpark function).
        *   Execute the relevant part of the PySpark script.
        *   Capture the output.
        *   Compare the actual output against the *expected* output for that specific transformation.

    ```python
    # Example Pytest for a specific transformation (e.g., aggregation)
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    def test_aggregation_logic():
        spark = SparkSession.builder.appName("test_agg").getOrCreate()
        
        # Define schema for input data
        schema = StructType([
            StructField("category", StringType(), True),
            StructField("value", IntegerType(), True)
        ])

        # Test data for aggregation
        data = [
            ("A", 10), ("A", 20), ("B", 5), ("B", 15), ("A", None), (None, 100)
        ]
        input_df = spark.createDataFrame(data, schema)

        # Expected output after aggregation (e.g., sum of value per category, ignoring NULLs)
        expected_data = [
            ("A", 30),
            ("B", 20),
            (None, 100)
        ]
        expected_df = spark.createDataFrame(expected_data, schema)

        # Apply the transformation (assuming this is part of your PySpark script logic)
        # For this example, let's assume the script has a function like:
        # from pyspark_scripts.r_ausd_bp_ta_bcp_iccid import apply_aggregation
        # actual_df = apply_aggregation(input_df)
        
        # Placeholder for actual transformation from the PySpark script
        actual_df = input_df.groupBy("category").sum("value").withColumnRenamed("sum(value)", "value")
        
        # Sort for consistent comparison
        actual_df = actual_df.orderBy("category").fillna(0, subset=["value"]) # Handle potential None/NaN from sum
        expected_df = expected_df.orderBy("category").fillna(0, subset=["value"])

        assert actual_df.collect() == expected_df.collect()
        spark.stop()
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**: The output for each specific transformation test case matches the pre-defined expected output.
    *   **Fail**: Any transformation produces an incorrect result for any test case.

#### Test Case 3.3: Edge Cases and Error Conditions (Post-KSH Analysis)

*   **Purpose**: To validate how the migrated job handles specific edge cases or error conditions identified in the legacy ksh script or during the design analysis.
*   **Setup**:
    *   The `r_ausd_bp_ta_bcp_iccid.py` script is fully implemented.
    *   Input data is crafted to trigger known edge cases (e.g., empty input files, malformed records, specific data values that caused issues in the legacy system, division by zero, date parsing errors).
*   **Action**:
    1.  Run the migrated Airflow DAG with the crafted edge-case input data.
    2.  Observe the job's behavior: Does it fail gracefully? Does it produce expected (possibly error) output? Are errors logged correctly?
*   **Pass/Fail Criterion**:
    *   **Pass**: The job handles the edge cases as expected (e.g., skips bad records, logs warnings, produces a specific default value, or fails with a clear error message if that's the intended behavior).
    *   **Fail**: The job crashes unexpectedly, produces incorrect output, or fails to log relevant information for the edge case.

### 4. Data Quality, Row Count, and Schema Assertions (Post-KSH Analysis)

**Note**: These tests are also entirely dependent on the detailed analysis of `r_ausd_bp_ta_bcp_iccid.ksh` and its complete translation into `r_ausd_bp_ta_bcp_iccid.py`.

#### Test Case 4.1: Output Schema Validation

*   **Purpose**: To ensure that the schema (column names, data types, nullability) of the output data produced by the migrated job matches the expected schema, which should be derived from the legacy job's output.
*   **Setup**:
    *   The `r_ausd_bp_ta_bcp_iccid.py` script is fully implemented.
    *   The expected output schema (e.g., for a BigQuery table or GCS Parquet file) is documented based on the legacy system's output.
*   **Action**:
    1.  Run the migrated Airflow DAG.
    2.  Inspect the schema of the generated output data in the target system.

    ```python
    # Example Pytest for BigQuery schema validation
    from google.cloud import bigquery

    def test_output_schema_matches_expected():
        client = bigquery.Client()
        output_table_id = "your_project.your_dataset.migrated_output"

        # Define expected schema based on legacy output
        expected_schema = [
            bigquery.SchemaField("ICCID", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("PRODUCT_ID", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("ACTIVATION_DATE", "DATE", mode="NULLABLE"),
            # ... add all expected fields
        ]

        table = client.get_table(output_table_id)
        actual_schema = table.schema

        # Compare field by field, considering order might not be strictly enforced by BQ
        # A more robust comparison might involve converting to dicts or sets
        assert len(actual_schema) == len(expected_schema)
        for expected_field in expected_schema:
            found = False
            for actual_field in actual_schema:
                if (actual_field.name == expected_field.name and
                    actual_field.field_type == expected_field.field_type and
                    actual_field.mode == expected_field.mode):
                    found = True
                    break
            assert found, f"Expected field {expected_field.name} not found or mismatched in actual schema."
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**: The output schema (column names, data types, and nullability) exactly matches the documented expected schema.
    *   **Fail**: Any discrepancies in column names, data types, or nullability are found.

#### Test Case 4.2: Output Row Count Validation

*   **Purpose**: To ensure that the number of records processed and generated by the migrated job is consistent with the legacy job's behavior for the same input.
*   **Setup**:
    *   The `r_ausd_bp_ta_bcp_iccid.py` script is fully implemented.
    *   A controlled input dataset is used.
    *   The expected row count in the output is known from running the legacy job with the same input.
*   **Action**:
    1.  Run the migrated Airflow DAG with the controlled input data.
    2.  Query the output target system to get the row count of the generated data.
    3.  Compare this count to the expected row count.

    ```python
    # Example Pytest for BigQuery row count validation
    from google.cloud import bigquery

    def test_output_row_count_matches_expected():
        client = bigquery.Client()
        output_table_id = "your_project.your_dataset.migrated_output"
        expected_row_count = 12345 # Derived from legacy job run

        query = f"SELECT COUNT(*) FROM `{output_table_id}`"
        job = client.query(query)
        result = job.result()
        actual_row_count = [row[0] for row in result][0]

        assert actual_row_count == expected_row_count, \
            f"Row count mismatch: Expected {expected_row_count}, Got {actual_row_count}"
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**: The row count of the migrated job's output matches the expected row count.
    *   **Fail**: The row count differs from the expected count.

#### Test Case 4.3: Data Quality Rule Validation

*   **Purpose**: To verify that specific data quality rules (e.g., uniqueness constraints, referential integrity, value ranges, format checks) that were implicitly or explicitly enforced by the legacy job are maintained in the migrated job.
*   **Setup**:
    *   The `r_ausd_bp_ta_bcp_iccid.py` script is fully implemented.
    *   Specific data quality rules for the output data are identified and documented (e.g., `ICCID` must be unique, `ACTIVATION_DATE` must be in the past, `PRODUCT_ID` must be positive).
    *   Input data is prepared to test these rules, including both valid and invalid scenarios.
*   **Action**:
    1.  Run the migrated Airflow DAG with the test input data.
    2.  After the job completes, execute SQL queries or PySpark assertions against the output data to check each data quality rule.

    ```sql
    -- Example SQL assertion for uniqueness in BigQuery
    SELECT ICCID, COUNT(*)
    FROM `your_project.your_dataset.migrated_output`
    GROUP BY ICCID
    HAVING COUNT(*) > 1;
    -- Expected result: 0 rows (no duplicates)

    -- Example SQL assertion for value range
    SELECT COUNT(*)
    FROM `your_project.your_dataset.migrated_output`
    WHERE PRODUCT_ID <= 0 OR PRODUCT_ID IS NULL;
    -- Expected result: 0 rows (PRODUCT_ID must be positive and not null)

    -- Example SQL assertion for date validity
    SELECT COUNT(*)
    FROM `your_project.your_dataset.migrated_output`
    WHERE ACTIVATION_DATE > CURRENT_DATE();
    -- Expected result: 0 rows (ACTIVATION_DATE must be in the past or today)
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**: All defined data quality rules are met by the output data.
    *   **Fail**: Any data quality rule is violated in the output data.

---

**Recommendation for Next Steps:**

The highest priority for this migration QA effort is to **fully analyze the `r_ausd_bp_ta_bcp_iccid.ksh` script**. This analysis must yield:
1.  **Exact data sources and targets**: File paths, database connections, table names.
2.  **Detailed transformation logic**: All SQL queries, shell commands manipulating data, filtering conditions, aggregation logic, join conditions, and any custom business rules.
3.  **Expected output schema and data characteristics**: Column names, data types, nullability, and example output data.
4.  **Error handling and logging behavior**: How the legacy script reacts to bad data or system errors.

Once this information is available, the placeholder PySpark script can be fully implemented, and the "Post-KSH Analysis" tests can be developed with concrete data and assertions. Without this, the migration carries significant risk of behavioral divergence.