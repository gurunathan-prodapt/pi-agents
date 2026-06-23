As a senior data-migration QA engineer, I've reviewed the migration design for `DW.BERT_AUSD_BP_TA_RN_VERTRAG`. The most significant challenge highlighted in the design is the **unknown detailed functionality of the legacy `r_ausd_bp_ta_rn_vertrag.ksh` shell script**. This means that while we can thoroughly test the orchestration and infrastructure aspects of the migration, detailed data-level transformation and output parity tests will require a complete analysis and translation of the legacy script's logic into PySpark.

The tests below are structured to address this:
*   **Orchestration and Infrastructure Tests**: These are concrete and can be executed immediately to validate the Airflow, Dataproc, GCS, and BigQuery integration.
*   **Data-Level Tests (Transformation, Output Parity, Data Quality)**: These are provided as templates and frameworks. They outline *how* these tests should be performed once the `r_ausd_bp_ta_rn_vertrag.ksh` script's logic is fully understood and implemented in `r_ausd_bp_ta_rn_vertrag.py`. For these, I've made plausible assumptions about the data flow (e.g., reading from a BigQuery source table, writing to a BigQuery target table) to demonstrate the testing approach.

---

## Migration Validation Tests: DW.BERT_AUSD_BP_TA_RN_VERTRAG

### 1. Output Parity

#### Test Case 1.1: End-to-End Job Execution and Success Status

*   **Purpose**: Verify that the migrated Airflow DAG successfully completes, indicating that the PySpark job ran without infrastructure or orchestration errors, mirroring a successful run of the legacy UC4 job. This is the highest-level output parity check.
*   **Setup**:
    1.  Ensure the Airflow DAG `dw_bert_ausd_bp_ta_rn_vertrag.py` is deployed to Cloud Composer.
    2.  Ensure the PySpark script `r_ausd_bp_ta_rn_vertrag.py` is uploaded to the specified GCS bucket (`gs://YOUR_GCS_BUCKET_NAME/pyspark_scripts/`).
    3.  Ensure the Dataproc cluster specified by `DATAPROC_CLUSTER_NAME` is running and accessible.
    4.  **Legacy Baseline**: Obtain a record of a recent successful run of the legacy `DW.BERT_AUSD_BP_TA_RN_VERTRAG` UC4 job, noting its completion status and any high-level log messages indicating successful processing.
*   **Action**:
    1.  Manually trigger the `dw_bert_ausd_bp_ta_rn_vertrag` Airflow DAG in Cloud Composer.
    2.  Monitor the DAG run and its tasks through the Airflow UI.
    3.  Review the Dataproc job logs for the submitted PySpark job.
*   **Pass/Fail Criterion**:
    *   **Pass**: The Airflow DAG run completes successfully (all tasks are marked green). The `run_dw_bert_ausd_bp_ta_rn_vertrag` task (Dataproc job) completes successfully. The Dataproc job logs indicate successful execution of the PySpark script (e.g., "PySpark job finished successfully.").
    *   **Fail**: The Airflow DAG run fails, any task within the DAG fails, or the Dataproc job fails/errors out.

#### Test Case 1.2: Data Output Parity (Requires Legacy Script Analysis)

*   **Purpose**: To ensure that given identical input data and parameters, the migrated PySpark job produces the exact same output data in BigQuery as the legacy `ksh` script produced in its target system (e.g., another database table, flat file). This test is critical but can only be fully implemented once the legacy `ksh` script's logic is translated.
*   **Setup**:
    1.  **Legacy Data Snapshot**: Identify the precise source data used by the legacy `r_ausd_bp_ta_rn_vertrag.ksh` script for a specific successful run. Create a snapshot of this source data.
    2.  **Legacy Output Snapshot**: Capture the exact output produced by the legacy `r_ausd_bp_ta_rn_vertrag.ksh` script for that run (e.g., the final state of its target table, or the content of any output files).
    3.  **Migrated Source Data**: Load the snapshot of the legacy source data into a BigQuery source table (e.g., `your_project_id.test_dataset.legacy_source_data`).
    4.  **Migrated Target Table**: Create an empty BigQuery target table (e.g., `your_project_id.test_dataset.migrated_target_data`) with the expected schema.
    5.  **PySpark Logic**: The `r_ausd_bp_ta_rn_vertrag.py` script must be fully implemented with the translated business logic, reading from `your_project_id.test_dataset.legacy_source_data` and writing its processed output to `your_project_id.test_dataset.migrated_target_data`.
    6.  **Parameters**: Note the `stichtag` and `wiederanlaufWert` parameters (and any other relevant parameters) used in the legacy run.
*   **Action**:
    1.  Trigger the `dw_bert_ausd_bp_ta_rn_vertrag` Airflow DAG, passing the exact `stichtag` and `wiederanlaufWert` parameters used in the legacy run.
    2.  Allow the DAG to complete successfully.
    3.  Query the `your_project_id.test_dataset.migrated_target_data` table in BigQuery.
*   **Pass/Fail Criterion**:
    *   **Pass**: The data in `your_project_id.test_dataset.migrated_target_data` is byte-for-byte identical (or semantically identical if data types differ but values are equivalent) to the legacy output snapshot. This includes row counts, column values, and order (if order is significant).
    *   **Fail**: Any discrepancy in data content, row count, or schema between the migrated output and the legacy output.

    ```python
    # Example pytest assertion (conceptual, requires data extraction and comparison logic)
    import pandas as pd
    from google.cloud import bigquery
    import json

    def test_data_output_parity(bigquery_client: bigquery.Client, legacy_output_file_path: str, migrated_target_table_id: str):
        """
        Compares the output of the migrated job in BigQuery with a baseline legacy output.
        This function assumes the legacy output is available in a comparable format (e.g., CSV, JSON).
        """
        # 1. Load legacy output baseline (e.g., from CSV, or a query result from legacy DB)
        # This part is highly dependent on how legacy output is captured.
        # For demonstration, assume it's a CSV file.
        try:
            df_legacy = pd.read_csv(legacy_output_file_path)
            print(f"Loaded {len(df_legacy)} rows from legacy output: {legacy_output_file_path}")
        except FileNotFoundError:
            raise Exception(f"Legacy output file not found: {legacy_output_file_path}. Please provide a baseline.")

        # 2. Query migrated output from BigQuery
        # Ensure to order by primary key(s) or all columns for consistent comparison
        query = f"SELECT * FROM `{migrated_target_table_id}` ORDER BY DWH_VERTRAG_ID, SOME_OTHER_KEY" # Adjust ORDER BY as per your table's primary key(s)
        df_migrated = bigquery_client.query(query).to_dataframe()
        print(f"Queried {len(df_migrated)} rows from migrated target table: {migrated_target_table_id}")

        # 3. Ensure column names and data types are consistent for comparison
        # This might involve renaming columns or casting types in one of the dataframes
        # For robust comparison, ensure both DFs have the same columns in the same order
        df_legacy = df_legacy[df_migrated.columns] # Select and reorder columns to match migrated
        for col in df_migrated.columns:
            if col in df_legacy.columns:
                # Attempt to cast legacy column to migrated column's type if different
                # This handles cases like int vs float, or string vs date
                try:
                    df_legacy[col] = df_legacy[col].astype(df_migrated[col].dtype)
                except Exception as e:
                    print(f"Warning: Could not cast legacy column '{col}' to type {df_migrated[col].dtype}. Error: {e}")

        # 4. Sort both DataFrames for robust comparison (if order isn't guaranteed by query)
        df_legacy = df_legacy.sort_values(by=list(df_legacy.columns)).reset_index(drop=True)
        df_migrated = df_migrated.sort_values(by=list(df_migrated.columns)).reset_index(drop=True)

        # 5. Compare DataFrames
        try:
            pd.testing.assert_frame_equal(df_legacy, df_migrated, check_dtype=True, check_exact=False) # check_exact=False for float comparisons
            print("Data output parity check PASSED: DataFrames are identical.")
        except AssertionError as e:
            print(f"Data output parity check FAILED: {e}")
            raise
    ```

### 2. Transformation Correctness

#### Test Case 2.1: Parameter Handling (`stichtag`, `wiederanlaufWert`)

*   **Purpose**: Verify that the `stichtag` and `wiederanlaufWert` parameters are correctly passed from the Airflow DAG to the PySpark script and are correctly interpreted by the script.
*   **Setup**:
    1.  Modify the `r_ausd_bp_ta_rn_vertrag.py` script temporarily to log the received `stichtag` and `wiederanlaufWert` values at the very beginning of its `main` function, and then exit immediately (or write them to a temporary GCS file).
    2.  Deploy this modified PySpark script to GCS.
    3.  Ensure the Airflow DAG is deployed.
*   **Action**:
    1.  Trigger the `dw_bert_ausd_bp_ta_rn_vertrag` Airflow DAG.
    2.  In the Airflow UI, navigate to the `run_dw_bert_ausd_bp_ta_rn_vertrag` task logs.
*   **Pass/Fail Criterion**:
    *   **Pass**: The Dataproc job logs clearly show the PySpark script receiving and logging the exact `stichtag` (e.g., `20230315`) and `wiederanlaufWert` (e.g., `12345`) values that were passed from the Airflow DAG.
    *   **Fail**: The logged values do not match the expected values, or the script fails to start due to parameter parsing issues.

    ```python
    # Excerpt from modified pyspark_scripts/r_ausd_bp_ta_rn_vertrag.py for testing:
    import argparse
    from pyspark.sql import SparkSession
    from datetime import datetime
    import sys # Import sys to exit early for testing

    def main():
        parser = argparse.ArgumentParser(description="PySpark script for BERT_AUSD_BP_TA_RN_VERTRAG.")
        parser.add_argument("--stichtag", type=str, help="Key date for processing (DDMMYYYY).")
        parser.add_argument("--wiederanlaufWert", type=int, default=0,
                            help="Restart value, only process contracts with DWH_VERTRAG_ID > this value.")
        args = parser.parse_args()

        spark = SparkSession.builder \
            .appName("DW.BERT_AUSD_BP_TA_RN_VERTRAG_PySpark") \
            .getOrCreate()

        # --- Test-specific logging and early exit ---
        spark.log4j.warn(f"TEST_PARAM_CHECK: Stichtag={args.stichtag}, Wiederanlaufwert={args.wiederanlaufWert}")
        spark.stop()
        sys.exit(0) # Exit successfully after logging parameters
        # --- End Test-specific code ---

        # ... rest of the original script logic would follow here ...
    ```

#### Test Case 2.2: Filtering Logic (Requires Legacy Script Analysis)

*   **Purpose**: Verify that the PySpark script correctly applies filtering logic based on `stichtag` and `wiederanlaufWert`, as well as any other filters present in the legacy `ksh` script (e.g., `Gueltig_von`, `Gueltig_bis`, `LADEDATUM`, `DWH_VERTRAG_ID`).
*   **Setup**:
    1.  Populate a BigQuery source table (e.g., `your_project_id.test_dataset.source_contracts`) with a diverse set of test data. This data should include rows that *should* be filtered in and out based on various combinations of `stichtag`, `wiederanlaufWert`, and other legacy filter conditions.
    2.  The `r_ausd_bp_ta_rn_vertrag.py` script must be fully implemented with the translated filtering logic.
    3.  Create an empty BigQuery target table (e.g., `your_project_id.test_dataset.filtered_output`).
*   **Action**:
    1.  Trigger the Airflow DAG with specific `stichtag` (e.g., `20230315`) and `wiederanlaufWert` (e.g., `100`).
    2.  After successful completion, query the `your_project_id.test_dataset.filtered_output` table.
*   **Pass/Fail Criterion**:
    *   **Pass**: The rows in `your_project_id.test_dataset.filtered_output` exactly match the expected rows that should pass the filtering criteria based on the provided parameters and the translated legacy logic.
    *   **Fail**: Incorrect rows are present, or expected rows are missing.

    ```sql
    -- Example SQL assertion for BigQuery (conceptual, based on assumed filtering logic)
    -- This example assumes the PySpark script filters `source_contracts` based on `DWH_VERTRAG_ID`
    -- and a date range defined by `Gueltig_von`, `Gueltig_bis`, and `LADEDATUM` against `stichtag`.

    -- Define expected parameters for this test run
    DECLARE test_stichtag STRING DEFAULT '20230315';
    DECLARE test_wiederanlaufWert INT64 DEFAULT 100;

    -- Expected result based on known source data and parameters
    CREATE OR REPLACE TEMPORARY TABLE expected_filtered_output AS
    SELECT * FROM `your_project_id.test_dataset.source_contracts`
    WHERE DWH_VERTRAG_ID > test_wiederanlaufWert
      AND PARSE_DATE('%Y%m%d', test_stichtag) BETWEEN Gueltig_von AND Gueltig_bis
      AND LADEDATUM <= PARSE_DATE('%Y%m%d', test_stichtag);

    -- Compare actual output from PySpark job with expected output
    SELECT
      (SELECT COUNT(1) FROM `your_project_id.test_dataset.filtered_output`) AS actual_row_count,
      (SELECT COUNT(1) FROM expected_filtered_output) AS expected_row_count,
      (SELECT COUNT(1) FROM `your_project_id.test_dataset.filtered_output` EXCEPT DISTINCT SELECT * FROM expected_filtered_output) AS diff_actual_not_expected,
      (SELECT COUNT(1) FROM expected_filtered_output EXCEPT DISTINCT SELECT * FROM `your_project_id.test_dataset.filtered_output`) AS diff_expected_not_actual;

    -- Pass if actual_row_count = expected_row_count AND diff_actual_not_expected = 0 AND diff_expected_not_actual = 0.
    ```

#### Test Case 2.3: Data Type and NULL Handling (Requires Legacy Script Analysis)

*   **Purpose**: Verify that data types are correctly handled during read/transform/write operations and that NULL values are processed according to the legacy logic (e.g., preserved, defaulted, or filtered).
*   **Setup**:
    1.  Populate a BigQuery source table (e.g., `your_project_id.test_dataset.source_data_types`) with test data that includes various data types (strings, integers, floats, dates, booleans) and explicit NULL values in different columns. Include edge cases like empty strings, zero values, and dates at boundary conditions.
    2.  The `r_ausd_bp_ta_rn_vertrag.py` script must be fully implemented with the translated logic, including any specific handling for data types or NULLs.
    3.  Create an empty BigQuery target table (e.g., `your_project_id.test_dataset.target_data_types`) with the expected schema.
*   **Action**:
    1.  Trigger the Airflow DAG.
    2.  After successful completion, query the `your_project_id.test_dataset.target_data_types` table and inspect the data.
*   **Pass/Fail Criterion**:
    *   **Pass**: All data types in the target table match the expected types, and NULL values are present or handled exactly as they were in the legacy system (e.g., NULLs remain NULL, or are correctly converted to a default value if that was the legacy behavior).
    *   **Fail**: Data type mismatches, or incorrect handling of NULL values (e.g., NULLs converted to empty strings, or non-NULLs converted to NULLs unexpectedly).

### 3. External-System Replacements

#### Test Case 3.1: GCS Access for PySpark Script

*   **Purpose**: Verify that the Dataproc cluster can successfully access the PySpark script stored in Google Cloud Storage.
*   **Setup**:
    1.  Ensure the PySpark script `r_ausd_bp_ta_rn_vertrag.py` is uploaded to `gs://YOUR_GCS_BUCKET_NAME/pyspark_scripts/`.
    2.  Ensure the Dataproc cluster's service account has `Storage Object Viewer` permissions on the GCS bucket.
    3.  Ensure the Airflow DAG is configured with the correct GCS path for `main_python_file_uri`.
*   **Action**:
    1.  Trigger the `dw_bert_ausd_bp_ta_rn_vertrag` Airflow DAG.
    2.  Monitor the Dataproc job logs in the Airflow UI or directly in Cloud Logging.
*   **Pass/Fail Criterion**:
    *   **Pass**: The Dataproc job starts and executes the PySpark script without errors related to GCS file access (e.g., "File not found", "Permission denied"). The job proceeds to execute the PySpark script's logic.
    *   **Fail**: The Dataproc job fails with an error indicating it could not find or access the PySpark script in GCS.

#### Test Case 3.2: BigQuery Read/Write Access from PySpark

*   **Purpose**: Verify that the PySpark script running on Dataproc can successfully read data from a BigQuery source table and write data to a BigQuery target table. This confirms the replacement of potential legacy database interactions.
*   **Setup**:
    1.  Create a dummy BigQuery source table (e.g., `your_project_id.test_dataset.dummy_source`) with a few rows of sample data.
    2.  Create an empty dummy BigQuery target table (e.g., `your_project_id.test_dataset.dummy_target`).
    3.  Modify the `r_ausd_bp_ta_rn_vertrag.py` script temporarily to perform a simple read from `dummy_source` and write to `dummy_target` (e.g., `spark.read.format("bigquery").load(...).write.format("bigquery").save(...)`).
    4.  Ensure the Dataproc cluster's service account has `BigQuery Data Editor` permissions on `your_project_id.test_dataset`.
    5.  Deploy the modified PySpark script to GCS.
*   **Action**:
    1.  Trigger the `dw_bert_ausd_bp_ta_rn_vertrag` Airflow DAG.
    2.  After successful completion, query `your_project_id.test_dataset.dummy_target` to check its content.
*   **Pass/Fail Criterion**:
    *   **Pass**: The Dataproc job completes successfully, and the `your_project_id.test_dataset.dummy_target` table contains the data that was read from `your_project_id.test_dataset.dummy_source`.
    *   **Fail**: The Dataproc job fails with BigQuery access errors (e.g., "Permission denied", "Table not found") or the target table is empty/incorrect.

    ```python
    # Excerpt from modified pyspark_scripts/r_ausd_bp_ta_rn_vertrag.py for testing:
    import argparse
    from pyspark.sql import SparkSession
    import sys

    def main():
        parser = argparse.ArgumentParser(description="PySpark script for BERT_AUSD_BP_TA_RN_VERTRAG.")
        parser.add_argument("--stichtag", type=str, help="Key date for processing (DDMMYYYY).")
        parser.add_argument("--wiederanlaufWert", type=int, default=0,
                            help="Restart value, only process contracts with DWH_VERTRAG_ID > this value.")
        args = parser.parse_args()

        spark = SparkSession.builder \
            .appName("DW.BERT_AUSD_BP_TA_RN_VERTRAG_PySpark") \
            .getOrCreate()

        spark.log4j.warn("TEST_BQ_ACCESS: Attempting to read from dummy_source and write to dummy_target.")

        try:
            # Read from dummy source table
            df_source = spark.read.format("bigquery") \
                .option("table", "gcp-project-id.test_dataset.dummy_source") \
                .load()
            spark.log4j.warn(f"TEST_BQ_ACCESS: Read {df_source.count()} rows from dummy_source.")

            # Write to dummy target table
            df_source.write.format("bigquery") \
                .option("table", "gcp-project-id.test_dataset.dummy_target") \
                .mode("overwrite") \
                .save()
            spark.log4j.warn("TEST_BQ_ACCESS: Successfully wrote to dummy_target.")

        except Exception as e:
            spark.log4j.error(f"TEST_BQ_ACCESS: BigQuery access failed: {e}")
            spark.stop()
            sys.exit(1) # Exit with error to fail the job

        spark.stop()
        sys.exit(0) # Exit successfully
    ```

### 4. Data Quality / Row Count / Schema Assertions

#### Test Case 4.1: Target Table Existence and Schema Validation

*   **Purpose**: Verify that the PySpark job successfully creates or updates the expected BigQuery target table with the correct schema.
*   **Setup**:
    1.  Ensure the `r_ausd_bp_ta_rn_vertrag.py` script is fully implemented to write to the final target BigQuery table (e.g., `your_project_id.your_dataset.bert_ausd_bp_ta_rn_vertrag`).
    2.  Have the expected schema definition for the target table readily available (e.g., in a JSON file or as a Python dictionary).
    3.  Optionally, start with an empty target table or a table with an outdated schema to ensure the job correctly handles schema evolution or creation.
*   **Action**:
    1.  Trigger the `dw_bert_ausd_bp_ta_rn_vertrag` Airflow DAG.
    2.  After successful completion, use the BigQuery API or `bq` command-line tool to inspect the target table's schema.
*   **Pass/Fail Criterion**:
    *   **Pass**: The target table `your_project_id.your_dataset.bert_ausd_bp_ta_rn_vertrag` exists, and its schema (column names, data types, nullability, and any partitioning/clustering) exactly matches the expected schema definition.
    *   **Fail**: The target table does not exist, or its schema deviates from the expected definition.

    ```python
    # Example pytest assertion using Google Cloud BigQuery client
    import json
    from google.cloud import bigquery

    def test_target_table_schema(bigquery_client: bigquery.Client, target_table_id: str, expected_schema_json_path: str):
        """
        Validates the schema of the BigQuery target table against a predefined JSON schema.
        """
        # Load expected schema from a JSON file
        with open(expected_schema_json_path, 'r') as f:
            expected_schema_dict = json.load(f)
        expected_schema = bigquery.Schema.from_api_repr(expected_schema_dict)

        # Get actual schema from BigQuery
        try:
            table = bigquery_client.get_table(target_table_id)
            actual_schema = table.schema
            print(f"Successfully retrieved schema for table: {target_table_id}")
        except Exception as e:
            raise Exception(f"Failed to retrieve schema for {target_table_id}: {e}")

        # Convert schemas to a comparable format (e.g., list of dicts)
        actual_schema_list = [field.to_api_repr() for field in actual_schema]
        expected_schema_list = [field.to_api_repr() for field in expected_schema]

        # Sort by name for consistent comparison
        actual_schema_list.sort(key=lambda x: x['name'])
        expected_schema_list.sort(key=lambda x: x['name'])

        # Compare schemas
        assert actual_schema_list == expected_schema_list, \
            f"Schema mismatch for {target_table_id}. \nExpected: {json.dumps(expected_schema_list, indent=2)}\nActual: {json.dumps(actual_schema_list, indent=2)}"
        print(f"Schema for {target_table_id} PASSED validation.")

    # Example expected_schema.json content:
    # [
    #   {"name": "DWH_VERTRAG_ID", "type": "INTEGER", "mode": "REQUIRED"},
    #   {"name": "Gueltig_von", "type": "DATE", "mode": "NULLABLE"},
    #   {"name": "Gueltig_bis", "type": "DATE", "mode": "NULLABLE"},
    #   {"name": "LADEDATUM", "type": "DATE", "mode": "NULLABLE"},
    #   {"name": "PRODUCT_NAME", "type": "STRING", "mode": "NULLABLE"}
    # ]
    ```

#### Test Case 4.2: Row Count Validation

*   **Purpose**: Verify that the number of rows processed and written to the target table is as expected, potentially comparing it to the source row count or a known baseline after transformations.
*   **Setup**:
    1.  Ensure the `r_ausd_bp_ta_rn_vertrag.py` script is fully implemented.
    2.  Populate the BigQuery source table with a known number of rows.
    3.  Determine the *expected* row count in the target table after transformations (e.g., if filtering reduces rows, or aggregations change the count). This expected count should be derived from the legacy script's logic.
*   **Action**:
    1.  Trigger the `dw_bert_ausd_bp_ta_rn_vertrag` Airflow DAG.
    2.  After successful completion, query the target BigQuery table for its row count.
*   **Pass/Fail Criterion**:
    *   **Pass**: The row count in the target table `your_project_id.your_dataset.bert_ausd_bp_ta_rn_vertrag` matches the expected row count.
    *   **Fail**: The row count deviates from the expected value.

    ```sql
    -- Example SQL assertion for BigQuery
    -- Assuming source_table_id and target_table_id are fully qualified BigQuery table IDs

    -- 1. Get actual row count from the migrated target table
    SELECT COUNT(1) FROM `your_project_id.your_dataset.bert_ausd_bp_ta_rn_vertrag`;
    -- Let's say this returns `actual_rows`.

    -- 2. Compare `actual_rows` to `expected_row_count` (pre-calculated based on legacy logic)
    -- For example, if the legacy script was expected to filter 100 rows from a source of 1000,
    -- and the source table for testing has 1000 rows, then expected_row_count would be 900.

    -- If a simple 1:1 mapping (no filters/aggregations) is expected:
    SELECT
      (SELECT COUNT(1) FROM `source_project.source_dataset.source_table`) AS source_rows,
      (SELECT COUNT(1) FROM `your_project_id.your_dataset.bert_ausd_bp_ta_rn_vertrag`) AS target_rows;
    -- Pass if source_rows = target_rows (or matches expected transformation ratio).
    ```

#### Test Case 4.3: Data Quality Checks (Requires Legacy Script Analysis)

*   **Purpose**: Verify specific data quality rules are maintained or enforced by the migrated job (e.g., uniqueness of primary keys, referential integrity, value ranges, absence of unexpected duplicates or invalid data). This test is highly dependent on the specific business rules embedded in the legacy `ksh` script.
*   **Setup**:
    1.  Populate the BigQuery source table with test data that includes both valid and invalid data according to the defined data quality rules (e.g., duplicate IDs, out-of-range values, missing mandatory fields).
    2.  The `r_ausd_bp_ta_rn_vertrag.py` script must be fully implemented with any data quality enforcement or transformation logic (e.g., dropping duplicates, defaulting values, rejecting invalid records).
*   **Action**:
    1.  Trigger the `dw_bert_ausd_bp_ta_rn_vertrag` Airflow DAG.
    2.  After successful completion, run SQL queries against the target BigQuery table to check data quality.
*   **Pass/Fail Criterion**:
    *   **Pass**: All data quality checks pass (e.g., no duplicate primary keys, all values within expected ranges, referential integrity holds if applicable, no unexpected NULLs).
    *   **Fail**: Any data quality check fails, indicating a deviation from the expected data quality behavior of the legacy system.

    ```sql
    -- Example SQL assertions for BigQuery (conceptual, based on common DQ rules)

    -- Check for duplicate primary keys (assuming DWH_VERTRAG_ID is the primary key)
    SELECT DWH_VERTRAG_ID, COUNT(1) as num_duplicates
    FROM `your_project_id.your_dataset.bert_ausd_bp_ta_rn_vertrag`
    GROUP BY DWH_VERTRAG_ID
    HAVING COUNT(1) > 1;
    -- Pass if this query returns 0 rows.

    -- Check for unexpected NULLs in a column that should always have a value
    SELECT COUNT(1) FROM `your_project_id.your_dataset.bert_ausd_bp_ta_rn_vertrag`
    WHERE MANDATORY_COLUMN IS NULL;
    -- Pass if this query returns 0 rows.

    -- Check for values outside an expected range (e.g., a percentage field should be between 0 and 100)
    SELECT COUNT(1) FROM `your_project_id.your_dataset.bert_ausd_bp_ta_rn_vertrag`
    WHERE PERCENTAGE_FIELD < 0 OR PERCENTAGE_FIELD > 100;
    -- Pass if this query returns 0 rows.

    -- Check for invalid date formats or out-of-range dates if applicable
    SELECT COUNT(1) FROM `your_project_id.your_dataset.bert_ausd_bp_ta_rn_vertrag`
    WHERE NOT SAFE.PARSE_DATE('%Y-%m-%d', DATE_COLUMN_AS_STRING) IS NOT NULL;
    -- Pass if this query returns 0 rows.
    ```