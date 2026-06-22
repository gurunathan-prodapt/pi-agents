As a senior data-migration QA engineer, I've designed a suite of validation tests for the `EXIS_SD_APT_BESTANDS` job migration. Given the critical information that the core transformation logic (`r_exis_v2` executable and `h_exis_apt_bestandsdaten.var` configuration) is *unknown* and the provided PySpark script contains *placeholder logic*, these tests are structured to:

1.  **Verify the migration framework** (BigQuery reads, GCS writes, Airflow orchestration) with the current placeholder logic.
2.  **Provide a clear roadmap** for testing the actual transformation logic once it has been reverse-engineered and implemented in the PySpark script.
3.  **Ensure data integrity** at various stages of the migration.

**Assumptions for all tests:**
*   Access to the legacy system (Oracle database, output files) for comparison data.
*   The BigQuery tables (`SOF_TA_BPR_OPTIONEN`, `SOF_VI_L_OPTIONZUORDNUNG`, `RPT_TA_S_D1_VERTRAG`) are populated with data identical to their Oracle counterparts.
*   A GCS bucket and Dataproc cluster are set up and accessible by the Airflow DAG.
*   The `r_exis_v2.py` script is uploaded to the specified GCS path.
*   The Airflow DAG `dw_dwh_exis_sd_apt_bestands` is deployed and configured with correct GCP project, region, cluster, and GCS paths.

---

## Migration Validation Tests: EXIS_SD_APT_BESTANDS

### 1. Output Parity

#### Test Case 1.1: End-to-End Output File Comparison (Golden File)

*   **Purpose**: To verify that the migrated job produces an output file that is functionally identical to the legacy job's output, given identical input data. This is the ultimate behavioral equivalence test.
*   **Setup**:
    1.  Ensure the BigQuery source tables (`SOF_TA_BPR_OPTIONEN`, `SOF_VI_L_OPTIONZUORDNUNG`, `RPT_TA_S_D1_VERTRAG`) are populated with a specific, known dataset that was used to generate a "golden" output file from the legacy system.
    2.  Obtain a "golden" `DWHM_APT_BESTANDSREPORT_<yyyymmddhhmmss>.csv.gz` file from a successful run of the legacy `DW.DWH_EXIS_SD_APT_BESTANDS` job with the corresponding input data.
    3.  Extract the content of the golden file (e.g., `golden_output.csv`).
    4.  **Crucially**: The `r_exis_v2.py` script must be fully implemented with the *actual* reverse-engineered transformation logic, not the placeholder.
*   **Action**:
    1.  Manually trigger the Airflow DAG `dw_dwh_exis_sd_apt_bestands`.
    2.  Wait for the DAG to complete successfully.
    3.  Download the generated `DWHM_APT_BESTANDSREPORT_<yyyymmddhhmmss>.csv.gz` file (or the directory containing part files) from the GCS output bucket.
    4.  Extract and concatenate the content of the downloaded file(s) into a single CSV (e.g., `migrated_output.csv`).
*   **Pass/Fail Criterion**:
    *   The Airflow DAG completes successfully.
    *   The extracted `migrated_output.csv` file, after normalizing for timestamp in filename and any non-deterministic row ordering (if applicable), is identical to the `golden_output.csv` file. This can be done by comparing sorted dataframes or using a robust data comparison tool.

    ```python
    # Example pytest assertion for output file comparison
    import pandas as pd
    import gzip
    import os
    from google.cloud import storage

    def download_gcs_output(bucket_name, prefix):
        """Downloads and concatenates gzipped CSV part files from GCS."""
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=prefix))

        # Find the latest output directory based on timestamp
        output_dirs = sorted([b.name.split('/')[1] for b in blobs if b.name.startswith(f"{prefix.split('/')[0]}/DWHM_APT_BESTANDSREPORT_") and 'part-' not in b.name], reverse=True)
        if not output_dirs:
            raise FileNotFoundError(f"No output directory found in gs://{bucket_name}/{prefix}")
        latest_output_dir = output_dirs[0]
        
        full_prefix = f"{prefix.split('/')[0]}/{latest_output_dir}/"
        part_blobs = [b for b in blobs if b.name.startswith(full_prefix) and b.name.endswith(".csv.gz")]

        if not part_blobs:
            raise FileNotFoundError(f"No part files found in gs://{bucket_name}/{full_prefix}")

        all_parts_df = []
        for blob in part_blobs:
            # Download blob content
            content = blob.download_as_bytes()
            with gzip.open(io.BytesIO(content), 'rt') as f:
                all_parts_df.append(pd.read_csv(f))
        
        if not all_parts_df:
            return pd.DataFrame() # Return empty if no data
        
        return pd.concat(all_parts_df, ignore_index=True)

    def test_output_parity_with_golden_file(gcs_bucket_name, golden_csv_path):
        """
        Compares the migrated job's output with a golden CSV file.
        Requires the PySpark script to have the actual transformation logic.
        """
        # Assuming 'exports/' is the prefix for output files
        migrated_df = download_gcs_output(gcs_bucket_name, "exports/")
        golden_df = pd.read_csv(golden_csv_path)

        # Sort DataFrames by all columns to handle non-deterministic output order
        # This assumes the order of columns is consistent.
        migrated_df_sorted = migrated_df.sort_values(by=list(migrated_df.columns)).reset_index(drop=True)
        golden_df_sorted = golden_df.sort_values(by=list(golden_df.columns)).reset_index(drop=True)

        pd.testing.assert_frame_equal(migrated_df_sorted, golden_df_sorted, check_dtype=True, check_exact=True)
    ```

### 2. Transformation Correctness

#### Test Case 2.1: Input Data Read Verification

*   **Purpose**: To verify that the PySpark application can successfully connect to and read data from all specified BigQuery source tables.
*   **Setup**:
    1.  Ensure the BigQuery tables `SOF_TA_BPR_OPTIONEN`, `SOF_VI_L_OPTIONZUORDNUNG`, `RPT_TA_S_D1_VERTRAG` exist and contain at least some sample data.
    2.  Deploy the Airflow DAG with the current `r_exis_v2.py` script.
*   **Action**:
    1.  Trigger the Airflow DAG `dw_dwh_exis_sd_apt_bestands`.
    2.  Monitor the Dataproc job logs for any errors related to BigQuery connectivity or table access.
*   **Pass/Fail Criterion**:
    *   The Dataproc job logs show successful reading of data from all three BigQuery tables without errors (e.g., "Reading data from BigQuery table: ...").
    *   The job completes successfully.

#### Test Case 2.2: Placeholder Transformation Logic Verification (Initial)

*   **Purpose**: To verify that the *current placeholder* transformation logic in `r_exis_v2.py` (`df_bpr_optionen.select("*")`) correctly selects all columns from `SOF_TA_BPR_OPTIONEN` and writes them to GCS. This test validates the framework's ability to process and output data, even if the logic is not yet final.
*   **Setup**:
    1.  Ensure `SOF_TA_BPR_OPTIONEN` BigQuery table has known sample data.
    2.  Deploy the Airflow DAG with the current `r_exis_v2.py` placeholder logic.
*   **Action**:
    1.  Trigger the Airflow DAG `dw_dwh_exis_sd_apt_bestands`.
    2.  Download the generated CSV.GZ file(s) from GCS.
    3.  Extract and concatenate the CSV(s) and inspect its content and schema.
*   **Pass/Fail Criterion**:
    *   The output CSV contains all columns and rows *only* from the `SOF_TA_BPR_OPTIONEN` BigQuery table.
    *   The row count of the output CSV matches the row count of `SOF_TA_BPR_OPTIONEN`.
    *   The column names and data types in the output CSV match the column names and types in `SOF_TA_BPR_OPTIONEN`.

#### Test Case 2.3: Transformation Correctness (Post-Reverse Engineering)

*   **Purpose**: To verify that the fully implemented transformation logic (joins, filters, aggregations, data types, NULL handling, edge cases) in `r_exis_v2.py` produces the expected results based on the reverse-engineered `r_exis_v2` and `h_exis_apt_bestandsdaten.var`.
*   **Setup**:
    1.  Complete the reverse-engineering of `r_exis_v2` and `h_exis_apt_bestandsdaten.var`.
    2.  Update `r_exis_v2.py` with the actual transformation logic.
    3.  Create specific test datasets in BigQuery that cover various scenarios:
        *   Standard joins (inner, left, right, full as per logic).
        *   Filtering conditions (e.g., date ranges, specific values, NULLs).
        *   Aggregation scenarios (e.g., empty groups, single-row groups, multiple-row groups).
        *   Data type conversions (e.g., string to int, date formats).
        *   NULL value propagation and handling (e.g., `COALESCE`, `IFNULL`).
        *   Edge cases identified during reverse engineering (e.g., empty input tables, tables with only NULLs, boundary values).
    4.  For each test dataset, pre-calculate the expected output (a "mini-golden" file for that specific scenario).
    5.  Deploy the updated Airflow DAG and `r_exis_v2.py`.
*   **Action**:
    1.  For each specific test dataset, trigger the Airflow DAG.
    2.  Download and extract the generated CSV.GZ file(s).
    3.  Compare the extracted CSV content with the pre-calculated expected output for that scenario.
*   **Pass/Fail Criterion**:
    *   The output CSV content for each test scenario exactly matches the pre-calculated expected output, considering potential non-deterministic ordering by sorting.
    *   All joins, filters, aggregations, data type handling, and NULL handling behave as specified by the reverse-engineered logic.

    ```python
    # Example pytest assertion for a specific transformation scenario
    import pandas as pd
    import gzip
    import io

    def test_transformation_logic_scenario(gcs_bucket_name, test_scenario_input_data_setup, expected_output_csv_path):
        """
        Tests a specific transformation scenario after r_exis_v2.py is fully implemented.
        `test_scenario_input_data_setup` would be a fixture that loads specific data
        into BigQuery for this scenario and triggers the DAG.
        """
        # Assuming the DAG run is complete and output is in GCS
        migrated_df = download_gcs_output(gcs_bucket_name, "exports/")
        expected_df = pd.read_csv(expected_output_csv_path)

        # Sort both DataFrames by all columns to handle non-deterministic output order
        migrated_df_sorted = migrated_df.sort_values(by=list(migrated_df.columns)).reset_index(drop=True)
        expected_df_sorted = expected_df.sort_values(by=list(expected_df.columns)).reset_index(drop=True)

        pd.testing.assert_frame_equal(migrated_df_sorted, expected_df_sorted, check_dtype=True, check_exact=True)
    ```

### 3. External-System Replacements

#### Test Case 3.1: BigQuery Source Data Integrity

*   **Purpose**: To ensure that the data migrated to BigQuery tables is an exact replica of the data in the legacy Oracle source tables.
*   **Setup**:
    1.  Access to both legacy Oracle database and BigQuery.
    2.  Tools to extract data from both systems (e.g., SQL clients, data export utilities).
*   **Action**:
    1.  For each source table (`SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG`, `RPT$TA_S_D1_VERTRAG`):
        *   Execute `SELECT * FROM <legacy_oracle_table>` and export to a CSV.
        *   Execute `SELECT * FROM <bigquery_table>` and export to a CSV.
        *   Compare the two CSV files (e.g., using `diff` or a data comparison tool).
*   **Pass/Fail Criterion**:
    *   For each table, the exported CSV from BigQuery is identical to the exported CSV from Oracle, accounting for potential differences in data type representation (e.g., `NUMBER` vs `FLOAT64`, `DATE` vs `TIMESTAMP`) but not value.
    *   Row counts and column counts must match exactly.

    ```sql
    -- Example SQL for BigQuery row count
    SELECT COUNT(*) FROM `YOUR_GCP_PROJECT_ID.YOUR_BIGQUERY_DATASET.SOF_TA_BPR_OPTIONEN`;

    -- Example SQL for Oracle row count (syntax may vary)
    SELECT COUNT(*) FROM SOF$TA_BPR_OPTIONEN;

    -- Example SQL for BigQuery schema
    SELECT column_name, data_type FROM `YOUR_GCP_PROJECT_ID.YOUR_BIGQUERY_DATASET.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'SOF_TA_BPR_OPTIONEN';

    -- Example SQL for Oracle schema (syntax may vary)
    SELECT column_name, data_type FROM ALL_TAB_COLUMNS WHERE OWNER = 'YOUR_SCHEMA' AND TABLE_NAME = 'SOF$TA_BPR_OPTIONEN';
    ```

#### Test Case 3.2: GCS Output File Generation and Naming Convention

*   **Purpose**: To verify that the PySpark application correctly writes the output to the specified GCS bucket, uses `gzip` compression, and adheres to the `DWHM_APT_BESTANDSREPORT_<yyyymmddhhmmss>.csv.gz` naming convention for the output directory.
*   **Setup**:
    1.  Ensure the Airflow DAG is deployed.
    2.  Note the expected GCS output bucket.
*   **Action**:
    1.  Trigger the Airflow DAG `dw_dwh_exis_sd_apt_bestands`.
    2.  After successful completion, browse the GCS bucket (`gs://YOUR_BUCKET_NAME/exports/`) using the GCP Console or `gsutil`.
    3.  Identify the generated directory and its contents.
*   **Pass/Fail Criterion**:
    *   A directory is created in `gs://YOUR_BUCKET_NAME/exports/`.
    *   The directory name starts with `DWHM_APT_BESTANDSREPORT_` followed by a timestamp in `YYYYMMDDHHMMSS` format.
    *   Inside this directory, there are one or more `part-*.csv.gz` files.
    *   At least one `part-*.csv.gz` file is not empty (contains data).
    *   The `part-*.csv.gz` files are indeed gzipped (can be verified by attempting to decompress them).

    ```python
    # Example pytest assertion for GCS file naming and compression
    import re
    from google.cloud import storage
    import gzip
    import io

    def test_gcs_output_file_properties(gcs_bucket_name):
        client = storage.Client()
        bucket = client.get_bucket(gcs_bucket_name)
        output_prefix = "exports/"
        
        # List all blobs under the output_prefix
        blobs = list(bucket.list_blobs(prefix=output_prefix))

        # Find the latest output directory based on the naming convention
        output_dirs = sorted([b.name.split('/')[1] for b in blobs if b.name.startswith(f"{output_prefix.split('/')[0]}/DWHM_APT_BESTANDSREPORT_") and 'part-' not in b.name], reverse=True)
        assert output_dirs, "No output directory with expected naming convention found."
        
        latest_output_dir_name = output_dirs[0]
        
        # Verify the timestamp format in the directory name
        timestamp_str = latest_output_dir_name.split('_')[-1]
        assert re.match(r"\d{14}", timestamp_str), f"Timestamp format incorrect in directory name: {timestamp_str}"

        # Check for part files inside the latest output directory
        part_file_blobs = [b for b in blobs if b.name.startswith(f"{output_prefix}{latest_output_dir_name}/part-") and b.name.endswith(".csv.gz")]
        assert part_file_blobs, "No gzipped part files found in the output directory."

        # Verify compression and non-empty content
        total_size = 0
        for blob in part_file_blobs:
            total_size += blob.size
            # Download a small portion to check for gzip magic number
            try:
                content_sample = blob.download_as_bytes(start_byte=0, end_byte=99)
                assert content_sample[0] == 0x1f and content_sample[1] == 0x8b, \
                    f"File {blob.name} is not gzipped (missing magic number)."
            except Exception as e:
                pytest.fail(f"Could not verify gzip compression for {blob.name}: {e}")
        
        assert total_size > 0, "Output files are empty."
    ```

#### Test Case 3.3: Airflow DAG Orchestration

*   **Purpose**: To verify that the Airflow DAG `dw_dwh_exis_sd_apt_bestands` successfully triggers the Dataproc PySpark job and completes without Airflow-level errors.
*   **Setup**:
    1.  Ensure the Airflow DAG is deployed and configured with correct GCP project, region, cluster, and GCS paths.
    2.  Ensure the Dataproc cluster is running and accessible.
*   **Action**:
    1.  Trigger the Airflow DAG `dw_dwh_exis_sd_apt_bestands` from the Airflow UI.
    2.  Monitor the DAG run in the Airflow UI.
*   **Pass/Fail Criterion**:
    *   The `dwh_exis_sd_apt_bestands_pyspark_export` task transitions to "success".
    *   The overall DAG run completes with a "success" status.
    *   No Airflow task logs indicate failures related to Dataproc job submission or execution.

#### Test Case 3.4: Logging Verification

*   **Purpose**: To ensure that the migrated job produces adequate logs in Airflow and Dataproc, providing sufficient information for monitoring and troubleshooting, similar to how `DW.LESE_LOG` might have functioned.
*   **Setup**:
    1.  Ensure the Airflow DAG is deployed.
*   **Action**:
    1.  Trigger the Airflow DAG.
    2.  After completion, review the logs for the `dwh_exis_sd_apt_bestands_pyspark_export` task in the Airflow UI.
    3.  Access the Dataproc job logs (via Airflow UI or GCP Console).
*   **Pass/Fail Criterion**:
    *   The Airflow task logs contain information about the Dataproc job submission and status.
    *   The Dataproc job logs contain output from the PySpark script, including `print` statements (e.g., "Reading data from BigQuery table:", "Writing transformed data to GCS:").
    *   No critical errors or unhandled exceptions are present in the logs.
    *   The logging level and content are sufficient for troubleshooting and operational monitoring.

### 4. Data Quality / Row Count / Schema Assertions

#### Test Case 4.1: Input Row Count Verification

*   **Purpose**: To confirm that the row counts of the BigQuery source tables match their legacy Oracle counterparts, ensuring data migration integrity.
*   **Setup**:
    1.  Access to legacy Oracle database and BigQuery.
*   **Action**:
    1.  For each source table:
        *   Execute `SELECT COUNT(*) FROM <legacy_oracle_table>` on Oracle.
        *   Execute `SELECT COUNT(*) FROM <bigquery_table>` on BigQuery.
*   **Pass/Fail Criterion**:
    *   The row count from BigQuery for each table exactly matches the row count from Oracle for the corresponding table.

    ```sql
    -- BigQuery
    SELECT COUNT(*) FROM `YOUR_GCP_PROJECT_ID.YOUR_BIGQUERY_DATASET.SOF_TA_BPR_OPTIONEN`;
    SELECT COUNT(*) FROM `YOUR_GCP_PROJECT_ID.YOUR_BIGQUERY_DATASET.SOF_VI_L_OPTIONZUORDNUNG`;
    SELECT COUNT(*) FROM `YOUR_GCP_PROJECT_ID.YOUR_BIGQUERY_DATASET.RPT_TA_S_D1_VERTRAG`;

    -- Oracle (example, actual syntax might vary)
    SELECT COUNT(*) FROM SOF$TA_BPR_OPTIONEN;
    SELECT COUNT(*) FROM SOF$VI_L_OPTIONZUORDNUNG;
    SELECT COUNT(*) FROM RPT$TA_S_D1_VERTRAG;
    ```

#### Test Case 4.2: Output Row Count Verification

*   **Purpose**: To verify that the number of records in the generated GCS output file matches the expected row count from the legacy system's output.
*   **Setup**:
    1.  Obtain the row count from a "golden" output file generated by the legacy system.
    2.  **Crucially**: The `r_exis_v2.py` script must be fully implemented with the *actual* reverse-engineered transformation logic, not the placeholder.
*   **Action**:
    1.  Trigger the Airflow DAG.
    2.  Download the generated CSV.GZ file(s) from GCS.
    3.  Extract and concatenate the CSV(s) and count the number of rows (excluding header).
*   **Pass/Fail Criterion**:
    *   The row count of the extracted CSV file matches the row count of the legacy golden output file.

    ```python
    # Example pytest assertion for output row count
    import pandas as pd
    import gzip
    import io

    def test_output_row_count(gcs_bucket_name, expected_row_count):
        """
        Tests the row count of the migrated output.
        Requires the PySpark script to have the actual transformation logic.
        """
        migrated_df = download_gcs_output(gcs_bucket_name, "exports/")
        actual_row_count = len(migrated_df)

        assert actual_row_count == expected_row_count, \
            f"Output row count mismatch. Expected: {expected_row_count}, Actual: {actual_row_count}"
    ```

#### Test Case 4.3: Output Schema and Header Verification

*   **Purpose**: To ensure that the generated GCS output CSV file has the correct column headers and schema (data types) as expected by downstream systems or as defined by the legacy output.
*   **Setup**:
    1.  Obtain the expected column headers and data types from the legacy system's output specification or a golden file.
    2.  **Crucially**: The `r_exis_v2.py` script must be fully implemented with the *actual* reverse-engineered transformation logic, not the placeholder.
*   **Action**:
    1.  Trigger the Airflow DAG.
    2.  Download the generated CSV.GZ file(s) from GCS.
    3.  Extract and concatenate the CSV(s) and inspect its header row and infer data types.
*   **Pass/Fail Criterion**:
    *   The header row of the extracted CSV exactly matches the expected header row (case-sensitive, order-sensitive).
    *   The inferred data types for each column in the CSV are compatible with the expected data types.

    ```python
    # Example pytest assertion for output schema/headers
    import pandas as pd
    import gzip
    import io

    def test_output_schema_and_headers(gcs_bucket_name, expected_columns, expected_dtypes):
        """
        Tests the schema and headers of the migrated output.
        Requires the PySpark script to have the actual transformation logic.
        """
        migrated_df = download_gcs_output(gcs_bucket_name, "exports/")

        # Check column names
        assert list(migrated_df.columns) == expected_columns, \
            f"Output column headers mismatch. Expected: {expected_columns}, Actual: {list(migrated_df.columns)}"

        # Check data types (Pandas dtypes might not directly map to Spark/BigQuery, but should be compatible)
        actual_dtypes = migrated_df.dtypes.apply(str).to_dict()
        for col, expected_dtype in expected_dtypes.items():
            assert col in actual_dtypes, f"Column {col} not found in output."
            # This is a simplified check. A robust check might involve type conversion and comparison.
            # For example, check if 'int64' is compatible with 'object' if it contains only integers.
            assert actual_dtypes[col] == expected_dtype, \
                f"Data type mismatch for column {col}. Expected: {expected_dtype}, Actual: {actual_dtypes[col]}"
    ```