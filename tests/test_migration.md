As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the `DW.DWH_APT_EXPORT_MONATLICH_JP` job migration. These tests aim to ensure behavioral equivalence between the legacy UC4 process and its new Airflow/Dataproc/PySpark implementation on GCP.

Given that the provided PySpark scripts are placeholders, tests related to the *actual data content and transformation logic* will be explicitly marked as dependent on the full implementation of the PySpark scripts after the `r_exis_v2` analysis. However, the framework, orchestration, and output structure can be thoroughly tested.

---

## Migration Validation Tests: DW.DWH_APT_EXPORT_MONATLICH_JP

### 1. Orchestration & Scheduling Tests

#### Test 1.1: Monthly Schedule Adherence

*   **Purpose**: Verify that the Airflow DAG `dw_dwh_apt_export_monatlich_jp` triggers according to its monthly schedule, mirroring the UC4 Event `DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT`.
*   **Setup**:
    1.  Ensure the `dw_dwh_apt_export_monatlich_jp` DAG is deployed to Airflow and unpaused.
    2.  Set the Airflow system clock or manually trigger a DAG run for a specific `data_interval_start` that aligns with the monthly schedule (e.g., `2023-01-01T00:00:00Z` for a `0 6 1 * *` schedule).
*   **Action**:
    1.  Observe Airflow DAG runs over a period covering at least two scheduled intervals.
    2.  Alternatively, use `airflow dags test` for a specific execution date.
*   **Pass/Fail Criterion**: The DAG must trigger exactly once per month, at 06:00 AM on the 1st day of the month, for each scheduled interval. No unscheduled runs should occur unless manually triggered.
*   **Test Code (Airflow CLI)**:
    ```bash
    # To simulate a run for a specific logical date (e.g., for January 2023)
    # This will create a DAG run with data_interval_start = 2023-01-01T00:00:00Z
    # and data_interval_end = 2023-02-01T00:00:00Z
    airflow dags test dw_dwh_apt_export_monatlich_jp 2023-01-01
    ```

#### Test 1.2: Prerequisite Success Handling

*   **Purpose**: Verify that the DAG proceeds only after its prerequisite DAGs (`dw_bert_stammdaten_jp` and `dw_accessp_sigma_gprs_monatlich_jp`) have successfully completed.
*   **Setup**:
    1.  Deploy the `dw_dwh_apt_export_monatlich_jp` DAG.
    2.  Ensure mock or actual DAGs for `dw_bert_stammdaten_jp` and `dw_accessp_sigma_gprs_monatlich_jp` are available in Airflow.
    3.  Trigger a run of `dw_dwh_apt_export_monatlich_jp`.
    4.  Manually trigger or allow the prerequisite DAGs to complete successfully for the same logical date.
*   **Action**:
    1.  Observe the state of the `wait_for_bert_stammdaten_jp` and `wait_for_accessp_sigma_gprs_monatlich_jp` tasks in the Airflow UI.
    2.  Verify that subsequent tasks (`dw_dwh_exis_sd_apt_nna_data`, `dw_dwh_exis_sd_apt_nna_voic`) only start after both `ExternalTaskSensor` tasks succeed.
*   **Pass/Fail Criterion**: The `ExternalTaskSensor` tasks must transition to `success` only when their respective external DAGs complete successfully for the corresponding logical date. The subsequent Dataproc tasks must then execute.

#### Test 1.3: Prerequisite Failure Handling

*   **Purpose**: Verify that the DAG correctly handles failures in its prerequisite DAGs, preventing downstream tasks from running.
*   **Setup**:
    1.  Deploy the `dw_dwh_apt_export_monatlich_jp` DAG.
    2.  Ensure mock or actual DAGs for `dw_bert_stammdaten_jp` and `dw_accessp_sigma_gprs_monatlich_jp` are available.
    3.  Trigger a run of `dw_dwh_apt_monatlich_jp`.
    4.  Manually trigger or allow *one or both* prerequisite DAGs to fail for the same logical date.
*   **Action**:
    1.  Observe the state of the `wait_for_bert_stammdaten_jp` and `wait_for_accessp_sigma_gprs_monatlich_jp` tasks.
    2.  Verify that the `ExternalTaskSensor` tasks eventually transition to `failed` (or `upstream_failed` if the external DAG fails before the sensor can even check).
    3.  Verify that the downstream Dataproc tasks (`dw_dwh_exis_sd_apt_nna_data`, `dw_dwh_exis_sd_apt_nna_voic`) do not start and are marked as `skipped` or `upstream_failed`.
*   **Pass/Fail Criterion**: If any prerequisite DAG fails, the corresponding `ExternalTaskSensor` must fail, and all subsequent tasks in `dw_dwh_apt_export_monatlich_jp` must be prevented from running.

#### Test 1.4: `max_active_runs=1` Enforcement

*   **Purpose**: Verify that the DAG's `max_active_runs=1` setting, combined with the `skip_if_running` task, prevents concurrent runs, mimicking the synchronous UC4 behavior.
*   **Setup**:
    1.  Deploy the `dw_dwh_apt_export_monatlich_jp` DAG.
    2.  Ensure no active runs are currently in progress.
*   **Action**:
    1.  Manually trigger the DAG. Let it run for a few minutes (e.g., until the sensor tasks are in `running` or `up_for_retry` state).
    2.  Immediately trigger the DAG *again* for the same logical date or a very close one.
    3.  Observe the second DAG run.
*   **Pass/Fail Criterion**: The `skip_if_running` task in the *second* DAG run must immediately transition to `skipped` (due to `AirflowSkipException`), preventing any further tasks in that run from executing. The first DAG run should continue normally.

#### Test 1.5: `on_failure_alarm` Callback Trigger

*   **Purpose**: Verify that the `on_failure_alarm` callback is correctly invoked when a Dataproc job fails, ensuring proper error handling and alerting.
*   **Setup**:
    1.  Deploy the `dw_dwh_apt_export_monatlich_jp` DAG.
    2.  Modify one of the PySpark scripts (e.g., `dw_dwh_exis_sd_apt_nna_data.py`) to intentionally raise an unhandled exception or exit with a non-zero status code (e.g., `raise ValueError("Simulated failure")` or `sys.exit(1)`).
    3.  Ensure the `on_failure_alarm` function has some observable side effect (e.g., logging to Cloud Logging with a specific tag, sending a test email, or writing to a dummy file).
*   **Action**:
    1.  Trigger a DAG run.
    2.  Allow the `dw_dwh_exis_sd_apt_nna_data` task to execute and fail.
    3.  Check the configured alerting mechanism or logs for evidence of the `on_failure_alarm` callback being invoked.
*   **Pass/Fail Criterion**: The `dw_dwh_exis_sd_apt_nna_data` task must fail, and the `on_failure_alarm` callback must be successfully triggered and its intended side effect observed.

#### Test 1.6: Parameter Passing to PySpark Jobs

*   **Purpose**: Verify that the `month_id` and `output_path` parameters are correctly templated and passed from the Airflow DAG to the PySpark scripts.
*   **Setup**:
    1.  Deploy the `dw_dwh_apt_export_monatlich_jp` DAG.
    2.  Modify the PySpark scripts (`dw_dwh_exis_sd_apt_nna_data.py`, `dw_dwh_exis_sd_apt_nna_voic.py`) to log the received `month_id` and `output_path` arguments at the beginning of their execution.
*   **Action**:
    1.  Trigger a DAG run for a specific logical date (e.g., `2023-03-01`).
    2.  After the Dataproc jobs complete, inspect the Dataproc job logs (available in Cloud Logging).
*   **Pass/Fail Criterion**:
    *   The `month_id` logged by the PySpark scripts must match the `YYYYMM` format derived from the DAG's logical date (e.g., `202303` for `2023-03-01`).
    *   The `output_path` logged must match the expected GCS path (e.g., `gs://YOUR_BUCKET_NAME/exports/apt_nna_data/`).
*   **Test Code (PySpark snippet for logging)**:
    ```python
    # Inside main() of PySpark script
    logger.info(f"Received month_id: {args.month_id}")
    logger.info(f"Received output_path: {args.output_path}")
    ```

### 2. PySpark Job Execution & Output Generation Tests

#### Test 2.1: PySpark Job Submission and Success

*   **Purpose**: Verify that the `DataprocSubmitJobOperator` successfully submits and executes the PySpark jobs on Dataproc.
*   **Setup**:
    1.  Ensure the PySpark scripts (`dw_dwh_exis_sd_apt_nna_data.py`, `dw_dwh_exis_sd_apt_nna_voic.py`) are uploaded to the specified GCS bucket path (e.g., `gs://YOUR_BUCKET_NAME/pyspark/`).
    2.  Ensure the Dataproc cluster specified in the DAG (`DATAPROC_CLUSTER_NAME`, `DATAPROC_REGION`) is running and accessible.
    3.  Trigger a successful DAG run (prerequisites met).
*   **Action**:
    1.  Observe the `dw_dwh_exis_sd_apt_nna_data` and `dw_dwh_exis_sd_apt_nna_voic` tasks in the Airflow UI.
    2.  Check the Dataproc Jobs page in the GCP Console to confirm job submission and completion status.
*   **Pass/Fail Criterion**: Both DataprocSubmitJobOperator tasks must complete successfully in Airflow, and corresponding jobs must appear as `SUCCEEDED` in the Dataproc Jobs console.

#### Test 2.2: GCS Output Path and Naming Convention

*   **Purpose**: Verify that the PySpark jobs write their output to the correct GCS paths and adhere to the specified file naming conventions (`DWHM_APT_NNA_Daten_<yyyymmddhhmmss>.csv.gz` and `DWHM_APT_NNA_Voic_<yyyymmddhhmmss>.csv.gz`).
*   **Setup**:
    1.  Ensure a successful DAG run has completed.
    2.  Note the logical date of the DAG run.
*   **Action**:
    1.  Navigate to the specified GCS bucket (`gs://YOUR_BUCKET_NAME/exports/`).
    2.  Check the subdirectories `apt_nna_data/` and `apt_nna_voic/`.
    3.  List the contents of these directories.
*   **Pass/Fail Criterion**:
    *   For `apt_nna_data/`, a single file matching `DWHM_APT_NNA_Daten_YYYYMMDDHHMMSS.csv.gz` must exist.
    *   For `apt_nna_voic/`, a single file matching `DWHM_APT_NNA_Voic_YYYYMMDDHHMMSS.csv.gz` must exist.
    *   The `YYYYMMDDHHMMSS` timestamp in the filename should roughly correspond to the job completion time.
*   **Test Code (gcloud CLI)**:
    ```bash
    # Replace with your bucket name and expected paths
    GCS_BUCKET="your-bucket-name"
    gcloud storage ls gs://${GCS_BUCKET}/exports/apt_nna_data/
    gcloud storage ls gs://${GCS_BUCKET}/exports/apt_nna_voic/

    # Example of checking for file pattern
    if gcloud storage ls gs://${GCS_BUCKET}/exports/apt_nna_data/DWHM_APT_NNA_Daten_*.csv.gz | grep -q "DWHM_APT_NNA_Daten_"; then
        echo "APT NNA Data file found."
    else
        echo "APT NNA Data file NOT found or incorrect naming."
    fi
    ```

#### Test 2.3: Output File Format (CSV, Header, Compression)

*   **Purpose**: Verify that the output files are compressed CSVs with a header, as specified.
*   **Setup**:
    1.  Ensure a successful DAG run has completed and output files exist in GCS.
    2.  Identify the latest generated files (e.g., `DWHM_APT_NNA_Daten_*.csv.gz`).
*   **Action**:
    1.  Download a sample of each output file from GCS.
    2.  Decompress the `.gz` file.
    3.  Inspect the file content using a text editor or a CSV reader.
*   **Pass/Fail Criterion**:
    *   The file must be a valid gzip compressed archive.
    *   After decompression, the file must be a valid CSV.
    *   The first line of the CSV must contain column headers.
    *   The content should be readable and delimited correctly (e.g., by commas).
*   **Test Code (Python snippet for local verification)**:
    ```python
    import gzip
    import pandas as pd
    import os

    def verify_csv_gz(gcs_path, local_path="temp.csv.gz"):
        # Simulate downloading from GCS
        # gcloud storage cp {gcs_path} {local_path}
        # For testing, ensure 'local_path' points to a downloaded file

        if not os.path.exists(local_path):
            print(f"Error: File not found at {local_path}. Please download it first.")
            return False

        try:
            with gzip.open(local_path, 'rt') as f:
                # Read first few lines to check header and format
                header = f.readline().strip()
                first_data_row = f.readline().strip()

                if not header:
                    print("Fail: CSV file is empty or has no header.")
                    return False
                if ',' not in header:
                    print("Fail: Header does not appear to be comma-separated.")
                    return False

                # Try to read with pandas to confirm CSV validity
                df = pd.read_csv(gzip.open(local_path, 'rt'), nrows=5) # Read a few rows
                print(f"Successfully read {len(df)} rows with pandas.")
                print("Header:", df.columns.tolist())
                print("First 2 rows:\n", df.head(2))
                return True
        except Exception as e:
            print(f"Fail: Error processing file {local_path}: {e}")
            return False

    # Example usage (after downloading a file from GCS)
    # gcloud storage cp gs://your-bucket-name/exports/apt_nna_data/DWHM_APT_NNA_Daten_20230101060000.csv.gz DWHM_APT_NNA_Daten_test.csv.gz
    # print(f"Verification result: {verify_csv_gz('DWHM_APT_NNA_Daten_test.csv.gz')}")
    ```

### 3. Data Transformation & Content Parity Tests (Post-implementation)

**NOTE**: These tests are critically dependent on the full implementation of the PySpark scripts (`dw_dwh_exis_sd_apt_nna_data.py`, `dw_dwh_exis_sd_apt_nna_voic.py`) based on the reverse-engineered logic of `r_exis_v2` and its `.var` configuration files. A baseline of legacy output data for a specific month is required for comparison.

#### Test 3.1: Row Count Parity

*   **Purpose**: Verify that the number of records in the migrated output matches the legacy output for the same input data.
*   **Setup**:
    1.  Obtain a baseline legacy output CSV file (e.g., `DWHM_APT_NNA_Daten_LEGACY.csv.gz`) generated by the UC4 job for a specific month.
    2.  Ensure the PySpark scripts are fully implemented and run successfully for the *same input data and month* as the legacy baseline.
    3.  Identify the corresponding migrated output file in GCS.
*   **Action**:
    1.  Count the rows in the legacy output file (after decompression, excluding header).
    2.  Count the rows in the migrated output file (after decompression, excluding header).
*   **Pass/Fail Criterion**: The row count from the migrated output must exactly match the row count from the legacy output.
*   **Test Code (Python with pandas)**:
    ```python
    import gzip
    import pandas as pd

    def get_row_count(file_path):
        try:
            with gzip.open(file_path, 'rt') as f:
                df = pd.read_csv(f)
                return len(df)
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            return -1

    # Assuming files are downloaded locally
    legacy_file = "DWHM_APT_NNA_Daten_LEGACY.csv.gz"
    migrated_file = "DWHM_APT_NNA_Daten_MIGRATED.csv.gz"

    legacy_count = get_row_count(legacy_file)
    migrated_count = get_row_count(migrated_file)

    print(f"Legacy row count: {legacy_count}")
    print(f"Migrated row count: {migrated_count}")

    if legacy_count == migrated_count:
        print("Pass: Row counts match.")
    else:
        print("Fail: Row counts do NOT match.")
    ```

#### Test 3.2: Schema Parity (Column Names, Order, Data Types)

*   **Purpose**: Verify that the column names, their order, and their inferred data types in the migrated output match the legacy output.
*   **Setup**: Same as Test 3.1.
*   **Action**:
    1.  Extract the header and infer data types from the legacy output file.
    2.  Extract the header and infer data types from the migrated output file.
    3.  Compare column names, their sequence, and their types.
*   **Pass/Fail Criterion**:
    *   The set of column names must be identical.
    *   The order of columns must be identical.
    *   Inferred data types (e.g., string, integer, float, date) should be compatible or identical.
*   **Test Code (Python with pandas)**:
    ```python
    import gzip
    import pandas as pd

    def get_schema_info(file_path):
        try:
            with gzip.open(file_path, 'rt') as f:
                df = pd.read_csv(f, nrows=100) # Read a sample to infer types
                return df.columns.tolist(), df.dtypes.apply(lambda x: str(x)).tolist()
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            return [], []

    legacy_file = "DWHM_APT_NNA_Daten_LEGACY.csv.gz"
    migrated_file = "DWHM_APT_NNA_Daten_MIGRATED.csv.gz"

    legacy_cols, legacy_types = get_schema_info(legacy_file)
    migrated_cols, migrated_types = get_schema_info(migrated_file)

    print(f"Legacy Columns: {legacy_cols}")
    print(f"Migrated Columns: {migrated_cols}")
    print(f"Legacy Types: {legacy_types}")
    print(f"Migrated Types: {migrated_types}")

    if legacy_cols == migrated_cols:
        print("Pass: Column names and order match.")
    else:
        print("Fail: Column names or order do NOT match.")

    # Note: Type comparison can be tricky due to different inference engines (pandas vs. legacy system)
    # Focus on functional equivalence (e.g., 'object' in pandas for string is fine if legacy was VARCHAR)
    if legacy_types == migrated_types: # This might be too strict, consider mapping types
        print("Pass: Inferred data types match.")
    else:
        print("Warning/Fail: Inferred data types do NOT match exactly. Manual review needed.")
    ```

#### Test 3.3: Data Content Parity (Record-by-Record)

*   **Purpose**: Verify that the actual data content in the migrated output is identical to the legacy output, record by record. This is the most stringent test for output parity.
*   **Setup**: Same as Test 3.1.
*   **Action**:
    1.  Load both legacy and migrated CSV files into DataFrames (e.g., pandas, Spark).
    2.  Sort both DataFrames by a common set of unique key columns (if available) or all columns to ensure consistent comparison.
    3.  Perform a row-by-row or DataFrame comparison.
*   **Pass/Fail Criterion**: The DataFrames representing the legacy and migrated output must be identical after sorting and handling any minor differences (e.g., floating-point precision, date format variations if explicitly allowed).
*   **Test Code (Python with pandas)**:
    ```python
    import gzip
    import pandas as pd

    def compare_dataframes(df_legacy, df_migrated, sort_cols=None):
        if sort_cols:
            df_legacy = df_legacy.sort_values(by=sort_cols).reset_index(drop=True)
            df_migrated = df_migrated.sort_values(by=sort_cols).reset_index(drop=True)
        else:
            # If no sort_cols, sort by all columns (can be slow for wide tables)
            df_legacy = df_legacy.sort_values(by=df_legacy.columns.tolist()).reset_index(drop=True)
            df_migrated = df_migrated.sort_values(by=df_migrated.columns.tolist()).reset_index(drop=True)

        # Handle potential type differences if necessary (e.g., convert all to string for comparison)
        df_legacy = df_legacy.astype(str)
        df_migrated = df_migrated.astype(str)

        if df_legacy.equals(df_migrated):
            return True, "DataFrames are identical."
        else:
            # Find differences
            diff = df_legacy.compare(df_migrated)
            return False, f"DataFrames differ. First few differences:\n{diff.head()}"

    legacy_file = "DWHM_APT_NNA_Daten_LEGACY.csv.gz"
    migrated_file = "DWHM_APT_NNA_Daten_MIGRATED.csv.gz"

    df_legacy = pd.read_csv(gzip.open(legacy_file, 'rt'))
    df_migrated = pd.read_csv(gzip.open(migrated_file, 'rt'))

    # Identify unique key columns from the schema analysis (e.g., ['id', 'month_id'])
    # If no natural key, sorting by all columns is an option but less robust for large datasets.
    sort_columns = ['id', 'month_id'] # Placeholder, replace with actual key columns

    result, message = compare_dataframes(df_legacy, df_migrated, sort_columns)
    print(message)
    if result:
        print("Pass: Data content matches record-by-record.")
    else:
        print("Fail: Data content does NOT match.")
    ```

#### Test 3.4: Transformation Logic Correctness (Filters, Joins, Aggregations)

*   **Purpose**: Verify that specific transformation rules (filters, joins, aggregations, derived columns) identified from the `r_exis_v2` and `.var` files are correctly applied in the PySpark scripts.
*   **Setup**:
    1.  Detailed documentation of the transformation logic from the `r_exis_v2` analysis.
    2.  Prepare specific input data scenarios that test each transformation rule.
    3.  Ensure PySpark scripts are fully implemented.
*   **Action**:
    1.  Run the PySpark jobs with the prepared input data.
    2.  Compare the output against expected results derived from the legacy logic. This might involve:
        *   Checking specific filtered rows are present/absent.
        *   Verifying join results (e.g., `LEFT JOIN` behavior with no match).
        *   Confirming aggregation sums, counts, averages.
        *   Validating derived column calculations.
*   **Pass/Fail Criterion**: The output data must precisely reflect the application of each specified transformation rule.
*   **Test Code (Conceptual - requires specific logic from `.var` files)**:
    ```python
    # Example: Test a specific filter condition
    # Legacy logic: WHERE status = 'ACTIVE' AND region = 'EMEA'
    # PySpark logic: df.filter((col("status") == "ACTIVE") & (col("region") == "EMEA"))

    # Setup: Create a small input DataFrame with known statuses and regions
    # Action: Run PySpark script, then filter the output DataFrame
    # Pass/Fail: Assert that only rows matching the filter are present.

    # Example: Test an aggregation
    # Legacy logic: SELECT region, COUNT(*) FROM source GROUP BY region
    # PySpark logic: df.groupBy("region").count()

    # Setup: Input data with multiple regions
    # Action: Run PySpark script, get aggregated output
    # Pass/Fail: Compare aggregated counts per region with expected values.
    ```

#### Test 3.5: NULL Handling Correctness

*   **Purpose**: Verify that NULL values are handled consistently between the legacy and migrated systems (e.g., how they are represented in CSV, how they affect calculations, filters, and joins).
*   **Setup**:
    1.  Identify specific columns and scenarios where NULL values can occur in the source data.
    2.  Prepare input data with various NULL scenarios (e.g., NULL in a key column, NULL in a numeric column used for aggregation, NULL in a string column).
*   **Action**:
    1.  Run the PySpark jobs with the NULL-rich input data.
    2.  Compare the output CSVs with the expected legacy output.
*   **Pass/Fail Criterion**: NULL values must be represented identically in the output CSVs (e.g., empty string, `NULL`, `\N`). Calculations, filters, and joins involving NULLs must produce identical results.

#### Test 3.6: Edge Case Handling (Empty Source, Special Characters)

*   **Purpose**: Verify that the PySpark jobs gracefully handle edge cases such as empty source data, data with special characters, or malformed records.
*   **Setup**:
    1.  **Empty Source**: Configure the source system (or mock it) to return no data.
    2.  **Special Characters**: Prepare input data containing various special characters (e.g., commas within fields, newlines, Unicode characters, delimiters within data).
    3.  **Malformed Records**: If the source allows, introduce records that might violate expected schema (e.g., too many/few columns).
*   **Action**:
    1.  Run the PySpark jobs for each edge case scenario.
    2.  **Empty Source**: Verify that an empty (or header-only) CSV file is generated, or that the job completes successfully without errors.
    3.  **Special Characters**: Verify that the special characters are correctly escaped or handled in the output CSV, and data integrity is maintained.
    4.  **Malformed Records**: Verify error handling (if expected) or data cleansing (if implemented).
*   **Pass/Fail Criterion**: The job must complete successfully for all valid edge cases. Output files must be correctly formatted and contain accurate data, or error handling must be triggered as expected for invalid cases.

### 4. External System Integration Tests

#### Test 4.1: Source System Connectivity (Oracle/BigQuery)

*   **Purpose**: Verify that the PySpark jobs can successfully connect to and read data from the designated source system (presumed Oracle or BigQuery).
*   **Setup**:
    1.  Ensure the PySpark scripts are updated to connect to the actual source database (e.g., Oracle via JDBC, or BigQuery).
    2.  Ensure network connectivity, credentials, and IAM permissions are correctly configured for Dataproc to access the source.
    3.  Ensure the source database contains test data.
*   **Action**:
    1.  Trigger a DAG run.
    2.  Monitor the Dataproc job logs for connection errors or successful data reads.
    3.  Verify that the output files contain data extracted from the source.
*   **Pass/Fail Criterion**: The PySpark jobs must successfully connect to the source database without errors and extract data. The presence of non-dummy data in the output files confirms successful extraction.

#### Test 4.2: GCS Output Accessibility

*   **Purpose**: Verify that the generated CSV files in GCS are accessible by downstream systems or users with appropriate permissions.
*   **Setup**:
    1.  Ensure a successful DAG run has completed, generating output files in GCS.
    2.  Identify a representative downstream system or user account that needs to access these files.
    3.  Configure IAM permissions for this system/user to read from the GCS bucket.
*   **Action**:
    1.  Attempt to access, list, and download the generated files from GCS using the downstream system's credentials or the designated user account.
*   **Pass/Fail Criterion**: The downstream system/user must be able to successfully list, read, and download the output files from the specified GCS path.

---

This comprehensive test plan covers the critical aspects of the migration, from orchestration to data integrity. The successful execution of these tests will provide high confidence in the behavioral equivalence of the migrated `DW.DWH_APT_EXPORT_MONATLICH_JP` job.