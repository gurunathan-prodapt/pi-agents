Here are migration validation tests for the `EXIS_SD_APT_BESTANDS` job, structured as requested.

**Assumptions for Testing:**

1.  **Legacy Environment Access**: We have access to the legacy UC4 job execution logs, output files, and potentially the `r_exis_v2` binary's behavior or documentation.
2.  **Source Data Snapshot**: We can take a consistent snapshot of the Oracle source tables (`SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG`, `RPT$TA_S_D1_VERTRAG`) for both legacy and migrated runs. This snapshot is then loaded into BigQuery for the migrated job.
3.  **`r_exis_v2` Logic Deciphered**: The exact transformation logic (joins, filters, aggregations, column selections, data type conversions, NULL handling) of the original `r_exis_v2` binary has been reverse-engineered or documented and correctly implemented in `pyspark_scripts/r_exis_v2.py`. The placeholder logic in the provided PySpark script *must* be replaced with the actual logic for these tests to be meaningful.
4.  **Configuration File `h_exis_apt_bestandsdaten.var`**: Its content and how `r_exis_v2` uses it are understood and correctly translated into PySpark parameters or logic.
5.  **GCP Resources**: Airflow Composer, Dataproc cluster, GCS buckets, and BigQuery dataset (`raw_oracle_data`) are provisioned and configured as per the design.
6.  **File Naming Convention**: The `yyyymmddhhmmss` part of the output filename `DWHM_APT_BESTANDSREPORT_<yyyymmddhhmmss>.csv.gz` is expected to be based on the job execution timestamp, and `ds_nodash` and `ts_nodash` Airflow macros are used for this.

---

### Test Case 1: End-to-End Output Parity with Representative Data

*   **Purpose**: To verify that the migrated job produces an identical output file (content and format) to the legacy job when given the same input data, covering the most common data scenarios. This is the primary validation for behavioral equivalence.
*   **Setup**:
    1.  Identify a representative set of data in the Oracle source tables (`SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG`, `RPT$TA_S_D1_VERTRAG`) from a specific date (e.g., `YYYY-MM-DD`).
    2.  Extract this data from Oracle and load it into the corresponding BigQuery tables (`raw_oracle_data.SOF_TA_BPR_OPTIONEN`, `raw_oracle_data.SOF_VI_L_OPTIONZUORDNUNG`, `raw_oracle_data.RPT_TA_S_D1_VERTRAG`). Ensure data types and values are preserved.
    3.  Obtain the `h_exis_apt_bestandsdaten.var` configuration file used by the legacy job for this run and upload it to `gs://<GCS_CONFIG_BUCKET>/apt/cfg/h_exis_apt_bestandsdaten.var`.
    4.  Ensure the `r_exis_v2.py` script contains the *actual* re-implemented logic, not the placeholder.
    5.  Configure Airflow variables (`gcp_project_id`, `dataproc_region`, `dataproc_cluster_name`, `gcs_code_bucket`, `gcs_output_bucket`, `gcs_config_bucket`) to point to the correct GCP resources.
*   **Action**:
    1.  Execute the legacy `EXIS_SD_APT_BESTANDS` job with the identified source data. Capture the output file `DWHM_APT_BESTANDSREPORT_<yyyymmddhhmmss>.csv.gz` and its exact content. Decompress it to `legacy_output.csv`.
    2.  Trigger the `dw_dwh_exis_sd_apt_bestands` Airflow DAG manually for the same date/context.
    3.  Once the DAG completes, download the generated gzipped CSV file from GCS (`gs://<GCS_OUTPUT_BUCKET>/DWHM_APT_BESTANDSREPORT_<yyyymmddhhmmss>.csv.gz`) and decompress it to `migrated_output.csv`.
*   **Pass/Fail Criterion**:
    *   The `dw_dwh_exis_sd_apt_bestands` Airflow DAG completes successfully.
    *   The `migrated_output.csv` file is byte-for-byte identical to `legacy_output.csv`.

    ```python
    import pandas as pd
    import gzip
    import os

    def compare_csv_files(legacy_gzipped_path, migrated_gzipped_path):
        """
        Compares two gzipped CSV files for identical content.
        Assumes files are downloaded locally.
        """
        try:
            with gzip.open(legacy_gzipped_path, 'rt') as f_legacy:
                df_legacy = pd.read_csv(f_legacy)

            with gzip.open(migrated_gzipped_path, 'rt') as f_migrated:
                df_migrated = pd.read_csv(f_migrated)

            # Sort both DataFrames by all columns to ensure consistent order for comparison.
            # This handles cases where row order might differ but content is the same.
            # If row order is strictly part of the contract, remove this sorting.
            df_legacy_sorted = df_legacy.sort_values(by=list(df_legacy.columns)).reset_index(drop=True)
            df_migrated_sorted = df_migrated.sort_values(by=list(df_migrated.columns)).reset_index(drop=True)

            if df_legacy_sorted.equals(df_migrated_sorted):
                print(f"PASS: Output parity confirmed. Files '{legacy_gzipped_path}' and '{migrated_gzipped_path}' are identical.")
                return True
            else:
                print(f"FAIL: Output parity failed. Files '{legacy_gzipped_path}' and '{migrated_gzipped_path}' differ.")
                # Optional: print differences for debugging
                # diff = df_legacy_sorted.compare(df_migrated_sorted)
                # print("Differences:\n", diff)
                return False
        except FileNotFoundError as e:
            print(f"FAIL: One or both files not found: {e}")
            return False
        except Exception as e:
            print(f"FAIL: An error occurred during comparison: {e}")
            return False

    # Example usage (assuming files are downloaded to a 'temp' directory)
    # legacy_file = "temp/legacy_DWHM_APT_BESTANDSREPORT_20231027_123456.csv.gz"
    # migrated_file = "temp/migrated_DWHM_APT_BESTANDSREPORT_20231027_123456.csv.gz"
    # compare_csv_files(legacy_file, migrated_file)
    ```

### Test Case 2: Transformation Correctness - Joins, Filters, and Aggregations

*   **Purpose**: To specifically validate that the join conditions, filtering logic, and any aggregations implemented in `r_exis_v2.py` precisely match the legacy `r_exis_v2` binary.
*   **Setup**:
    1.  Create a small, controlled dataset in BigQuery for `SOF_TA_BPR_OPTIONEN`, `SOF_VI_L_OPTIONZUORDNUNG`, `RPT_TA_S_D1_VERTRAG` that includes:
        *   Rows that should join successfully across all tables.
        *   Rows that should *not* join due to missing keys in one or more tables.
        *   Rows that should be filtered out by specific conditions (if any are known from `r_exis_v2` logic).
        *   Data points that would test any known aggregation logic (e.g., sums, counts, averages).
    2.  Obtain the `h_exis_apt_bestandsdaten.var` config and upload it.
    3.  Ensure `r_exis_v2.py` has the *actual* join, filter, and aggregation logic.
*   **Action**:
    1.  Manually execute the core SQL query (or equivalent logic) that `r_exis_v2` performs on the controlled BigQuery dataset. This can be done directly in BigQuery or using a local PySpark script. Save the expected output as `expected_output.csv`.
    2.  Trigger the `dw_dwh_exis_sd_apt_bestands` Airflow DAG.
    3.  Download and decompress the output CSV from GCS to `migrated_output.csv`.
*   **Pass/Fail Criterion**:
    *   The `migrated_output.csv` from the migrated job matches the manually verified `expected_output.csv` for the controlled dataset.
    *   Specifically, verify:
        *   All expected joined rows are present.
        *   No incorrectly joined rows are present.
        *   All expected filtered rows are absent.
        *   No incorrectly filtered rows are absent.
        *   Aggregated values (if any) match the expected calculations.

    ```sql
    -- Example SQL for manual verification (REPLACE with actual logic from r_exis_v2)
    -- This query should be run against the BigQuery tables with the controlled dataset.
    SELECT
        t1.option_id AS OptionIdentifier,
        t2.assignment_type AS AssignmentType,
        SUM(t3.contract_value) AS TotalContractValue,
        COUNT(DISTINCT t3.contract_id) AS DistinctContracts
    FROM
        `your-gcp-project-id.raw_oracle_data.SOF_TA_BPR_OPTIONEN` AS t1
    INNER JOIN
        `your-gcp-project-id.raw_oracle_data.SOF_VI_L_OPTIONZUORDNUNG` AS t2
        ON t1.option_id = t2.option_id -- Actual join condition
    LEFT JOIN -- Example: if some contracts might not have options
        `your-gcp-project-id.raw_oracle_data.RPT_TA_S_D1_VERTRAG` AS t3
        ON t2.contract_ref_id = t3.contract_id -- Actual join condition
    WHERE
        t1.status = 'ACTIVE' -- Example filter
        AND t3.start_date >= '2023-01-01' -- Example date filter
    GROUP BY
        t1.option_id, t2.assignment_type
    HAVING
        SUM(t3.contract_value) > 1000 -- Example aggregation filter
    ORDER BY
        OptionIdentifier, AssignmentType;
    ```
    The result of this query should be compared to the `migrated_output.csv` using the `compare_csv_files` function from Test Case 1.

### Test Case 3: Transformation Correctness - Data Type and NULL Handling

*   **Purpose**: To ensure that data types are correctly handled during the migration (Oracle -> BigQuery -> PySpark -> CSV) and that NULL values are consistently represented in the output CSV.
*   **Setup**:
    1.  Create a small, controlled dataset in BigQuery for the source tables, including:
        *   Columns with various data types (strings, integers, decimals, dates, timestamps, booleans) that are expected in the output.
        *   Rows with NULL values in each of these data types.
        *   Rows with empty strings vs. NULLs (if applicable to Oracle source).
    2.  Ensure `r_exis_v2.py` explicitly handles type conversions if necessary and defines how NULLs should appear in the CSV (e.g., empty string, `NULL`, `\N`).
*   **Action**:
    1.  Execute the legacy `EXIS_SD_APT_BESTANDS` job with this controlled data. Capture the output CSV and note how different data types and NULLs are represented.
    2.  Trigger the `dw_dwh_exis_sd_apt_bestands` Airflow DAG.
    3.  Download and decompress the output CSV from GCS.
*   **Pass/Fail Criterion**:
    *   The data types in the `migrated_output.csv` (when read back into a DataFrame or spreadsheet) match the expected types and values from the legacy output.
    *   NULL values are represented identically in the `migrated_output.csv` as they were in the `legacy_output.csv`.

    ```python
    import pandas as pd
    import gzip

    def check_data_types_and_nulls(legacy_gzipped_path, migrated_gzipped_path):
        """
        Checks for consistency in data types and NULL representation between two gzipped CSVs.
        """
        try:
            with gzip.open(legacy_gzipped_path, 'rt') as f_legacy:
                df_legacy = pd.read_csv(f_legacy, keep_default_na=False) # Keep NA as actual strings if present
            with gzip.open(migrated_gzipped_path, 'rt') as f_migrated:
                df_migrated = pd.read_csv(f_migrated, keep_default_na=False)

            # 1. Check column names and order (prerequisite for further checks)
            if not list(df_legacy.columns) == list(df_migrated.columns):
                print("FAIL: Column names or order differ.")
                print("Legacy columns:", list(df_legacy.columns))
                print("Migrated columns:", list(df_migrated.columns))
                return False

            # 2. Check NULL representation (e.g., empty string vs. 'NULL' vs. actual NaN)
            # This is highly dependent on how legacy job handles NULLs.
            # For a robust check, convert both to a canonical NULL representation (e.g., pd.NA)
            # and then compare.
            # Assuming legacy output uses empty strings for NULLs, and migrated should too.
            df_legacy_normalized = df_legacy.replace('', pd.NA).fillna(pd.NA)
            df_migrated_normalized = df_migrated.replace('', pd.NA).fillna(pd.NA)

            if not df_legacy_normalized.equals(df_migrated_normalized):
                print("FAIL: Data content (including NULL/empty string representation) differs.")
                # Optional: print differences
                # diff = df_legacy_normalized.compare(df_migrated_normalized)
                # print("Differences:\n", diff)
                return False

            # 3. Check inferred data types (Pandas infers, so this is a soft check)
            # More robust: check specific columns for specific types if known.
            type_mismatches = []
            for col in df_legacy.columns:
                if not df_legacy[col].dtype == df_migrated[col].dtype:
                    type_mismatches.append(f"Column '{col}': Legacy={df_legacy[col].dtype}, Migrated={df_migrated[col].dtype}")

            if type_mismatches:
                print("WARNING: Inferred data type mismatches found (may be acceptable if values are equivalent):")
                for msg in type_mismatches:
                    print(f"- {msg}")
            else:
                print("PASS: Inferred data types appear consistent.")

            print("PASS: Data types and NULL handling appear consistent.")
            return True

        except FileNotFoundError as e:
            print(f"FAIL: One or both files not found: {e}")
            return False
        except Exception as e:
            print(f"FAIL: An error occurred during comparison: {e}")
            return False

    # Example usage
    # check_data_types_and_nulls("temp/legacy_output.csv.gz", "temp/migrated_output.csv.gz")
    ```

### Test Case 4: External System Replacement - BigQuery Read and GCS Write

*   **Purpose**: To confirm that the PySpark job correctly reads from BigQuery and writes the gzipped CSV to the specified GCS location with the correct naming convention.
*   **Setup**:
    1.  Ensure the BigQuery source tables (`raw_oracle_data.SOF_TA_BPR_OPTIONEN`, etc.) contain some data.
    2.  Ensure the `gcs_output_bucket` and `gcs_code_bucket` Airflow variables are correctly set.
    3.  Ensure the `r_exis_v2.py` script is uploaded to `gs://<GCS_CODE_BUCKET>/pyspark_scripts/r_exis_v2.py`.
*   **Action**:
    1.  Trigger the `dw_dwh_exis_sd_apt_bestands` Airflow DAG.
    2.  Monitor the Dataproc job logs for BigQuery read operations.
    3.  After completion, check the specified GCS output bucket.
*   **Pass/Fail Criterion**:
    *   The `DataprocSubmitJobOperator` task (`run_dwh_exis_sd_apt_bestands`) completes successfully in Airflow.
    *   A gzipped CSV file is created in `gs://<GCS_OUTPUT_BUCKET>/` with a name matching the pattern `DWHM_APT_BESTANDSREPORT_<yyyymmdd>_<hhmmss>.csv.gz` (where `yyyymmdd` is `ds_nodash` and `hhmmss` is part of `ts_nodash`).
    *   The file is indeed gzipped (can be verified by attempting to decompress it).
    *   The file is not empty (contains header and at least one data row if source data exists).

    ```python
    from google.cloud import storage
    import re
    import datetime
    import gzip

    def verify_gcs_output_file(gcs_output_bucket, expected_prefix="DWHM_APT_BESTANDSREPORT"):
        """
        Verifies the existence, naming, and gzip integrity of the output file in GCS.
        """
        client = storage.Client()
        bucket = client.get_bucket(gcs_output_bucket)

        # The DAG uses {{ ds_nodash }} and {{ ts_nodash }}
        # ds_nodash is YYYYMMDD
        # ts_nodash is YYYYMMDDTHHMMSS (but the example output filename suggests YYYYMMDD_HHMMSS)
        # Let's assume the ts_nodash part in the filename is just HHMMSS or YYYYMMDDHHMMSS
        # The DAG's output path: f"gs://{GCS_OUTPUT_BUCKET}/DWHM_APT_BESTANDSREPORT_{{{{ ds_nodash }}}}_{{{{ ts_nodash }}}}.csv.gz"
        # This means the filename will be like DWHM_APT_BESTANDSREPORT_20231027_20231027T123456.csv.gz
        # Let's refine the pattern based on the actual DAG code's macro usage.
        # The example in the design doc is DWHM_APT_BESTANDSREPORT_<yyyymmddhhmmss>.csv.gz
        # The DAG code uses {{ ds_nodash }}_{{ ts_nodash }}
        # So it will be DWHM_APT_BESTANDSREPORT_YYYYMMDD_YYYYMMDDTHHMMSS.csv.gz
        # Let's adjust the pattern to match this.

        # For testing, we might need to know the exact execution date/time used by Airflow.
        # For a general check, we can look for the pattern.
        # A more precise test would pass the expected ds_nodash and ts_nodash.
        # For now, let's assume we're looking for a file from "today" or a recent run.
        today_ds_nodash = datetime.datetime.utcnow().strftime("%Y%m%d")
        filename_regex = re.compile(rf"^{expected_prefix}_{today_ds_nodash}_\d{{8}}T\d{{6}}\.csv\.gz$")

        found_blob = None
        for blob in bucket.list_blobs(prefix=f"{expected_prefix}_{today_ds_nodash}"):
            if filename_regex.match(blob.name):
                found_blob = blob
                break

        if not found_blob:
            print(f"FAIL: No output file found matching pattern '{filename_regex.pattern}' in bucket '{gcs_output_bucket}'.")
            return False

        print(f"PASS: Found matching output file: {found_blob.name}")

        # Verify file is not empty
        if found_blob.size == 0:
            print(f"FAIL: Output file '{found_blob.name}' is empty.")
            return False
        print(f"PASS: Output file '{found_blob.name}' is not empty (size: {found_blob.size} bytes).")

        # Verify file is a valid gzip by attempting to decompress a small part
        try:
            # Download first few bytes to check gzip magic number
            first_bytes = found_blob.download_as_bytes(start_byte=0, end_byte=10)
            if first_bytes[:2] == b'\x1f\x8b': # Gzip magic number
                print(f"PASS: File '{found_blob.name}' appears to be a valid gzip file (magic number check).")
            else:
                print(f"FAIL: File '{found_blob.name}' does not have gzip magic number.")
                return False
        except Exception as e:
            print(f"FAIL: Could not verify gzip integrity for '{found_blob.name}': {e}")
            return False

        print("PASS: GCS output file created correctly, is not empty, and is gzipped.")
        return True

    # Example usage (replace with your actual GCS output bucket)
    # verify_gcs_output_file(gcs_output_bucket="your-gcs-output-bucket")
    ```

### Test Case 5: Data Quality - Row Count and Schema Assertion

*   **Purpose**: To ensure that the migrated job produces the correct number of rows and that the output CSV schema (column names and order) matches the legacy output.
*   **Setup**:
    1.  Use the same representative dataset as in Test Case 1.
    2.  Ensure `r_exis_v2.py` has the *actual* re-implemented logic.
*   **Action**:
    1.  Execute the legacy `EXIS_SD_APT_BESTANDS` job. Record the row count of the output CSV and its header row (column names and order).
    2.  Trigger the `dw_dwh_exis_sd_apt_bestands` Airflow DAG.
    3.  Download and decompress the output CSV from GCS.
*   **Pass/Fail Criterion**:
    *   The row count of the `migrated_output.csv` (excluding header) is identical to the row count of the `legacy_output.csv`.
    *   The header row (column names and their order) in `migrated_output.csv` is identical to `legacy_output.csv`.

    ```python
    import pandas as pd
    import gzip

    def check_row_count_and_schema(legacy_gzipped_path, migrated_gzipped_path):
        """
        Checks row count and schema (column names and order) consistency.
        """
        try:
            with gzip.open(legacy_gzipped_path, 'rt') as f_legacy:
                df_legacy = pd.read_csv(f_legacy)
            with gzip.open(migrated_gzipped_path, 'rt') as f_migrated:
                df_migrated = pd.read_csv(f_migrated)

            # Check row count
            if len(df_legacy) != len(df_migrated):
                print(f"FAIL: Row count mismatch. Legacy: {len(df_legacy)}, Migrated: {len(df_migrated)}")
                return False
            else:
                print(f"PASS: Row count matches: {len(df_legacy)} rows.")

            # Check schema (column names and order)
            if not list(df_legacy.columns) == list(df_migrated.columns):
                print("FAIL: Column names or order mismatch.")
                print("Legacy columns:", list(df_legacy.columns))
                print("Migrated columns:", list(df_migrated.columns))
                return False
            else:
                print("PASS: Column names and order match.")

            return True

        except FileNotFoundError as e:
            print(f"FAIL: One or both files not found: {e}")
            return False
        except Exception as e:
            print(f"FAIL: An error occurred during comparison: {e}")
            return False

    # Example usage
    # check_row_count_and_schema("temp/legacy_output.csv.gz", "temp/migrated_output.csv.gz")
    ```

### Test Case 6: Edge Case - Empty Source Tables

*   **Purpose**: To verify the job handles scenarios where one or all source tables are empty, producing an empty (or header-only) output file without errors.
*   **Setup**:
    1.  Ensure the BigQuery source tables (`raw_oracle_data.SOF_TA_BPR_OPTIONEN`, `raw_oracle_data.SOF_VI_L_OPTIONZUORDNUNG`, `raw_oracle_data.RPT_TA_S_D1_VERTRAG`) are completely empty.
    2.  Ensure `r_exis_v2.py` has the *actual* re-implemented logic.
*   **Action**:
    1.  Execute the legacy `EXIS_SD_APT_BESTANDS` job with empty source tables. Capture the output file. It should likely be an empty file or a file with only a header.
    2.  Trigger the `dw_dwh_exis_sd_apt_bestands` Airflow DAG.
    3.  Download and decompress the output CSV from GCS.
*   **Pass/Fail Criterion**:
    *   The `dw_dwh_exis_sd_apt_bestands` Airflow DAG completes successfully.
    *   The `migrated_output.csv` file is either empty or contains only the header row, matching the behavior of the legacy job.

    ```python
    import pandas as pd
    import gzip
    import os

    def check_empty_source_output(migrated_gzipped_path, legacy_gzipped_path=None):
        """
        Checks if the migrated output file is empty or header-only, matching legacy behavior.
        """
        try:
            # Determine expected behavior from legacy output if available
            legacy_is_header_only = False
            if legacy_gzipped_path and os.path.exists(legacy_gzipped_path):
                with gzip.open(legacy_gzipped_path, 'rt') as f_legacy:
                    legacy_content = f_legacy.read().strip()
                    if '\n' not in legacy_content and legacy_content: # Only one line (header)
                        legacy_is_header_only = True
                    elif not legacy_content: # Truly empty file
                        legacy_is_header_only = False # Treat as truly empty

            # Check migrated output
            with gzip.open(migrated_gzipped_path, 'rt') as f_migrated:
                migrated_content = f_migrated.read().strip()

            if not migrated_content: # Truly empty file
                if not legacy_is_header_only:
                    print("PASS: Migrated output is an empty file, matching legacy behavior.")
                    return True
                else:
                    print("FAIL: Migrated output is empty, but legacy output had a header.")
                    return False
            elif '\n' not in migrated_content: # Only one line (header)
                if legacy_is_header_only:
                    print("PASS: Migrated output is header-only, matching legacy behavior.")
                    return True
                else:
                    print("FAIL: Migrated output is header-only, but legacy output was not (or was truly empty).")
                    return False
            else:
                print(f"FAIL: Migrated output contains data rows ({migrated_content.count('\n')} data rows), expected empty or header-only.")
                return False

        except FileNotFoundError as e:
            print(f"FAIL: Migrated output file not found: {e}")
            return False
        except Exception as e:
            print(f"FAIL: An error occurred during check: {e}")
            return False

    # Example usage
    # check_empty_source_output("temp/migrated_empty_output.csv.gz", "temp/legacy_empty_output.csv.gz")
    ```

### Test Case 7: Configuration File Parameter Handling

*   **Purpose**: To verify that parameters defined in `h_exis_apt_bestandsdaten.var` are correctly read and applied by the `r_exis_v2.py` script.
*   **Setup**:
    1.  Create a specific `h_exis_apt_bestandsdaten.var` file with known parameters that influence the output (e.g., a filter condition, a default value, a specific column selection, a date range).
    2.  Upload this config file to `gs://<GCS_CONFIG_BUCKET>/apt/cfg/h_exis_apt_bestandsdaten.var`.
    3.  Ensure `r_exis_v2.py` has the *actual* logic to read and use these parameters.
    4.  Prepare BigQuery source data that will clearly show the effect of these parameters.
*   **Action**:
    1.  Manually apply the parameters from the config file to the source data and determine the *expected* output.
    2.  Trigger the `dw_dwh_exis_sd_apt_bestands` Airflow DAG.
    3.  Download and decompress the output CSV from GCS.
*   **Pass/Fail Criterion**:
    *   The `migrated_output.csv` matches the *expected* output derived from applying the configuration parameters.
    *   For example, if a config parameter `REPORT_TYPE=FULL` means no filtering, and `REPORT_TYPE=ACTIVE` means only active records, verify the row count and content based on the specific parameter value. This can be validated using the `compare_csv_files` function from Test Case 1.

### Test Case 8: Airflow DAG Orchestration and Logging

*   **Purpose**: To verify that the Airflow DAG executes correctly, tasks succeed, and logs are generated as expected.
*   **Setup**:
    1.  Deploy the `dw_dwh_exis_sd_apt_bestands.py` DAG to Cloud Composer.
    2.  Ensure all Airflow variables (`gcp_project_id`, `dataproc_region`, `dataproc_cluster_name`, `gcs_code_bucket`, `gcs_output_bucket`, `gcs_config_bucket`) are correctly set.
    3.  Ensure the PySpark script (`r_exis_v2.py`) is uploaded to `gs://<GCS_CODE_BUCKET>/pyspark_scripts/r_exis_v2.py`.
    4.  Ensure BigQuery source tables contain some data for a successful run.
*   **Action**:
    1.  Trigger the `dw_dwh_exis_sd_apt_bestands` Airflow DAG manually from the Airflow UI.
    2.  Monitor the Airflow UI for task status.
    3.  Review the logs for the `run_dwh_exis_sd_apt_bestands` task.
*   **Pass/Fail Criterion**:
    *   The `start` task completes successfully.
    *   The `run_dwh_exis_sd_apt_bestands` task completes successfully (green in Airflow UI).
    *   The `end` task completes successfully.
    *   The task logs for `run_dwh_exis_sd_apt_bestands` contain expected messages from the PySpark script (e.g., "Starting PySpark job...", "Reading from BigQuery...", "Writing transformed data to GCS...", "Successfully exported data to GCS.").
    *   No unexpected errors or warnings are present in the logs.
    *   The `DataprocSubmitJobOperator` correctly passes arguments to the PySpark script (can be inferred from logs or by inspecting the Dataproc job details in GCP console).