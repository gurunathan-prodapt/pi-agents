Here are migration validation tests for the `EXIS_SD_APT_NNA_VOIC` job, structured as requested.

---

## Migration Validation Tests for EXIS_SD_APT_NNA_VOIC

**General Pre-requisites for all tests:**

*   **Golden Dataset**: A small, controlled, and representative dataset has been loaded into the BigQuery tables (`DWH_VI_L_MAP_FA_TARIF`, `BL_D_TARIF`, `DWH_VI_C_VERTRAG`, `DWH_VI_F_NNV_TVD_12_MONATE`, `DWH_VI_L_TVD_LEISTUNGSKLASSE`). This dataset should cover:
    *   Rows that satisfy all `WHERE` conditions.
    *   Rows that are filtered out by each `WHERE` condition (e.g., `RAHMENVERTRAG IS NULL`, `MONATS_ID` mismatch, `GUELTIG_BIS` mismatch, `LEISTUNGSKLASSE_ID` edge cases).
    *   Rows with `NULL` values in columns involved in calculations (`DAUER_SEK`, `RBETRAG_VBUD_NETTO_CENT`).
    *   Edge cases for `LEISTUNGSKLASSE_ID` (e.g., `1`, `299`, `300`, `399`, `400`, `622000`, `699999`, `700000`).
    *   Values that result in `SAFE_DIVIDE` returning `NULL` or `0`.
*   **Legacy Golden Output**: The legacy `EXIS_SD_APT_NNA_VOIC` job has been executed with the *exact same golden dataset* in its Oracle DWH source. The resulting gzipped CSV file (including its header and trailer) is stored as the "golden standard" for comparison.
*   **GCP Environment**: A GCP project with BigQuery, Cloud Storage, Dataproc, Cloud Composer (Airflow), and Cloud Run configured.
*   **Deployed Code**: The `d_exis_apt_nna_voice.bqsql`, `r_exis_v2.py`, `dw_dwh_exis_sd_apt_nna_voic.py`, and `cloud_run_sftp_service.py` files are deployed to their respective GCP services (GCS for scripts, Airflow for DAG, Cloud Run for service).
*   **Airflow Variables**: All required Airflow variables (`gcp_project_id`, `gcp_region`, `dataproc_cluster_name`, `dataproc_code_bucket`, `dags_code_bucket`, `gcs_temp_bucket_name`, `gcs_output_bucket_name`, `gcs_output_prefix`, `bq_dataset_name`) are configured correctly.
*   **Cloud Run SFTP Service**: The Cloud Run service for SFTP (`sftp-transfer-service`) is deployed, running, and configured with valid SFTP credentials (preferably via Secret Manager) and the correct external SFTP host/port/path.
*   **External SFTP Server**: An external SFTP server is available for testing, with appropriate credentials and permissions for the Cloud Run service to write to.

---

### Test Case 1: End-to-End Output Parity (Golden File Comparison)

*   **Purpose**: To verify that the entire migrated workflow produces an output file that is byte-for-byte identical (or functionally identical after decompression) to the legacy system's output for the same input data. This is the ultimate test of behavioral equivalence.
*   **Setup**:
    1.  Ensure the BigQuery source tables are populated with the "golden dataset".
    2.  Ensure the legacy job has been run with the *exact same* golden dataset and its output (`DWHM_APT_NNA_Daten_<TIMESTAMP>.csv.gz`) is available as `legacy_golden_output.csv.gz`.
    3.  Configure the Airflow DAG to process the `MONAT_ID` corresponding to the golden dataset.
    4.  Ensure the external SFTP server is accessible and configured to receive files from the Cloud Run service.
*   **Action**:
    1.  Trigger the `dw_dwh_exis_sd_apt_nna_voic` Airflow DAG.
    2.  Monitor the DAG execution until it completes successfully.
    3.  Retrieve the final gzipped CSV file from the external SFTP server (or from the GCS output bucket if SFTP transfer is temporarily disabled for comparison). Let's call this `migrated_output.csv.gz`.
*   **Pass/Fail Criterion**:
    1.  The Airflow DAG completes successfully without errors.
    2.  The `migrated_output.csv.gz` file exists on the SFTP server (or GCS).
    3.  Decompress both `legacy_golden_output.csv.gz` and `migrated_output.csv.gz`.
    4.  Compare the decompressed CSV files. They must be identical, ignoring the `PROCESS_DATE` timestamp in the header/trailer if it's the only difference. If the `PROCESS_DATE` is the only difference, replace it with a placeholder in both files before comparison.
    5.  The record count in the trailer of `migrated_output.csv` matches the record count in `legacy_golden_output.csv`.

    ```python
    import gzip
    import filecmp
    import re

    def compare_gzipped_csvs(legacy_path, migrated_path):
        def normalize_csv_content(file_path):
            with gzip.open(file_path, 'rt') as f:
                content = f.read()
            # Normalize PROCESS_DATE in header/trailer for comparison
            content = re.sub(r'#H;(\d{6});\d{14}', r'#H;\1;PROCESS_DATE_PLACEHOLDER', content)
            content = re.sub(r'#T;(\d+);(\d{6});\d{14}', r'#T;\1;\2;PROCESS_DATE_PLACEHOLDER', content)
            return content

        legacy_content = normalize_csv_content(legacy_path)
        migrated_content = normalize_csv_content(migrated_path)

        if legacy_content == migrated_content:
            print(f"PASS: Decompressed CSV contents are identical (after normalizing timestamps).")
            return True
        else:
            print(f"FAIL: Decompressed CSV contents differ.")
            # Optional: write diff to a file for analysis
            with open("legacy_normalized.csv", "w") as f: f.write(legacy_content)
            with open("migrated_normalized.csv", "w") as f: f.write(migrated_content)
            print("Differences written to legacy_normalized.csv and migrated_normalized.csv")
            return False

    # Example usage (replace with actual paths)
    # assert compare_gzipped_csvs("path/to/legacy_golden_output.csv.gz", "path/to/migrated_output.csv.gz")
    ```

---

### Test Case 2: BigQuery SQL Transformation Correctness

*   **Purpose**: To verify that the BigQuery SQL query (`d_exis_apt_nna_voice.bqsql`) correctly translates the Oracle SQL logic, including joins, filters, column selections, and data type conversions/formatting.
*   **Setup**:
    1.  Ensure the BigQuery source tables are populated with the "golden dataset".
    2.  Identify the `MONAT_ID` (e.g., `202301`) used in the golden dataset.
    3.  Extract the expected output from the legacy Oracle SQL query (before `nawk`/`gzip`) for the golden dataset. This can be done by running the Oracle SQL directly and exporting to CSV. Let's call this `legacy_sql_output.csv`.
*   **Action**:
    1.  Execute the `d_exis_apt_nna_voice.bqsql` query directly in BigQuery, passing the `MONAT_ID` parameter.
    2.  Export the BigQuery query results to a CSV file (without header) to Cloud Storage, then download it locally. Let's call this `migrated_bq_sql_output.csv`.
*   **Pass/Fail Criterion**:
    1.  The BigQuery query executes successfully.
    2.  Compare `legacy_sql_output.csv` and `migrated_bq_sql_output.csv`. They must be identical in terms of content, column order, and data types (allowing for minor floating-point precision differences if applicable, which should be handled by `ROUND` in the SQL).
    3.  Verify specific transformations:
        *   `TARIF` column: `CONCAT` logic is correct.
        *   `DAUER_MIN` and `RBETRAG_VBUD_NETTO_EURO`: `ROUND` and `SAFE_DIVIDE` logic is correct, including `NULL` handling for division by zero.
        *   `LEISTUNGSKLASSE_ID` filtering: Rows matching the complex `WHERE` clause are included, others are excluded.
        *   `RAHMENVERTRAG IS NOT NULL`, `MONATS_ID`, `GUELTIG_BIS` filters are correctly applied.

    ```python
    import pandas as pd

    def compare_csv_dataframes(legacy_df, migrated_df):
        # Sort by all columns to ensure consistent order for comparison
        legacy_df_sorted = legacy_df.sort_values(by=list(legacy_df.columns)).reset_index(drop=True)
        migrated_df_sorted = migrated_df.sort_values(by=list(migrated_df.columns)).reset_index(drop=True)

        # Compare shapes
        if legacy_df_sorted.shape != migrated_df_sorted.shape:
            print(f"FAIL: DataFrames have different shapes. Legacy: {legacy_df_sorted.shape}, Migrated: {migrated_df_sorted.shape}")
            return False

        # Compare data types
        if not legacy_df_sorted.dtypes.equals(migrated_df_sorted.dtypes):
            print(f"WARNING: DataFrames have different dtypes. This might indicate type handling issues.")
            print("Legacy dtypes:\n", legacy_df_sorted.dtypes)
            print("Migrated dtypes:\n", migrated_df_sorted.dtypes)

        # Compare content, allowing for minor float differences
        comparison_result = legacy_df_sorted.compare(migrated_df_sorted, align_axis=1)
        if comparison_result.empty:
            print(f"PASS: BigQuery SQL output is identical to legacy SQL output.")
            return True
        else:
            print(f"FAIL: BigQuery SQL output differs from legacy SQL output.")
            print("Differences:\n", comparison_result)
            return False

    # Example usage (assuming CSVs are downloaded)
    # legacy_df = pd.read_csv("path/to/legacy_sql_output.csv", header=None, names=[...list of column names...])
    # migrated_df = pd.read_csv("path/to/migrated_bq_sql_output.csv", header=None, names=[...list of column names...])
    # assert compare_csv_dataframes(legacy_df, migrated_df)
    ```

---

### Test Case 3: `r_exis_v2.py` Post-Processing and Compression

*   **Purpose**: To verify that the `r_exis_v2.py` script correctly implements the `nawk`-like header/trailer addition, accurate record counting, and `gzip` compression, as well as the file naming convention.
*   **Setup**:
    1.  Obtain a raw CSV output from the BigQuery SQL query (e.g., `migrated_bq_sql_output.csv` from Test Case 2). This will serve as the input to `r_exis_v2.py`.
    2.  Identify the `MONAT_ID` (e.g., `202301`) and a specific `PROCESS_DATE` (e.g., `20230115103000`) to be used for testing.
    3.  Ensure the GCS output bucket and prefix are configured.
*   **Action**:
    1.  Manually execute the `r_exis_v2.py` script locally or via a Dataproc job, providing the `migrated_bq_sql_output.csv` as input, the chosen `MONAT_ID`, and a mocked `PROCESS_DATE` (or capture the actual one if running through Dataproc).
    2.  Retrieve the resulting gzipped CSV file from GCS. Let's call this `post_processed_output.csv.gz`.
*   **Pass/Fail Criterion**:
    1.  The script executes successfully.
    2.  The file `post_processed_output.csv.gz` is created in the specified GCS location.
    3.  **File Naming**: The filename matches `DWHM_APT_NNA_Daten_<SYSDATE YYYYMMDDHH24MISS>.csv.gz`.
    4.  **Compression**: The file is a valid gzip archive.
    5.  **Header**: Decompress the file and verify the first line is `#H;<MONAT_ID>;<PROCESS_DATE>`.
    6.  **Trailer**: Verify the last line is `#T;<RECORD_COUNT>;<MONAT_ID>;<PROCESS_DATE>`.
    7.  **Record Count**: The `<RECORD_COUNT>` in the trailer accurately reflects the number of data rows between the header and trailer (excluding header/trailer themselves).
    8.  **Content**: The data rows between the header and trailer are identical to the `migrated_bq_sql_output.csv` input.

    ```python
    import gzip
    import os
    import pandas as pd

    def validate_post_processed_file(gzipped_file_path, expected_monat_id, expected_input_csv_path):
        with gzip.open(gzipped_file_path, 'rt') as f:
            lines = f.readlines()

        if not lines:
            print("FAIL: Gzipped file is empty.")
            return False

        header = lines[0].strip()
        trailer = lines[-1].strip()
        data_lines = lines[1:-1]

        # 1. Validate Header
        if not header.startswith(f"#H;{expected_monat_id};"):
            print(f"FAIL: Header format incorrect. Expected start: #H;{expected_monat_id};, Got: {header}")
            return False
        
        # Extract process_date from header for trailer validation
        header_parts = header.split(';')
        if len(header_parts) != 3:
            print(f"FAIL: Header has incorrect number of parts: {header}")
            return False
        process_date_from_header = header_parts[2]

        # 2. Validate Trailer
        trailer_parts = trailer.split(';')
        if len(trailer_parts) != 4:
            print(f"FAIL: Trailer has incorrect number of parts: {trailer}")
            return False
        
        if not trailer.startswith(f"#T;"):
            print(f"FAIL: Trailer format incorrect. Expected start: #T;, Got: {trailer}")
            return False
        
        record_count_str = trailer_parts[1]
        trailer_monat_id = trailer_parts[2]
        trailer_process_date = trailer_parts[3]

        if trailer_monat_id != expected_monat_id:
            print(f"FAIL: MONAT_ID in trailer mismatch. Expected: {expected_monat_id}, Got: {trailer_monat_id}")
            return False
        if trailer_process_date != process_date_from_header:
            print(f"FAIL: PROCESS_DATE in trailer mismatch with header. Header: {process_date_from_header}, Trailer: {trailer_process_date}")
            return False

        # 3. Validate Record Count
        try:
            actual_record_count = int(record_count_str)
            if actual_record_count != len(data_lines):
                print(f"FAIL: Record count mismatch. Expected: {len(data_lines)}, Got: {actual_record_count}")
                return False
        except ValueError:
            print(f"FAIL: Record count in trailer is not a valid integer: {record_count_str}")
            return False

        # 4. Validate Data Content
        expected_df = pd.read_csv(expected_input_csv_path, header=None)
        actual_df = pd.read_csv(pd.io.common.StringIO(''.join(data_lines)), header=None)

        if not expected_df.equals(actual_df):
            print("FAIL: Data content between header/trailer does not match input CSV.")
            print("Differences:\n", expected_df.compare(actual_df))
            return False

        print("PASS: Post-processing and compression validation successful.")
        return True

    # Example usage
    # assert validate_post_processed_file(
    #     "path/to/post_processed_output.csv.gz",
    #     "202301",
    #     "path/to/migrated_bq_sql_output.csv"
    # )
    ```

---

### Test Case 4: `MONAT_ID` Derivation in Airflow

*   **Purpose**: To ensure the Airflow DAG correctly calculates the `MONAT_ID` parameter, which is crucial for filtering data in BigQuery.
*   **Setup**:
    1.  The `dw_dwh_exis_sd_apt_nna_voic` Airflow DAG is deployed.
    2.  The `calculate_monat_id` and `get_monat_id_var` tasks are configured.
*   **Action**:
    1.  Manually trigger the `dw_dwh_exis_sd_apt_nna_voic` Airflow DAG.
    2.  Inspect the logs of the `get_monat_id_var` task.
*   **Pass/Fail Criterion**:
    1.  The `get_monat_id_var` task completes successfully.
    2.  The log output for `get_monat_id_var` shows `Fetched MONAT_ID: YYYYMM`, where `YYYYMM` is the expected previous month's ID (e.g., if today is March 15, 2023, it should be `202302`).
    3.  The `MONAT_ID` passed to the `submit_dataproc_job` (visible in its logs or XComs) matches the expected value.

    ```python
    # Airflow task log assertion (conceptual)
    # In a test framework like `pytest-airflow`, you might mock XComs or inspect task logs.
    # For manual verification, check the Airflow UI logs.

    # Expected MONAT_ID calculation in Python:
    from datetime import datetime, timedelta
    import pendulum

    def get_expected_monat_id():
        # Assuming "last month" logic as per DAG
        today = pendulum.today('UTC')
        last_month = today.subtract(months=1)
        return last_month.strftime("%Y%m")

    # In your test:
    # expected_monat_id = get_expected_monat_id()
    # assert "Fetched MONAT_ID: " + expected_monat_id in task_logs_for_get_monat_id_var
    # assert expected_monat_id in xcom_value_for_monat_id_param_to_dataproc
    ```

---

### Test Case 5: External System Replacement (SFTP Distribution)

*   **Purpose**: To verify that the Cloud Run service correctly transfers the final gzipped CSV file from GCS to the external SFTP server.
*   **Setup**:
    1.  Ensure the `dw_dwh_exis_sd_apt_nna_voic` Airflow DAG has run successfully, placing a gzipped CSV file in the GCS output bucket.
    2.  The `sftp-transfer-service` Cloud Run job is deployed and configured with correct SFTP credentials and target path.
    3.  The external SFTP server is running and accessible.
*   **Action**:
    1.  Trigger the `distribute_via_sftp` task in the Airflow DAG (or manually invoke the Cloud Run service with the GCS path of the generated file).
    2.  Monitor the Cloud Run job logs for success/failure.
    3.  Log in to the external SFTP server.
*   **Pass/Fail Criterion**:
    1.  The `distribute_via_sftp` task (or manual Cloud Run invocation) completes successfully.
    2.  The Cloud Run service logs indicate a successful SFTP upload.
    3.  The gzipped CSV file (with the correct filename) is present in the specified remote directory on the external SFTP server.
    4.  The file size on the SFTP server matches the file size in GCS.
    5.  (Optional but recommended): Download the file from SFTP and verify its integrity (e.g., decompress and check content).

    ```python
    # Conceptual test for SFTP verification
    import paramiko
    import os
    from google.cloud import storage

    def verify_sftp_transfer(gcs_path, sftp_host, sftp_port, sftp_username, sftp_password, sftp_remote_path):
        # 1. Get file size from GCS
        client = storage.Client()
        bucket_name = gcs_path.split("gs://")[1].split("/")[0]
        blob_name = "/".join(gcs_path.split("gs://")[1].split("/")[1:])
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        gcs_file_size = blob.size
        gcs_filename = os.path.basename(blob_name)

        # 2. Connect to SFTP and verify file presence and size
        transport = paramiko.Transport((sftp_host, sftp_port))
        transport.connect(username=sftp_username, password=sftp_password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        remote_full_path = os.path.join(sftp_remote_path, gcs_filename)
        try:
            sftp_stat = sftp.stat(remote_full_path)
            sftp_file_size = sftp_stat.st_size

            if sftp_file_size == gcs_file_size:
                print(f"PASS: File '{gcs_filename}' found on SFTP with matching size ({gcs_file_size} bytes).")
                return True
            else:
                print(f"FAIL: File '{gcs_filename}' found on SFTP, but size mismatch. GCS: {gcs_file_size}, SFTP: {sftp_file_size}.")
                return False
        except FileNotFoundError:
            print(f"FAIL: File '{gcs_filename}' not found on SFTP at '{sftp_remote_path}'.")
            return False
        finally:
            sftp.close()
            transport.close()

    # Example usage (replace with actual values)
    # gcs_file_path = "gs://your-output-bucket/exis_data/nna_voice/DWHM_APT_NNA_Daten_20230315103000.csv.gz"
    # sftp_host = os.environ.get("SFTP_HOST")
    # sftp_port = int(os.environ.get("SFTP_PORT", 22))
    # sftp_username = os.environ.get("SFTP_USERNAME")
    # sftp_password = os.environ.get("SFTP_PASSWORD")
    # sftp_remote_path = os.environ.get("SFTP_REMOTE_PATH", "/remote/incoming/")
    # assert verify_sftp_transfer(gcs_file_path, sftp_host, sftp_port, sftp_username, sftp_password, sftp_remote_path)
    ```

---

### Test Case 6: Data Quality - NULL Handling

*   **Purpose**: To verify that `NULL` values in source columns are handled correctly by the BigQuery SQL and subsequent processing, especially for numeric conversions and `SAFE_DIVIDE`.
*   **Setup**:
    1.  Populate the BigQuery source tables with the "golden dataset" that includes:
        *   Rows where `NNA.DAUER_SEK` is `NULL`.
        *   Rows where `NNA.RBETRAG_VBUD_NETTO_CENT` is `NULL`.
        *   Rows where `NNA.RAHMENVERTRAG` is `NULL` (these should be filtered out).
    2.  Ensure the legacy job's output for this specific dataset is available.
*   **Action**:
    1.  Execute the `dw_dwh_exis_sd_apt_nna_voic` Airflow DAG.
    2.  Retrieve the final gzipped CSV from GCS or SFTP.
    3.  Decompress the file and inspect the `DAUER_MIN` and `RBETRAG_VBUD_NETTO_EURO` columns for rows where the source was `NULL`.
*   **Pass/Fail Criterion**:
    1.  For rows where `NNA.DAUER_SEK` was `NULL`, the `DAUER_MIN` column in the output CSV should be `NULL` (or an empty string, depending on CSV export behavior for `NULL`s).
    2.  For rows where `NNA.RBETRAG_VBUD_NETTO_CENT` was `NULL`, the `RBETRAG_VBUD_NETTO_EURO` column in the output CSV should be `NULL` (or an empty string).
    3.  Rows where `NNA.RAHMENVERTRAG` was `NULL` are correctly excluded from the output.
    4.  The behavior matches the legacy system's output for these `NULL` scenarios.

    ```sql
    -- Example BigQuery assertion for NULL handling (conceptual)
    -- This would be part of a more detailed SQL test, not the end-to-end.
    SELECT
      DAUER_MIN,
      RBETRAG_VBUD_NETTO_EURO
    FROM (
      -- Your full migrated BigQuery SQL query here
      -- ...
    )
    WHERE MONATS_ID = @FROM_YYYYMM
      AND (DAUER_MIN IS NULL OR RBETRAG_VBUD_NETTO_EURO IS NULL);

    -- Expected result:
    -- If DAUER_SEK was NULL, DAUER_MIN should be NULL.
    -- If RBETRAG_VBUD_NETTO_CENT was NULL, RBETRAG_VBUD_NETTO_EURO should be NULL.
    ```

---

### Test Case 7: Row Count and Schema Assertions

*   **Purpose**: To verify that the number of records and the output schema (number of columns, column order) remain consistent with the legacy job.
*   **Setup**:
    1.  Ensure the BigQuery source tables are populated with the "golden dataset".
    2.  The legacy job's output for this specific dataset is available.
*   **Action**:
    1.  Execute the `dw_dwh_exis_sd_apt_nna_voic` Airflow DAG.
    2.  Retrieve the final gzipped CSV from GCS or SFTP.
    3.  Decompress the file.
*   **Pass/Fail Criterion**:
    1.  **Row Count**: The `RECORD_COUNT` in the trailer of the migrated output CSV matches the `RECORD_COUNT` in the trailer of the legacy golden output CSV.
    2.  **Column Count**: The number of columns in each data row of the migrated output CSV matches the number of columns in the legacy golden output CSV (16 columns as per the `SELECT` statement).
    3.  **Column Order**: The order of columns in the migrated output CSV matches the order in the legacy golden output CSV.

    ```python
    import gzip
    import csv

    def validate_row_count_and_schema(gzipped_file_path, expected_row_count, expected_column_count):
        with gzip.open(gzipped_file_path, 'rt') as f:
            lines = f.readlines()

        if not lines:
            print("FAIL: Gzipped file is empty.")
            return False

        # Validate row count from trailer
        trailer = lines[-1].strip()
        trailer_parts = trailer.split(';')
        if len(trailer_parts) != 4 or not trailer_parts[0].startswith("#T"):
            print(f"FAIL: Invalid trailer format: {trailer}")
            return False
        
        actual_record_count = int(trailer_parts[1])
        if actual_record_count != expected_row_count:
            print(f"FAIL: Row count mismatch. Expected: {expected_row_count}, Actual: {actual_record_count}")
            return False
        print(f"PASS: Row count matches expected: {actual_record_count}")

        # Validate column count for data rows
        data_lines = lines[1:-1] # Exclude header and trailer
        if not data_lines:
            print("WARNING: No data rows to validate column count.")
            return True # If no data, count is 0, which is valid

        # Use csv reader to handle potential delimiters within fields
        reader = csv.reader(data_lines)
        for i, row in enumerate(reader):
            if len(row) != expected_column_count:
                print(f"FAIL: Column count mismatch in data row {i+1}. Expected: {expected_column_count}, Actual: {len(row)}")
                print(f"Row content: {row}")
                return False
        print(f"PASS: Column count matches expected: {expected_column_count} for all data rows.")
        return True

    # Example usage
    # expected_rows = 100 # Get this from legacy golden output trailer
    # expected_cols = 16  # Count from the SELECT statement
    # assert validate_row_count_and_schema("path/to/migrated_output.csv.gz", expected_rows, expected_cols)
    ```

---