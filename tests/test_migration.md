As a senior data-migration QA engineer, I've reviewed the migration design and the generated code for `EXIS_SD_APT_NNA_VOIC`. The following test plan outlines a comprehensive approach to validate the migrated job, ensuring behavioral equivalence and correctness across all specified areas.

---

## Migration Validation Test Plan: EXIS_SD_APT_NNA_VOIC

### Pre-requisites for all Tests:

1.  **Data Synchronization:** The BigQuery source tables (`your_project.your_dataset.DWH_VI_L_MAP_FA_TARIF`, `your_project.your_dataset.BL_D_TARIF`, `your_project.your_dataset.DWH_VI_C_VERTRAG`, `your_project.your_dataset.DWH_VI_F_NNV_TVD_12_MONATE`, `your_project.your_dataset.DWH_VI_L_TVD_LEISTUNGSKLASSE`) must be populated with a representative dataset that is *identical* to the data present in the legacy Oracle DWH tables at the time the legacy job was run for comparison. This dataset should cover:
    *   Typical production data.
    *   Records that match all filter conditions.
    *   Records that are filtered out by each condition.
    *   Records with `NULL` values in relevant columns (e.g., `RAHMENVERTRAG`, `MP_MARKTPRODUKT_BEZ`).
    *   Records with edge cases for `LEISTUNGSKLASSE_ID` filtering.
    *   Records that result in various rounding scenarios for `DAUER_MIN` and `RBETRAG_VBUD_NETTO_EURO`.
    *   Records with `GUELTIG_BIS` dates other than `4712-12-31`.
2.  **Legacy Output Baseline:** The legacy `EXIS_SD_APT_NNA_VOIC` job must have been executed with the *exact same* source data as prepared for BigQuery, and its final gzipped CSV output file (including header/trailer) must be available as a baseline for comparison.
3.  **Airflow Environment:** The `dw_dwh_exis_sd_apt_nna_voic` DAG must be deployed to a Cloud Composer/Airflow environment.
4.  **SFTP Target:** A dedicated test SFTP server must be configured and accessible from the Airflow environment, with appropriate credentials (e.g., via Google Secret Manager or Airflow Connections).
5.  **GCP Resources:** The specified BigQuery project, dataset, and GCS bucket must exist and be accessible by the Airflow service account.
6.  **Python Dependencies:** The `paramiko` library must be installed in the Airflow environment.

---

### 1. Output Parity Tests

#### Test Case 1.1: End-to-End File Content Comparison

*   **Purpose:** To verify that the final gzipped CSV file produced by the migrated Airflow DAG is byte-for-byte identical (or functionally identical after accounting for dynamic elements) to the file produced by the legacy UC4 job when processing the same input data. This is the ultimate test of output parity.
*   **Setup:**
    1.  Ensure all pre-requisites are met, especially data synchronization and availability of the legacy output file.
    2.  Identify a specific `execution_date` (e.g., `2023-10-01`) for which both legacy and migrated jobs will run, ensuring `MONATS_ID` matches (e.g., `202310`).
    3.  The legacy job's output file (`DWHM_APT_NNA_Voice_YYYYMMDD.csv.gz`) is stored locally.
    4.  Configure the Airflow DAG's `SFTP_HOST`, `SFTP_PORT`, `SFTP_USERNAME`, `SFTP_PASSWORD`, and `SFTP_REMOTE_PATH` to point to the test SFTP server.
*   **Action:**
    1.  Trigger the `dw_dwh_exis_sd_apt_nna_voic` Airflow DAG for the chosen `execution_date`.
    2.  Monitor the DAG execution to ensure all tasks complete successfully.
    3.  Once complete, retrieve the generated gzipped CSV file from the test SFTP server.
    4.  Decompress both the legacy and migrated output files.
*   **Pass/Fail Criterion:**
    *   **Pass:** The decompressed content of the migrated CSV file is identical to the decompressed content of the legacy CSV file, including the header and trailer lines.
    *   **Fail:** Any difference in content, row count, column order, or data values between the two files.

*   **Test Code (Conceptual Python/Bash for comparison):**

    ```python
    import gzip
    import os
    import filecmp

    def compare_gzipped_csv_files(legacy_file_path, migrated_file_path, temp_dir="/tmp"):
        """
        Decompresses and compares two gzipped CSV files.
        """
        legacy_decompressed_path = os.path.join(temp_dir, "legacy_output.csv")
        migrated_decompressed_path = os.path.join(temp_dir, "migrated_output.csv")

        with gzip.open(legacy_file_path, 'rb') as f_in, open(legacy_decompressed_path, 'wb') as f_out:
            f_out.write(f_in.read())

        with gzip.open(migrated_file_path, 'rb') as f_in, open(migrated_decompressed_path, 'wb') as f_out:
            f_out.write(f_in.read())

        # Compare the decompressed files
        are_identical = filecmp.cmp(legacy_decompressed_path, migrated_decompressed_path, shallow=False)

        # Clean up temporary files
        os.remove(legacy_decompressed_path)
        os.remove(migrated_decompressed_path)

        return are_identical

    # Example usage in a pytest fixture or test function
    def test_end_to_end_output_parity(legacy_output_path, migrated_sftp_path):
        # Assume legacy_output_path is the path to the legacy .csv.gz
        # Assume migrated_sftp_path is the path to the file downloaded from test SFTP
        assert compare_gzipped_csv_files(legacy_output_path, migrated_sftp_path)
    ```

#### Test Case 1.2: Record Count Parity (Data Rows)

*   **Purpose:** To specifically verify that the number of data rows (excluding header/trailer) generated by the migrated job matches the legacy job.
*   **Setup:** Same as Test Case 1.1.
*   **Action:**
    1.  Execute the migrated DAG.
    2.  Retrieve the final gzipped CSV from the test SFTP server.
    3.  Decompress the file and count the data rows (lines between 'H' and 'X' records).
    4.  Obtain the data row count from the legacy output file.
*   **Pass/Fail Criterion:**
    *   **Pass:** The count of data rows in the migrated output file (excluding header/trailer) is exactly equal to the count of data rows in the legacy output file.
    *   **Fail:** Any discrepancy in data row counts.

*   **Test Code (Python):**

    ```python
    import gzip
    import io
    import csv

    def get_data_row_count(gzipped_file_content):
        with gzip.open(io.BytesIO(gzipped_file_content), 'rt', encoding='utf-8') as f:
            lines = f.readlines()
            data_lines = [line for line in lines if not line.startswith('H|') and not line.startswith('X|')]
            return len(data_lines)

    def test_record_count_parity(legacy_gzipped_content, migrated_gzipped_content):
        legacy_count = get_data_row_count(legacy_gzipped_content)
        migrated_count = get_data_row_count(migrated_gzipped_content)
        assert legacy_count == migrated_count, \
            f"Record count mismatch: Legacy={legacy_count}, Migrated={migrated_count}"
    ```

---

### 2. Transformation Correctness Tests

These tests focus on the BigQuery SQL logic. It's often beneficial to run the core BigQuery SQL query directly and compare its output to the legacy Oracle query output, rather than relying solely on the final CSV.

#### Test Case 2.1: Join Logic Verification

*   **Purpose:** To ensure that all join conditions (`DWH_TARIF_ID`, `DWH_VERTRAG_ID`, `LEISTUNGSKLASSE_ID`) correctly link records between the BigQuery tables, producing the same intermediate result set as the legacy Oracle query.
*   **Setup:**
    1.  Ensure BigQuery source tables are populated with data mirroring Oracle.
    2.  Extract the core SQL query from the `build_bigquery_sql` function in the DAG.
    3.  Obtain the equivalent SQL query from the legacy `d_exis_apt_nna_voice.sql` (or its functional equivalent).
*   **Action:**
    1.  Execute the core BigQuery SQL query (up to the `SELECT` statement, without `CREATE OR REPLACE TABLE`) against the BigQuery source tables.
    2.  Execute the legacy Oracle SQL query against the Oracle DWH.
    3.  Compare the result sets. This can involve comparing row counts, checksums of specific columns, or a full data comparison after exporting both to a common format.
*   **Pass/Fail Criterion:**
    *   **Pass:** The result set (row count, column values, and order) from the BigQuery query is identical to the result set from the legacy Oracle query.
    *   **Fail:** Any discrepancy in the joined data.

*   **Test Code (SQL Assertion - using `bq query` and Python for comparison):**

    ```python
    import subprocess
    import pandas as pd

    def run_bq_query_and_get_df(sql_query, project_id):
        cmd = ["bq", "--project_id", project_id, "query", "--format=csv", "--use_legacy_sql=false", sql_query]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        df = pd.read_csv(io.StringIO(result.stdout))
        return df

    def test_join_logic_correctness(legacy_oracle_df):
        # Assume legacy_oracle_df is a pandas DataFrame obtained from the legacy Oracle query
        # (e.g., via cx_Oracle, then to_pandas)
        project_id = os.environ.get("GCP_PROJECT_ID", "your_project")
        dataset_id = os.environ.get("GCP_DATASET_ID", "your_dataset")
        monats_id = "202310" # Example for a specific month

        bq_sql = f"""
        SELECT
          NNA.MONATS_ID, NNA.RAHMENVERTRAG, VER.MSISDN, VER.KUNDENKONTO, VER.T_MOBILE_KUNDENNUMMER,
          TAR.TARIF_ID,
          CONCAT(TAR.MP_MARKTPRODUKT_BEZ, ',', TAR.MP_EG_JN_BEZ, ',', TAR.MP_GENERATION_BEZ) AS TARIF,
          TVD.LEISTUNGSKLASSE_ID, TVD.LEISTUNGSKLASSE_TEXT, NNA.VERBINDUNGEN,
          ROUND(NNA.DAUER_SEK / 60, 2) AS DAUER_MIN,
          ROUND(NNA.RBETRAG_VBUD_NETTO_CENT / 100, 2) AS RBETRAG_VBUD_NETTO_EURO,
          TAR.MP_EG_JN_ID, TAR.MP_EG_JN_BEZ, TAR.MP_GENERATION_ID, TAR.MP_GENERATION_BEZ
        FROM (
          SELECT
            TRF.DWH_TARIF_ID, TRF.TARIF_ID, D.MP_MARKTPRODUKT_BEZ, D.MP_EG_JN_BEZ, D.MP_GENERATION_BEZ,
            TRF.GUELTIG_BIS, D.MP_EG_JN_ID, D.MP_GENERATION_ID
          FROM `{project_id}.{dataset_id}.DWH_VI_L_MAP_FA_TARIF` AS TRF
          JOIN `{project_id}.{dataset_id}.BL_D_TARIF` AS D ON TRF.TARIF_ID = D.TARIF_ID
        ) AS TAR
        JOIN `{project_id}.{dataset_id}.DWH_VI_C_VERTRAG` AS VER ON TAR.DWH_TARIF_ID = VER.DWH_TARIF_ID
        JOIN `{project_id}.{dataset_id}.DWH_VI_F_NNV_TVD_12_MONATE` AS NNA ON VER.DWH_VERTRAG_ID = NNA.DWH_VERTRAG_ID
        JOIN `{project_id}.{dataset_id}.DWH_VI_L_TVD_LEISTUNGSKLASSE` AS TVD ON NNA.LEISTUNGSKLASSE_ID = TVD.LEISTUNGSKLASSE_ID
        WHERE NNA.RAHMENVERTRAG IS NOT NULL
          AND NNA.MONATS_ID = CAST('{monats_id}' AS INT64)
          AND TAR.GUELTIG_BIS = DATE '4712-12-31'
          AND (
            (TVD.LEISTUNGSKLASSEGR_ID = 1 AND (TVD.LEISTUNGSKLASSE_ID < 300 OR TVD.LEISTUNGSKLASSE_ID > 399))
            OR (
              LENGTH(TRIM(CAST(TVD.LEISTUNGSKLASSE_ID AS STRING))) = 6
              AND TVD.LEISTUNGSKLASSE_ID < 699999
              AND CAST(FLOOR(TVD.LEISTUNGSKLASSE_ID / 1000) AS INT64) <> 622
            )
          );
        """
        bq_df = run_bq_query_and_get_df(bq_sql, project_id)

        # Sort both DataFrames to ensure consistent comparison
        # Identify common columns for sorting, e.g., primary keys or a combination
        sort_cols = ['MONATS_ID', 'RAHMENVERTRAG', 'MSISDN', 'TARIF_ID', 'LEISTUNGSKLASSE_ID']
        bq_df = bq_df.sort_values(by=sort_cols).reset_index(drop=True)
        legacy_oracle_df = legacy_oracle_df.sort_values(by=sort_cols).reset_index(drop=True)

        pd.testing.assert_frame_equal(bq_df, legacy_oracle_df, check_dtype=False, check_exact=False, rtol=1e-2) # rtol for float comparisons
    ```

#### Test Case 2.2: Filtering Logic Verification

*   **Purpose:** To confirm that all `WHERE` clause conditions (`RAHMENVERTRAG IS NOT NULL`, `MONATS_ID`, `GUELTIG_BIS`, `LEISTUNGSKLASSE_ID` complex logic) are correctly translated and applied in BigQuery, filtering out the same records as the legacy Oracle job.
*   **Setup:**
    1.  Populate BigQuery source tables with specific test data that includes:
        *   `NNA.RAHMENVERTRAG` as `NULL` and non-`NULL`.
        *   `NNA.MONATS_ID` matching the test month and not matching.
        *   `TAR.GUELTIG_BIS` as `DATE '4712-12-31'` and other dates.
        *   `TVD.LEISTUNGSKLASSE_ID` and `LEISTUNGSKLASSEGR_ID` values that specifically test each branch of the complex `OR` condition (e.g., `LEISTUNGSKLASSEGR_ID = 1` with `LEISTUNGSKLASSE_ID` < 300, > 399, and between 300-399; `LENGTH(TRIM(CAST(TVD.LEISTUNGSKLASSE_ID AS STRING))) = 6` with `TVD.LEISTUNGSKLASSE_ID` < 699999 and `FLOOR(TVD.LEISTUNGSKLASSE_ID / 1000)` being 622 or not).
    2.  Run the legacy job with this test data and capture the output.
*   **Action:**
    1.  Execute the `process_voice_export` task in the Airflow DAG for the relevant `execution_date`.
    2.  Query the resulting `your_project.your_dataset.DWHM_APT_NNA_Voice` table in BigQuery.
    3.  Compare the records in this BigQuery table with the data rows from the legacy output.
*   **Pass/Fail Criterion:**
    *   **Pass:** The set of records in the BigQuery output table exactly matches the set of data records in the legacy output file. Specifically, records expected to be filtered out are absent, and records expected to be included are present.
    *   **Fail:** Any record present in one output but not the other, or any record incorrectly filtered.

*   **Test Code (SQL Assertion - example for `LEISTUNGSKLASSE_ID`):**

    ```sql
    -- Verify records that should be filtered out by LEISTUNGSKLASSEGR_ID = 1 and (300 <= ID <= 399)
    SELECT COUNT(*)
    FROM `your_project.your_dataset.DWHM_APT_NNA_Voice`
    WHERE LEISTUNGSKLASSEGR_ID = 1 AND LEISTUNGSKLASSE_ID BETWEEN 300 AND 399;
    -- Expected: 0

    -- Verify records that should be filtered out by LENGTH(ID) != 6 or ID >= 699999 or FLOOR(ID/1000) = 622
    SELECT COUNT(*)
    FROM `your_project.your_dataset.DWHM_APT_NNA_Voice`
    WHERE (
        LENGTH(TRIM(CAST(LEISTUNGSKLASSE_ID AS STRING))) != 6
        OR LEISTUNGSKLASSE_ID >= 699999
        OR CAST(FLOOR(LEISTUNGSKLASSE_ID / 1000) AS INT64) = 622
    ) AND NOT (LEISTUNGSKLASSEGR_ID = 1 AND (LEISTUNGSKLASSE_ID < 300 OR LEISTUNGSKLASSE_ID > 399));
    -- Expected: 0
    ```

#### Test Case 2.3: Column Transformations and Type Handling

*   **Purpose:** To verify that specific column transformations (`CONCAT`, `ROUND`, division) and data type handling are correctly implemented in BigQuery, producing identical values and types as the legacy Oracle job.
*   **Setup:**
    1.  Populate BigQuery source tables with data that specifically tests:
        *   `TARIF` concatenation: `NULL` values in `MP_MARKTPRODUKT_BEZ`, `MP_EG_JN_BEZ`, `MP_GENERATION_BEZ`.
        *   `DAUER_MIN`: `DAUER_SEK` values like 0, 1, 59, 60, 61, 123.45 (if possible), to test rounding to 2 decimal places.
        *   `RBETRAG_VBUD_NETTO_EURO`: `RBETRAG_VBUD_NETTO_CENT` values like 0, 1, 99, 100, 101, 12345 to test rounding.
    2.  Run the legacy job with this test data and capture the output.
*   **Action:**
    1.  Execute the `process_voice_export` task in the Airflow DAG.
    2.  Query the `your_project.your_dataset.DWHM_APT_NNA_Voice` table.
    3.  Compare the transformed column values (e.g., `TARIF`, `DAUER_MIN`, `RBETRAG_VBUD_NETTO_EURO`) with the corresponding values in the legacy output.
*   **Pass/Fail Criterion:**
    *   **Pass:** All transformed column values in the BigQuery output table exactly match the legacy output, considering floating-point precision. Data types in BigQuery are consistent with the DDL and expected output.
    *   **Fail:** Any discrepancy in transformed values or unexpected type conversions.

*   **Test Code (SQL Assertion):**

    ```sql
    -- Verify TARIF concatenation with NULLs
    SELECT TARIF, MP_MARKTPRODUKT_BEZ, MP_EG_JN_BEZ, MP_GENERATION_BEZ
    FROM `your_project.your_dataset.DWHM_APT_NNA_Voice`
    WHERE MP_MARKTPRODUKT_BEZ IS NULL OR MP_EG_JN_BEZ IS NULL OR MP_GENERATION_BEZ IS NULL;
    -- Expected: CONCAT behavior should match Oracle's (e.g., 'A,,C' if B is NULL)

    -- Verify DAUER_MIN rounding
    SELECT DAUER_MIN, VERBINDUNGEN, RBETRAG_VBUD_NETTO_EURO
    FROM `your_project.your_dataset.DWHM_APT_NNA_Voice`
    WHERE DAUER_MIN = 0.00 OR DAUER_MIN = 1.00 OR DAUER_MIN = 0.98; -- Example values
    -- Expected: Values match legacy output to 2 decimal places.

    -- Verify RBETRAG_VBUD_NETTO_EURO rounding
    SELECT RBETRAG_VBUD_NETTO_EURO
    FROM `your_project.your_dataset.DWHM_APT_NNA_Voice`
    WHERE RBETRAG_VBUD_NETTO_EURO = 0.00 OR RBETRAG_VBUD_NETTO_EURO = 1.00 OR RBETRAG_VBUD_NETTO_EURO = 0.99; -- Example values
    -- Expected: Values match legacy output to 2 decimal places.
    ```

#### Test Case 2.4: NULL Handling Consistency

*   **Purpose:** To ensure that `NULL` values in source columns are handled consistently across BigQuery and Oracle, especially in filters and concatenations.
*   **Setup:**
    1.  Populate BigQuery source tables with records containing `NULL` values in `RAHMENVERTRAG`, `MP_MARKTPRODUKT_BEZ`, `MP_EG_JN_BEZ`, `MP_GENERATION_BEZ`.
    2.  Run the legacy job with this data.
*   **Action:**
    1.  Execute the `process_voice_export` task.
    2.  Query the `DWHM_APT_NNA_Voice` table.
    3.  Inspect records that should be filtered by `RAHMENVERTRAG IS NOT NULL` and records where `TARIF` is concatenated with `NULL` components.
*   **Pass/Fail Criterion:**
    *   **Pass:** Records with `RAHMENVERTRAG IS NULL` are correctly excluded. `TARIF` column values with `NULL` components match the legacy Oracle behavior (e.g., `CONCAT('A', NULL, 'C')` results in `'A,,C'` in BigQuery, which is typically consistent with Oracle's `||` operator).
    *   **Fail:** `NULL` values lead to unexpected filtering or concatenation results.

---

### 3. External-System Replacements Tests

#### Test Case 3.1: GCS Export Functionality

*   **Purpose:** To verify that the `BigQueryToGCSOperator` correctly exports the data from the BigQuery table to a gzipped CSV file in the specified GCS bucket, with the correct naming and `print_header=False` setting.
*   **Setup:**
    1.  Ensure the `process_voice_export` task has successfully populated the `DWHM_APT_NNA_Voice` table.
    2.  Set `GCS_BUCKET` in the DAG to a test bucket.
*   **Action:**
    1.  Trigger the `export_to_gcs` task (or run the full DAG up to this point).
    2.  Use `gsutil` or the GCP Console to inspect the target GCS bucket.
*   **Pass/Fail Criterion:**
    *   **Pass:** A gzipped CSV file named `DWHM_APT_NNA_Voice_{ds_nodash}.csv.gz` exists in `gs://{GCS_BUCKET}/exis_sd_apt_nna_voic/`. The file is gzipped, contains the data from the BigQuery table, and *does not* include a header row.
    *   **Fail:** File not found, incorrect naming, wrong compression, or contains a header row.

*   **Test Code (Bash/Python using `gsutil`):**

    ```bash
    # From a shell with gsutil configured
    GCS_BUCKET="your-gcs-bucket"
    DS_NODASH="20231001" # Replace with actual execution date
    GCS_PATH="gs://${GCS_BUCKET}/exis_sd_apt_nna_voic/DWHM_APT_NNA_Voice_${DS_NODASH}.csv.gz"

    # Check if file exists
    gsutil ls "${GCS_PATH}"

    # Download and inspect content (first few lines)
    gsutil cat "${GCS_PATH}" | gzip -d | head -n 5
    # Expected: Data rows, no header.
    ```

#### Test Case 3.2: Header/Trailer and SFTP Distribution

*   **Purpose:** To verify that the `add_header_trailer_and_sftp` Python function correctly:
    1.  Downloads the GCS file.
    2.  Adds the `H` (header) and `X` (trailer) lines with correct dynamic values (`ds_nodash`, `num_records`).
    3.  Re-compresses the file.
    4.  Successfully transfers the final gzipped CSV to the external SFTP server with the correct filename.
*   **Setup:**
    1.  Ensure the `export_to_gcs` task has successfully placed the intermediate file in GCS.
    2.  Configure the Airflow DAG's SFTP credentials and path to point to the test SFTP server.
    3.  Ensure the test SFTP server is running and accessible.
*   **Action:**
    1.  Trigger the `add_header_trailer_and_sftp_task` (or run the full DAG).
    2.  Monitor Airflow logs for SFTP success/failure messages.
    3.  Connect to the test SFTP server and retrieve the file `DWHM_APT_NNA_Voice_{ds_nodash}.csv.gz`.
    4.  Decompress the retrieved file and inspect its content.
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   The file is successfully transferred to the specified `SFTP_REMOTE_PATH` on the SFTP server.
        *   The filename is `DWHM_APT_NNA_Voice_{ds_nodash}.csv.gz`.
        *   The decompressed file contains an `H` header line and an `X` trailer line.
        *   The header and trailer lines contain the correct `ds_nodash` values.
        *   The `num_records` in the trailer line accurately reflects the count of data rows between the header and trailer.
        *   The file is correctly gzipped.
    *   **Fail:** SFTP transfer fails, incorrect filename, missing/malformed header/trailer, incorrect record count in trailer, or corrupted file.

*   **Test Code (Python using `paramiko` to check SFTP server):**

    ```python
    import paramiko
    import gzip
    import io
    import csv
    import os

    def check_sftp_file_content(sftp_host, sftp_port, sftp_username, sftp_password, remote_path, expected_ds_nodash):
        transport = None
        sftp = None
        try:
            transport = paramiko.Transport((sftp_host, sftp_port))
            transport.connect(username=sftp_username, password=sftp_password)
            sftp = paramiko.SFTPClient.from_transport(transport)

            sftp_filename = f"DWHM_APT_NNA_Voice_{expected_ds_nodash}.csv.gz"
            full_remote_path = os.path.join(remote_path, sftp_filename)

            # Check if file exists
            try:
                sftp.stat(full_remote_path)
            except FileNotFoundError:
                raise AssertionError(f"SFTP file not found: {full_remote_path}")

            # Download and decompress
            file_buffer = io.BytesIO()
            sftp.getfo(full_remote_path, file_buffer)
            file_buffer.seek(0) # Reset buffer position

            with gzip.open(file_buffer, 'rt', encoding='utf-8') as f:
                lines = f.readlines()

            # Assert header and trailer
            assert lines[0].startswith(f"H|{expected_ds_nodash}|V_F_NNA_Voice|{expected_ds_nodash}"), \
                f"Header mismatch: {lines[0]}"
            
            trailer_line = lines[-1].strip()
            assert trailer_line.startswith(f"X|{sftp_filename}|{expected_ds_nodash}|"), \
                f"Trailer start mismatch: {trailer_line}"
            
            # Extract record count from trailer
            trailer_parts = trailer_line.split('|')
            actual_record_count = int(trailer_parts[3])
            
            # Count data rows
            data_rows = [line for line in lines[1:-1] if line.strip()] # Exclude header/trailer and empty lines
            assert actual_record_count == len(data_rows), \
                f"Record count in trailer ({actual_record_count}) does not match actual data rows ({len(data_rows)})"

            logging.info(f"SFTP file {full_remote_path} content verified successfully.")

        finally:
            if sftp: sftp.close()
            if transport: transport.close()

    def test_sftp_distribution_and_header_trailer():
        # These should come from Airflow config or test environment variables
        SFTP_HOST = os.environ.get("SFTP_HOST", "sftp.example.com")
        SFTP_PORT = int(os.environ.get("SFTP_PORT", 22))
        SFTP_USERNAME = os.environ.get("SFTP_USERNAME", "sftpuser")
        SFTP_PASSWORD = os.environ.get("SFTP_PASSWORD", "sftppassword")
        SFTP_REMOTE_PATH = os.environ.get("SFTP_REMOTE_PATH", "/upload/nna_voice")
        
        # This should be the ds_nodash used for the Airflow DAG run
        TEST_DS_NODASH = "20231001" 

        check_sftp_file_content(SFTP_HOST, SFTP_PORT, SFTP_USERNAME, SFTP_PASSWORD, SFTP_REMOTE_PATH, TEST_DS_NODASH)
    ```

---

### 4. Data Quality / Row Count / Schema Assertions

#### Test Case 4.1: BigQuery Target Table Schema Conformance

*   **Purpose:** To verify that the `DWHM_APT_NNA_Voice` BigQuery table created by the DAG conforms to the expected schema (column names, order, and data types) defined in `sql/ddl/DWHM_APT_NNA_Voice.sql`.
*   **Setup:**
    1.  Ensure the `process_voice_export` task has successfully run at least once, creating the target table.
*   **Action:**
    1.  Use BigQuery's `DESCRIBE TABLE` command or the `bq show` command to retrieve the schema of `your_project.your_dataset.DWHM_APT_NNA_Voice`.
    2.  Compare this retrieved schema against the DDL provided in `sql/ddl/DWHM_APT_NNA_Voice.sql`.
*   **Pass/Fail Criterion:**
    *   **Pass:** The actual BigQuery table schema (column names, data types, and order) exactly matches the defined DDL.
    *   **Fail:** Any discrepancy in schema definition.

*   **Test Code (Bash/Python using `bq show`):**

    ```python
    import subprocess
    import json

    def get_bq_table_schema(project_id, dataset_id, table_id):
        cmd = ["bq", "--project_id", project_id, "show", "--schema", "--format=json", f"{dataset_id}.{table_id}"]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return json.loads(result.stdout)

    def test_target_table_schema_conformance():
        project_id = os.environ.get("GCP_PROJECT_ID", "your_project")
        dataset_id = os.environ.get("GCP_DATASET_ID", "your_dataset")
        table_id = "DWHM_APT_NNA_Voice"

        actual_schema = get_bq_table_schema(project_id, dataset_id, table_id)

        # Expected schema based on sql/ddl/DWHM_APT_NNA_Voice.sql
        expected_schema_fields = [
            {"name": "MONATS_ID", "type": "INTEGER"},
            {"name": "RAHMENVERTRAG", "type": "STRING"},
            {"name": "MSISDN", "type": "STRING"},
            {"name": "KUNDENKONTO", "type": "STRING"},
            {"name": "T_MOBILE_KUNDENNUMMER", "type": "STRING"},
            {"name": "TARIF_ID", "type": "INTEGER"},
            {"name": "TARIF", "type": "STRING"},
            {"name": "LEISTUNGSKLASSE_ID", "type": "INTEGER"},
            {"name": "LEISTUNGSKLASSE_TEXT", "type": "STRING"},
            {"name": "VERBINDUNGEN", "type": "INTEGER"},
            {"name": "DAUER_MIN", "type": "FLOAT"},
            {"name": "RBETRAG_VBUD_NETTO_EURO", "type": "FLOAT"},
            {"name": "MP_EG_JN_ID", "type": "INTEGER"},
            {"name": "MP_EG_JN_BEZ", "type": "STRING"},
            {"name": "MP_GENERATION_ID", "type": "INTEGER"},
            {"name": "MP_GENERATION_BEZ", "type": "STRING"},
        ]
        
        # Compare field names and types (order might not be strictly enforced by BQ, but good to check)
        actual_fields_map = {f['name']: f['type'] for f in actual_schema['schema']['fields']}
        expected_fields_map = {f['name']: f['type'] for f in expected_schema_fields}

        assert actual_fields_map == expected_fields_map, \
            f"Schema mismatch. Actual: {actual_fields_map}, Expected: {expected_fields_map}"
        
        # Optionally, check order if critical
        actual_field_names = [f['name'] for f in actual_schema['schema']['fields']]
        expected_field_names = [f['name'] for f in expected_schema_fields]
        assert actual_field_names == expected_field_names, \
            f"Column order mismatch. Actual: {actual_field_names}, Expected: {expected_field_names}"
    ```

#### Test Case 4.2: Row Count Validation (BigQuery Table)

*   **Purpose:** To verify that the number of rows in the `DWHM_APT_NNA_Voice` BigQuery table matches the expected count from the legacy job's output.
*   **Setup:**
    1.  Ensure the `process_voice_export` task has successfully run.
    2.  Obtain the expected row count from the legacy job's output (excluding header/trailer).
*   **Action:**
    1.  Execute a `SELECT COUNT(*)` query on `your_project.your_dataset.DWHM_APT_NNA_Voice`.
*   **Pass/Fail Criterion:**
    *   **Pass:** The count from the BigQuery table matches the expected data row count from the legacy output.
    *   **Fail:** Any discrepancy in row counts.

*   **Test Code (SQL Assertion):**

    ```sql
    SELECT COUNT(*) FROM `your_project.your_dataset.DWHM_APT_NNA_Voice`;
    -- Expected: <count_from_legacy_output>
    ```

#### Test Case 4.3: Empty Source Data Handling

*   **Purpose:** To ensure the job handles scenarios where one or more source tables are empty or the filters result in no matching data gracefully, producing an empty (but correctly formatted) output file.
*   **Setup:**
    1.  Create a test dataset where:
        *   All source tables are empty.
        *   Source tables contain data, but the `WHERE` clause filters out all records.
    2.  Run the legacy job with this empty/no-match data and capture its (likely empty) output.
*   **Action:**
    1.  Trigger the Airflow DAG with the test dataset.
    2.  Retrieve the output file from the test SFTP server.
*   **Pass/Fail Criterion:**
    *   **Pass:** The job completes successfully without errors. The output file is gzipped, contains only the header and trailer lines (with `num_records` as 0), and no data rows, matching the legacy empty output.
    *   **Fail:** Job fails, output file is malformed, or contains unexpected data.

*   **Test Code (Python for checking empty output):**

    ```python
    def test_empty_source_data_handling(migrated_gzipped_content):
        with gzip.open(io.BytesIO(migrated_gzipped_content), 'rt', encoding='utf-8') as f:
            lines = f.readlines()
            assert len(lines) == 2, "Expected only header and trailer for empty data"
            assert lines[0].startswith("H|"), "Missing header"
            assert lines[1].startswith("X|"), "Missing trailer"
            
            trailer_parts = lines[1].split('|')
            assert int(trailer_parts[3]) == 0, "Trailer record count should be 0 for empty data"
    ```

---

This comprehensive test plan covers the critical aspects of the migration. Remember to execute these tests in a controlled environment, ideally with automated tooling for data setup, job execution, and result comparison. Prioritize fixing any discrepancies found, especially those related to output parity and transformation correctness, as they directly impact data integrity.