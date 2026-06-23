As a senior data-migration QA engineer, I've analyzed the legacy KornShell script (`k_ausd_bp_ta_bpr_basis_his.ksh`) and its migration to Google Cloud Platform (Airflow and BigQuery). The migration involves re-platforming orchestration to Airflow and translating core SQL logic and file-based post-processing into BigQuery Stored Procedures.

The following test cases are designed to ensure behavioral equivalence, covering output parity, transformation correctness, external system replacements, and data quality assertions.

---

## Migration Validation Tests for `k_ausd_bp_ta_bpr_basis_his`

**Target Environment:** Google Cloud Platform (BigQuery, Airflow)
**Legacy Source:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh`
**Migrated Code:**
*   Airflow DAG: `dags/k_ausd_bp_ta_bpr_basis_his_dag.py`
*   BigQuery Stored Procedure (Core Logic): `bigquery/stored_procedures/sof.d_ausd_bp_ta_bpr_basis_his.sql`
*   BigQuery Stored Procedure (Post-Processing): `bigquery/stored_procedures/isbert_dataset.post_process_cibasis_data.sql`
*   BigQuery DDLs: `cds.ta_cntrct`, `pds.ta_bpri_com`, `sof.ta_bpr_basis_his`, `isbert_dataset.job_control`

---

### 1. Output Parity Tests

#### Test Case 1.1: End-to-End Data Parity - Main Processing

*   **Purpose:** Verify that the migrated Airflow DAG, when executed with identical inputs, produces the exact same final data in the `sof.ta_bpr_basis_his` table as the legacy KornShell script. This is the ultimate test of behavioral equivalence for the core data transformation.
*   **Setup:**
    1.  **Golden Dataset Creation:**
        *   Identify a representative set of input data for the legacy Oracle tables (`cds.ta_cntrct`, `pds.ta_bpri_com`). This dataset should include various scenarios covering all filters and transformations (e.g., active/ended contracts, different `bpr_id` values, various date combinations, NULLs).
        *   Load this exact dataset into the legacy Oracle environment.
        *   Execute the legacy `k_ausd_bp_ta_bpr_basis_his.ksh` script with a specific `Stichtag` (e.g., `01012023`), `JobKennung`, and `EintragsNr`.
        *   Extract the resulting data from the legacy Oracle `sof.ta_bpr_basis_his` table into a canonical format (e.g., CSV, JSON) and store it as the "golden output."
    2.  **BigQuery Setup:**
        *   Create the BigQuery tables `your-gcp-project.cds.ta_cntrct` and `your-gcp-project.pds.ta_bpri_com` using the provided DDLs.
        *   Load the *exact same* golden dataset into these BigQuery tables.
        *   Ensure the BigQuery target table `your-gcp-project.sof.ta_bpr_basis_his` is empty before running the test.
*   **Action:**
    1.  Trigger the Airflow DAG `k_ausd_bp_ta_bpr_basis_his` with the same parameters used for the legacy run (e.g., `JobKennung='TEST_JOB'`, `EintragsNr='123'`, `Stichtag='01012023'`).
    2.  Wait for the DAG to complete successfully.
    3.  Extract the data from the BigQuery `your-gcp-project.sof.ta_bpr_basis_his` table.
*   **Pass/Fail Criterion:**
    *   The extracted data from BigQuery `your-gcp-project.sof.ta_bpr_basis_his` must be **byte-for-byte identical** to the "golden output" from the legacy Oracle system, after accounting for any expected data type conversions (e.g., Oracle `DATE` to BigQuery `DATE`, `VARCHAR2` to `STRING`). Row order should not matter if the comparison is set-based.

#### Test Case 1.2: End-to-End Data Parity - Post-Processing Output

*   **Purpose:** Verify that the migrated BigQuery stored procedure for post-processing (`isbert_dataset.post_process_cibasis_data`), which replaces the commented-out `sed`, `sort`, `join` operations, produces the same final output as the legacy file-based operations.
*   **Setup:**
    1.  **Golden Files Creation:**
        *   Create sample input files: `cibasis_data24.dat`, `cibasis_data96.dat`, `cibasis_fax.dat`. These files should contain various scenarios, including:
            *   Lines with leading/trailing/internal spaces.
            *   Duplicate keys for `sort -u -k 1`.
            *   Keys present in all files, only in one file, or in two files, to test `join` behavior (especially `-a` and `-o` options).
        *   Manually execute the commented-out `sed`, `sort`, `join` commands from the legacy `.ksh` script using these sample files.
        *   Store the resulting `cibasisprodukt.csv` as the "golden post-processing output."
    2.  **BigQuery Setup:**
        *   Upload the sample input files (`cibasis_data24.dat`, etc.) to a Cloud Storage bucket.
        *   Create BigQuery external tables (`your-gcp-project.isbert_dataset.cibasis_data24_ext`, etc.) pointing to these files.
        *   Ensure the target table `your-gcp-project.isbert_dataset.cibasisprodukt_csv` is empty.
*   **Action:**
    1.  Trigger the Airflow DAG `k_ausd_bp_ta_bpr_basis_his` (the `post_process_cibasis_files` task will execute the BigQuery stored procedure).
    2.  Wait for the DAG to complete successfully.
    3.  Extract the data from the BigQuery `your-gcp-project.isbert_dataset.cibasisprodukt_csv` table.
*   **Pass/Fail Criterion:**
    *   The extracted data from BigQuery `your-gcp-project.isbert_dataset.cibasisprodukt_csv` must be **byte-for-byte identical** to the "golden post-processing output" `cibasisprodukt.csv`. This includes field order and delimiter.

---

### 2. Transformation Correctness Tests

#### Test Case 2.1: Parameter Validation - Missing Mandatory Parameters

*   **Purpose:** Verify that the Airflow DAG's `validate_and_prepare_params` task correctly identifies and fails when mandatory parameters (`JobKennung`, `EintragsNr`, `Stichtag`) are missing.
*   **Setup:** N/A
*   **Action:**
    1.  Trigger the Airflow DAG `k_ausd_bp_ta_bpr_basis_his` via the Airflow UI or CLI.
    2.  **Scenario A:** Do not provide `JobKennung`.
    3.  **Scenario B:** Do not provide `EintragsNr`.
    4.  **Scenario C:** Do not provide `Stichtag`.
*   **Pass/Fail Criterion:**
    *   For each scenario, the `validate_and_prepare_params` task must fail with an `AirflowException`.
    *   The error message in the task logs must clearly indicate the missing parameter (e.g., "Parameter JobKennung is missing.").

#### Test Case 2.2: Parameter Validation - Invalid `Stichtag` Format

*   **Purpose:** Verify that the `validate_and_prepare_params` task correctly validates the `Stichtag` format (DDMMYYYY).
*   **Setup:** N/A
*   **Action:**
    1.  Trigger the Airflow DAG `k_ausd_bp_ta_bpr_basis_his`.
    2.  Provide `JobKennung`, `EintragsNr`, but set `Stichtag` to an invalid format (e.g., "2023-01-01", "01/01/2023", "ABCDEFGH").
*   **Pass/Fail Criterion:**
    *   The `validate_and_prepare_params` task must fail with an `AirflowException`.
    *   The error message in the task logs must be similar to "Invalid Stichtag format: [provided_value]. Expected DDMMYYYY."

#### Test Case 2.3: Date Derivations (`gestern.ksh` Replacement)

*   **Purpose:** Verify that the `validate_and_prepare_params` task correctly calculates `p_datum_heute` and `p_datum_gestern` (equivalent to `gestern.ksh`).
*   **Setup:** N/A
*   **Action:**
    1.  Trigger the Airflow DAG `k_ausd_bp_ta_bpr_basis_his` with valid parameters.
    2.  Inspect the XCom output of the `validate_and_prepare_params` task.
*   **Pass/Fail Criterion:**
    *   `p_datum_heute` must be the current date in ISO format (YYYY-MM-DD).
    *   `p_datum_gestern` must be the previous day's date in ISO format (YYYY-MM-DD).

#### Test Case 2.4: Core Logic - `iccid` Concatenation

*   **Purpose:** Verify the `iccid` string concatenation logic in `sof.d_ausd_bp_ta_bpr_basis_his`.
*   **Setup:**
    1.  Insert a specific row into `your-gcp-project.pds.ta_bpri_com` with known `iccid_mi`, `iccid_ii`, `iccid_iai`, `iccid_nr`, `iccid_cd` values (e.g., '123', '45', '6789', '0', 'X').
    2.  Ensure a corresponding `your-gcp-project.cds.ta_cntrct` row exists and that both rows pass all other filters for a given `p_process_date`.
    3.  Ensure `your-gcp-project.sof.ta_bpr_basis_his` is empty.
*   **Action:**
    1.  Execute the `process_basisprodukt_data` task (or the entire DAG) with `p_process_date` set to include the test data.
    2.  Query `your-gcp-project.sof.ta_bpr_basis_his`.
*   **Pass/Fail Criterion:**
    *   The `iccid` column for the inserted row in `your-gcp-project.sof.ta_bpr_basis_his` must be '123-45-6789-0-X'.

#### Test Case 2.5: Core Logic - Filtering `c.cntrct_st`

*   **Purpose:** Verify the `c.cntrct_st IN (5, 6)` filter in `sof.d_ausd_bp_ta_bpr_basis_his`.
*   **Setup:**
    1.  Insert rows into `your-gcp-project.cds.ta_cntrct` with `cntrct_st` values: 5, 6, 1, 7, NULL.
    2.  For each `cntrct_st` value, ensure a corresponding `your-gcp-project.pds.ta_bpri_com` row exists and that all other filters are met.
    3.  Ensure `your-gcp-project.sof.ta_bpr_basis_his` is empty.
*   **Action:**
    1.  Execute the `process_basisprodukt_data` task.
    2.  Query `your-gcp-project.sof.ta_bpr_basis_his`.
*   **Pass/Fail Criterion:**
    *   Only rows originating from `cds.ta_cntrct` with `cntrct_st` values of 5 or 6 must be present in `your-gcp-project.sof.ta_bpr_basis_his`. Rows with 1, 7, or NULL `cntrct_st` must be excluded.

#### Test Case 2.6: Core Logic - Date Filters (Edge Cases)

*   **Purpose:** Verify all date-related filters (`insert_at`, `modified_at`, `valid_from`, `valid_to`) using `v_process_date` (derived from `p_process_date`), including NULL handling.
*   **Setup:**
    1.  Set `p_process_date` to a specific date, e.g., '2023-01-15'.
    2.  Insert various test cases into `your-gcp-project.cds.ta_cntrct` and `your-gcp-project.pds.ta_bpri_com` where dates are:
        *   `insert_at` before, on, and after '2023-01-15'.
        *   `modified_at` IS NULL, before, on, and after '2023-01-15'.
        *   `valid_from` before, on, and after '2023-01-15'.
        *   `valid_to` IS NULL, before, on, and after '2023-01-15'.
    3.  Ensure `your-gcp-project.sof.ta_bpr_basis_his` is empty.
*   **Action:**
    1.  Execute the `process_basisprodukt_data` task with `p_process_date = '2023-01-15'`.
    2.  Query `your-gcp-project.sof.ta_bpr_basis_his`.
*   **Pass/Fail Criterion:**
    *   Only rows satisfying ALL date conditions must be present:
        *   `insert_at <= '2023-01-15'`
        *   `(modified_at IS NULL OR modified_at > '2023-01-15')`
        *   `valid_from <= '2023-01-15'`
        *   `(valid_to IS NULL OR valid_to > '2023-01-15')`
    *   This applies to both `cds.ta_cntrct` and `pds.ta_bpri_com` date columns.

#### Test Case 2.7: Post-Processing - `sed` (Whitespace Removal)

*   **Purpose:** Verify `TRIM(REPLACE(line, ' ', ''))` correctly removes all spaces from input lines.
*   **Setup:**
    1.  Insert a row into `your-gcp-project.isbert_dataset.cibasis_data24_ext` with `line = '  value 1 ; value 2  '`.
    2.  Ensure `your-gcp-project.isbert_dataset.cibasisprodukt_csv` is empty.
*   **Action:**
    1.  Execute the `post_process_cibasis_files` task.
    2.  Query the temporary table `tmp_cibasis_data24_sed` within the stored procedure (requires modifying the procedure for testing or inspecting logs if possible).
*   **Pass/Fail Criterion:**
    *   The `processed_line` column for the inserted row in `tmp_cibasis_data24_sed` must be 'value1;value2'.

#### Test Case 2.8: Post-Processing - `sort -u -k 1` (Deduplication and Key Extraction)

*   **Purpose:** Verify `ROW_NUMBER()` and `SPLIT` correctly perform unique sorting by the first field.
*   **Setup:**
    1.  Insert rows into `your-gcp-project.isbert_dataset.cibasis_data24_ext`:
        *   `'KEY1;VAL_A;EXTRA'`
        *   `'KEY2;VAL_B;EXTRA'`
        *   `'KEY1;VAL_C;EXTRA'` (duplicate key, different value)
        *   `'KEY3;VAL_D;EXTRA'`
    2.  Ensure `your-gcp-project.isbert_dataset.cibasisprodukt_csv` is empty.
*   **Action:**
    1.  Execute the `post_process_cibasis_files` task.
    2.  Query the temporary table `tmp_cibasis_data24_sorted`.
*   **Pass/Fail Criterion:**
    *   `tmp_cibasis_data24_sorted` must contain exactly three rows (one for each unique key: 'KEY1', 'KEY2', 'KEY3').
    *   For 'KEY1', only one of the original lines ('KEY1;VAL_A;EXTRA' or 'KEY1;VAL_C;EXTRA') should be present, depending on the `ORDER BY` clause in `ROW_NUMBER()`. The `key_column` must be correctly extracted as 'KEY1', 'KEY2', 'KEY3'.

#### Test Case 2.9: Post-Processing - `join` Logic with `-o` and `-a` Options

*   **Purpose:** Verify the complex `join` logic, including field selection (`-o`) and unpaired line handling (`-a`), is correctly translated to BigQuery SQL joins.
*   **Setup:**
    1.  Populate `your-gcp-project.isbert_dataset.cibasis_data24_ext`, `cibasis_data96_ext`, `cibasis_fax_ext` with specific data to cover various join scenarios:
        *   `cibasis_data24_ext`: `'K1;A1;A2'`, `'K2;B1;B2'`, `'K5;E1;E2'`
        *   `cibasis_data96_ext`: `'K1;C1;C2'`, `'K3;D1;D2'`, `'K5;F1;F2'`
        *   `cibasis_fax_ext`: `'K1;G1;G2'`, `'K4;H1;H2'`
    2.  Ensure `your-gcp-project.isbert_dataset.cibasisprodukt_csv` is empty.
*   **Action:**
    1.  Execute the `post_process_cibasis_files` task.
    2.  Query the final target table `your-gcp-project.isbert_dataset.cibasisprodukt_csv`.
*   **Pass/Fail Criterion:**
    *   The `tmp_cibasis_24_96` (intermediate table for the first join) should contain:
        *   `f2_f1_key='K1', f1_f2='A2', f2_f2='C2'` (match K1 in 24 and 96)
        *   `f2_f1_key='K3', f1_f2=NULL, f2_f2='D2'` (only in 96, due to `RIGHT JOIN`)
        *   `f2_f1_key='K5', f1_f2='E2', f2_f2='F2'` (match K5 in 24 and 96)
    *   The final `your-gcp-project.isbert_dataset.cibasisprodukt_csv` should contain:
        *   `f2_f1_key='K1', f1_f2='A2', f2_f2='C2', f2_f2_from_fax='G2'` (match K1 in 2496 and fax)
        *   `f2_f1_key='K3', f1_f2=NULL, f2_f2='D2', f2_f2_from_fax=NULL` (only in 2496, due to `LEFT JOIN`)
        *   `f2_f1_key='K5', f1_f2='E2', f2_f2='F2', f2_f2_from_fax=NULL` (only in 2496, due to `LEFT JOIN`)
    *   The exact field extraction (`OFFSET(0)`, `OFFSET(1)`) must correctly map to the legacy `join -o` output.

---

### 3. External-System Replacements Tests

#### Test Case 3.1: BigQuery Source Table Reads

*   **Purpose:** Verify that the BigQuery stored procedure correctly reads data from `your-gcp-project.cds.ta_cntrct` and `your-gcp-project.pds.ta_bpri_com`.
*   **Setup:**
    1.  Populate `your-gcp-project.cds.ta_cntrct` and `your-gcp-project.pds.ta_bpri_com` with a small, controlled dataset (e.g., 10-20 rows that should pass all filters).
    2.  Ensure `your-gcp-project.sof.ta_bpr_basis_his` is empty.
*   **Action:**
    1.  Execute the `process_basisprodukt_data` task.
    2.  Query `your-gcp-project.sof.ta_bpr_basis_his`.
*   **Pass/Fail Criterion:**
    *   The `your-gcp-project.sof.ta_bpr_basis_his` table must be populated with exactly the expected number of rows and values, confirming successful reads from the source tables.

#### Test Case 3.2: External File Ingestion (for Post-Processing)

*   **Purpose:** Verify that the BigQuery external tables (`cibasis_data24_ext`, `cibasis_data96_ext`, `cibasis_fax_ext`) are correctly configured and can read data from Cloud Storage.
*   **Setup:**
    1.  Place sample `cibasis_data24.dat`, `cibasis_data96.dat`, `cibasis_fax.dat` files in the designated Cloud Storage bucket.
    2.  Ensure the BigQuery external tables are created and point to these files.
*   **Action:**
    1.  Directly query each external table in BigQuery:
        ```sql
        SELECT * FROM `your-gcp-project.isbert_dataset.cibasis_data24_ext` LIMIT 10;
        SELECT * FROM `your-gcp-project.isbert_dataset.cibasis_data96_ext` LIMIT 10;
        SELECT * FROM `your-gcp-project.isbert_dataset.cibasis_fax_ext` LIMIT 10;
        ```
*   **Pass/Fail Criterion:**
    *   Each query must return data, and the `line` column content must accurately reflect the lines in the corresponding Cloud Storage files.

#### Test Case 3.3: Job Control Logging

*   **Purpose:** Verify that job start/end events, status, and record counts are correctly logged to the `your-gcp-project.isbert_dataset.job_control` table.
*   **Setup:**
    1.  Ensure the `your-gcp-project.isbert_dataset.job_control` table exists.
    2.  **Crucially, uncomment and implement the actual BigQuery INSERT statements in the `log_job_start` and `log_job_end` tasks within the Airflow DAG.**
    3.  Ensure `your-gcp-project.sof.ta_bpr_basis_his` is empty.
*   **Action:**
    1.  Trigger the Airflow DAG `k_ausd_bp_ta_bpr_basis_his` with valid parameters.
    2.  Allow the DAG to complete successfully.
    3.  Query the `your-gcp-project.isbert_dataset.job_control` table.
*   **Pass/Fail Criterion:**
    *   There must be two entries for the specific DAG run: one with `status='RUNNING'` (from `log_job_start`) and one with `status='SUCCESS'` (from `log_job_end`).
    *   The `start_time`, `end_time`, `process_date`, `job_name`, `dag_run_id`, and `task_id` fields must be correctly populated.
    *   The `rows_processed` field in the 'SUCCESS' entry must accurately reflect the number of rows inserted into `your-gcp-project.sof.ta_bpr_basis_his` by the `process_basisprodukt_data` task.

---

### 4. Data Quality / Row Count / Schema Assertions

#### Test Case 4.1: Row Count Parity - Main Processing

*   **Purpose:** Verify that the number of rows inserted into `sof.ta_bpr_basis_his` by the migrated job matches the number of records reported by the legacy job.
*   **Setup:**
    1.  Execute the legacy `k_ausd_bp_ta_bpr_basis_his.ksh` script with a specific `Stichtag` and input data.
    2.  Capture the `v_records` value from the `$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_basis_his.tmp` file. This is the "golden row count."
    3.  Ensure `your-gcp-project.sof.ta_bpr_basis_his` is empty.
*   **Action:**
    1.  Trigger the Airflow DAG `k_ausd_bp_ta_bpr_basis_his` with the same `Stichtag` and input data.
    2.  Query the `your-gcp-project.sof.ta_bpr_basis_his` table to get the row count.
    3.  Alternatively, check the `rows_inserted` value returned by the `d_ausd_bp_ta_bpr_basis_his` stored procedure (if captured) or logged in `job_control`.
*   **Pass/Fail Criterion:**
    *   The row count in `your-gcp-project.sof.ta_bpr_basis_his` (or `rows_inserted` from the stored procedure/log) must be **identical** to the "golden row count" from the legacy system.

#### Test Case 4.2: Schema Parity - `sof.ta_bpr_basis_his`

*   **Purpose:** Verify that the schema (column names, data types, nullability) of the target table `your-gcp-project.sof.ta_bpr_basis_his` in BigQuery precisely matches the legacy Oracle table.
*   **Setup:**
    1.  Obtain the DDL for the legacy Oracle `sof.ta_bpr_basis_his` table.
    2.  Obtain the DDL for the BigQuery `your-gcp-project.sof.ta_bpr_basis_his` table.
*   **Action:** Compare the two DDLs.
*   **Pass/Fail Criterion:**
    *   All column names must be identical (case-sensitivity might need to be considered if Oracle was case-sensitive).
    *   Data types must be functionally equivalent (e.g., Oracle `VARCHAR2(X)` to BigQuery `STRING`, Oracle `NUMBER` to BigQuery `INT64` or `BIGNUMERIC`, Oracle `DATE` to BigQuery `DATE`).
    *   Nullability constraints (`NOT NULL`) must match.

#### Test Case 4.3: Data Integrity - `NOT NULL` Constraints

*   **Purpose:** Verify that `NOT NULL` constraints defined in the BigQuery DDL for `sof.ta_bpr_basis_his` are enforced and no NULL values are inserted into these columns.
*   **Setup:**
    1.  Ensure `your-gcp-project.sof.ta_bpr_basis_his` is empty.
    2.  If possible, prepare source data in `cds.ta_cntrct` or `pds.ta_bpri_com` that *would* result in a NULL for `cntrct_id` or `bpr_id` if the `NOT NULL` constraint was not respected (e.g., a `cntrct_id` that is NULL in `ta_cntrct` but somehow passes filters, or a `bpr_id` that is NULL in `ta_bpri_com`).
*   **Action:**
    1.  Execute the `process_basisprodukt_data` task.
    2.  Run the following BigQuery SQL assertion:
        ```sql
        SELECT
            COUNT(*)
        FROM
            `your-gcp-project.sof.ta_bpr_basis_his`
        WHERE
            cntrct_id IS NULL OR bpr_id IS NULL;
        ```
*   **Pass/Fail Criterion:**
    *   The query must return `0`. Any non-zero result indicates a violation of the `NOT NULL` constraint or an unexpected NULL value.

#### Test Case 4.4: Idempotency (Restartability)

*   **Purpose:** Verify that running the migrated job multiple times with the same parameters produces the same result, confirming the `TRUNCATE TABLE` and reload behavior.
*   **Setup:**
    1.  Populate `your-gcp-project.cds.ta_cntrct` and `your-gcp-project.pds.ta_bpri_com` with a fixed dataset.
    2.  Ensure `your-gcp-project.sof.ta_bpr_basis_his` is empty.
*   **Action:**
    1.  Trigger the Airflow DAG `k_ausd_bp_ta_bpr_basis_his` with specific parameters (e.g., `Stichtag='01012023'`).
    2.  After successful completion, capture the content of `your-gcp-project.sof.ta_bpr_basis_his` (e.g., into a temporary table or file).
    3.  Trigger the Airflow DAG *again* with the exact same parameters.
    4.  After successful completion, capture the content of `your-gcp-project.sof.ta_bpr_basis_his` again.
*   **Pass/Fail Criterion:**
    *   The content of `your-gcp-project.sof.ta_bpr_basis_his` after the second run must be **identical** to the content after the first run. The `rows_processed` count should also be the same for both runs.

#### Test Case 4.5: Error Handling - Stored Procedure Failure

*   **Purpose:** Verify that errors within the BigQuery stored procedure are caught, logged, and propagate to cause the Airflow DAG to fail.
*   **Setup:**
    1.  Temporarily modify the `your-gcp-project.sof.d_ausd_bp_ta_bpr_basis_his` stored procedure to force an error (e.g., `SELECT 1/0;` or attempt to insert a string into an `INT64` column).
    2.  Ensure the `log_job_end` task is configured to log on failure.
*   **Action:**
    1.  Trigger the Airflow DAG `k_ausd_bp_ta_bpr_basis_his` with valid parameters.
    2.  Observe the DAG run status and logs.
    3.  Query the `your-gcp-project.isbert_dataset.job_control` table.
*   **Pass/Fail Criterion:**
    *   The `process_basisprodukt_data` task must fail.
    *   The overall Airflow DAG run must be marked as 'failed'.
    *   An entry in `your-gcp-project.isbert_dataset.job_control` must exist for the DAG run with `status='FAILED'` and a relevant `error_message` from the BigQuery procedure.

---