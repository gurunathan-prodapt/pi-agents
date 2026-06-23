As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the migration of `r_ausd_bp_ta_apn_vertrag.ksh` to Google Cloud Platform. These tests aim to ensure behavioral equivalence across output parity, transformation correctness, external system replacements, and data quality.

Given that `k_ausd_bp_ta_apn_vertrag.sql` is currently a placeholder, tests related to its core logic (transformation correctness, data quality, and row count parity) will assume a future implementation that adheres to the described filtering rules. For these tests, mock source data will be required.

All tests assume the BigQuery DDLs for `job_registry`, `job_log`, `fos_contract_cache`, and the stored procedures `ausd_bp_ta_apn_vertrag_wrapper`, `k_ausd_bp_ta_apn_vertrag` have been deployed to `your_gcp_project.your_bq_dataset`.

---

## Migration Validation Tests: `r_ausd_bp_ta_apn_vertrag.ksh` to BigQuery

### 1. Output Parity & Wrapper Behavior

These tests focus on the `ausd_bp_ta_apn_vertrag_wrapper` BigQuery Stored Procedure, verifying its parameter handling, defaulting logic, and logging behavior against the legacy `r_ausd_bp_ta_apn_vertrag.ksh` script.

#### Test Case 1.1: Default Stichtag and Wiederanlaufwert

*   **Purpose**: Verify that when `p_stichtag` and `p_wiederanlaufWert` are not provided (or are `NULL`/empty strings), the wrapper correctly defaults `v_stichtag` to `CURRENT_DATE()` (formatted as `DDMMYYYY`) and `v_restart_value` to `'0'`.
*   **Setup**:
    1.  Ensure `job_registry` and `job_log` tables are empty or truncated.
    2.  Note the current system date in `DDMMYYYY` format (e.g., `01012023`).
*   **Action**: Execute the `ausd_bp_ta_apn_vertrag_wrapper` procedure without passing any parameters for `p_stichtag` and `p_wiederanlaufWert`.
    ```sql
    CALL `your_gcp_project.your_bq_dataset.ausd_bp_ta_apn_vertrag_wrapper`(NULL, NULL);
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**:
        1.  One entry exists in `your_gcp_project.your_bq_dataset.job_registry` with `status = 'SUCCESS'`.
        2.  The `parameters` JSON in `job_registry` shows `p_stichtag_actual` matching the current system date (`DDMMYYYY`) and `p_wiederanlaufWert_actual` as `'0'`.
        3.  `your_gcp_project.your_bq_dataset.job_log` contains `INFO` entries confirming the job start and successful completion, reflecting the defaulted parameters.
    *   **Fail**: Any deviation from the above, such as incorrect default values, job failure, or missing log entries.

#### Test Case 1.2: Explicit Stichtag and Wiederanlaufwert

*   **Purpose**: Verify that explicitly provided `p_stichtag` and `p_wiederanlaufWert` are correctly received, validated, and passed to the core procedure.
*   **Setup**:
    1.  Ensure `job_registry` and `job_log` tables are empty or truncated.
    2.  Choose specific valid values, e.g., `p_stichtag = '15032023'`, `p_wiederanlaufWert = '12345'`.
*   **Action**: Execute the `ausd_bp_ta_apn_vertrag_wrapper` procedure with the chosen parameters.
    ```sql
    CALL `your_gcp_project.your_bq_dataset.ausd_bp_ta_apn_vertrag_wrapper`('15032023', '12345');
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**:
        1.  One entry exists in `your_gcp_project.your_bq_dataset.job_registry` with `status = 'SUCCESS'`.
        2.  The `parameters` JSON in `job_registry` shows `p_stichtag_actual` as `'15032023'` and `p_wiederanlaufWert_actual` as `'12345'`.
        3.  `your_gcp_project.your_bq_dataset.job_log` contains `INFO` entries confirming the job start and successful completion, reflecting the explicit parameters.
    *   **Fail**: Any deviation from the above, such as incorrect parameter values in logs, job failure, or missing log entries.

#### Test Case 1.3: Stichtag Validation (Missing/Empty after defaulting)

*   **Purpose**: Verify that if `p_stichtag` cannot be determined (e.g., `CURRENT_DATE()` fails or is `NULL` in an edge case, or an empty string is passed and `CURRENT_DATE()` is also `NULL`), the wrapper correctly identifies this as an error and fails the job. This simulates the `pruefeParameterGesetzt Stichtag p_stichtag` behavior.
*   **Setup**:
    1.  Ensure `job_registry` and `job_log` tables are empty or truncated.
    2.  (Hypothetical) Configure the environment such that `CURRENT_DATE()` or `FORMAT_DATE` returns `NULL` or an empty string for `v_sysdate` if possible, or simply pass an empty string for `p_stichtag` and ensure `v_sysdate` is also empty. For BigQuery, `CURRENT_DATE()` will always return a date, so this test primarily covers explicit empty string input.
*   **Action**: Execute the `ausd_bp_ta_apn_vertrag_wrapper` procedure with an empty string for `p_stichtag`.
    ```sql
    CALL `your_gcp_project.your_bq_dataset.ausd_bp_ta_apn_vertrag_wrapper`('', NULL);
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**:
        1.  The procedure call `SIGNAL`s an error (e.g., `SQLSTATE '45000'`).
        2.  One entry exists in `your_gcp_project.your_bq_dataset.job_registry` with `status = 'FAILED'`.
        3.  The `error_message` in `job_registry` contains text similar to `'ERROR: Stichtag parameter is missing or empty after defaulting.'`.
        4.  `your_gcp_project.your_bq_dataset.job_log` contains an `ERROR` entry with the corresponding error message.
    *   **Fail**: The job completes successfully, or fails with a different error, or logging is incorrect.

#### Test Case 1.4: Logging of Job Start/End and Error Trapping

*   **Purpose**: Verify that `job_registry` and `job_log` accurately record job start, end, and status for both successful and failed executions, and that the error message is captured. This replaces the `DWMSG_*` functions and `trap` logic.
*   **Setup**:
    1.  Ensure `job_registry` and `job_log` tables are empty or truncated.
    2.  (For failure scenario) Temporarily modify `k_ausd_bp_ta_apn_vertrag` to `SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated core logic error.';` at its beginning.
*   **Action**:
    1.  **Scenario A (Success)**: Execute the wrapper with valid parameters (e.g., `CALL your_gcp_project.your_bq_dataset.ausd_bp_ta_apn_vertrag_wrapper('01012023', '0');`).
    2.  **Scenario B (Failure)**: Execute the wrapper with valid parameters, but with the modified `k_ausd_bp_ta_apn_vertrag` (e.g., `CALL your_gcp_project.your_bq_dataset.ausd_bp_ta_apn_vertrag_wrapper('01012023', '0');`).
*   **Pass/Fail Criterion**:
    *   **Pass (Scenario A)**:
        1.  One `job_registry` entry with `status = 'SUCCESS'`, `start_timestamp` and `end_timestamp` populated.
        2.  `job_log` contains `INFO` entries for job start and successful completion, linked to the `job_id`.
    *   **Pass (Scenario B)**:
        1.  The wrapper call `SIGNAL`s an error.
        2.  One `job_registry` entry with `status = 'FAILED'`, `start_timestamp` and `end_timestamp` populated, and `error_message` containing the simulated error.
        3.  `job_log` contains an `INFO` entry for job start and an `ERROR` entry for the failure, linked to the `job_id`.
    *   **Fail**: Any deviation from the above, including incorrect timestamps, status, or missing/malformed log entries.

### 2. Transformation Correctness

These tests verify the data filtering and manipulation logic described for the core processing, assuming `k_ausd_bp_ta_apn_vertrag` will implement these rules.

#### Test Case 2.1: Date Filtering Logic (`Gueltig_von <= Stichtag < Gueltig_bis AND LADEDATUM < Stichtag`)

*   **Purpose**: Verify that records are filtered based on the specified date conditions.
*   **Setup**:
    1.  Create a mock source table `your_gcp_project.your_bq_dataset.dwh_contract_cache` with columns `contract_id`, `gueltig_von_date`, `gueltig_bis_date`, `laden_datum`.
    2.  Populate `dwh_contract_cache` with diverse data, including records that *should* pass and *should not* pass for a `Stichtag` of `01012023`.
        *   **Pass**: `gueltig_von_date = '2022-01-01'`, `gueltig_bis_date = '2023-01-02'`, `laden_datum = '2022-12-31'`
        *   **Fail (Gueltig_von > Stichtag)**: `gueltig_von_date = '2023-01-02'`, `gueltig_bis_date = '2024-01-01'`, `laden_datum = '2022-12-31'`
        *   **Fail (Stichtag >= Gueltig_bis)**: `gueltig_von_date = '2022-01-01'`, `gueltig_bis_date = '2023-01-01'`, `laden_datum = '2022-12-31'`
        *   **Fail (LADEDATUM >= Stichtag)**: `gueltig_von_date = '2022-01-01'`, `gueltig_bis_date = '2024-01-01'`, `laden_datum = '2023-01-01'`
    3.  Ensure `fos_contract_cache` is empty.
*   **Action**: Execute the wrapper with `p_stichtag = '01012023'` and `p_wiederanlaufWert = '0'`.
    ```sql
    CALL `your_gcp_project.your_bq_dataset.ausd_bp_ta_apn_vertrag_wrapper`('01012023', '0');
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**: `your_gcp_project.your_bq_dataset.fos_contract_cache` contains *only* the records expected to pass the date filters.
    *   **Fail**: Any record that should have been filtered out is present, or a record that should have passed is missing.

#### Test Case 2.2: Wiederanlaufwert Filtering Logic (`DWH_VERTRAG_ID > Wiederanlaufwert`)

*   **Purpose**: Verify that records are filtered based on the `DWH_VERTRAG_ID` (mapped to `contract_id`) being greater than `p_wiederanlaufWert`.
*   **Setup**:
    1.  Populate `your_gcp_project.your_bq_dataset.dwh_contract_cache` with data where all date conditions (from Test 2.1) would pass for `Stichtag = '01012023'`.
    2.  Include `contract_id` values both above and below a chosen `Wiederanlaufwert` (e.g., `100`).
        *   **Pass**: `contract_id = '101'`, `contract_id = '200'`
        *   **Fail**: `contract_id = '99'`, `contract_id = '100'`
    3.  Ensure `fos_contract_cache` is empty.
*   **Action**: Execute the wrapper with `p_stichtag = '01012023'` and `p_wiederanlaufWert = '100'`.
    ```sql
    CALL `your_gcp_project.your_bq_dataset.ausd_bp_ta_apn_vertrag_wrapper`('01012023', '100');
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**: `your_gcp_project.your_bq_dataset.fos_contract_cache` contains *only* the records where `contract_id` is greater than `100`.
    *   **Fail**: Any record that should have been filtered out is present, or a record that should have passed is missing.

#### Test Case 2.3: Combined Filtering

*   **Purpose**: Verify that both date and `Wiederanlaufwert` filters are applied correctly in conjunction.
*   **Setup**:
    1.  Populate `your_gcp_project.your_bq_dataset.dwh_contract_cache` with data that requires both filters to be applied.
        *   Record A: Passes date, passes `Wiederanlaufwert`. (Expected in output)
        *   Record B: Fails date, passes `Wiederanlaufwert`. (Not expected)
        *   Record C: Passes date, fails `Wiederanlaufwert`. (Not expected)
        *   Record D: Fails both. (Not expected)
    2.  Choose `Stichtag = '01012023'` and `Wiederanlaufwert = '100'`.
    3.  Ensure `fos_contract_cache` is empty.
*   **Action**: Execute the wrapper with `p_stichtag = '01012023'` and `p_wiederanlaufWert = '100'`.
    ```sql
    CALL `your_gcp_project.your_bq_dataset.ausd_bp_ta_apn_vertrag_wrapper`('01012023', '100');
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**: `your_gcp_project.your_bq_dataset.fos_contract_cache` contains *only* Record A.
    *   **Fail**: Any other records are present, or Record A is missing.

#### Test Case 2.4: NULL Handling in Date Filters (e.g., `Gueltig_bis` NULL)

*   **Purpose**: Verify how `NULL` values in date fields, specifically `Gueltig_bis`, are handled by the filter `Stichtag < Gueltig_bis`. In standard SQL, `NULL` comparisons often result in `UNKNOWN`, effectively filtering out the row. This test confirms this behavior or any specific handling.
*   **Setup**:
    1.  Populate `your_gcp_project.your_bq_dataset.dwh_contract_cache` with:
        *   Record A: `gueltig_von_date = '2022-01-01'`, `gueltig_bis_date = NULL`, `laden_datum = '2022-12-31'`, `contract_id = '1'`
        *   Record B: `gueltig_von_date = '2022-01-01'`, `gueltig_bis_date = '2023-01-02'`, `laden_datum = '2022-12-31'`, `contract_id = '2'`
    2.  Choose `Stichtag = '01012023'`.
    3.  Ensure `fos_contract_cache` is empty.
*   **Action**: Execute the wrapper with `p_stichtag = '01012023'` and `p_wiederanlaufWert = '0'`.
    ```sql
    CALL `your_gcp_project.your_bq_dataset.ausd_bp_ta_apn_vertrag_wrapper`('01012023', '0');
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**: `your_gcp_project.your_bq_dataset.fos_contract_cache` contains *only* Record B. Record A (with `NULL` `gueltig_bis_date`) should be excluded, as `Stichtag < NULL` evaluates to `FALSE`/`UNKNOWN`.
    *   **Fail**: Record A is present, or Record B is missing.

### 3. External-System Replacements

The design document states no external system dependencies for the wrapper. The "external" dependencies are the DWH source and FOS target, which are now internal BigQuery tables. The replacement of shell utilities is covered by logging tests.

#### Test Case 3.1: DWH Source Data Availability (Prerequisite Check)

*   **Purpose**: Confirm that the necessary DWH source tables are ingested into BigQuery and are accessible to the `k_ausd_bp_ta_apn_vertrag` procedure. This is a prerequisite for the job to function.
*   **Setup**:
    1.  Identify the specific source tables (e.g., `dwh_contract_cache`) that `k_ausd_bp_ta_apn_vertrag` will read from.
*   **Action**: Query BigQuery metadata and sample data from the identified source tables.
    ```sql
    SELECT table_name, row_count FROM `your_gcp_project.your_bq_dataset.__TABLES__` WHERE table_name = 'dwh_contract_cache';
    SELECT * FROM `your_gcp_project.your_bq_dataset.dwh_contract_cache` LIMIT 10;
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**: The source tables exist, have the expected schema, and contain recent, representative data. The `k_ausd_bp_ta_apn_vertrag` procedure can successfully query these tables (e.g., a simple `SELECT COUNT(*) FROM dwh_contract_cache` within the procedure does not error).
    *   **Fail**: Source tables are missing, have incorrect schemas, are empty when they shouldn't be, or are inaccessible.

### 4. Data Quality / Row Count / Schema Assertions

These tests ensure the integrity and correctness of the output data in `fos_contract_cache`.

#### Test Case 4.1: Target Table Schema Conformance

*   **Purpose**: Verify that the `fos_contract_cache` table schema matches the expected structure for "Forderungsscoring" and the DDL provided.
*   **Setup**: Ensure `your_gcp_project.your_bq_dataset.fos_contract_cache` has been created using the provided DDL.
*   **Action**: Query BigQuery's `INFORMATION_SCHEMA` for the table schema.
    ```sql
    SELECT column_name, data_type, is_nullable
    FROM `your_gcp_project.your_bq_dataset.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'fos_contract_cache'
    ORDER BY ordinal_position;
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**: The queried schema (column names, data types, nullability) precisely matches the `fos_contract_cache.sql` DDL and any documented requirements from the "Forderungsscoring" team.
    *   **Fail**: Any mismatch in column names, data types, or nullability.

#### Test Case 4.2: Row Count Parity (End-to-End)

*   **Purpose**: Verify that the number of rows loaded into `fos_contract_cache` matches the legacy job's output for identical inputs.
*   **Setup**:
    1.  Run the *legacy* `r_ausd_bp_ta_apn_vertrag.ksh` script with a specific `Stichtag` and `Wiederanlaufwert` (e.g., `01012023`, `0`).
    2.  Record the exact row count produced by the legacy job in its target "FOS-Tabelle".
    3.  Create an identical mock `your_gcp_project.your_bq_dataset.dwh_contract_cache` in BigQuery that would produce the same output as the legacy source for the chosen parameters.
    4.  Ensure `fos_contract_cache` is empty.
*   **Action**: Execute the migrated wrapper with the same `Stichtag` and `Wiederanlaufwert`.
    ```sql
    CALL `your_gcp_project.your_bq_dataset.ausd_bp_ta_apn_vertrag_wrapper`('01012023', '0');
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**: The `COUNT(*)` from `your_gcp_project.your_bq_dataset.fos_contract_cache` exactly matches the row count recorded from the legacy job.
    *   **Fail**: The row counts differ.

#### Test Case 4.3: Data Content Parity (End-to-End)

*   **Purpose**: Verify that the actual data content in `fos_contract_cache` is identical to the legacy job's output for identical inputs.
*   **Setup**:
    1.  Run the *legacy* `r_ausd_bp_ta_apn_vertrag.ksh` script with a specific `Stichtag` and `Wiederanlaufwert`.
    2.  Extract the full output of the legacy job from its target "FOS-Tabelle" into a canonical format (e.g., CSV, sorted by primary key).
    3.  Create an identical mock `your_gcp_project.your_bq_dataset.dwh_contract_cache` in BigQuery.
    4.  Ensure `fos_contract_cache` is empty.
*   **Action**: Execute the migrated wrapper with the same `Stichtag` and `Wiederanlaufwert`.
    ```sql
    CALL `your_gcp_project.your_bq_dataset.ausd_bp_ta_apn_vertrag_wrapper`('01012023', '0');
    ```
    Then, extract the full output from `your_gcp_project.your_bq_dataset.fos_contract_cache` into the same canonical format.
*   **Pass/Fail Criterion**:
    *   **Pass**: A byte-for-byte or record-by-record comparison of the extracted legacy and migrated output files shows no differences (after sorting and handling any minor format variations like date representations).
    *   **Fail**: Any differences in data content are found.

#### Test Case 4.4: Idempotency (DELETE + INSERT / MERGE behavior)

*   **Purpose**: Verify that running the job multiple times with the same parameters produces the same final state in `fos_contract_cache`, implying correct `DELETE` + `INSERT` or `MERGE` logic within `k_ausd_bp_ta_apn_vertrag`.
*   **Setup**:
    1.  Populate `your_gcp_project.your_bq_dataset.dwh_contract_cache` with a representative dataset.
    2.  Ensure `fos_contract_cache` is empty.
*   **Action**:
    1.  Execute the wrapper with specific parameters (e.g., `p_stichtag = '01012023'`, `p_wiederanlaufWert = '0'`).
    2.  Record the `COUNT(*)` and a checksum/hash of the data in `fos_contract_cache`.
    3.  Execute the wrapper *again* with the exact same parameters.
*   **Pass/Fail Criterion**:
    *   **Pass**: The `COUNT(*)` and data checksum/hash of `fos_contract_cache` after the second run are identical to those recorded after the first run.
    *   **Fail**: Row counts differ, or data content has changed (e.g., duplicates introduced, existing data modified incorrectly).

#### Test Case 4.5: Data Quality - No Duplicates

*   **Purpose**: Ensure no duplicate records are introduced into `fos_contract_cache` for a given `Stichtag` and primary key combination.
*   **Setup**:
    1.  Populate `your_gcp_project.your_bq_dataset.dwh_contract_cache` with data that, after transformation, should result in unique records in `fos_contract_cache`.
    2.  Ensure `fos_contract_cache` is empty.
*   **Action**: Execute the wrapper with specific parameters.
    ```sql
    CALL `your_gcp_project.your_bq_dataset.ausd_bp_ta_apn_vertrag_wrapper`('01012023', '0');
    ```
    Then, query `fos_contract_cache` for duplicate primary keys (e.g., `contract_id` and `stichtag_processed`).
    ```sql
    SELECT contract_id, stichtag_processed, COUNT(*)
    FROM `your_gcp_project.your_bq_dataset.fos_contract_cache`
    GROUP BY contract_id, stichtag_processed
    HAVING COUNT(*) > 1;
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**: The query returns zero rows, indicating no duplicate primary key combinations.
    *   **Fail**: The query returns one or more rows, indicating duplicate records.

#### Test Case 4.6: Data Quality - Not Null Constraints (Implicit)

*   **Purpose**: Verify that critical fields in `fos_contract_cache` (e.g., `contract_id`, `product_type`) are not `NULL` if they were never `NULL` in the legacy output and are expected to be mandatory.
*   **Setup**:
    1.  Populate `your_gcp_project.your_bq_dataset.dwh_contract_cache` with data, including some records where critical fields might be `NULL` in the source to test robustness (if the legacy system allowed this).
    2.  Ensure `fos_contract_cache` is empty.
*   **Action**: Execute the wrapper with specific parameters.
    ```sql
    CALL `your_gcp_project.your_bq_dataset.ausd_bp_ta_apn_vertrag_wrapper`('01012023', '0');
    ```
    Then, query `fos_contract_cache` for `NULL` values in expected non-nullable columns.
    ```sql
    SELECT COUNT(*) FROM `your_gcp_project.your_bq_dataset.fos_contract_cache` WHERE contract_id IS NULL;
    SELECT COUNT(*) FROM `your_gcp_project.your_bq_dataset.fos_contract_cache` WHERE product_type IS NULL;
    -- Add more critical columns as needed
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**: All queries for `NULL` values in critical columns return `0`.
    *   **Fail**: Any query returns a count greater than `0`, indicating unexpected `NULL` values.