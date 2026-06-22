As a senior data-migration QA engineer, I've reviewed the migration design and the provided generated Airflow DAG and BigQuery SQL. The following test cases are designed to validate the migration of `r_ausd_rechempf.ksh` to its BigQuery/Airflow equivalent, covering output parity, transformation correctness, external system replacements, and data quality.

A critical observation during this review is that the provided BigQuery SQL for `execute_bq_load_main_script` **does not implement the `p_wiederanlaufWert` (restart value) logic** as described in the migration design document and the legacy script's `usage` text. The `restart_value` parameter is declared but unused in the SQL. This will be highlighted in the relevant test case.

---

## Migration Validation Tests for `r_ausd_rechempf.ksh`

### Prerequisites for all Tests:

1.  **Legacy Environment Access**: Access to the legacy system where `r_ausd_rechempf.ksh` can be executed, and its output (logs, database tables) can be inspected.
2.  **Migrated Environment Deployment**: The Airflow DAG `isbert_r_ausd_rechempf_dag` is deployed and runnable in the target GCP environment. All BigQuery source tables (`carmen_source_dataset.ta_means_of_payment`, `carmen_source_dataset.ta_bank`, `carmen_source_dataset.ta_bank_international`, `fos_source_dataset.ta_e_regulierer`, `fos_source_dataset.ta_e_reach_re`, `fos_source_dataset.ta_e_business_re`, `dwh_source_dataset.vi_s_ibasisprodukt`) are populated with representative data.
3.  **Data Synchronization**: For output parity tests, the source data in the legacy Oracle database and the target BigQuery tables must be identical at the start of each test run.
4.  **BigQuery Access**: Permissions to query and inspect BigQuery tables in the target project and datasets.

---

### 1. Output Parity Tests

These tests ensure that for the same inputs, the migrated job produces the exact same final output data as the legacy job.

#### Test Case 1.1: Default Stichtag and No Restart Value

*   **Purpose**: Verify that the migrated DAG produces identical final output tables (`sof_ta_p_rech_empf`, `sof_ta_p_d1_vpn`) as the legacy script when executed with default parameters (current system date as `Stichtag`, `restart_value=0`).
*   **Setup**:
    *   Ensure source data in legacy Oracle and BigQuery is identical.
    *   Ensure the current system date on both environments is the same or controlled for the test.
    *   Target tables (`fos_target_dataset.sof_ta_p_rech_empf`, `fos_target_dataset.sof_ta_p_d1_vpn`) are empty or truncated before execution.
*   **Action**:
    1.  **Legacy**: Execute `r_ausd_rechempf.ksh` without any command-line arguments:
        ```bash
        ./r_ausd_rechempf.ksh
        ```
        Record the system date used by the script (from logs) and capture the full data from the legacy target tables (e.g., `FOS_P_RECH_EMPF`, `FOS_P_D1_VPN`).
    2.  **Migrated**: Trigger the Airflow DAG `isbert_r_ausd_rechempf_dag` without specifying `stichtag_ddmmyyyy` or `restart_value` parameters.
        ```python
        # Example Airflow CLI command to trigger DAG
        airflow dags trigger isbert_r_ausd_rechempf_dag -c '{}'
        ```
        Capture the full data from the BigQuery target tables (`fos_target_dataset.sof_ta_p_rech_empf`, `fos_target_dataset.sof_ta_p_d1_vpn`).
*   **Pass/Fail Criterion**:
    *   The row counts for `sof_ta_p_rech_empf` (BigQuery) and `FOS_P_RECH_EMPF` (Legacy) are identical.
    *   The row counts for `sof_ta_p_d1_vpn` (BigQuery) and `FOS_P_D1_VPN` (Legacy) are identical.
    *   A deep data comparison (e.g., using `MINUS` in SQL or a data diff tool) confirms that all columns and rows in the BigQuery target tables are identical to their legacy counterparts.

    ```sql
    -- Example BigQuery assertion for sof_ta_p_rech_empf (assuming legacy data is loaded into a temp BQ table for comparison)
    SELECT
        COUNT(*)
    FROM
        (SELECT * FROM `your-gcp-project-id.fos_target.sof_ta_p_rech_empf` EXCEPT DISTINCT SELECT * FROM `your-gcp-project-id.legacy_comparison_dataset.fos_p_rech_empf_legacy_snapshot`)
    UNION ALL
    SELECT
        COUNT(*)
    FROM
        (SELECT * FROM `your-gcp-project-id.legacy_comparison_dataset.fos_p_rech_empf_legacy_snapshot` EXCEPT DISTINCT SELECT * FROM `your-gcp-project-id.fos_target.sof_ta_p_rech_empf`);
    -- Expected result: 0 rows, indicating perfect match.
    ```

#### Test Case 1.2: Specific Stichtag and No Restart Value

*   **Purpose**: Verify output parity when a specific `Stichtag` is provided, simulating a historical run or a specific snapshot date.
*   **Setup**:
    *   Identical source data in legacy Oracle and BigQuery.
    *   Choose a `Stichtag` (e.g., `01012022`) that is relevant to the historical data in the source tables.
    *   Target tables are empty or truncated before execution.
*   **Action**:
    1.  **Legacy**: Execute `r_ausd_rechempf.ksh` with the chosen `Stichtag`:
        ```bash
        ./r_ausd_rechempf.ksh -s 01012022
        ```
        Capture the full data from the legacy target tables.
    2.  **Migrated**: Trigger the Airflow DAG with the `stichtag_ddmmyyyy` parameter:
        ```python
        airflow dags trigger isbert_r_ausd_rechempf_dag -c '{"stichtag_ddmmyyyy": "01012022"}'
        ```
        Capture the full data from the BigQuery target tables.
*   **Pass/Fail Criterion**:
    *   Identical row counts for both final target tables.
    *   Deep data comparison confirms all columns and rows in BigQuery target tables are identical to their legacy counterparts.

#### Test Case 1.3: Specific Stichtag and Restart Value (CRITICAL DISCREPANCY)

*   **Purpose**: Verify output parity when both `Stichtag` and `Wiederanlaufwert` are provided. This tests the incremental/restart logic.
*   **Setup**:
    *   Identical source data in legacy Oracle and BigQuery.
    *   Choose a `Stichtag` (e.g., `01012022`).
    *   Choose a `Wiederanlaufwert` (e.g., `1000`) such that some `DWH_VERTRAG_ID`s are greater than this value, and some are less.
    *   **Crucially**: For the legacy system, ensure there are existing records in the target table that would be affected by the deletion logic (`DWH_VERTRAG_ID >= Wiederanlaufwert`).
    *   Target tables are in a state where the restart logic can be observed (e.g., pre-populated with some data, then run with restart value).
*   **Action**:
    1.  **Legacy**: Execute `r_ausd_rechempf.ksh` with both parameters:
        ```bash
        ./r_ausd_rechempf.ksh -s 01012022 -l 1000
        ```
        Capture the full data from the legacy target tables.
    2.  **Migrated**: Trigger the Airflow DAG with both parameters:
        ```python
        airflow dags trigger isbert_r_ausd_rechempf_dag -c '{"stichtag_ddmmyyyy": "01012022", "restart_value": 1000}'
        ```
        Capture the full data from the BigQuery target tables.
*   **Pass/Fail Criterion**:
    *   **EXPECTED FAILURE**: Based on the provided BigQuery SQL, the `restart_value` parameter is declared but not used in any `WHERE` clause or `DELETE` statement. Therefore, the BigQuery job will likely perform a full load based on `Stichtag` without applying the restart logic.
    *   The row counts and data content for `sof_ta_p_rech_empf` and `sof_ta_p_d1_vpn` in BigQuery will **not** match the legacy output if the legacy script correctly implemented the restart logic.
    *   **Action Required**: If this test fails, it indicates a functional gap in the migrated BigQuery SQL. The SQL needs to be updated to incorporate the `restart_value` for filtering and potentially deletion/insertion logic as described in the design document.

---

### 2. Transformation Correctness Tests

These tests focus on specific logic within the BigQuery SQL, ensuring joins, filters, aggregations, and data type handling are correct.

#### Test Case 2.1: Date Filtering Logic

*   **Purpose**: Verify the date filtering logic (`insert_at <= stichtag_date AND (modified_at IS NULL OR modified_at > stichtag_date)` and `valid_from <= stichtag_date AND (valid_to IS NULL OR valid_to > stichtag_date)`) is correctly applied to source tables.
*   **Setup**:
    *   Populate `carmen_source_dataset.ta_means_of_payment`, `carmen_source_dataset.ta_bank`, `carmen_source_dataset.ta_bank_international` with mock data covering various date scenarios relative to a chosen `Stichtag` (e.g., records valid before, during, and after `Stichtag`; records with `NULL` `modified_at`/`valid_to`).
    *   Example data for `ta_means_of_payment` for `Stichtag = '2023-01-01'`:
        *   Record A: `insert_at = '2022-12-01'`, `modified_at = NULL`, `valid_from = '2022-11-01'`, `valid_to = NULL` (Should be included)
        *   Record B: `insert_at = '2022-12-01'`, `modified_at = '2023-01-02'`, `valid_from = '2022-11-01'`, `valid_to = NULL` (Should be included)
        *   Record C: `insert_at = '2022-12-01'`, `modified_at = '2022-12-15'`, `valid_from = '2022-11-01'`, `valid_to = NULL` (Should be excluded by `modified_at`)
        *   Record D: `insert_at = '2023-01-02'`, `modified_at = NULL`, `valid_from = '2022-11-01'`, `valid_to = NULL` (Should be excluded by `insert_at`)
        *   Record E: `insert_at = '2022-12-01'`, `modified_at = NULL`, `valid_from = '2022-11-01'`, `valid_to = '2022-12-31'` (Should be excluded by `valid_to`)
*   **Action**:
    1.  Trigger the Airflow DAG with a specific `stichtag_ddmmyyyy` (e.g., `01012023`).
    2.  Query the intermediate tables (`fos_target_dataset.sof_ta_means_of_pay`, `fos_target_dataset.sof_ta_bank`) and verify their contents against expected results based on the mock data and date logic.
*   **Pass/Fail Criterion**:
    *   The intermediate tables contain exactly the records expected by the date filtering logic.

#### Test Case 2.2: Join Logic and `sof_ta_bank` UNION ALL

*   **Purpose**: Verify the correctness of `JOIN` conditions across `sof_ta_means_of_pay`, `sof_ta_bank`, `ta_e_regulierer`, `ta_e_reach_re`, `ta_e_business_re`, and the `UNION ALL` in `sof_ta_bank`.
*   **Setup**:
    *   Populate source tables with mock data to cover various join scenarios:
        *   Matching `MP.BANK_ID_ACC = BA.BANK_ID`
        *   Matching `mp.BANK_INTERNATIONAL_ID = ba.BANK_INTERNATIONAL_ID`
        *   Records that should join, and records that should not.
        *   Data for `ta_bank` and `ta_bank_international` to test the `UNION ALL` and `NULL` assignments for `BIC`/`BANK_INTERNATIONAL_ID`.
*   **Action**:
    1.  Trigger the Airflow DAG.
    2.  Query `fos_target_dataset.sof_ta_bank_verb`, `fos_target_dataset.sof_ta_bank_zuord`, and `fos_target_dataset.sof_ta_p_rech_empf`.
*   **Pass/Fail Criterion**:
    *   The joined data in `sof_ta_bank_verb`, `sof_ta_bank_zuord`, and `sof_ta_p_rech_empf` matches the expected results based on the mock data and join conditions.
    *   `sof_ta_bank` correctly combines data from `ta_bank` and `ta_bank_international`, with `NULL` values assigned to `BIC` and `BANK_INTERNATIONAL_ID` as per the SQL.

#### Test Case 2.3: `sof_ta_p_rech_empf` Complex CASE Logic

*   **Purpose**: Verify the complex `CASE` statements used to derive `RECHNUNGSEMPFAENGER`, `AKAD_TITEL`, `FIRMA`, `VORNAME`, `NACHNAME`, and `STRASSE` in `sof_ta_p_rech_empf`.
*   **Setup**:
    *   Populate `fos_source_dataset.ta_e_reach_re` and `fos_source_dataset.ta_e_business_re` with mock data to cover all branches of the `CASE` statements:
        *   `corp_unit` IS NULL / NOT NULL
        *   `organisation_name` IS NULL / NOT NULL
        *   `surname_s` IS NULL / NOT NULL
        *   `pobox` IS NULL / NOT NULL (for `STRASSE` derivation)
        *   Combinations of the above.
*   **Action**:
    1.  Trigger the Airflow DAG.
    2.  Query `fos_target_dataset.sof_ta_p_rech_empf`.
*   **Pass/Fail Criterion**:
    *   The values in the derived columns (`RECHNUNGSEMPFAENGER`, `AKAD_TITEL`, `FIRMA`, `VORNAME`, `NACHNAME`, `STRASSE`) match the expected output for each mock record based on the `CASE` logic.

#### Test Case 2.4: `sof_ta_p_d1_vpn` Filtering

*   **Purpose**: Verify the filtering logic for `vpn_id IS NOT NULL` and `basisprodukt_id IN (2828, 2831)` in the `sof_ta_p_d1_vpn` table.
*   **Setup**:
    *   Populate `dwh_source_dataset.vi_s_ibasisprodukt` with mock data including:
        *   Records with `vpn_id IS NOT NULL` and `basisprodukt_id` in `(2828, 2831)`. (Should be included)
        *   Records with `vpn_id IS NULL`. (Should be excluded)
        *   Records with `basisprodukt_id` not in `(2828, 2831)`. (Should be excluded)
        *   Records with both `vpn_id IS NULL` and `basisprodukt_id` not in `(2828, 2831)`. (Should be excluded)
*   **Action**:
    1.  Trigger the Airflow DAG.
    2.  Query `fos_target_dataset.sof_ta_p_d1_vpn`.
*   **Pass/Fail Criterion**:
    *   Only records where `vpn_id IS NOT NULL` AND `basisprodukt_id IN (2828, 2831)` are present in `sof_ta_p_d1_vpn`.

---

### 3. External-System Replacements Tests

These tests verify that the Airflow DAG correctly replaces the functionality of the legacy shell utilities and orchestrates BigQuery operations.

#### Test Case 3.1: Parameter Parsing and Date Handling

*   **Purpose**: Verify that the `parse_params_and_setup` task correctly handles input parameters (`stichtag_ddmmyyyy`, `restart_value`) and derives date values (`stichtag_yyyymmdd`, `today_yyyymmdd`, `yesterday_yyyymmdd`), replacing `getopts`, `h_alis_parameter.ksh`, and `h_alis_date.ksh`.
*   **Setup**: None specific.
*   **Action**:
    1.  Trigger the DAG with no parameters.
    2.  Trigger the DAG with `stichtag_ddmmyyyy = "15062023"`.
    3.  Trigger the DAG with `restart_value = 500`.
    4.  Trigger the DAG with both `stichtag_ddmmyyyy = "01012022"` and `restart_value = 100`.
    5.  Inspect the XCom values pushed by the `parse_params_and_setup` task for each run.
*   **Pass/Fail Criterion**:
    *   When no `stichtag_ddmmyyyy` is provided, `stichtag_yyyymmdd` in XCom matches the current system date (YYYYMMDD format).
    *   When `stichtag_ddmmyyyy` is provided, `stichtag_yyyymmdd` in XCom correctly reflects the parsed date in YYYYMMDD format.
    *   `restart_value` in XCom matches the provided value or defaults to `0`.
    *   `today_yyyymmdd` and `yesterday_yyyymmdd` are correctly calculated relative to the DAG's execution date.

#### Test Case 3.2: Logging and Error Handling

*   **Purpose**: Verify that Airflow's native logging and error handling mechanisms effectively replace the custom `DWMSG_*` functions from the legacy script.
*   **Setup**:
    *   Ensure Airflow logging is configured and accessible.
*   **Action**:
    1.  **Successful Run**: Trigger a DAG run that is expected to complete successfully.
        *   Inspect Airflow task logs for `parse_params_and_setup` and `log_status_task`.
    2.  **Failed Run (SQL Error)**: Introduce a deliberate syntax error or a non-existent table reference in the `execute_bq_load_main_script` task's SQL. Trigger the DAG.
        *   Inspect Airflow task logs for `execute_bq_load_main_script`.
        *   Observe the DAG run status.
*   **Pass/Fail Criterion**:
    *   For successful runs, the `log_status_task` prints the expected completion message, and the DAG run status is "success".
    *   For failed runs, the `execute_bq_load_main_script` task fails, Airflow logs show the BigQuery error message, and the DAG run status is "failed".
    *   No unhandled exceptions or silent failures occur.

---

### 4. Data Quality / Row Count / Schema Assertions

These tests ensure the integrity, structure, and volume of the migrated data.

#### Test Case 4.1: Row Count Parity (Specific Tables)

*   **Purpose**: Verify that the final target tables (`sof_ta_p_rech_empf`, `sof_ta_p_d1_vpn`) have the same number of rows as their legacy counterparts for a given set of inputs.
*   **Setup**:
    *   Identical source data in legacy Oracle and BigQuery.
    *   Run the legacy script and the migrated DAG with the same parameters (e.g., default `Stichtag`).
*   **Action**:
    1.  Query the row count of `FOS_P_RECH_EMPF` in the legacy Oracle database.
    2.  Query the row count of `sof_ta_p_rech_empf` in BigQuery.
    3.  Repeat for `FOS_P_D1_VPN` and `sof_ta_p_d1_vpn`.
*   **Pass/Fail Criterion**:
    *   `COUNT(*)` from `fos_target_dataset.sof_ta_p_rech_empf` matches `COUNT(*)` from legacy `FOS_P_RECH_EMPF`.
    *   `COUNT(*)` from `fos_target_dataset.sof_ta_p_d1_vpn` matches `COUNT(*)` from legacy `FOS_P_D1_VPN`.

    ```sql
    -- Example BigQuery assertion (assuming legacy count is known, e.g., 12345)
    SELECT
        COUNT(*)
    FROM
        `your-gcp-project-id.fos_target.sof_ta_p_rech_empf`
    HAVING
        COUNT(*) = 12345; -- Replace 12345 with actual legacy count
    -- Expected result: 1 row with the count, if it matches. Otherwise, no rows or an error.
    ```

#### Test Case 4.2: Schema Parity

*   **Purpose**: Verify that the schema (column names, data types, nullability) of the final target tables in BigQuery (`sof_ta_p_rech_empf`, `sof_ta_p_d1_vpn`) functionally matches the legacy Oracle tables.
*   **Setup**: None.
*   **Action**:
    1.  Obtain the schema definition for `FOS_P_RECH_EMPF` and `FOS_P_D1_VPN` from the legacy Oracle database (e.g., using `DESCRIBE` or `ALL_TAB_COLUMNS`).
    2.  Obtain the schema definition for `fos_target_dataset.sof_ta_p_rech_empf` and `fos_target_dataset.sof_ta_p_d1_vpn` from BigQuery (e.g., using `INFORMATION_SCHEMA.COLUMNS` or `bq show --schema`).
    3.  Compare the schemas.
*   **Pass/Fail Criterion**:
    *   All column names are identical (case-sensitivity might need adjustment, but functional names should match).
    *   Data types are functionally equivalent (e.g., `VARCHAR2(X)` in Oracle maps to `STRING` in BigQuery, `NUMBER` maps to `INT64` or `FLOAT64` as appropriate, `DATE` maps to `DATE`).
    *   Nullability constraints are preserved where critical (e.g., primary keys are `NOT NULL`).
    *   No unexpected columns are present, and no expected columns are missing.

#### Test Case 4.3: Data Integrity Assertions

*   **Purpose**: Verify critical data integrity constraints within the BigQuery target tables.
*   **Setup**: Run the migrated DAG with representative data.
*   **Action**: Execute the following SQL assertions against the BigQuery target tables:

    ```sql
    -- Test for sof_ta_p_rech_empf
    -- 1. KUNDENKONTO always '0'
    SELECT COUNT(*) FROM `your-gcp-project-id.fos_target.sof_ta_p_rech_empf` WHERE KUNDENKONTO != '0';
    -- Expected: 0

    -- 2. DPPS_KONTONUMMER always '0'
    SELECT COUNT(*) FROM `your-gcp-project-id.fos_target.sof_ta_p_rech_empf` WHERE DPPS_KONTONUMMER != '0';
    -- Expected: 0

    -- 3. RECHDEF_ID is not NULL and unique
    SELECT COUNT(RECHDEF_ID) FROM `your-gcp-project-id.fos_target.sof_ta_p_rech_empf` WHERE RECHDEF_ID IS NULL;
    -- Expected: 0
    SELECT COUNT(DISTINCT RECHDEF_ID) FROM `your-gcp-project-id.fos_target.sof_ta_p_rech_empf`
    HAVING COUNT(RECHDEF_ID) != COUNT(DISTINCT RECHDEF_ID);
    -- Expected: 0 (or 1 if the count is 0, meaning no duplicates)

    -- 4. QUELLE is always 'C'
    SELECT COUNT(*) FROM `your-gcp-project-id.fos_target.sof_ta_p_rech_empf` WHERE QUELLE != 'C';
    -- Expected: 0

    -- Test for sof_ta_p_d1_vpn
    -- 1. vertrags_id and vpn_id are not NULL
    SELECT COUNT(*) FROM `your-gcp-project-id.fos_target.sof_ta_p_d1_vpn` WHERE vertrags_id IS NULL OR vpn_id IS NULL;
    -- Expected: 0

    -- 2. vpn_id is unique (if it's a primary key or expected to be unique)
    SELECT COUNT(DISTINCT vpn_id) FROM `your-gcp-project-id.fos_target.sof_ta_p_d1_vpn`
    HAVING COUNT(vpn_id) != COUNT(DISTINCT vpn_id);
    -- Expected: 0
    ```
*   **Pass/Fail Criterion**: All SQL assertions return 0 (or the expected value indicating no violations).

#### Test Case 4.4: Data Type and Value Range Handling

*   **Purpose**: Verify that data types are correctly mapped and that no data loss, truncation, or unexpected conversions occur for specific columns, especially for numeric and date fields.
*   **Setup**:
    *   Populate source tables with mock data that includes:
        *   Maximum/minimum values for numeric fields.
        *   Long strings for text fields to test for truncation.
        *   Edge case dates (e.g., leap years, start/end of months).
        *   `NULL` values for all nullable columns.
*   **Action**:
    1.  Run the migrated DAG.
    2.  Select a sample of records (including edge cases) from the BigQuery target tables.
    3.  Compare the values of these records against the expected values derived from the legacy source data.
*   **Pass/Fail Criterion**:
    *   All selected values match their expected counterparts without truncation, rounding errors, or incorrect date interpretations.
    *   `NULL` values are correctly propagated where expected.

---