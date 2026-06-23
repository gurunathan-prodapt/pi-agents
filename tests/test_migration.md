As a senior data-migration QA engineer, I've analyzed the provided migration design for `k_ausd_bp_ta_apn_carmen.ksh` to BigQuery. The core challenge lies in the unprovided `d_ausd_bp_ta_apn_carmen.sql` and the commented-out file processing logic. For the purpose of these tests, I will define a hypothetical behavior for `d_ausd_bp_ta_apn_carmen.sql` based on the design's pseudocode and assume the commented file processing logic is *not* active unless explicitly stated in a test case.

The tests are organized into categories covering parameter validation, core data processing, external system replacements, and data quality. Each test case includes its purpose, setup, action, and concrete pass/fail criteria, often with runnable SQL or Python (pytest) assertions.

---

## Migration Validation Tests: `k_ausd_bp_ta_apn_carmen.ksh` to BigQuery

### Pre-requisites for all tests:

1.  **BigQuery Project and Dataset:** All BigQuery tables and stored procedures (`project.dataset.*`) are assumed to reside in a specific BigQuery project and dataset.
2.  **Mock Source Table:** A mock source table `project.dataset.source_table_for_carmen` is required for the core data processing logic.
    ```sql
    CREATE TABLE IF NOT EXISTS `project.dataset.source_table_for_carmen`
    (
        id                  INT64,
        process_date_col    DATE,
        restart_value_col   INT64,
        data_field_1        STRING,
        data_field_2        STRING
    );
    ```
3.  **Legacy Environment Mocking:** For legacy tests, a shell environment capable of running `.ksh` scripts is needed. Utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`, `starteSQLSkript`) must be mocked or configured to simulate their expected behavior and outputs.
    *   **`f_alis_msgerr.ksh`:** Should print error messages to stderr and potentially set an exit code.
    *   **`h_alis_date.ksh`:** `DWDate_Datum_Check` should validate `DDMMYYYY` format.
    *   **`h_alis_parameter.ksh`:** `pruefeParameterGesetzt` should check for non-empty variables.
    *   **`gestern.ksh`:** Should echo `YYYY-MM-DD` for today and yesterday. E.g., `echo "2023-01-02 2023-01-01"`.
    *   **`starteSQLSkript`:** This is the most complex. It should simulate executing `d_ausd_bp_ta_apn_carmen.sql` and writing a record count to the `tmpFile`.
        ```bash
        #!/bin/bash
        # Mock starteSQLSkript
        # Args: $1=EintragsNr, $2=SQL_Script_Name, $3=EintragsNr, $4=JobKennung, $5=Stichtag, $6=tmpFile, $7=BERT_DIR_ROOT, $8=datum_heute, $9=datum_gestern
        
        TMP_FILE=$6
        STICHTAG=$5
        RESTART_VAL=${10:-0} # Assuming restart value is passed as 10th arg, or defaults to 0
        
        # Simulate d_ausd_bp_ta_apn_carmen.sql logic
        # For simplicity, let's say it processes 5 records if Stichtag is '01012023' and restart_val is 0
        # and 2 records if restart_val is 1
        RECORD_COUNT=0
        if [ "$STICHTAG" = "01012023" ]; then
            if [ "$RESTART_VAL" -eq 0 ]; then
                RECORD_COUNT=5
            elif [ "$RESTART_VAL" -eq 1 ]; then
                RECORD_COUNT=2
            fi
        fi
        
        echo "$RECORD_COUNT" > "$TMP_FILE"
        echo "SQL script executed successfully. Records: $RECORD_COUNT" >&2
        exit 0
        ```
    *   **`d_ausd_bp_ta_apn_carmen.sql`:** For legacy, this would be an actual SQL script. For testing, it should be a simple script that, when executed by `sqlplus` (via `starteSQLSkript`), would insert data into a mock `PoolBasisprodukt` table and return a record count.

### 1. Parameter Validation & Error Handling

These tests verify that the BigQuery Stored Procedure (`sp_k_ausd_bp_ta_apn_carmen`) handles missing or invalid input parameters exactly as the legacy KornShell script.

#### Test Case 1.1: Missing Mandatory Parameter (`p_JobKennung`)

*   **Purpose:** Verify that the job fails and logs an error when `p_JobKennung` is missing or empty.
*   **Setup:**
    *   **Legacy:** Ensure `job_error_log` (or equivalent) is empty.
    *   **Migrated:** Ensure `project.dataset.job_error_log` is empty.
*   **Action:**
    *   **Legacy:** Execute `k_ausd_bp_ta_apn_carmen.ksh -f 001 -s 01012023`.
    *   **Migrated:** Call `CALL `project.dataset.sp_k_ausd_bp_ta_apn_carmen`(NULL, '001', '01012023', 0);`
*   **Pass/Fail Criteria:**
    *   **Legacy:** The script exits with a non-zero status. Standard error output contains "Jobkennung fehlt".
    *   **Migrated:** The stored procedure execution fails with an error message containing "Jobkennung fehlt (p_JobKennung).". A new entry is found in `project.dataset.job_error_log` with `error_msg = 'Jobkennung fehlt (p_JobKennung).'` and `error_nr = 1`.

#### Test Case 1.2: Missing Mandatory Parameter (`p_EintragsNr`)

*   **Purpose:** Verify that the job fails and logs an error when `p_EintragsNr` is missing or empty.
*   **Setup:**
    *   **Legacy:** Ensure `job_error_log` is empty.
    *   **Migrated:** Ensure `project.dataset.job_error_log` is empty.
*   **Action:**
    *   **Legacy:** Execute `k_ausd_bp_ta_apn_carmen.ksh -j TEST_JOB -s 01012023`.
    *   **Migrated:** Call `CALL `project.dataset.sp_k_ausd_bp_ta_apn_carmen`('TEST_JOB', NULL, '01012023', 0);`
*   **Pass/Fail Criteria:**
    *   **Legacy:** The script exits with a non-zero status. Standard error output contains "EintragsNr fehlt".
    *   **Migrated:** The stored procedure execution fails with an error message containing "Eintragsnummer fehlt (p_EintragsNr).". A new entry is found in `project.dataset.job_error_log` with `error_msg = 'Eintragsnummer fehlt (p_EintragsNr).'` and `error_nr = 2`.

#### Test Case 1.3: Missing Mandatory Parameter (`p_Stichtag`)

*   **Purpose:** Verify that the job fails and logs an error when `p_Stichtag` is missing or empty.
*   **Setup:**
    *   **Legacy:** Ensure `job_error_log` is empty.
    *   **Migrated:** Ensure `project.dataset.job_error_log` is empty.
*   **Action:**
    *   **Legacy:** Execute `k_ausd_bp_ta_apn_carmen.ksh -j TEST_JOB -f 001`.
    *   **Migrated:** Call `CALL `project.dataset.sp_k_ausd_bp_ta_apn_carmen`('TEST_JOB', '001', NULL, 0);`
*   **Pass/Fail Criteria:**
    *   **Legacy:** The script exits with a non-zero status. Standard error output contains "Stichtag fehlt".
    *   **Migrated:** The stored procedure execution fails with an error message containing "Stichtag fehlt (p_Stichtag).". A new entry is found in `project.dataset.job_error_log` with `error_msg = 'Stichtag fehlt (p_Stichtag).'` and `error_nr = 3`.

#### Test Case 1.4: Invalid `p_Stichtag` Format

*   **Purpose:** Verify that the job fails and logs an error when `p_Stichtag` is not in `DDMMYYYY` format.
*   **Setup:**
    *   **Legacy:** Ensure `job_error_log` is empty.
    *   **Migrated:** Ensure `project.dataset.job_error_log` is empty.
*   **Action:**
    *   **Legacy:** Execute `k_ausd_bp_ta_apn_carmen.ksh -j TEST_JOB -f 001 -s 2023-01-01`.
    *   **Migrated:** Call `CALL `project.dataset.sp_k_ausd_bp_ta_apn_carmen`('TEST_JOB', '001', '2023-01-01', 0);`
*   **Pass/Fail Criteria:**
    *   **Legacy:** The script exits with a non-zero status. Standard error output indicates a date format error.
    *   **Migrated:** The stored procedure execution fails with an error message containing "Stichtag 2023-01-01 hat ungueltiges Format DDMMYYYY.". A new entry is found in `project.dataset.job_error_log` with `error_msg` matching the expected message and `error_nr = 4`.

#### Test Case 1.5: `p_wiederanlaufWert` Defaults to 0

*   **Purpose:** Verify that `p_wiederanlaufWert` is correctly initialized to 0 when not provided.
*   **Setup:**
    *   **Legacy:** Configure `starteSQLSkript` mock to log the received `p_wiederanlaufWert`.
    *   **Migrated:** No specific setup needed beyond standard.
*   **Action:**
    *   **Legacy:** Execute `k_ausd_bp_ta_apn_carmen.ksh -j TEST_JOB -f 001 -s 01012023`. (Omitting `-l` parameter).
    *   **Migrated:** Call `CALL `project.dataset.sp_k_ausd_bp_ta_apn_carmen`('TEST_JOB', '001', '01012023', NULL);`
*   **Pass/Fail Criteria:**
    *   **Legacy:** The `starteSQLSkript` mock confirms that the `p_wiederanlaufWert` parameter was passed as 0.
    *   **Migrated:** The `v_resolved_wiederanlaufWert` variable within the stored procedure is 0. This can be verified by inspecting the `payload` of the inserted data in `PoolBasisprodukt_target` (if `d_ausd_bp_ta_apn_carmen_bq.sql` uses it and logs it) or by asserting on the `job_control_log` entry if `p_wiederanlaufWert` is logged there.

### 2. Core Data Processing & Output Parity

These tests focus on the behavior of the `d_ausd_bp_ta_apn_carmen.sql` equivalent logic and the final data output.

#### Test Case 2.1: Successful Data Transformation and Load

*   **Purpose:** Verify that the core data processing logic correctly transforms and loads data into `PoolBasisprodukt_target`, matching the legacy output.
*   **Setup:**
    *   **Legacy:**
        *   Populate source tables for `d_ausd_bp_ta_apn_carmen.sql` with a known dataset.
        *   Ensure `PoolBasisprodukt` is empty.
        *   Configure `starteSQLSkript` mock to simulate `d_ausd_bp_ta_apn_carmen.sql` inserting 5 records into a mock `PoolBasisprodukt` table and writing "5" to `tmpFile`.
    *   **Migrated:**
        *   Populate `project.dataset.source_table_for_carmen` with 5 records where `process_date_col = '2023-01-01'` and `restart_value_col >= 0`.
        *   Ensure `project.dataset.PoolBasisprodukt_target` is empty.
*   **Action:**
    *   **Legacy:** Execute `k_ausd_bp_ta_apn_carmen.ksh -j TEST_JOB -f 001 -s 01012023 -l 0`.
    *   **Migrated:** Call `CALL `project.dataset.sp_k_ausd_bp_ta_apn_carmen`('TEST_JOB', '001', '01012023', 0);`
*   **Pass/Fail Criteria:**
    *   **Legacy:** The `tmpFile` contains "5". The mock `PoolBasisprodukt` contains 5 records.
    *   **Migrated:**
        *   `SELECT COUNT(*) FROM `project.dataset.PoolBasisprodukt_target`` returns 5.
        *   The data in `project.dataset.PoolBasisprodukt_target` (e.g., `product_id`, `process_date`, `source_system_id`, `payload` content) matches the expected output from the legacy `d_ausd_bp_ta_apn_carmen.sql`.
        *   A new entry in `project.dataset.job_control_log` has `record_count = 5`.

    ```sql
    -- Example SQL assertion for data parity (assuming legacy data can be loaded into a temp table)
    -- This requires a detailed understanding of the actual schema and transformation logic of d_ausd_bp_ta_apn_carmen.sql
    -- For this example, we'll assume a simple mapping.
    
    -- Setup: Load legacy output into a temporary table for comparison
    -- CREATE OR REPLACE TEMPORARY TABLE `project.dataset.legacy_poolbasisprodukt` AS
    -- SELECT 'BP_PROD_123' AS product_id, DATE('2023-01-01') AS process_date, 'APN_CARMEN' AS source_system_id,
    --        JSON '{"field1":"data_field_1", "field2":"data_field_2", "restart_val_used":0}' AS payload
    -- UNION ALL ... (all 5 expected records)
    
    SELECT
        (SELECT COUNT(*) FROM `project.dataset.PoolBasisprodukt_target`) = 5 AS target_row_count_match,
        (SELECT COUNT(*) FROM `project.dataset.PoolBasisprodukt_target` EXCEPT DISTINCT SELECT * FROM `project.dataset.legacy_poolbasisprodukt`) = 0 AS data_match_target_to_legacy,
        (SELECT COUNT(*) FROM `project.dataset.legacy_poolbasisprodukt` EXCEPT DISTINCT SELECT * FROM `project.dataset.PoolBasisprodukt_target`) = 0 AS data_match_legacy_to_target;
    ```

#### Test Case 2.2: Zero Records Processed

*   **Purpose:** Verify correct behavior when no records match the processing criteria.
*   **Setup:**
    *   **Legacy:**
        *   Configure source tables such that `d_ausd_bp_ta_apn_carmen.sql` finds no matching records.
        *   Configure `starteSQLSkript` mock to write "0" to `tmpFile`.
    *   **Migrated:**
        *   Populate `project.dataset.source_table_for_carmen` such that no records match `process_date_col = '2023-01-01'` and `restart_value_col >= 0` (e.g., use a different `process_date_col`).
        *   Ensure `project.dataset.PoolBasisprodukt_target` is empty.
*   **Action:**
    *   **Legacy:** Execute `k_ausd_bp_ta_apn_carmen.ksh -j TEST_JOB -f 001 -s 01012023 -l 0`.
    *   **Migrated:** Call `CALL `project.dataset.sp_k_ausd_bp_ta_apn_carmen`('TEST_JOB', '001', '01012023', 0);`
*   **Pass/Fail Criteria:**
    *   **Legacy:** The `tmpFile` contains "0". The mock `PoolBasisprodukt` remains empty.
    *   **Migrated:**
        *   `SELECT COUNT(*) FROM `project.dataset.PoolBasisprodukt_target`` returns 0.
        *   A new entry in `project.dataset.job_control_log` has `record_count = 0`.

#### Test Case 2.3: `p_wiederanlaufWert` Filtering

*   **Purpose:** Verify that the `p_wiederanlaufWert` parameter correctly filters data based on the `restart_value_col`.
*   **Setup:**
    *   **Legacy:**
        *   Populate source tables with records having `restart_value_col` values: 1, 3, 5, 7, 9 (all for `process_date_col = '2023-01-01'`).
        *   Configure `starteSQLSkript` mock to write "3" (for records with 5, 7, 9) to `tmpFile`.
    *   **Migrated:**
        *   Populate `project.dataset.source_table_for_carmen` with records:
            `(1, '2023-01-01', 1, 'd1', 'd2')`, `(2, '2023-01-01', 3, 'd3', 'd4')`,
            `(3, '2023-01-01', 5, 'd5', 'd6')`, `(4, '2023-01-01', 7, 'd7', 'd8')`,
            `(5, '2023-01-01', 9, 'd9', 'd10')`.
        *   Ensure `project.dataset.PoolBasisprodukt_target` is empty.
*   **Action:**
    *   **Legacy:** Execute `k_ausd_bp_ta_apn_carmen.ksh -j TEST_JOB -f 001 -s 01012023 -l 5`.
    *   **Migrated:** Call `CALL `project.dataset.sp_k_ausd_bp_ta_apn_carmen`('TEST_JOB', '001', '01012023', 5);`
*   **Pass/Fail Criteria:**
    *   **Legacy:** The `tmpFile` contains "3". The mock `PoolBasisprodukt` contains 3 records (those with `restart_value_col` 5, 7, 9).
    *   **Migrated:**
        *   `SELECT COUNT(*) FROM `project.dataset.PoolBasisprodukt_target`` returns 3.
        *   The `product_id`s (or other unique identifiers) in `project.dataset.PoolBasisprodukt_target` correspond to the records with `restart_value_col` 5, 7, 9.
        *   A new entry in `project.dataset.job_control_log` has `record_count = 3`.

### 3. External System Replacements & Logging

These tests verify the correct replacement of legacy utility scripts and logging mechanisms.

#### Test Case 3.1: Date Derivation Parity (`gestern.ksh` vs. BQ functions)

*   **Purpose:** Verify that `v_datum_heute` and `v_datum_gestern` in BigQuery match the output of `gestern.ksh`.
*   **Setup:**
    *   **Legacy:** Set system date to `2023-01-02`. Configure `gestern.ksh` mock to return "2023-01-02 2023-01-01".
    *   **Migrated:** No specific setup, as `CURRENT_DATE()` is deterministic.
*   **Action:**
    *   **Legacy:** Execute `k_ausd_bp_ta_apn_carmen.ksh` on `2023-01-02` (or mock `gestern.ksh` to return specific dates). The `starteSQLSkript` mock should log the received date parameters.
    *   **Migrated:** Call `CALL `project.dataset.sp_k_ausd_bp_ta_apn_carmen`('TEST_JOB', '001', '01012023', 0);` on `2023-01-02`.
*   **Pass/Fail Criteria:**
    *   **Legacy:** The `starteSQLSkript` mock confirms that `p_datum_heute` was '2023-01-02' and `p_datum_gestern` was '2023-01-01'.
    *   **Migrated:** The `v_datum_heute` variable within the SP is `DATE '2023-01-02'` and `v_datum_gestern` is `DATE '2023-01-01'`. This can be verified by inspecting the `job_control_log` entry's `created_at` or by adding temporary `RAISE NOTICE` statements in the SP for debugging.

#### Test Case 3.2: Job Control Log Entry (Successful Execution)

*   **Purpose:** Verify that `project.dataset.job_control_log` is populated correctly upon successful completion.
*   **Setup:**
    *   **Legacy:** A successful run (as in Test 2.1).
    *   **Migrated:** A successful run of the SP (as in Test 2.1).
*   **Action:**
    *   **Legacy:** Execute `k_ausd_bp_ta_apn_carmen.ksh -j TEST_JOB -f 001 -s 01012023 -l 0`.
    *   **Migrated:** Call `CALL `project.dataset.sp_k_ausd_bp_ta_apn_carmen`('TEST_JOB', '001', '01012023', 0);`
*   **Pass/Fail Criteria:**
    *   **Legacy:** (If `FOSJobErzeugeEintrag` were active) A new entry would be created in the legacy job control system with matching details.
    *   **Migrated:** A new entry exists in `project.dataset.job_control_log` with the following values:
        *   `tab_name = 'PoolBasisprodukt'`
        *   `status_a = 'A'`, `status_i = 'I'`
        *   `stichtag_from = DATE '2023-01-01'`, `stichtag_to = DATE '2023-01-01'`
        *   `job_type = 'J'`, `active_flag = 'N'`
        *   `record_count = 5` (matching the records processed in Test 2.1)
        *   `comment_text = 'Initialbefuellung durch sp_k_ausd_bp_ta_apn_carmen'`
        *   `job_name = 'TEST_JOB'`, `entry_nr = '001'`
        *   `created_at` is a recent timestamp.

    ```sql
    -- SQL Assertion for job_control_log
    SELECT
        COUNT(*) = 1
    FROM `project.dataset.job_control_log`
    WHERE
        tab_name = 'PoolBasisprodukt' AND
        status_a = 'A' AND
        status_i = 'I' AND
        stichtag_from = DATE('2023-01-01') AND
        stichtag_to = DATE('2023-01-01') AND
        job_type = 'J' AND
        active_flag = 'N' AND
        record_count = 5 AND -- Assuming 5 records processed from Test 2.1
        comment_text = 'Initialbefuellung durch sp_k_ausd_bp_ta_apn_carmen' AND
        job_name = 'TEST_JOB' AND
        entry_nr = '001' AND
        created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE); -- Check for recent entry
    ```

#### Test Case 3.3: Error Logging on Unhandled Exception

*   **Purpose:** Verify that `project.dataset.job_error_log` is populated for unexpected SQL errors within the stored procedure.
*   **Setup:**
    *   **Legacy:** Modify `d_ausd_bp_ta_apn_carmen.sql` to cause a runtime error (e.g., `SELECT 1/0 FROM DUAL;`).
    *   **Migrated:** Temporarily modify `sp_k_ausd_bp_ta_apn_carmen` to introduce an unhandled SQL error (e.g., `SELECT 1/0;` within the core processing block).
*   **Action:**
    *   **Legacy:** Execute `k_ausd_bp_ta_apn_carmen.ksh -j TEST_JOB -f 001 -s 01012023 -l 0`.
    *   **Migrated:** Call `CALL `project.dataset.sp_k_ausd_bp_ta_apn_carmen`('TEST_JOB', '001', '01012023', 0);`
*   **Pass/Fail Criteria:**
    *   **Legacy:** The script exits with a non-zero status. Standard error output contains details of the SQL error.
    *   **Migrated:** The stored procedure execution fails. A new entry is found in `project.dataset.job_error_log` with `job_name = 'TEST_JOB'`, `entry_nr = '001'`, `error_nr = -1`, and `error_msg` containing "Unhandled SQL error: division by zero" (or similar).

### 4. Data Quality / Schema Assertions

These tests focus on the structural integrity and quality of the data in the target BigQuery table.

#### Test Case 4.1: Target Table Schema Conformance

*   **Purpose:** Verify that the schema of `project.dataset.PoolBasisprodukt_target` matches the expected schema derived from the `d_ausd_bp_ta_apn_carmen.sql` translation.
*   **Setup:** Ensure `project.dataset.PoolBasisprodukt_target` exists and has been populated by a successful run.
*   **Action:** Query BigQuery's `INFORMATION_SCHEMA` for the table schema.
*   **Pass/Fail Criteria:** The column names, data types, and nullability modes of `project.dataset.PoolBasisprodukt_target` exactly match the defined expected schema.

    ```python
    import pytest
    from google.cloud import bigquery

    # Assuming 'project' and 'dataset' are configured in your environment or client
    PROJECT_ID = "your-gcp-project-id"
    DATASET_ID = "your_dataset_id"

    @pytest.fixture(scope="module")
    def bq_client():
        return bigquery.Client(project=PROJECT_ID)

    def test_poolbasisprodukt_target_schema(bq_client):
        table_id = f"{PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt_target"
        table = bq_client.get_table(table_id)

        # Define the expected schema based on the design document's placeholder and
        # the actual translation of d_ausd_bp_ta_apn_carmen.sql
        expected_schema = {
            "product_id": ("STRING", "NULLABLE"),
            "process_date": ("DATE", "NULLABLE"),
            "source_system_id": ("STRING", "NULLABLE"),
            "created_at": ("TIMESTAMP", "NULLABLE"),
            "payload": ("JSON", "NULLABLE"),
            # Add all other columns and their expected types/modes here
            # Example: "some_numeric_field": ("INT64", "REQUIRED"),
        }

        actual_schema = {field.name: (field.field_type, field.mode) for field in table.schema}

        for col_name, (col_type, col_mode) in expected_schema.items():
            assert col_name in actual_schema, f"Column '{col_name}' missing from target schema."
            assert actual_schema[col_name][0] == col_type, \
                f"Column '{col_name}' type mismatch: expected '{col_type}', got '{actual_schema[col_name][0]}'."
            assert actual_schema[col_name][1] == col_mode, \
                f"Column '{col_name}' mode mismatch: expected '{col_mode}', got '{actual_schema[col_name][1]}'."

        assert len(actual_schema) == len(expected_schema), \
            f"Number of columns in target schema mismatch. Expected {len(expected_schema)}, got {len(actual_schema)}."
    ```

#### Test Case 4.2: Data Quality - NULL Handling

*   **Purpose:** Verify that NULL values from source data are handled consistently and propagated correctly to `PoolBasisprodukt_target`.
*   **Setup:**
    *   **Legacy:** Populate source tables with specific records containing NULLs in various columns that are part of the transformation.
    *   **Migrated:** Populate `project.dataset.source_table_for_carmen` with equivalent records, including NULLs.
*   **Action:**
    *   **Legacy:** Execute `k_ausd_bp_ta_apn_carmen.ksh` with parameters that process the NULL-containing data.
    *   **Migrated:** Call `CALL `project.dataset.sp_k_ausd_bp_ta_apn_carmen`(...)` with parameters that process the NULL-containing data.
*   **Pass/Fail Criteria:**
    *   **Legacy:** The mock `PoolBasisprodukt` contains NULLs in the expected columns and records.
    *   **Migrated:** The `project.dataset.PoolBasisprodukt_target` table contains NULLs in the exact same records and columns as the legacy output.

    ```sql
    -- Example SQL assertion for NULL count parity (assuming 'some_nullable_field' exists)
    -- This requires a detailed understanding of the actual schema and transformation logic.
    SELECT
        (SELECT COUNT(*) FROM `project.dataset.PoolBasisprodukt_target` WHERE JSON_VALUE(payload, '$.some_nullable_field') IS NULL) =
        (SELECT COUNT(*) FROM `project.dataset.legacy_poolbasisprodukt` WHERE JSON_VALUE(payload, '$.some_nullable_field') IS NULL) AS null_count_match;
    ```

### 5. Commented File Processing (Conditional)

These tests are **conditional** and only apply if the commented `sed`, `sort`, `join` logic in the original `.ksh` script is determined to be active and migrated to BigQuery. The design document indicates this is an optional step.

#### Test Case 5.1: `sed` (REGEXP_REPLACE) Equivalence

*   **Purpose:** Verify that `sed s/\ //g` (removing spaces) is correctly translated to `REGEXP_REPLACE` in BigQuery.
*   **Setup:**
    *   **Legacy:** Create `cibasis_data24.dat` with lines like "data with spaces".
    *   **Migrated:** Populate `project.dataset.cibasis_data24_staging` with the same content.
*   **Action:**
    *   **Legacy:** Manually execute the `sed` command: `sed s/\ //g cibasis_data24.dat > cibasis_data24.sed`.
    *   **Migrated:** Execute the BigQuery SQL equivalent (e.g., `SELECT REGEXP_REPLACE(line_content, ' ', '') FROM `project.dataset.cibasis_data24_staging``).
*   **Pass/Fail Criteria:**
    *   **Legacy:** The `cibasis_data24.sed` file contains the original lines with all spaces removed.
    *   **Migrated:** The BigQuery query result (or the content of a subsequent staging table) exactly matches the content of `cibasis_data24.sed`.

#### Test Case 5.2: `sort -u -k 1 -t ';'` Equivalence

*   **Purpose:** Verify that `sort -u -k 1 -t ';'` (unique sort by first semicolon-delimited field) is correctly translated.
*   **Setup:**
    *   **Legacy:** Create `cibasis_data24.sed` with unsorted lines and duplicates based on the first field (e.g., "A;1", "C;3", "A;2", "B;4").
    *   **Migrated:** Populate `project.dataset.cibasis_data24_staging` with the same content.
*   **Action:**
    *   **Legacy:** Manually execute the `sort` command: `sort -u -k 1 -t ';' cibasis_data24.sed > cibasis_data24.dat`.
    *   **Migrated:** Execute the BigQuery SQL equivalent (e.g., `SELECT DISTINCT SPLIT(line_content, ';')[OFFSET(0)] AS key_col, line_content FROM `project.dataset.cibasis_data24_staging` ORDER BY key_col`).
*   **Pass/Fail Criteria:**
    *   **Legacy:** The `cibasis_data24.dat` file contains unique lines, sorted by the first field.
    *   **Migrated:** The BigQuery query result (or the content of a subsequent staging table) exactly matches the content of `cibasis_data24.dat`.

#### Test Case 5.3: `join` Equivalence

*   **Purpose:** Verify that the `join` commands are correctly translated to BigQuery SQL `JOIN` operations.
*   **Setup:**
    *   **Legacy:** Create `cibasis_data24.dat`, `cibasis_data96.dat`, `cibasis_fax.dat` with specific data that allows for successful joins as per the original script's logic.
    *   **Migrated:** Populate `project.dataset.cibasis_data24_staging`, `project.dataset.cibasis_data96_staging`, `project.dataset.cibasis_fax_staging` with equivalent data.
*   **Action:**
    *   **Legacy:** Manually execute the `join` commands as specified in the original script to produce `cibasisprodukt.csv`.
    *   **Migrated:** Execute the BigQuery SQL equivalent, performing the `JOIN` operations on the staging tables to produce a final result set.
*   **Pass/Fail Criteria:**
    *   **Legacy:** The `cibasisprodukt.csv` file contains the expected joined data.
    *   **Migrated:** The final BigQuery result set (or the content of a target table like `cibasisprodukt_target`) exactly matches the content of `cibasisprodukt.csv` in terms of row count and data values.

    ```sql
    -- Example SQL assertion for join output parity
    -- Assuming `legacy_cibasisprodukt_csv` is a temporary table with the content of the legacy CSV
    -- And `project.dataset.cibasisprodukt_target` is the final BQ table after joins
    SELECT
        (SELECT COUNT(*) FROM `project.dataset.cibasisprodukt_target`) = (SELECT COUNT(*) FROM `project.dataset.legacy_cibasisprodukt_csv`) AS row_count_match,
        (SELECT COUNT(*) FROM `project.dataset.cibasisprodukt_target` EXCEPT DISTINCT SELECT * FROM `project.dataset.legacy_cibasisprodukt_csv`) = 0 AS data_match_bq_to_legacy,
        (SELECT COUNT(*) FROM `project.dataset.legacy_cibasisprodukt_csv` EXCEPT DISTINCT SELECT * FROM `project.dataset.cibasisprodukt_target`) = 0 AS data_match_legacy_to_bq;
    ```