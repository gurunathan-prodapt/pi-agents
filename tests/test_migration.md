The migration of `r_ausd_bp_ta_msisdn_his.ksh` to BigQuery and Airflow involves transforming a KornShell wrapper script and its invoked core logic (`k_ausd_bp_ta_msisdn_his.ksh`) into BigQuery Stored Procedures orchestrated by an Airflow DAG.

A significant challenge is that the detailed logic of `k_ausd_bp_ta_msisdn_his.ksh` is unknown and represented by a placeholder in the migrated code. Therefore, the tests for "Transformation correctness" and "Output parity" for the core logic will be based on the *assumed logic* described in the migration design document (Section 5b). Once the actual `k_ausd_bp_ta_msisdn_his.ksh` is analyzed, these tests must be refined.

The tests are organized into the four requested categories.

---

## Pre-requisites & Setup for all Tests

Before running any tests, ensure the following BigQuery resources are set up:

1.  **BigQuery Project and Dataset:** Replace `project` and `dataset` with your actual GCP project ID and BigQuery dataset ID.
2.  **Audit Tables:** Create `job_audit` and `job_error_log` tables using the provided DDLs.

    ```sql
    -- job_audit_ddl.sql
    CREATE TABLE IF NOT EXISTS `project.dataset.job_audit` (
        job_id STRING NOT NULL,
        job_name STRING,
        start_timestamp TIMESTAMP NOT NULL,
        end_timestamp TIMESTAMP,
        status STRING NOT NULL, -- e.g., 'RUNNING', 'SUCCESS', 'FAILED'
        parameters JSON,
        message STRING
    );

    -- job_error_log_ddl.sql
    CREATE TABLE IF NOT EXISTS `project.dataset.job_error_log` (
        job_id STRING NOT NULL,
        job_name STRING,
        error_timestamp TIMESTAMP NOT NULL,
        error_message STRING NOT NULL,
        error_details JSON
    );
    ```

3.  **Mock Source Table (`dwh_contract_cache`):** Create a mock table to simulate the DWH contract cache. This table will be populated with test data for various scenarios.

    ```sql
    CREATE OR REPLACE TABLE `project.dataset.dwh_contract_cache` (
        DWH_VERTRAG_ID INT64 NOT NULL,
        Gueltig_von DATE,
        Gueltig_bis DATE,
        LADEDATUM DATE,
        product_type STRING,
        msisdn STRING
    );
    ```

4.  **Mock Target Table (`fos_target_table`):** Create a mock target table for "Forderungsscoring". This table will be truncated before each test run that involves data insertion.

    ```sql
    CREATE OR REPLACE TABLE `project.dataset.fos_target_table` (
        dwh_vertrag_id INT64 NOT NULL,
        gueltig_von DATE,
        gueltig_bis DATE,
        ladedatum DATE,
        basic_product_info STRING,
        creation_timestamp TIMESTAMP
    );
    ```

5.  **BigQuery Stored Procedures:** Deploy `ausd_bp_ta_msisdn_his_core_sp` and `ausd_bp_ta_msisdn_his_wrapper_sp` using the provided SQL.

6.  **Airflow Environment:** Ensure an Airflow environment is set up, and the `r_ausd_bp_ta_msisdn_his_dag.py` DAG is deployed and available. Configure a BigQuery connection in Airflow.

---

## 1. Output Parity Tests

These tests verify that given the same inputs, the migrated job produces the same logical outputs as the legacy job. Due to the placeholder core logic, we focus on the wrapper's parameter handling and the *expected* data output based on the assumed core logic.

### Test Case 1.1: Default Parameter Handling (Stichtag and Wiederanlaufwert)

*   **Purpose:** Verify that when no `p_stichtag_input` or `p_wiederanlaufWert_input` is provided, the wrapper SP correctly defaults them to `CURRENT_DATE()` (DDMMYYYY) and `0` respectively, and passes these to the core SP.
*   **Setup:**
    1.  Truncate `project.dataset.job_audit` and `project.dataset.fos_target_table`.
    2.  Insert sample data into `project.dataset.dwh_contract_cache`. Ensure some data matches `CURRENT_DATE()` and `DWH_VERTRAG_ID > 0`.
        ```sql
        -- Example data for dwh_contract_cache
        INSERT INTO `project.dataset.dwh_contract_cache` (DWH_VERTRAG_ID, Gueltig_von, Gueltig_bis, LADEDATUM, product_type, msisdn) VALUES
        (101, CURRENT_DATE() - INTERVAL 10 DAY, CURRENT_DATE() + INTERVAL 10 DAY, CURRENT_DATE() - INTERVAL 15 DAY, 'FAX', '12345'), -- Should be selected
        (102, CURRENT_DATE() - INTERVAL 5 DAY, CURRENT_DATE() + INTERVAL 5 DAY, CURRENT_DATE() - INTERVAL 10 DAY, 'Data24', '67890'), -- Should be selected
        (5, CURRENT_DATE() - INTERVAL 20 DAY, CURRENT_DATE() + INTERVAL 20 DAY, CURRENT_DATE() - INTERVAL 25 DAY, 'Voice', '11111'), -- DWH_VERTRAG_ID <= 0, should NOT be selected
        (103, CURRENT_DATE() + INTERVAL 1 DAY, CURRENT_DATE() + INTERVAL 10 DAY, CURRENT_DATE() - INTERVAL 5 DAY, 'SMS', '22222'); -- Gueltig_von > Stichtag, should NOT be selected
        ```
*   **Action:** Execute the wrapper stored procedure without providing `p_stichtag_input` or `p_wiederanlaufWert_input`.
    ```sql
    CALL `project.dataset.ausd_bp_ta_msisdn_his_wrapper_sp`(NULL, NULL);
    ```
*   **Pass/Fail Criterion:**
    1.  **Audit Log:** A single `SUCCESS` entry exists in `job_audit` where `parameters.stichtag` is `FORMAT_DATE('%d%m%Y', CURRENT_DATE())` and `parameters.wiederanlaufwert` is `0`.
        ```sql
        SELECT
            status,
            JSON_VALUE(parameters, '$.stichtag') AS stichtag_param,
            CAST(JSON_VALUE(parameters, '$.wiederanlaufwert') AS INT64) AS wiederanlaufwert_param
        FROM `project.dataset.job_audit`
        WHERE status = 'SUCCESS'
        ORDER BY start_timestamp DESC
        LIMIT 1;
        -- Expected: status='SUCCESS', stichtag_param=FORMAT_DATE('%d%m%Y', CURRENT_DATE()), wiederanlaufwert_param=0
        ```
    2.  **Target Data:** `fos_target_table` contains 2 rows (for DWH_VERTRAG_ID 101, 102) matching the default `Stichtag` (current date) and `Wiederanlaufwert` (0).
        ```sql
        SELECT COUNT(*) FROM `project.dataset.fos_target_table`;
        -- Expected: 2
        SELECT dwh_vertrag_id FROM `project.dataset.fos_target_table` ORDER BY dwh_vertrag_id;
        -- Expected: 101, 102
        ```

### Test Case 1.2: Explicit Parameter Handling

*   **Purpose:** Verify that explicit `p_stichtag_input` and `p_wiederanlaufWert_input` are correctly parsed and passed to the core SP.
*   **Setup:**
    1.  Truncate `project.dataset.job_audit` and `project.dataset.fos_target_table`.
    2.  Insert sample data into `project.dataset.dwh_contract_cache`.
        ```sql
        -- Example data for dwh_contract_cache
        INSERT INTO `project.dataset.dwh_contract_cache` (DWH_VERTRAG_ID, Gueltig_von, Gueltig_bis, LADEDATUM, product_type, msisdn) VALUES
        (10, DATE '2023-01-01', DATE '2023-01-31', DATE '2022-12-25', 'FAX', '10001'),
        (20, DATE '2023-01-10', DATE '2023-02-10', DATE '2023-01-05', 'Data24', '10002'),
        (30, DATE '2023-01-15', DATE '2023-01-20', DATE '2023-01-12', 'Voice', '10003'), -- Gueltig_bis <= Stichtag
        (40, DATE '2023-01-05', DATE '2023-01-15', DATE '2023-01-01', 'SMS', '10004'), -- DWH_VERTRAG_ID > 20, but Gueltig_bis <= Stichtag
        (50, DATE '2023-01-20', DATE '2023-02-20', DATE '2023-01-18', 'MMS', '10005'); -- DWH_VERTRAG_ID > 20, Gueltig_von > Stichtag
        ```
*   **Action:** Execute the wrapper stored procedure with specific `Stichtag` ('15012023') and `Wiederanlaufwert` (15).
    ```sql
    CALL `project.dataset.ausd_bp_ta_msisdn_his_wrapper_sp`('15012023', 15);
    ```
*   **Pass/Fail Criterion:**
    1.  **Audit Log:** A single `SUCCESS` entry exists in `job_audit` where `parameters.stichtag` is `'15012023'` and `parameters.wiederanlaufwert` is `15`.
        ```sql
        SELECT
            status,
            JSON_VALUE(parameters, '$.stichtag') AS stichtag_param,
            CAST(JSON_VALUE(parameters, '$.wiederanlaufwert') AS INT64) AS wiederanlaufwert_param
        FROM `project.dataset.job_audit`
        WHERE status = 'SUCCESS'
        ORDER BY start_timestamp DESC
        LIMIT 1;
        -- Expected: status='SUCCESS', stichtag_param='15012023', wiederanlaufwert_param=15
        ```
    2.  **Target Data:** `fos_target_table` contains 1 row (for DWH_VERTRAG_ID 20) matching the explicit `Stichtag` (DATE '2023-01-15') and `Wiederanlaufwert` (15).
        *   Row 10: `Gueltig_von` <= '2023-01-15' < `Gueltig_bis`, `LADEDATUM` < '2023-01-15', `DWH_VERTRAG_ID` > 15. -> **YES**
        *   Row 20: `Gueltig_von` <= '2023-01-15' < `Gueltig_bis`, `LADEDATUM` < '2023-01-15', `DWH_VERTRAG_ID` > 15. -> **YES**
        *   Row 30: `Gueltig_von` <= '2023-01-15' < `Gueltig_bis` (FALSE, 2023-01-15 < 2023-01-20), `LADEDATUM` < '2023-01-15', `DWH_VERTRAG_ID` > 15. -> **NO** (Gueltig_bis is 2023-01-20, so 2023-01-15 < 2023-01-20 is TRUE. Let's re-evaluate. `Gueltig_von <= Stichtag < Gueltig_bis`. For 30: 2023-01-15 <= 2023-01-15 < 2023-01-20. This is TRUE. `LADEDATUM < Stichtag`: 2023-01-12 < 2023-01-15. This is TRUE. `DWH_VERTRAG_ID > 15`: 30 > 15. This is TRUE. So row 30 should be selected.)
        *   Row 40: `Gueltig_von` <= '2023-01-15' < `Gueltig_bis` (FALSE, 2023-01-15 < 2023-01-15 is FALSE). -> **NO**
        *   Row 50: `Gueltig_von` <= '2023-01-15' (FALSE, 2023-01-20 <= 2023-01-15 is FALSE). -> **NO**
        *   Expected rows: 10, 20, 30.
        ```sql
        SELECT COUNT(*) FROM `project.dataset.fos_target_table`;
        -- Expected: 3
        SELECT dwh_vertrag_id FROM `project.dataset.fos_target_table` ORDER BY dwh_vertrag_id;
        -- Expected: 10, 20, 30
        ```

### Test Case 1.3: Empty String Stichtag

*   **Purpose:** Verify that an empty string for `p_stichtag_input` also defaults to `CURRENT_DATE()`.
*   **Setup:**
    1.  Truncate `project.dataset.job_audit` and `project.dataset.fos_target_table`.
    2.  Insert sample data into `project.dataset.dwh_contract_cache` (same as Test 1.1).
*   **Action:** Execute the wrapper stored procedure with an empty string for `p_stichtag_input` and `NULL` for `p_wiederanlaufWert_input`.
    ```sql
    CALL `project.dataset.ausd_bp_ta_msisdn_his_wrapper_sp`('', NULL);
    ```
*   **Pass/Fail Criterion:**
    1.  **Audit Log:** A single `SUCCESS` entry exists in `job_audit` where `parameters.stichtag` is `FORMAT_DATE('%d%m%Y', CURRENT_DATE())` and `parameters.wiederanlaufwert` is `0`.
        ```sql
        SELECT
            status,
            JSON_VALUE(parameters, '$.stichtag') AS stichtag_param,
            CAST(JSON_VALUE(parameters, '$.wiederanlaufwert') AS INT64) AS wiederanlaufwert_param
        FROM `project.dataset.job_audit`
        WHERE status = 'SUCCESS'
        ORDER BY start_timestamp DESC
        LIMIT 1;
        -- Expected: status='SUCCESS', stichtag_param=FORMAT_DATE('%d%m%Y', CURRENT_DATE()), wiederanlaufwert_param=0
        ```
    2.  **Target Data:** `fos_target_table` contains 2 rows (for DWH_VERTRAG_ID 101, 102) matching the default `Stichtag` (current date) and `Wiederanlaufwert` (0).
        ```sql
        SELECT COUNT(*) FROM `project.dataset.fos_target_table`;
        -- Expected: 2
        SELECT dwh_vertrag_id FROM `project.dataset.fos_target_table` ORDER BY dwh_vertrag_id;
        -- Expected: 101, 102
        ```

---

## 2. Transformation Correctness Tests

These tests focus on the correctness of the assumed core logic's filtering, type handling, and NULL handling.

### Test Case 2.1: Date Filtering Logic (`Gueltig_von`, `Gueltig_bis`, `LADEDATUM`)

*   **Purpose:** Verify that the core SP correctly applies the date filters: `Gueltig_von <= p_stichtag < Gueltig_bis AND LADEDATUM < p_stichtag`.
*   **Setup:**
    1.  Truncate `project.dataset.job_audit` and `project.dataset.fos_target_table`.
    2.  Insert diverse sample data into `project.dataset.dwh_contract_cache` to test all date conditions.
        ```sql
        -- Stichtag for this test: '15012023' (DATE '2023-01-15')
        INSERT INTO `project.dataset.dwh_contract_cache` (DWH_VERTRAG_ID, Gueltig_von, Gueltig_bis, LADEDATUM, product_type, msisdn) VALUES
        (1, DATE '2023-01-01', DATE '2023-01-31', DATE '2023-01-10', 'BP1', '111'), -- Selected: All conditions met
        (2, DATE '2023-01-15', DATE '2023-01-31', DATE '2023-01-10', 'BP2', '222'), -- Selected: Gueltig_von = Stichtag
        (3, DATE '2023-01-01', DATE '2023-01-15', DATE '2023-01-10', 'BP3', '333'), -- NOT Selected: Stichtag < Gueltig_bis (FALSE, 2023-01-15 < 2023-01-15 is FALSE)
        (4, DATE '2023-01-01', DATE '2023-01-31', DATE '2023-01-15', 'BP4', '444'), -- NOT Selected: LADEDATUM < Stichtag (FALSE, 2023-01-15 < 2023-01-15 is FALSE)
        (5, DATE '2023-01-20', DATE '2023-01-31', DATE '2023-01-10', 'BP5', '555'), -- NOT Selected: Gueltig_von <= Stichtag (FALSE, 2023-01-20 <= 2023-01-15 is FALSE)
        (6, NULL, DATE '2023-01-31', DATE '2023-01-10', 'BP6', '666'), -- NOT Selected: NULL Gueltig_von
        (7, DATE '2023-01-01', NULL, DATE '2023-01-10', 'BP7', '777'), -- NOT Selected: NULL Gueltig_bis (Stichtag < NULL is NULL, so filter fails)
        (8, DATE '2023-01-01', DATE '2023-01-31', NULL, 'BP8', '888'); -- NOT Selected: NULL LADEDATUM
        ```
*   **Action:** Execute the wrapper SP with `Stichtag` '15012023' and `Wiederanlaufwert` 0.
    ```sql
    CALL `project.dataset.ausd_bp_ta_msisdn_his_wrapper_sp`('15012023', 0);
    ```
*   **Pass/Fail Criterion:**
    1.  **Target Data:** `fos_target_table` contains 2 rows (for DWH_VERTRAG_ID 1, 2).
        ```sql
        SELECT COUNT(*) FROM `project.dataset.fos_target_table`;
        -- Expected: 2
        SELECT dwh_vertrag_id FROM `project.dataset.fos_target_table` ORDER BY dwh_vertrag_id;
        -- Expected: 1, 2
        ```

### Test Case 2.2: Restart Logic (`DWH_VERTRAG_ID > p_wiederanlaufWert`)

*   **Purpose:** Verify that the core SP correctly applies the restart filter `DWH_VERTRAG_ID > p_wiederanlaufWert`.
*   **Setup:**
    1.  Truncate `project.dataset.job_audit` and `project.dataset.fos_target_table`.
    2.  Insert sample data into `project.dataset.dwh_contract_cache`. Ensure all date conditions are met for simplicity.
        ```sql
        -- Stichtag for this test: '10012023' (DATE '2023-01-10')
        INSERT INTO `project.dataset.dwh_contract_cache` (DWH_VERTRAG_ID, Gueltig_von, Gueltig_bis, LADEDATUM, product_type, msisdn) VALUES
        (5, DATE '2023-01-01', DATE '2023-01-31', DATE '2023-01-05', 'BP_A', 'A1'), -- DWH_VERTRAG_ID <= 10
        (10, DATE '2023-01-01', DATE '2023-01-31', DATE '2023-01-05', 'BP_B', 'B2'), -- DWH_VERTRAG_ID <= 10
        (15, DATE '2023-01-01', DATE '2023-01-31', DATE '2023-01-05', 'BP_C', 'C3'), -- DWH_VERTRAG_ID > 10
        (20, DATE '2023-01-01', DATE '2023-01-31', DATE '2023-01-05', 'BP_D', 'D4'); -- DWH_VERTRAG_ID > 10
        ```
*   **Action:** Execute the wrapper SP with `Stichtag` '10012023' and `Wiederanlaufwert` 10.
    ```sql
    CALL `project.dataset.ausd_bp_ta_msisdn_his_wrapper_sp`('10012023', 10);
    ```
*   **Pass/Fail Criterion:**
    1.  **Target Data:** `fos_target_table` contains 2 rows (for DWH_VERTRAG_ID 15, 20).
        ```sql
        SELECT COUNT(*) FROM `project.dataset.fos_target_table`;
        -- Expected: 2
        SELECT dwh_vertrag_id FROM `project.dataset.fos_target_table` ORDER BY dwh_vertrag_id;
        -- Expected: 15, 20
        ```

### Test Case 2.3: `Stichtag` Type Handling and Validation (Invalid Format)

*   **Purpose:** Verify that an invalid `Stichtag` format (e.g., not DDMMYYYY) raises an error and is correctly logged.
*   **Setup:**
    1.  Truncate `project.dataset.job_audit` and `project.dataset.job_error_log`.
*   **Action:** Execute the wrapper stored procedure with an invalid `Stichtag` format ('2023-01-15').
    ```sql
    CALL `project.dataset.ausd_bp_ta_msisdn_his_wrapper_sp`('2023-01-15', 0);
    ```
*   **Pass/Fail Criterion:**
    1.  **Wrapper Status:** The `CALL` statement should fail (raise an exception).
    2.  **Audit Log:** A single `FAILED` entry exists in `job_audit` for the job.
        ```sql
        SELECT status FROM `project.dataset.job_audit` ORDER BY start_timestamp DESC LIMIT 1;
        -- Expected: 'FAILED'
        ```
    3.  **Error Log:** A single entry exists in `job_error_log` with an `error_message` indicating an invalid `Stichtag` format.
        ```sql
        SELECT error_message FROM `project.dataset.job_error_log` ORDER BY error_timestamp DESC LIMIT 1;
        -- Expected: error_message LIKE '%Invalid Stichtag format provided%'
        ```
    4.  **Target Data:** `fos_target_table` remains empty.
        ```sql
        SELECT COUNT(*) FROM `project.dataset.fos_target_table`;
        -- Expected: 0
        ```

### Test Case 2.4: NULL Handling for `p_wiederanlaufWert`

*   **Purpose:** Verify that `NULL` `p_wiederanlaufWert_input` correctly defaults to `0` in the wrapper SP.
*   **Setup:**
    1.  Truncate `project.dataset.job_audit` and `project.dataset.fos_target_table`.
    2.  Insert sample data into `project.dataset.dwh_contract_cache` (same as Test 1.1).
*   **Action:** Execute the wrapper stored procedure with a valid `Stichtag` and `NULL` for `p_wiederanlaufWert_input`.
    ```sql
    CALL `project.dataset.ausd_bp_ta_msisdn_his_wrapper_sp`('01012023', NULL);
    ```
*   **Pass/Fail Criterion:**
    1.  **Audit Log:** A single `SUCCESS` entry exists in `job_audit` where `parameters.wiederanlaufwert` is `0`.
        ```sql
        SELECT
            status,
            CAST(JSON_VALUE(parameters, '$.wiederanlaufwert') AS INT64) AS wiederanlaufwert_param
        FROM `project.dataset.job_audit`
        WHERE status = 'SUCCESS'
        ORDER BY start_timestamp DESC
        LIMIT 1;
        -- Expected: status='SUCCESS', wiederanlaufwert_param=0
        ```
    2.  **Target Data:** `fos_target_table` contains rows filtered by `DWH_VERTRAG_ID > 0` (assuming `Stichtag` '01012023' and the sample data from Test 1.1, this would be 0 rows as the data is for `CURRENT_DATE()`). Let's adjust the sample data for this test to match '01012023'.
        ```sql
        -- Example data for dwh_contract_cache for Stichtag '01012023'
        INSERT INTO `project.dataset.dwh_contract_cache` (DWH_VERTRAG_ID, Gueltig_von, Gueltig_bis, LADEDATUM, product_type, msisdn) VALUES
        (10, DATE '2022-12-20', DATE '2023-01-10', DATE '2022-12-15', 'FAX', '12345'), -- Selected (DWH_VERTRAG_ID > 0)
        (5, DATE '2022-12-25', DATE '2023-01-05', DATE '2022-12-20', 'Data24', '67890'), -- Selected (DWH_VERTRAG_ID > 0)
        (0, DATE '2022-12-20', DATE '2023-01-10', DATE '2022-12-15', 'Voice', '11111'); -- NOT Selected (DWH_VERTRAG_ID <= 0)
        ```
        With this data, `fos_target_table` should contain 2 rows.
        ```sql
        SELECT COUNT(*) FROM `project.dataset.fos_target_table`;
        -- Expected: 2
        SELECT dwh_vertrag_id FROM `project.dataset.fos_target_table` ORDER BY dwh_vertrag_id;
        -- Expected: 5, 10
        ```

---

## 3. External-System Replacements Tests

These tests verify the correct functioning of replacements for legacy external systems, primarily focusing on logging and Airflow orchestration.

### Test Case 3.1: Audit Logging (`job_audit` table)

*   **Purpose:** Verify that job start, success, and failure events are correctly logged in the `job_audit` table, replacing the legacy file-based logging.
*   **Setup:**
    1.  Truncate `project.dataset.job_audit`.
*   **Action:**
    1.  Execute the wrapper SP with valid parameters (simulating success).
        ```sql
        CALL `project.dataset.ausd_bp_ta_msisdn_his_wrapper_sp`('01012023', 10);
        ```
    2.  Execute the wrapper SP with invalid parameters (simulating failure, e.g., invalid `Stichtag`).
        ```sql
        -- This call is expected to fail and be caught by the wrapper's EXCEPTION block
        BEGIN
            CALL `project.dataset.ausd_bp_ta_msisdn_his_wrapper_sp`('INVALID_DATE', 0);
        EXCEPTION WHEN ERROR THEN
            -- Do nothing, just catch the error so the test can continue
        END;
        ```
*   **Pass/Fail Criterion:**
    1.  **Audit Log:** Two entries exist in `job_audit`: one with `status = 'SUCCESS'` and one with `status = 'FAILED'`. Both entries should have valid `job_id`, `job_name`, `start_timestamp`, `end_timestamp`, `parameters` (JSON), and `message`.
        ```sql
        SELECT
            status,
            job_id IS NOT NULL AS has_job_id,
            job_name = 'r_ausd_bp_ta_msisdn_his' AS has_job_name,
            start_timestamp IS NOT NULL AS has_start_ts,
            end_timestamp IS NOT NULL AS has_end_ts,
            JSON_VALID(parameters) AS is_params_json,
            message IS NOT NULL AS has_message
        FROM `project.dataset.job_audit`
        ORDER BY start_timestamp;
        -- Expected: Two rows, one with status 'SUCCESS', one with 'FAILED'. All boolean flags true.
        ```

### Test Case 3.2: Error Logging (`job_error_log` table)

*   **Purpose:** Verify that detailed error information is captured in the `job_error_log` table when the job fails, replacing legacy error messaging utilities.
*   **Setup:**
    1.  Truncate `project.dataset.job_error_log`.
*   **Action:** Execute the wrapper stored procedure with an invalid `Stichtag` format.
    ```sql
    -- This call is expected to fail and be caught by the wrapper's EXCEPTION block
    BEGIN
        CALL `project.dataset.ausd_bp_ta_msisdn_his_wrapper_sp`('BAD_DATE_FORMAT', 0);
    EXCEPTION WHEN ERROR THEN
        -- Do nothing, just catch the error so the test can continue
    END;
    ```
*   **Pass/Fail Criterion:**
    1.  **Error Log:** A single entry exists in `job_error_log` with `job_id`, `job_name`, `error_timestamp`, `error_message` (containing details about the invalid date format), and `error_details` (valid JSON with stack trace, etc.).
        ```sql
        SELECT
            job_id IS NOT NULL AS has_job_id,
            job_name = 'r_ausd_bp_ta_msisdn_his' AS has_job_name,
            error_timestamp IS NOT NULL AS has_error_ts,
            error_message LIKE '%Invalid Stichtag format provided%' AS has_correct_error_msg,
            JSON_VALID(error_details) AS is_error_details_json
        FROM `project.dataset.job_error_log`
        ORDER BY error_timestamp DESC
        LIMIT 1;
        -- Expected: All boolean flags true.
        ```

### Test Case 3.3: Airflow DAG Invocation

*   **Purpose:** Verify that the Airflow DAG correctly triggers the BigQuery wrapper SP and passes parameters as expected, replacing the legacy ksh orchestration.
*   **Setup:**
    1.  Ensure the `r_ausd_bp_ta_msisdn_his_dag.py` DAG is deployed in Airflow.
    2.  Truncate `project.dataset.job_audit` and `project.dataset.fos_target_table`.
    3.  Insert sample data into `project.dataset.dwh_contract_cache` that would be selected by the DAG's default `Stichtag` (execution date) and `Wiederanlaufwert` (0).
        ```sql
        -- Example data for dwh_contract_cache, assuming DAG runs for today's date
        INSERT INTO `project.dataset.dwh_contract_cache` (DWH_VERTRAG_ID, Gueltig_von, Gueltig_bis, LADEDATUM, product_type, msisdn) VALUES
        (1000, CURRENT_DATE() - INTERVAL 5 DAY, CURRENT_DATE() + INTERVAL 5 DAY, CURRENT_DATE() - INTERVAL 10 DAY, 'DAG_BP1', 'DAG1'),
        (1001, CURRENT_DATE() - INTERVAL 1 DAY, CURRENT_DATE() + INTERVAL 1 DAY, CURRENT_DATE() - INTERVAL 2 DAY, 'DAG_BP2', 'DAG2');
        ```
*   **Action:** Trigger the `r_ausd_bp_ta_msisdn_his_dag` in Airflow, either manually or by waiting for its schedule.
*   **Pass/Fail Criterion:**
    1.  **Airflow UI:** The DAG run completes successfully, and the `call_ausd_bp_ta_msisdn_his_wrapper_sp` task shows a `success` status.
    2.  **Audit Log:** A `SUCCESS` entry exists in `job_audit` where `parameters.stichtag` matches the Airflow execution date (e.g., `{{ ds[8:10] }}{{ ds[5:7] }}{{ ds[0:4] }}` for the DAG run's `ds`) and `parameters.wiederanlaufwert` is `0`.
        ```sql
        -- In BigQuery, after the DAG run:
        SELECT
            status,
            JSON_VALUE(parameters, '$.stichtag') AS stichtag_param,
            CAST(JSON_VALUE(parameters, '$.wiederanlaufwert') AS INT64) AS wiederanlaufwert_param
        FROM `project.dataset.job_audit`
        WHERE status = 'SUCCESS'
        ORDER BY start_timestamp DESC
        LIMIT 1;
        -- Expected: status='SUCCESS', stichtag_param=FORMAT_DATE('%d%m%Y', CURRENT_DATE()), wiederanlaufwert_param=0
        ```
    3.  **Target Data:** `fos_target_table` contains the 2 rows (for DWH_VERTRAG_ID 1000, 1001) inserted by the core SP based on the Airflow-provided `Stichtag` and `Wiederanlaufwert`.
        ```sql
        SELECT COUNT(*) FROM `project.dataset.fos_target_table`;
        -- Expected: 2
        SELECT dwh_vertrag_id FROM `project.dataset.fos_target_table` ORDER BY dwh_vertrag_id;
        -- Expected: 1000, 1001
        ```

---

## 4. Data-quality / Row-count / Schema Assertions

These tests focus on the structural integrity and basic data quality of the output tables.

### Test Case 4.1: `job_audit` Schema and Data Quality

*   **Purpose:** Verify the schema of `job_audit` and the data types/nullability of inserted records.
*   **Setup:**
    1.  Truncate `project.dataset.job_audit`.
*   **Action:** Execute the wrapper SP with valid parameters.
    ```sql
    CALL `project.dataset.ausd_bp_ta_msisdn_his_wrapper_sp`('01012023', 0);
    ```
*   **Pass/Fail Criterion:**
    1.  **Schema:** The `job_audit` table schema matches the DDL (e.g., `job_id` is `STRING NOT NULL`, `start_timestamp` is `TIMESTAMP NOT NULL`, `status` is `STRING NOT NULL`).
        ```python
        # Using pytest and BigQuery client
        from google.cloud import bigquery

        def test_job_audit_schema():
            client = bigquery.Client()
            table_ref = client.dataset('dataset').table('job_audit')
            table = client.get_table(table_ref)

            expected_schema = {
                'job_id': ('STRING', 'REQUIRED'),
                'job_name': ('STRING', 'NULLABLE'),
                'start_timestamp': ('TIMESTAMP', 'REQUIRED'),
                'end_timestamp': ('TIMESTAMP', 'NULLABLE'),
                'status': ('STRING', 'REQUIRED'),
                'parameters': ('JSON', 'NULLABLE'),
                'message': ('STRING', 'NULLABLE'),
            }

            for field in table.schema:
                assert field.name in expected_schema, f"Unexpected field: {field.name}"
                assert field.field_type == expected_schema[field.name][0], \
                    f"Field {field.name} type mismatch: expected {expected_schema[field.name][0]}, got {field.field_type}"
                assert field.mode == expected_schema[field.name][1], \
                    f"Field {field.name} mode mismatch: expected {expected_schema[field.name][1]}, got {field.mode}"
            assert len(table.schema) == len(expected_schema), "Schema field count mismatch"
        ```
    2.  **Data Quality:** The inserted record has non-NULL values for `job_id`, `start_timestamp`, `status`, and `parameters` is valid JSON.
        ```sql
        SELECT
            job_id IS NOT NULL AS job_id_not_null,
            start_timestamp IS NOT NULL AS start_ts_not_null,
            status IS NOT NULL AS status_not_null,
            JSON_VALID(parameters) AS parameters_is_json
        FROM `project.dataset.job_audit`
        ORDER BY start_timestamp DESC
        LIMIT 1;
        -- Expected: All boolean flags true.
        ```

### Test Case 4.2: `job_error_log` Schema and Data Quality

*   **Purpose:** Verify the schema of `job_error_log` and the data types/nullability of inserted records.
*   **Setup:**
    1.  Truncate `project.dataset.job_error_log`.
*   **Action:** Execute the wrapper SP with invalid parameters to trigger an error.
    ```sql
    BEGIN
        CALL `project.dataset.ausd_bp_ta_msisdn_his_wrapper_sp`('INVALID_DATE', 0);
    EXCEPTION WHEN ERROR THEN
        -- Do nothing
    END;
    ```
*   **Pass/Fail Criterion:**
    1.  **Schema:** The `job_error_log` table schema matches the DDL.
        ```python
        # Using pytest and BigQuery client
        from google.cloud import bigquery

        def test_job_error_log_schema():
            client = bigquery.Client()
            table_ref = client.dataset('dataset').table('job_error_log')
            table = client.get_table(table_ref)

            expected_schema = {
                'job_id': ('STRING', 'REQUIRED'),
                'job_name': ('STRING', 'NULLABLE'),
                'error_timestamp': ('TIMESTAMP', 'REQUIRED'),
                'error_message': ('STRING', 'REQUIRED'),
                'error_details': ('JSON', 'NULLABLE'),
            }

            for field in table.schema:
                assert field.name in expected_schema, f"Unexpected field: {field.name}"
                assert field.field_type == expected_schema[field.name][0], \
                    f"Field {field.name} type mismatch: expected {expected_schema[field.name][0]}, got {field.field_type}"
                assert field.mode == expected_schema[field.name][1], \
                    f"Field {field.name} mode mismatch: expected {expected_schema[field.name][1]}, got {field.mode}"
            assert len(table.schema) == len(expected_schema), "Schema field count mismatch"
        ```
    2.  **Data Quality:** The inserted error record has non-NULL values for `job_id`, `error_timestamp`, `error_message`, and `error_details` is valid JSON.
        ```sql
        SELECT
            job_id IS NOT NULL AS job_id_not_null,
            error_timestamp IS NOT NULL AS error_ts_not_null,
            error_message IS NOT NULL AS error_msg_not_null,
            JSON_VALID(error_details) AS error_details_is_json
        FROM `project.dataset.job_error_log`
        ORDER BY error_timestamp DESC
        LIMIT 1;
        -- Expected: All boolean flags true.
        ```

### Test Case 4.3: `fos_target_table` Row Count and Data Integrity

*   **Purpose:** Verify that the number of rows inserted into `fos_target_table` matches the expected count based on the filtering logic, and that data types are preserved.
*   **Setup:**
    1.  Truncate `project.dataset.fos_target_table`.
    2.  Insert specific sample data into `project.dataset.dwh_contract_cache` that will result in a known number of output rows.
        ```sql
        -- Stichtag for this test: '05022023' (DATE '2023-02-05')
        -- Wiederanlaufwert: 10
        INSERT INTO `project.dataset.dwh_contract_cache` (DWH_VERTRAG_ID, Gueltig_von, Gueltig_bis, LADEDATUM, product_type, msisdn) VALUES
        (5, DATE '2023-01-01', DATE '2023-02-10', DATE '2023-01-20', 'BP_X', 'X1'), -- DWH_VERTRAG_ID <= 10, NOT selected
        (15, DATE '2023-01-01', DATE '2023-02-10', DATE '2023-01-20', 'BP_Y', 'Y2'), -- Selected
        (20, DATE '2023-02-01', DATE '2023-02-28', DATE '2023-02-01', 'BP_Z', 'Z3'), -- Selected
        (25, DATE '2023-02-01', DATE '2023-02-28', DATE '2023-02-05', 'BP_A', 'A4'); -- LADEDATUM < Stichtag (FALSE), NOT selected
        ```
*   **Action:** Execute the wrapper SP with `Stichtag` '05022023' and `Wiederanlaufwert` 10.
    ```sql
    CALL `project.dataset.ausd_bp_ta_msisdn_his_wrapper_sp`('05022023', 10);
    ```
*   **Pass/Fail Criterion:**
    1.  **Row Count:** `fos_target_table` contains 2 rows.
        ```sql
        SELECT COUNT(*) FROM `project.dataset.fos_target_table`;
        -- Expected: 2
        ```
    2.  **Data Integrity:** The `dwh_vertrag_id`, `gueltig_von`, `gueltig_bis`, `ladedatum` columns in `fos_target_table` for the selected rows (15, 20) match the source data, and `basic_product_info` is correctly formatted.
        ```sql
        SELECT
            dwh_vertrag_id,
            gueltig_von,
            gueltig_bis,
            ladedatum,
            basic_product_info
        FROM `project.dataset.fos_target_table`
        ORDER BY dwh_vertrag_id;
        -- Expected:
        -- dwh_vertrag_id | gueltig_von | gueltig_bis | ladedatum  | basic_product_info
        -- ---------------|-------------|-------------|------------|---------------------------------
        -- 15             | 2023-01-01  | 2023-02-10  | 2023-01-20 | BP_TYPE: BP_Y, MSISDN: Y2
        -- 20             | 2023-02-01  | 2023-02-28  | 2023-02-01 | BP_TYPE: BP_Z, MSISDN: Z3
        ```
    3.  **Schema:** The `fos_target_table` schema matches its DDL.
        ```python
        # Using pytest and BigQuery client
        from google.cloud import bigquery

        def test_fos_target_table_schema():
            client = bigquery.Client()
            table_ref = client.dataset('dataset').table('fos_target_table')
            table = client.get_table(table_ref)

            expected_schema = {
                'dwh_vertrag_id': ('INT64', 'REQUIRED'),
                'gueltig_von': ('DATE', 'NULLABLE'),
                'gueltig_bis': ('DATE', 'NULLABLE'),
                'ladedatum': ('DATE', 'NULLABLE'),
                'basic_product_info': ('STRING', 'NULLABLE'),
                'creation_timestamp': ('TIMESTAMP', 'NULLABLE'),
            }

            for field in table.schema:
                assert field.name in expected_schema, f"Unexpected field: {field.name}"
                assert field.field_type == expected_schema[field.name][0], \
                    f"Field {field.name} type mismatch: expected {expected_schema[field.name][0]}, got {field.field_type}"
                assert field.mode == expected_schema[field.name][1], \
                    f"Field {field.name} mode mismatch: expected {expected_schema[field.name][1]}, got {field.mode}"
            assert len(table.schema) == len(expected_schema), "Schema field count mismatch"
        ```