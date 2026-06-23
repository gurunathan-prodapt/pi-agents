As a senior data-migration QA engineer, I've designed a suite of migration validation tests for the `r_ausd_bp_ta_bpr_apn.ksh` KornShell script, targeting its BigQuery Stored Procedure equivalent, `ausd_bp_ta_bpr_apn_wrapper`. These tests cover output parity, transformation correctness, external system interactions, and data quality assertions, ensuring the migrated code behaves identically to the legacy source.

---

## Pre-requisite Setup for All Tests

Before running any tests, ensure the following BigQuery objects are created. These provide the necessary environment for the `ausd_bp_ta_bpr_apn_wrapper` procedure and allow for verification of its internal calls.

**1. `job_control` Table DDL:**

```sql
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_control` (
  job_nr INT64,
  job_kennung STRING,
  script_name STRING,
  log_file STRING,
  sysdate DATE,
  stichtag DATE,
  restart_value INT64,
  status STRING,
  error_message STRING,
  created_at TIMESTAMP,
  finished_at TIMESTAMP
);
```

**2. Mock Table for `k_ausd_bp_ta_bpr_apn` Calls:**
This table will record the parameters passed to the mocked core business logic procedure, allowing us to verify the wrapper's parameter handling.

```sql
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.mock_k_ausd_bp_ta_bpr_apn_calls` (
  call_id INT64 OPTIONS(description="Unique ID for each call"),
  p_job_kennung STRING,
  p_stichtag STRING,
  p_dw_eintragsnr INT64,
  p_wiederanlaufWert INT64,
  call_timestamp TIMESTAMP
);
```

**3. Mock `k_ausd_bp_ta_bpr_apn` Stored Procedure:**
This mock procedure simulates the behavior of the core business logic. For most tests, it simply records its inputs and succeeds. For error-handling tests, it can be modified to raise an error.

```sql
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.k_ausd_bp_ta_bpr_apn`(
  IN p_job_kennung STRING,
  IN p_stichtag STRING,
  IN p_dw_eintragsnr INT64,
  IN p_wiederanlaufWert INT64
)
BEGIN
  -- Record the parameters passed to this mock procedure
  INSERT INTO `my_project.my_dataset.mock_k_ausd_bp_ta_bpr_apn_calls` (
    call_id, p_job_kennung, p_stichtag, p_dw_eintragsnr, p_wiederanlaufWert, call_timestamp
  )
  VALUES (
    (SELECT IFNULL(MAX(call_id), 0) + 1 FROM `my_project.my_dataset.mock_k_ausd_bp_ta_bpr_apn_calls`),
    p_job_kennung,
    p_stichtag,
    p_dw_eintragsnr,
    p_wiederanlaufWert,
    CURRENT_TIMESTAMP()
  );
  -- Simulate successful execution of the core logic
END;
```

---

## Test Cases

### Test Case 1: `job_control` Table Schema Assertion

*   **Purpose**: Verify that the `job_control` table is created with the correct schema, data types, and nullability as specified in the migration design. This ensures data integrity for logging.
*   **Setup**: Ensure the `my_project.my_dataset.job_control` table has been created using the provided DDL.
*   **Action**: Query BigQuery's `INFORMATION_SCHEMA.COLUMNS` view for the `job_control` table.

    ```sql
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM
        `my_project.my_dataset.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = 'job_control'
    ORDER BY
        ordinal_position;
    ```
*   **Pass/Fail Criterion**: The query result matches the expected schema:

| column_name   | data_type | is_nullable |
| :------------ | :-------- | :---------- |
| job_nr        | INT64     | NO          |
| job_kennung   | STRING    | NO          |
| script_name   | STRING    | NO          |
| log_file      | STRING    | NO          |
| sysdate       | DATE      | NO          |
| stichtag      | DATE      | NO          |
| restart_value | INT64     | NO          |
| status        | STRING    | NO          |
| error_message | STRING    | YES         |
| created_at    | TIMESTAMP | NO          |
| finished_at   | TIMESTAMP | YES         |

### Test Case 2: Successful Execution - No Parameters (Defaults)

*   **Purpose**: Verify that the `ausd_bp_ta_bpr_apn_wrapper` procedure executes successfully when no `stichtag` or `wiederanlaufWert` parameters are provided, correctly applying default values (`CURRENT_DATE()` for `stichtag` and `0` for `wiederanlaufWert`). This covers output parity and transformation correctness for default handling.
*   **Setup**:
    1.  Clear the `my_project.my_dataset.job_control` table: `TRUNCATE TABLE my_project.my_dataset.job_control;`
    2.  Clear the `my_project.my_dataset.mock_k_ausd_bp_ta_bpr_apn_calls` table: `TRUNCATE TABLE my_project.my_dataset.mock_k_ausd_bp_ta_bpr_apn_calls;`
    3.  Ensure the mock `k_ausd_bp_ta_bpr_apn` procedure is configured to succeed (as defined in the pre-requisites).
*   **Action**: Call the wrapper procedure with `NULL` for both parameters.

    ```sql
    CALL `my_project.my_dataset.ausd_bp_ta_bpr_apn_wrapper`(NULL, NULL);
    ```
*   **Pass/Fail Criterion**:
    1.  The call completes without raising an error.
    2.  One record exists in `my_project.my_dataset.job_control` with:
        *   `status = 'OK'`
        *   `stichtag = CURRENT_DATE()` (on the day the test is run)
        *   `restart_value = 0`
        *   `job_kennung = 'ausd_bp_ta_bpr_apn'`
        *   `script_name = 'ausd_bp_ta_bpr_apn_wrapper'`
        *   `log_file` matches `CONCAT('job_', CAST(job_nr AS STRING), '_ausd_bp_ta_bpr_apn.log')`
    3.  One record exists in `my_project.my_dataset.mock_k_ausd_bp_ta_bpr_apn_calls` with:
        *   `p_job_kennung = 'ausd_bp_ta_bpr_apn'`
        *   `p_stichtag = FORMAT_DATE('%d%m%Y', CURRENT_DATE())`
        *   `p_dw_eintragsnr` matching the `job_nr` from `job_control`
        *   `p_wiederanlaufWert = 0`

### Test Case 3: Successful Execution - `stichtag` Provided

*   **Purpose**: Verify that the wrapper correctly uses a provided `stichtag` and defaults `wiederanlaufWert` to `0`. This tests parameter parsing and defaulting logic.
*   **Setup**:
    1.  Clear `job_control` and `mock_k_ausd_bp_ta_bpr_apn_calls` tables.
    2.  Ensure the mock `k_ausd_bp_ta_bpr_apn` procedure is configured to succeed.
    3.  Define a test `stichtag`: `'15032023'`.
*   **Action**: Call the wrapper procedure with the specified `stichtag` and `NULL` for `wiederanlaufWert`.

    ```sql
    CALL `my_project.my_dataset.ausd_bp_ta_bpr_apn_wrapper`('15032023', NULL);
    ```
*   **Pass/Fail Criterion**:
    1.  The call completes without raising an error.
    2.  One record exists in `my_project.my_dataset.job_control` with:
        *   `status = 'OK'`
        *   `stichtag = DATE('2023-03-15')`
        *   `restart_value = 0`
    3.  One record exists in `my_project.my_dataset.mock_k_ausd_bp_ta_bpr_apn_calls` with:
        *   `p_stichtag = '15032023'`
        *   `p_wiederanlaufWert = 0`

### Test Case 4: Successful Execution - `wiederanlaufWert` Provided

*   **Purpose**: Verify that the wrapper correctly uses a provided `wiederanlaufWert` and defaults `stichtag` to `CURRENT_DATE()`. This tests parameter parsing and defaulting logic.
*   **Setup**:
    1.  Clear `job_control` and `mock_k_ausd_bp_ta_bpr_apn_calls` tables.
    2.  Ensure the mock `k_ausd_bp_ta_bpr_apn` procedure is configured to succeed.
    3.  Define a test `wiederanlaufWert`: `12345`.
*   **Action**: Call the wrapper procedure with `NULL` for `stichtag` and the specified `wiederanlaufWert`.

    ```sql
    CALL `my_project.my_dataset.ausd_bp_ta_bpr_apn_wrapper`(NULL, 12345);
    ```
*   **Pass/Fail Criterion**:
    1.  The call completes without raising an error.
    2.  One record exists in `my_project.my_dataset.job_control` with:
        *   `status = 'OK'`
        *   `stichtag = CURRENT_DATE()`
        *   `restart_value = 12345`
    3.  One record exists in `my_project.my_dataset.mock_k_ausd_bp_ta_bpr_apn_calls` with:
        *   `p_stichtag = FORMAT_DATE('%d%m%Y', CURRENT_DATE())`
        *   `p_wiederanlaufWert = 12345`

### Test Case 5: Successful Execution - Both Parameters Provided

*   **Purpose**: Verify that the wrapper correctly uses both provided `stichtag` and `wiederanlaufWert`. This confirms full parameter handling.
*   **Setup**:
    1.  Clear `job_control` and `mock_k_ausd_bp_ta_bpr_apn_calls` tables.
    2.  Ensure the mock `k_ausd_bp_ta_bpr_apn` procedure is configured to succeed.
    3.  Define test parameters: `stichtag = '01012024'`, `wiederanlaufWert = 98765`.
*   **Action**: Call the wrapper procedure with both parameters.

    ```sql
    CALL `my_project.my_dataset.ausd_bp_ta_bpr_apn_wrapper`('01012024', 98765);
    ```
*   **Pass/Fail Criterion**:
    1.  The call completes without raising an error.
    2.  One record exists in `my_project.my_dataset.job_control` with:
        *   `status = 'OK'`
        *   `stichtag = DATE('2024-01-01')`
        *   `restart_value = 98765`
    3.  One record exists in `my_project.my_dataset.mock_k_ausd_bp_ta_bpr_apn_calls` with:
        *   `p_stichtag = '01012024'`
        *   `p_wiederanlaufWert = 98765`

### Test Case 6: Error Handling - Invalid `stichtag` Format

*   **Purpose**: Verify that the wrapper correctly handles an invalid `stichtag` format, logs the error, and sets the job status to 'ERROR' without invoking the core business logic. This tests transformation correctness for error handling and type handling.
*   **Setup**:
    1.  Clear `job_control` and `mock_k_ausd_bp_ta_bpr_apn_calls` tables.
    2.  Ensure the mock `k_ausd_bp_ta_bpr_apn` procedure is configured to succeed (it should not be called).
*   **Action**: Call the wrapper procedure with an invalid `stichtag` format (e.g., 'YYYY-MM-DD' instead of 'DDMMYYYY').

    ```sql
    -- This call is expected to fail and raise an error
    CALL `my_project.my_dataset.ausd_bp_ta_bpr_apn_wrapper`('2023-03-15', NULL);
    ```
*   **Pass/Fail Criterion**:
    1.  The call raises an error, specifically a `Bad date format` error from `PARSE_DATE`.
    2.  One record exists in `my_project.my_dataset.job_control` with:
        *   `status = 'ERROR'`
        *   `error_message` containing text similar to "Bad date format: '2023-03-15'"
        *   `stichtag` is `NULL` (as `PARSE_DATE` failed)
    3.  No records exist in `my_project.my_dataset.mock_k_ausd_bp_ta_bpr_apn_calls` (the core logic was not invoked).

### Test Case 7: Error Handling - Empty String `stichtag`

*   **Purpose**: Verify that an empty string for `p_stichtag` is treated the same as `NULL`, defaulting to `CURRENT_DATE()`, ensuring consistent NULL handling.
*   **Setup**:
    1.  Clear `job_control` and `mock_k_ausd_bp_ta_bpr_apn_calls` tables.
    2.  Ensure the mock `k_ausd_bp_ta_bpr_apn` procedure is configured to succeed.
*   **Action**: Call the wrapper procedure with an empty string for `p_stichtag`.

    ```sql
    CALL `my_project.my_dataset.ausd_bp_ta_bpr_apn_wrapper`('', NULL);
    ```
*   **Pass/Fail Criterion**:
    1.  The call completes without raising an error.
    2.  One record exists in `my_project.my_dataset.job_control` with:
        *   `status = 'OK'`
        *   `stichtag = CURRENT_DATE()`
        *   `restart_value = 0`
    3.  One record exists in `my_project.my_dataset.mock_k_ausd_bp_ta_bpr_apn_calls` with:
        *   `p_stichtag = FORMAT_DATE('%d%m%Y', CURRENT_DATE())`
        *   `p_wiederanlaufWert = 0`

### Test Case 8: Error Handling - Core Logic (`k_ausd_bp_ta_bpr_apn`) Fails

*   **Purpose**: Verify that if the invoked core business logic procedure (`k_ausd_bp_ta_bpr_apn`) fails, the wrapper catches the error, logs it in `job_control`, and sets the job status to 'ERROR'. This tests the `EXCEPTION WHEN ERROR` block.
*   **Setup**:
    1.  Clear `job_control` and `mock_k_ausd_bp_ta_bpr_apn_calls` tables.
    2.  **Modify the mock `k_ausd_bp_ta_bpr_apn` procedure to always raise an error**:

        ```sql
        CREATE OR REPLACE PROCEDURE `my_project.my_dataset.k_ausd_bp_ta_bpr_apn`(
          IN p_job_kennung STRING,
          IN p_stichtag STRING,
          IN p_dw_eintragsnr INT64,
          IN p_wiederanlaufWert INT64
        )
        BEGIN
          -- Record the parameters passed to this mock procedure (optional, but good for debugging)
          INSERT INTO `my_project.my_dataset.mock_k_ausd_bp_ta_bpr_apn_calls` (
            call_id, p_job_kennung, p_stichtag, p_dw_eintragsnr, p_wiederanlaufWert, call_timestamp
          )
          VALUES (
            (SELECT IFNULL(MAX(call_id), 0) + 1 FROM `my_project.my_dataset.mock_k_ausd_bp_ta_bpr_apn_calls`),
            p_job_kennung,
            p_stichtag,
            p_dw_eintragsnr,
            p_wiederanlaufWert,
            CURRENT_TIMESTAMP()
          );
          -- Simulate failure of the core logic
          RAISE USING MESSAGE = 'Simulated failure in k_ausd_bp_ta_bpr_apn';
        END;
        ```
*   **Action**: Call the wrapper procedure with any valid parameters (e.g., `NULL, NULL`).

    ```sql
    -- This call is expected to fail and raise an error
    CALL `my_project.my_dataset.ausd_bp_ta_bpr_apn_wrapper`(NULL, NULL);
    ```
*   **Pass/Fail Criterion**:
    1.  The call raises an error, and the error message should indicate it originated from the wrapper's `EXCEPTION` block (e.g., "AppError: Simulated failure in k_ausd_bp_ta_bpr_apn").
    2.  One record exists in `my_project.my_dataset.job_control` with:
        *   `status = 'ERROR'`
        *   `error_message` containing "Simulated failure in k_ausd_bp_ta_bpr_apn"
    3.  One record exists in `my_project.my_dataset.mock_k_ausd_bp_ta_bpr_apn_calls` (confirming the core logic was attempted before failure).
*   **Cleanup**: Revert the mock `k_ausd_bp_ta_bpr_apn` procedure to its successful state for subsequent tests.

### Test Case 9: `job_nr` Incrementation

*   **Purpose**: Verify that the `job_nr` in the `job_control` table is correctly incremented for successive job runs, ensuring unique job identification. This tests data quality and aggregation correctness (`MAX(job_nr) + 1`).
*   **Setup**:
    1.  Clear `job_control` and `mock_k_ausd_bp_ta_bpr_apn_calls` tables.
    2.  Ensure the mock `k_ausd_bp_ta_bpr_apn` procedure is configured to succeed.
*   **Action**: Call the wrapper procedure twice consecutively.

    ```sql
    CALL `my_project.my_dataset.ausd_bp_ta_bpr_apn_wrapper`(NULL, NULL);
    CALL `my_project.my_dataset.ausd_bp_ta_bpr_apn_wrapper`('01012023', 100);
    ```
*   **Pass/Fail Criterion**:
    1.  Both calls complete without error.
    2.  Two records exist in `my_project.my_dataset.job_control`.
    3.  The `job_nr` for the first record is `1`.
    4.  The `job_nr` for the second record is `2`.
    5.  The `job_nr` in `mock_k_ausd_bp_ta_bpr_apn_calls` for each call matches the corresponding `job_nr` in `job_control`.

### Test Case 10: `log_file` Naming Convention

*   **Purpose**: Verify that the `log_file` name generated and stored in `job_control` adheres to the expected naming convention. This tests string manipulation and output parity.
*   **Setup**:
    1.  Clear `job_control` and `mock_k_ausd_bp_ta_bpr_apn_calls` tables.
    2.  Ensure the mock `k_ausd_bp_ta_bpr_apn` procedure is configured to succeed.
*   **Action**: Call the wrapper procedure once.

    ```sql
    CALL `my_project.my_dataset.ausd_bp_ta_bpr_apn_wrapper`(NULL, NULL);
    ```
*   **Pass/Fail Criterion**:
    1.  The call completes without error.
    2.  One record exists in `my_project.my_dataset.job_control`.
    3.  The `log_file` column for this record matches the pattern `job_<job_nr>_ausd_bp_ta_bpr_apn.log`, where `<job_nr>` is the actual `job_nr` assigned (e.g., `job_1_ausd_bp_ta_bpr_apn.log`).

---