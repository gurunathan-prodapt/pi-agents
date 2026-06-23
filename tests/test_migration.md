The migration of `r_ausd_v_ta_p_discount.ksh` to a BigQuery Stored Procedure `BERT_V_TA_P_DISCOUNT` primarily involves translating orchestration, logging, and error handling logic. The core data processing is delegated to `k_ausd_v_ta_p_discount.ksh`, which is assumed to be migrated to `my_project.my_dataset.k_ausd_v_ta_p_discount`.

The following test cases validate the migrated wrapper procedure, focusing on its orchestration capabilities, logging, and error handling.

---

### Pre-requisite: BigQuery Table and Mock Procedure Definitions

Before running any tests, ensure the following DDLs are applied and mock procedures are defined in your BigQuery test environment.

**1. BigQuery Table DDLs:**

```sql
-- DDL for job_control table
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_control` (
  job_entry_nr INT64 NOT NULL,
  job_name STRING NOT NULL,
  script_name STRING,
  log_file STRING,
  status STRING,
  stichtag STRING,
  created_at TIMESTAMP,
  finished_at TIMESTAMP
);

-- DDL for job_log table
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_log` (
  job_entry_nr INT64 NOT NULL,
  job_name STRING NOT NULL,
  log_message STRING,
  created_at TIMESTAMP
);

-- DDL for job_error_log table
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_error_log` (
  job_name STRING NOT NULL,
  job_entry_nr INT64 NOT NULL,
  error_nr INT64,
  error_arg STRING,
  created_at TIMESTAMP
);
```

**2. Mock `k_ausd_v_ta_p_discount` Procedures:**

We need two versions of the mock core procedure to simulate success and failure scenarios for the wrapper. The test runner should `CREATE OR REPLACE` the `my_project.my_dataset.k_ausd_v_ta_p_discount` procedure with the appropriate mock before each test case that calls it.

**Mock for Successful Core Script Execution:**
```sql
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.k_ausd_v_ta_p_discount`(
  IN p_job_kennung STRING,
  IN p_dw_eintrags_nr INT64
)
BEGIN
  INSERT INTO `my_project.my_dataset.job_log`
  (job_entry_nr, job_name, log_message, created_at)
  VALUES
  (p_dw_eintrags_nr, p_job_kennung, 'Mock k_ausd_v_ta_p_discount: Core logic executed successfully.', CURRENT_TIMESTAMP());
END;
```

**Mock for Failing Core Script Execution:**
```sql
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.k_ausd_v_ta_p_discount`(
  IN p_job_kennung STRING,
  IN p_dw_eintrags_nr INT64
)
BEGIN
  INSERT INTO `my_project.my_dataset.job_log`
  (job_entry_nr, job_name, log_message, created_at)
  VALUES
  (p_dw_eintrags_nr, p_job_kennung, 'Mock k_ausd_v_ta_p_discount: Simulating core logic failure.', CURRENT_TIMESTAMP());
  -- Simulate an error, e.g., division by zero
  SELECT 1/0;
END;
```

---

### Test Case 1: Successful Job Execution

*   **Purpose**: Verify that the migrated wrapper procedure executes successfully, records correct job metadata, logs success messages, and correctly calls the core script when the core script succeeds. This covers output parity, transformation correctness (JobKennung uppercase, date formatting, DW_EintragsNr generation, LogDatei naming), and data quality assertions for logging tables.
*   **Setup**:
    1.  Clear all rows from `my_project.my_dataset.job_control`, `my_project.my_dataset.job_log`, and `my_project.my_dataset.job_error_log`.
    2.  Deploy the "Mock for Successful Core Script Execution" version of `my_project.my_dataset.k_ausd_v_ta_p_discount`.
*   **Action**:
    1.  Execute the migrated wrapper procedure:
        ```sql
        CALL `my_project.my_dataset.BERT_V_TA_P_DISCOUNT`(NULL, NULL, FALSE);
        ```
*   **Pass/Fail Criterion**:
    1.  The procedure call completes without raising an error.
    2.  **`my_project.my_dataset.job_control`**: Contains exactly one row with:
        *   `job_name = 'BERT_V_TA_P_DISCOUNT'`
        *   `status = 'OK'`
        *   `stichtag` matches `FORMAT_DATE('%d%m%Y', CURRENT_DATE())` (e.g., '27032024')
        *   `log_file` matches the pattern `'BERT_V_TA_P_DISCOUNT_<job_entry_nr>.log'`
        *   `created_at` and `finished_at` are populated, and `finished_at` is after `created_at`.
    3.  **`my_project.my_dataset.job_log`**: Contains at least two rows for the executed `job_entry_nr`:
        *   One with `log_message` containing `'Mock k_ausd_v_ta_p_discount: Core logic executed successfully.'`.
        *   One with `log_message = 'Die Abarbeitung wurde ohne erkennbare Fehler beendet'`.
    4.  **`my_project.my_dataset.job_error_log`**: Contains zero rows.

---

### Test Case 2: Core Script Failure Handling

*   **Purpose**: Verify that the migrated wrapper procedure correctly handles errors originating from the core script, updates the job status to 'ERROR', and logs appropriate error messages. This tests the `EXCEPTION WHEN ERROR` block and external system replacement (shell `trap` to BigQuery `EXCEPTION`).
*   **Setup**:
    1.  Clear all rows from `my_project.my_dataset.job_control`, `my_project.my_dataset.job_log`, and `my_project.my_dataset.job_error_log`.
    2.  Deploy the "Mock for Failing Core Script Execution" version of `my_project.my_dataset.k_ausd_v_ta_p_discount`.
*   **Action**:
    1.  Execute the migrated wrapper procedure:
        ```sql
        CALL `my_project.my_dataset.BERT_V_TA_P_DISCOUNT`(NULL, NULL, FALSE);
        ```
*   **Pass/Fail Criterion**:
    1.  The procedure call raises an error with `MESSAGE_TEXT = 'Job aborted due to error'`.
    2.  **`my_project.my_dataset.job_control`**: Contains exactly one row with:
        *   `job_name = 'BERT_V_TA_P_DISCOUNT'`
        *   `status = 'ERROR'`
        *   `created_at` and `finished_at` are populated.
    3.  **`my_project.my_dataset.job_log`**: Contains at least two rows for the executed `job_entry_nr`:
        *   One with `log_message` containing `'Mock k_ausd_v_ta_p_discount: Simulating core logic failure.'`.
        *   One with `log_message` containing `'AppError: Abbruch - '` followed by an error message (e.g., `division by zero`).
    4.  **`my_project.my_dataset.job_error_log`**: Contains zero rows (as the error is from the core script, not parameter validation in the wrapper).

---

### Test Case 3: Help Parameter (`-h`) Functionality

*   **Purpose**: Verify that the migrated procedure correctly handles the `-h` parameter by displaying usage information and exiting without performing any job processing or logging, mirroring the original script's behavior. This covers output parity and transformation correctness.
*   **Setup**:
    1.  Clear all rows from `my_project.my_dataset.job_control`, `my_project.my_dataset.job_log`, and `my_project.my_dataset.job_error_log`.
    2.  Deploy the "Mock for Successful Core Script Execution" version of `my_project.my_dataset.k_ausd_v_ta_p_discount` (though it should not be called).
*   **Action**:
    1.  Execute the migrated wrapper procedure with the help parameter:
        ```sql
        CALL `my_project.my_dataset.BERT_V_TA_P_DISCOUNT`(NULL, NULL, TRUE);
        ```
*   **Pass/Fail Criterion**:
    1.  The procedure call completes successfully and returns a result set containing:
        *   A column `Programm` with value `'Vertragsdatenabgleich'`.
        *   A column `Version` with value `'V1.0.0'`.
        *   A column `Beschreibung` with value `'Aufruf: Parameter -h zeigt diese Seite an'`.
    2.  **`my_project.my_dataset.job_control`**: Contains zero rows.
    3.  **`my_project.my_dataset.job_log`**: Contains zero rows.
    4.  **`my_project.my_dataset.job_error_log`**: Contains zero rows.
    5.  The mock `k_ausd_v_ta_p_discount` procedure is *not* called (verified by checking `job_log` for mock messages).

---

### Test Case 4: Parameter Error Logging Mechanism (Discrepancy Highlight)

*   **Purpose**: Verify that the error logging mechanism for parameter validation (the `IF ErrNr != 0` block) functions correctly if `ErrNr` is explicitly set to a non-zero value. This test highlights that the BigQuery pseudocode, as provided, does not replicate the `getopts` error handling for unknown parameters or missing arguments from the original ksh script, but confirms the *structure* for parameter error handling is in place.
*   **Setup**:
    1.  Clear all rows from `my_project.my_dataset.job_control`, `my_project.my_dataset.job_log`, and `my_project.my_dataset.job_error_log`.
    2.  **Temporarily modify the `BERT_V_TA_P_DISCOUNT` procedure** to force `ErrNr` and `ErrArg` before the `IF ErrNr != 0` block, simulating a parameter parsing error.
        ```sql
        -- Inside `my_project.my_dataset.BERT_V_TA_P_DISCOUNT`,
        -- place these lines before the `IF p_h = TRUE THEN` block:
        SET ErrNr = 192;
        SET ErrArg = 'unknown_param_X';
        ```
*   **Action**:
    1.  Execute the modified migrated wrapper procedure:
        ```sql
        CALL `my_project.my_dataset.BERT_V_TA_P_DISCOUNT`(NULL, NULL, FALSE);
        ```
*   **Pass/Fail Criterion**:
    1.  The procedure call raises an error with `MESSAGE_TEXT = 'Parameter validation failed'`.
    2.  **`my_project.my_dataset.job_error_log`**: Contains exactly one row with:
        *   `job_name = 'BERT_V_TA_P_DISCOUNT'`
        *   `job_entry_nr = 0` (as `DW_EintragsNr` is not yet set)
        *   `error_nr = 192`
        *   `error_arg = 'unknown_param_X'`
        *   `created_at` is populated.
    3.  **`my_project.my_dataset.job_control`**: Contains zero rows (as the error occurs before job entry creation).
    4.  **`my_project.my_dataset.job_log`**: Contains zero rows.

---

### Test Case 5: Idempotency of `DW_EintragsNr` Generation

*   **Purpose**: Verify that `DW_EintragsNr` is correctly incremented for each new job execution, ensuring unique job entries and proper data quality for job tracking.
*   **Setup**:
    1.  Clear all rows from `my_project.my_dataset.job_control`, `my_project.my_dataset.job_log`, and `my_project.my_dataset.job_error_log`.
    2.  Deploy the "Mock for Successful Core Script Execution" version of `my_project.my_dataset.k_ausd_v_ta_p_discount`.
*   **Action**:
    1.  Execute the migrated wrapper procedure for the first time:
        ```sql
        CALL `my_project.my_dataset.BERT_V_TA_P_DISCOUNT`(NULL, NULL, FALSE);
        ```
    2.  Execute the migrated wrapper procedure for the second time:
        ```sql
        CALL `my_project.my_dataset.BERT_V_TA_P_DISCOUNT`(NULL, NULL, FALSE);
        ```
*   **Pass/Fail Criterion**:
    1.  Both procedure calls complete successfully.
    2.  **`my_project.my_dataset.job_control`**: Contains exactly two rows, both with `status = 'OK'`.
    3.  The `job_entry_nr` for the second entry is exactly `1` greater than the `job_entry_nr` of the first entry.
    4.  The `log_file` names for the two entries are distinct, reflecting their respective `job_entry_nr` values.
    5.  **`my_project.my_dataset.job_log`**: Contains at least four rows (two for each job execution, including mock messages).
    6.  **`my_project.my_dataset.job_error_log`**: Contains zero rows.

---

### Test Case 6: `p_s` and `p_l` Parameter Acceptance

*   **Purpose**: Verify that the `p_s` and `p_l` parameters are accepted by the procedure without causing errors, even if they are not explicitly used in the wrapper's logic, mirroring the original script's behavior where these parameters were declared but not used in the wrapper. This covers output parity and transformation correctness.
*   **Setup**:
    1.  Clear all rows from `my_project.my_dataset.job_control`, `my_project.my_dataset.job_log`, and `my_project.my_dataset.job_error_log`.
    2.  Deploy the "Mock for Successful Core Script Execution" version of `my_project.my_dataset.k_ausd_v_ta_p_discount`.
*   **Action**:
    1.  Execute the migrated wrapper procedure with non-NULL values for `p_s` and `p_l`:
        ```sql
        CALL `my_project.my_dataset.BERT_V_TA_P_DISCOUNT`('some_source_system', 'some_log_level', FALSE);
        ```
*   **Pass/Fail Criterion**:
    1.  The procedure call completes successfully without raising any errors related to parameter handling.
    2.  The job executes as a normal successful run, as per Test Case 1 (i.e., one 'OK' entry in `job_control`, success messages in `job_log`, zero errors in `job_error_log`).
    3.  The values passed for `p_s` and `p_l` do not interfere with the wrapper's execution flow.