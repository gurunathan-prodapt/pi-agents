As a senior data-migration QA engineer, I've analyzed the migration design for `r_ausd_bp_ta_rn_da_vda_tk.ksh` to BigQuery. The primary focus of this migration is the wrapper script's parameter handling, logging, and orchestration, as the core data transformation logic within `k_ausd_bp_ta_rn_da_vda_tk.ksh` is explicitly noted as a stub and "to be determined."

Therefore, these tests will concentrate on validating the behavioral equivalence of the wrapper's functionality, including parameter defaults, validation, error handling, and the correct population of the new BigQuery logging and control tables.

---

## Migration Validation Tests for `r_ausd_bp_ta_rn_da_vda_tk.ksh`

**Assumptions & Pre-requisites:**

*   BigQuery project and dataset (`project.dataset`) are configured and accessible.
*   The DDLs for `project.dataset.job_log`, `project.dataset.job_status`, and `project.dataset.job_control` have been executed, creating these tables.
*   The BigQuery Stored Procedures `project.dataset.bereitstellung_basisprodukte_bert` and `project.dataset.k_ausd_bp_ta_rn_da_vda_tk` have been deployed.
*   The `project.dataset.k_ausd_bp_ta_rn_da_vda_tk` procedure is deployed as a stub, meaning it logs its start/end but performs no actual data manipulation unless explicitly modified for a test case.
*   For legacy script execution, a shell environment with the original `ksh` script and its dependencies (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) is available.
*   `CURRENT_DATE()` in BigQuery is equivalent to `sysdate` in the legacy environment for date defaulting purposes.

---

### Test Case 1: Default Parameter Handling (No Parameters)

**Purpose:** Verify that when no `Stichtag` or `Wiederanlaufwert` is provided, the `Stichtag` defaults to the current system date (DDMMYYYY) and `Wiederanlaufwert` defaults to `0` in the migrated BigQuery procedure, mirroring the legacy script's behavior.

**Setup:**
1.  Ensure the BigQuery logging tables (`job_log`, `job_status`, `job_control`) are empty or truncated before execution.
    ```sql
    TRUNCATE TABLE `project.dataset.job_log`;
    TRUNCATE TABLE `project.dataset.job_status`;
    TRUNCATE TABLE `project.dataset.job_control`;
    ```
2.  Note the current system date in `DDMMYYYY` format (e.g., `01012024`).

**Action (Legacy):**
Execute the legacy KornShell script without any parameters.
```bash
./r_ausd_bp_ta_rn_da_vda_tk.ksh
```
Observe the console output and the generated log file (e.g., `ausd_bp_ta_rn_da_vda_tk_12345.log`).

**Action (Migrated):**
Execute the BigQuery Stored Procedure with `NULL` for both parameters.
```sql
CALL `project.dataset.bereitstellung_basisprodukte_bert`(NULL, NULL);
```

**Pass/Fail Criterion:**

*   **Legacy:**
    *   The log file should contain lines similar to:
        ```
        Stichtag  : '<CURRENT_DATE_DDMMYYYY>'
        ...
        ./k_ausd_bp_ta_rn_da_vda_tk.ksh -j ausd_bp_ta_rn_da_vda_tk -s <CURRENT_DATE_DDMMYYYY> -f <JOB_ENTRY_NR> -l 0
        ```
    *   The script should exit with status `0`.
*   **Migrated:**
    *   The `CALL` statement completes successfully.
    *   Query `project.dataset.job_control`:
        ```sql
        SELECT stichtag, restart_value FROM `project.dataset.job_control` ORDER BY created_at DESC LIMIT 1;
        ```
        Expected result: `stichtag` should be `FORMAT_DATE('%d%m%Y', CURRENT_DATE())` and `restart_value` should be `0`.
    *   Query `project.dataset.job_status`:
        ```sql
        SELECT status FROM `project.dataset.job_status` ORDER BY updated_at DESC LIMIT 1;
        ```
        Expected result: `status` should be `'SUCCESS'`.
    *   Query `project.dataset.job_log`:
        ```sql
        SELECT log_level, log_message FROM `project.dataset.job_log` WHERE job_kennung = 'ausd_bp_ta_rn_da_vda_tk' ORDER BY created_at DESC;
        ```
        Expected: Multiple 'INFO' entries, including job start/end and kernel start/end, reflecting the default parameters.

---

### Test Case 2: Specific Stichtag Parameter

**Purpose:** Verify that the provided `Stichtag` is correctly used and `Wiederanlaufwert` defaults to `0`.

**Setup:**
1.  Clear BigQuery logging tables.
    ```sql
    TRUNCATE TABLE `project.dataset.job_log`;
    TRUNCATE TABLE `project.dataset.job_status`;
    TRUNCATE TABLE `project.dataset.job_control`;
    ```
2.  Choose a specific `Stichtag` (e.g., `01012023`).

**Action (Legacy):**
Execute the legacy KornShell script with a specific `Stichtag`.
```bash
./r_ausd_bp_ta_rn_da_vda_tk.ksh -s 01012023
```
Observe the console output and the generated log file.

**Action (Migrated):**
Execute the BigQuery Stored Procedure with a specific `Stichtag` and `NULL` for `Wiederanlaufwert`.
```sql
CALL `project.dataset.bereitstellung_basisprodukte_bert`('01012023', NULL);
```

**Pass/Fail Criterion:**

*   **Legacy:**
    *   The log file should contain lines similar to:
        ```
        Stichtag  : '01012023'
        ...
        ./k_ausd_bp_ta_rn_da_vda_tk.ksh -j ausd_bp_ta_rn_da_vda_tk -s 01012023 -f <JOB_ENTRY_NR> -l 0
        ```
    *   The script should exit with status `0`.
*   **Migrated:**
    *   The `CALL` statement completes successfully.
    *   Query `project.dataset.job_control`:
        ```sql
        SELECT stichtag, restart_value FROM `project.dataset.job_control` ORDER BY created_at DESC LIMIT 1;
        ```
        Expected result: `stichtag` should be `'01012023'` and `restart_value` should be `0`.
    *   Query `project.dataset.job_status`:
        ```sql
        SELECT status FROM `project.dataset.job_status` ORDER BY updated_at DESC LIMIT 1;
        ```
        Expected result: `status` should be `'SUCCESS'`.

---

### Test Case 3: Specific Wiederanlaufwert Parameter

**Purpose:** Verify that the provided `Wiederanlaufwert` is correctly used and `Stichtag` defaults to the current system date.

**Setup:**
1.  Clear BigQuery logging tables.
    ```sql
    TRUNCATE TABLE `project.dataset.job_log`;
    TRUNCATE TABLE `project.dataset.job_status`;
    TRUNCATE TABLE `project.dataset.job_control`;
    ```
2.  Choose a specific `Wiederanlaufwert` (e.g., `12345`).
3.  Note the current system date in `DDMMYYYY` format.

**Action (Legacy):**
Execute the legacy KornShell script with a specific `Wiederanlaufwert`.
```bash
./r_ausd_bp_ta_rn_da_vda_tk.ksh -l 12345
```
Observe the console output and the generated log file.

**Action (Migrated):**
Execute the BigQuery Stored Procedure with `NULL` for `Stichtag` and a specific `Wiederanlaufwert`.
```sql
CALL `project.dataset.bereitstellung_basisprodukte_bert`(NULL, 12345);
```

**Pass/Fail Criterion:**

*   **Legacy:**
    *   The log file should contain lines similar to:
        ```
        Stichtag  : '<CURRENT_DATE_DDMMYYYY>'
        ...
        ./k_ausd_bp_ta_rn_da_vda_tk.ksh -j ausd_bp_ta_rn_da_vda_tk -s <CURRENT_DATE_DDMMYYYY> -f <JOB_ENTRY_NR> -l 12345
        ```
    *   The script should exit with status `0`.
*   **Migrated:**
    *   The `CALL` statement completes successfully.
    *   Query `project.dataset.job_control`:
        ```sql
        SELECT stichtag, restart_value FROM `project.dataset.job_control` ORDER BY created_at DESC LIMIT 1;
        ```
        Expected result: `stichtag` should be `FORMAT_DATE('%d%m%Y', CURRENT_DATE())` and `restart_value` should be `12345`.
    *   Query `project.dataset.job_status`:
        ```sql
        SELECT status FROM `project.dataset.job_status` ORDER BY updated_at DESC LIMIT 1;
        ```
        Expected result: `status` should be `'SUCCESS'`.

---

### Test Case 4: Both Parameters Provided

**Purpose:** Verify that both provided `Stichtag` and `Wiederanlaufwert` are correctly used.

**Setup:**
1.  Clear BigQuery logging tables.
    ```sql
    TRUNCATE TABLE `project.dataset.job_log`;
    TRUNCATE TABLE `project.dataset.job_status`;
    TRUNCATE TABLE `project.dataset.job_control`;
    ```
2.  Choose specific values for `Stichtag` (e.g., `15062024`) and `Wiederanlaufwert` (e.g., `98765`).

**Action (Legacy):**
Execute the legacy KornShell script with both parameters.
```bash
./r_ausd_bp_ta_rn_da_vda_tk.ksh -s 15062024 -l 98765
```
Observe the console output and the generated log file.

**Action (Migrated):**
Execute the BigQuery Stored Procedure with both parameters.
```sql
CALL `project.dataset.bereitstellung_basisprodukte_bert`('15062024', 98765);
```

**Pass/Fail Criterion:**

*   **Legacy:**
    *   The log file should contain lines similar to:
        ```
        Stichtag  : '15062024'
        ...
        ./k_ausd_bp_ta_rn_da_vda_tk.ksh -j ausd_bp_ta_rn_da_vda_tk -s 15062024 -f <JOB_ENTRY_NR> -l 98765
        ```
    *   The script should exit with status `0`.
*   **Migrated:**
    *   The `CALL` statement completes successfully.
    *   Query `project.dataset.job_control`:
        ```sql
        SELECT stichtag, restart_value FROM `project.dataset.job_control` ORDER BY created_at DESC LIMIT 1;
        ```
        Expected result: `stichtag` should be `'15062024'` and `restart_value` should be `98765`.
    *   Query `project.dataset.job_status`:
        ```sql
        SELECT status FROM `project.dataset.job_status` ORDER BY updated_at DESC LIMIT 1;
        ```
        Expected result: `status` should be `'SUCCESS'`.

---

### Test Case 5: Invalid Stichtag Format

**Purpose:** Verify that the migrated BigQuery procedure correctly validates the `Stichtag` format (DDMMYYYY) and fails gracefully, logging the error. This is an improvement over the legacy script which would pass the invalid format to the kernel.

**Setup:**
1.  Clear BigQuery logging tables.
    ```sql
    TRUNCATE TABLE `project.dataset.job_log`;
    TRUNCATE TABLE `project.dataset.job_status`;
    TRUNCATE TABLE `project.dataset.job_control`;
    ```
2.  Choose an invalid `Stichtag` format (e.g., `2023-01-01`).

**Action (Legacy):**
Execute the legacy KornShell script with an invalid `Stichtag` format.
```bash
./r_ausd_bp_ta_rn_da_vda_tk.ksh -s 2023-01-01
```
Observe the console output and the generated log file.

**Action (Migrated):**
Execute the BigQuery Stored Procedure with an invalid `Stichtag` format.
```sql
CALL `project.dataset.bereitstellung_basisprodukte_bert`('2023-01-01', NULL);
```

**Pass/Fail Criterion:**

*   **Legacy:**
    *   The legacy script's wrapper would likely pass `'2023-01-01'` to the kernel script, as its `pruefeParameterGesetzt` only checks for non-emptiness. The error would occur later within the kernel script if it attempts to parse the date. The wrapper itself would report success unless the kernel explicitly signals an error back.
*   **Migrated:**
    *   The `CALL` statement should fail with a `SQLSTATE '45000'` error. The error message should contain "Invalid Stichtag format. Expected DDMMYYYY, got: 2023-01-01".
    *   Query `project.dataset.job_status`:
        ```sql
        SELECT status FROM `project.dataset.job_status` ORDER BY updated_at DESC LIMIT 1;
        ```
        Expected result: `status` should be `'FAILED'`.
    *   Query `project.dataset.job_log`:
        ```sql
        SELECT log_level, log_message FROM `project.dataset.job_log` WHERE job_kennung = 'ausd_bp_ta_rn_da_vda_tk' ORDER BY created_at DESC LIMIT 1;
        ```
        Expected: An 'ERROR' entry with a message indicating the invalid Stichtag format.

---

### Test Case 6: Empty Stichtag Parameter (Explicit Empty String)

**Purpose:** Verify that an explicitly empty `Stichtag` string (`''`) is treated as `NULL` and defaults to the current system date, consistent with the legacy script's `if [[ -z "$p_stichtag" ]]` check.

**Setup:**
1.  Clear BigQuery logging tables.
    ```sql
    TRUNCATE TABLE `project.dataset.job_log`;
    TRUNCATE TABLE `project.dataset.job_status`;
    TRUNCATE TABLE `project.dataset.job_control`;
    ```
2.  Note the current system date in `DDMMYYYY` format.

**Action (Legacy):**
Execute the legacy KornShell script with an empty `Stichtag`.
```bash
./r_ausd_bp_ta_rn_da_vda_tk.ksh -s ""
```
Observe the console output and the generated log file.

**Action (Migrated):**
Execute the BigQuery Stored Procedure with an empty string for `p_stichtag`.
```sql
CALL `project.dataset.bereitstellung_basisprodukte_bert`('', NULL);
```

**Pass/Fail Criterion:**

*   **Legacy:**
    *   The log file should show `Stichtag  : '<CURRENT_DATE_DDMMYYYY>'`.
    *   The script should exit with status `0`.
*   **Migrated:**
    *   The `CALL` statement completes successfully.
    *   Query `project.dataset.job_control`:
        ```sql
        SELECT stichtag, restart_value FROM `project.dataset.job_control` ORDER BY created_at DESC LIMIT 1;
        ```
        Expected result: `stichtag` should be `FORMAT_DATE('%d%m%Y', CURRENT_DATE())` and `restart_value` should be `0`.
    *   Query `project.dataset.job_status`:
        ```sql
        SELECT status FROM `project.dataset.job_status` ORDER BY updated_at DESC LIMIT 1;
        ```
        Expected result: `status` should be `'SUCCESS'`.

---

### Test Case 7: Error Propagation from Kernel Script

**Purpose:** Verify that an error originating in the `k_ausd_bp_ta_rn_da_vda_tk` (kernel) script is correctly caught, logged, and propagated by the wrapper, leading to a 'FAILED' status.

**Setup:**
1.  Clear BigQuery logging tables.
    ```sql
    TRUNCATE TABLE `project.dataset.job_log`;
    TRUNCATE TABLE `project.dataset.job_status`;
    TRUNCATE TABLE `project.dataset.job_control`;
    ```
2.  **Temporarily modify the BigQuery `k_ausd_bp_ta_rn_da_vda_tk` stored procedure** to simulate an error. Insert the `SIGNAL` statement after the initial `INSERT` into `job_log`.
    ```sql
    CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_bp_ta_rn_da_vda_tk`(
        v_job_kennung STRING,
        v_stichtag STRING,
        v_job_eintragsnr INT64,
        v_restart_value INT64
    )
    OPTIONS (
        description = 'Core data processing for BERT base products (Legacy: k_ausd_bp_ta_rn_da_vda_tk.ksh - content to be implemented)'
    )
    BEGIN
        INSERT INTO `project.dataset.job_log` (job_name, job_kennung, log_level, log_message, created_at)
        VALUES (
            'k_ausd_bp_ta_rn_da_vda_tk',
            v_job_kennung,
            'INFO',
            FORMAT("Core logic started for Stichtag: %s, Restart Value: %d, Job Entry Nr: %d", v_stichtag, v_restart_value, v_job_eintragsnr),
            CURRENT_TIMESTAMP()
        );

        -- SIMULATED ERROR FOR TESTING
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated kernel script failure for testing purposes.';

        -- ... (rest of the original stub code)
    END;
    ```
3.  For the legacy test, assume the `k_ausd_bp_ta_rn_da_vda_tk.ksh` script is modified to `exit 1` or similar to trigger the `trap ERR` in the wrapper.

**Action (Legacy):**
Execute the legacy KornShell script (which will call the error-simulating kernel script).
```bash
./r_ausd_bp_ta_rn_da_vda_tk.ksh -s 01012023
```
Observe the console output and the generated log file.

**Action (Migrated):**
Execute the BigQuery Stored Procedure.
```sql
CALL `project.dataset.bereitstellung_basisprodukte_bert`('01012023', NULL);
```

**Pass/Fail Criterion:**

*   **Legacy:**
    *   The log file should contain `DWMSG_Fehlerbehandlung` messages and an indication of an error/abnormal termination (e.g., `AppError: Abbruch`).
    *   The script should exit with a non-zero status code.
*   **Migrated:**
    *   The `CALL` statement should fail with a `SQLSTATE '45000'` error. The error message should contain "Simulated kernel script failure for testing purposes."
    *   Query `project.dataset.job_status`:
        ```sql
        SELECT status FROM `project.dataset.job_status` ORDER BY updated_at DESC LIMIT 1;
        ```
        Expected result: `status` should be `'FAILED'`.
    *   Query `project.dataset.job_log`:
        ```sql
        SELECT log_level, log_message FROM `project.dataset.job_log` WHERE job_kennung = 'ausd_bp_ta_rn_da_vda_tk' ORDER BY created_at DESC LIMIT 1;
        ```
        Expected: An 'ERROR' entry with a message detailing the simulated kernel error.

**Cleanup:**
*   **Revert the `k_ausd_bp_ta_rn_da_vda_tk` stored procedure** to its original stub implementation after this test.

---

### Test Case 8: Logging Table Schema and Data Integrity

**Purpose:** Verify that the DDLs for the logging tables are correctly implemented and that data is inserted with the expected schema, data types, and values for both successful and failed runs.

**Setup:**
1.  Clear BigQuery logging tables.
    ```sql
    TRUNCATE TABLE `project.dataset.job_log`;
    TRUNCATE TABLE `project.dataset.job_status`;
    TRUNCATE TABLE `project.dataset.job_control`;
    ```
2.  Execute a successful run:
    ```sql
    CALL `project.dataset.bereitstellung_basisprodukte_bert`('01012023', 100);
    ```
3.  Execute a failed run (e.g., invalid date format, as in Test Case 5):
    ```sql
    -- This call is expected to fail
    BEGIN
        CALL `project.dataset.bereitstellung_basisprodukte_bert`('INVALID_DATE', NULL);
    EXCEPTION WHEN ERROR THEN
        -- Catch the error to allow subsequent queries to run
        SELECT 'Caught expected error for invalid date' AS message;
    END;
    ```

**Action:**
Query the logging tables to inspect their schema and content.

**Pass/Fail Criterion:**

*   **Schema Validation:**
    *   `project.dataset.job_log`:
        ```sql
        SELECT column_name, data_type FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'job_log';
        ```
        Expected: `job_name` (STRING), `job_version` (STRING), `job_kennung` (STRING), `log_level` (STRING), `log_message` (STRING), `created_at` (TIMESTAMP). All `NOT NULL` constraints should be respected.
    *   `project.dataset.job_status`:
        ```sql
        SELECT column_name, data_type FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'job_status';
        ```
        Expected: `job_kennung` (STRING), `job_entry_nr` (INT64), `status` (STRING), `updated_at` (TIMESTAMP). All `NOT NULL` constraints should be respected.
    *   `project.dataset.job_control`:
        ```sql
        SELECT column_name, data_type FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'job_control';
        ```
        Expected: `job_kennung` (STRING), `stichtag` (STRING), `sysdate_ddmmyyyy` (STRING), `restart_value` (INT64), `created_at` (TIMESTAMP). All `NOT NULL` constraints (except `restart_value`) should be respected.

*   **Data Integrity & Row Counts:**
    *   `project.dataset.job_log`:
        ```sql
        SELECT COUNT(1) AS row_count,
               COUNTIF(log_level = 'INFO' AND log_message LIKE 'Job started%') AS job_start_info,
               COUNTIF(log_level = 'INFO' AND log_message LIKE 'Job completed%') AS job_end_info,
               COUNTIF(log_level = 'INFO' AND log_message LIKE 'Core logic started%') AS kernel_start_info,
               COUNTIF(log_level = 'INFO' AND log_message LIKE 'Core logic completed%') AS kernel_end_info,
               COUNTIF(log_level = 'ERROR') AS error_count
        FROM `project.dataset.job_log` WHERE job_kennung = 'ausd_bp_ta_rn_da_vda_tk';
        ```
        Expected: `row_count` >= 5 (2 wrapper INFOs + 2 kernel INFOs for success + 1 ERROR for failure). `job_start_info` = 2, `job_end_info` = 1, `kernel_start_info` = 1, `kernel_end_info` = 1, `error_count` = 1.
        Verify `job_name`, `job_version`, `job_kennung`, `log_level`, `log_message`, `created_at` are populated with meaningful values.
    *   `project.dataset.job_status`:
        ```sql
        SELECT COUNT(1) AS row_count,
               COUNTIF(status = 'SUCCESS') AS success_count,
               COUNTIF(status = 'FAILED') AS failed_count
        FROM `project.dataset.job_status` WHERE job_kennung = 'ausd_bp_ta_rn_da_vda_tk';
        ```
        Expected: `row_count` = 2, `success_count` = 1, `failed_count` = 1.
        Verify `job_kennung`, `job_entry_nr`, `status`, `updated_at` are correctly populated.
    *   `project.dataset.job_control`:
        ```sql
        SELECT COUNT(1) AS row_count,
               COUNTIF(stichtag = '01012023' AND restart_value = 100) AS success_params,
               COUNTIF(stichtag = 'INVALID_DATE' AND restart_value IS NULL) AS failed_params
        FROM `project.dataset.job_control` WHERE job_kennung = 'ausd_bp_ta_rn_da_vda_tk';
        ```
        Expected: `row_count` = 2. `success_params` = 1. `failed_params` = 1 (parameters are logged before validation, so the invalid date is recorded).
        Verify `job_kennung`, `stichtag`, `sysdate_ddmmyyyy`, `restart_value`, `created_at` are correctly populated.

---

### Test Case 9: Orchestration (Cloud Composer DAG)

**Purpose:** Verify that the Cloud Composer DAG correctly triggers the BigQuery Stored Procedure and passes parameters, including defaulting behavior.

**Setup:**
1.  Deploy the `dags/bereitstellung_basisprodukte_bert_dag.py` to your Cloud Composer environment.
2.  Ensure your Airflow GCP connection (`google_cloud_default`) is correctly configured.
3.  Clear BigQuery logging tables.
    ```sql
    TRUNCATE TABLE `project.dataset.job_log`;
    TRUNCATE TABLE `project.dataset.job_status`;
    TRUNCATE TABLE `project.dataset.job_control`;
    ```
4.  Note the current system date in `DDMMYYYY` format.

**Action:**
1.  **Trigger DAG with specific parameters:** Manually trigger the `bereitstellung_basisprodukte_bert_dag` from the Airflow UI, setting the DAG run configuration to:
    ```json
    {
        "stichtag_param_value": "01012023",
        "wiederanlaufwert_param_value": 100
    }
    ```
2.  **Trigger DAG with default parameters:** Manually trigger the `bereitstellung_basisprodukte_bert_dag` again, setting the DAG run configuration to:
    ```json
    {
        "stichtag_param_value": null,
        "wiederanlaufwert_param_value": null
    }
    ```
    (Or simply trigger without any config, if the DAG's default `None` values are used).

**Pass/Fail Criterion:**

*   **Airflow UI:**
    *   Both DAG runs should complete successfully (green status).
*   **BigQuery `project.dataset.job_control` table:**
    ```sql
    SELECT stichtag, restart_value, sysdate_ddmmyyyy
    FROM `project.dataset.job_control`
    WHERE job_kennung = 'ausd_bp_ta_rn_da_vda_tk'
    ORDER BY created_at DESC;
    ```
    Expected: Two entries.
    *   One entry with `stichtag = '01012023'` and `restart_value = 100`.
    *   One entry with `stichtag = '<CURRENT_DATE_DDMMYYYY>'` and `restart_value = 0`.
    *   `sysdate_ddmmyyyy` should match the system date at the time of each run.
*   **BigQuery `project.dataset.job_status` table:**
    ```sql
    SELECT status, COUNT(1) FROM `project.dataset.job_status` WHERE job_kennung = 'ausd_bp_ta_rn_da_vda_tk' GROUP BY status;
    ```
    Expected: Two entries with `status = 'SUCCESS'`.
*   **BigQuery `project.dataset.job_log` table:**
    ```sql
    SELECT log_level, log_message FROM `project.dataset.job_log` WHERE job_kennung = 'ausd_bp_ta_rn_da_vda_tk' ORDER BY created_at DESC;
    ```
    Expected: Multiple 'INFO' entries for both runs, reflecting the start, parameter values, and successful completion of the wrapper and kernel procedures.

---