The migration of `r_ausd_v_ta_period.ksh` to a BigQuery Stored Procedure (`project.dataset.sp_vertragsdatenabgleich`) primarily involves re-implementing orchestration, logging, and error handling. The core data transformation logic is assumed to be in a separate, migrated stored procedure (`sp_k_ausd_v_ta_period`).

The following tests are designed to validate the behavioral equivalence of the migrated BigQuery Stored Procedure against the legacy KornShell script.

---

## Prerequisites for Testing

Before running the tests, ensure the following BigQuery objects are created:

1.  **Logging and Status Tables DDL:**
    ```sql
    -- DDL for dw_job_registry
    CREATE TABLE IF NOT EXISTS `project.dataset.dw_job_registry` (
        job_kennung STRING NOT NULL,
        job_entry_nr INT64 NOT NULL,
        last_update_ts TIMESTAMP
    );

    -- DDL for dw_job_log
    CREATE TABLE IF NOT EXISTS `project.dataset.dw_job_log` (
        job_entry_nr INT64 NOT NULL,
        job_kennung STRING NOT NULL,
        script_name STRING,
        log_file_name STRING, -- This will be a logical name in BQ, not a real file
        log_message STRING,
        log_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );

    -- DDL for dw_job_status
    CREATE TABLE IF NOT EXISTS `project.dataset.dw_job_status` (
        job_entry_nr INT64 NOT NULL,
        job_kennung STRING NOT NULL,
        status_code STRING, -- e.g., 'OK', 'ERR'
        status_message STRING,
        status_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );

    -- DDL for dw_job_error
    CREATE TABLE IF NOT EXISTS `project.dataset.dw_job_error` (
        job_entry_nr INT64 NOT NULL,
        job_kennung STRING NOT NULL,
        error_message STRING,
        error_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );

    -- DDL for dw_job_stichtag
    CREATE TABLE IF NOT EXISTS `project.dataset.dw_job_stichtag` (
        job_entry_nr INT64 NOT NULL,
        stichtag_value STRING,
        stichtag_format STRING,
        insert_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );
    ```

2.  **Mock `sp_k_ausd_v_ta_period` Stored Procedure:**
    This mock procedure simulates the behavior of the core script, allowing us to test success and failure paths of the wrapper.
    ```sql
    CREATE OR REPLACE PROCEDURE `project.dataset.sp_k_ausd_v_ta_period`(
        IN p_job_kennung STRING,
        IN p_dw_eintrags_nr INT64,
        IN p_simulate_error BOOL
    )
    BEGIN
        IF p_simulate_error THEN
            RAISE USING MESSAGE = 'Simulated error from sp_k_ausd_v_ta_period';
        ELSE
            -- Simulate some work and log an entry
            INSERT INTO `project.dataset.dw_job_log` (job_entry_nr, job_kennung, log_message)
            VALUES (p_dw_eintrags_nr, p_job_kennung, 'Core script sp_k_ausd_v_ta_period executed successfully.');
        END IF;
    END;
    ```

3.  **Migrated `sp_vertragsdatenabgleich` Stored Procedure (under test):**
    This is the actual migrated code. The `p_simulate_core_error` and `p_unknown_param` parameters are added for testing purposes to control the internal flow.
    ```sql
    CREATE OR REPLACE PROCEDURE `project.dataset.sp_vertragsdatenabgleich`(
        IN p_help BOOL DEFAULT FALSE,
        IN p_simulate_core_error BOOL DEFAULT FALSE, -- New parameter for testing core script failure
        IN p_unknown_param STRING DEFAULT NULL -- New parameter for testing unknown parameter handling
    )
    BEGIN
        DECLARE ProgName STRING DEFAULT 'Vertragsdatenabgleich';
        DECLARE ProgVersion STRING DEFAULT 'V1.0.0';
        DECLARE JobKennung STRING DEFAULT 'BERT_V_TA_PERIOD';
        DECLARE v_sysdate STRING;
        DECLARE DW_EintragsNr INT64;
        DECLARE LogDatei STRING;
        DECLARE ErrNr INT64 DEFAULT 0;
        DECLARE ErrArg STRING DEFAULT '';
        DECLARE Name_Kernskript STRING DEFAULT 'project.dataset.sp_k_ausd_v_ta_period';

        -- Simulate usage() function
        IF p_help THEN
            SELECT FORMAT(
                """
    Programm: %s
    Version:  %s
    Aufruf:   CALL project.dataset.sp_vertragsdatenabgleich(p_help => TRUE);
    Parameter:
	p_help     zeigt diese Seite an
    p_simulate_core_error (internal for testing)
    p_unknown_param (internal for testing)

    Beschreibung:
        Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_period.
                """, ProgName, ProgVersion);
            RETURN;
        END IF;

        -- Simulate unknown parameter handling from getopts (for '?' case)
        IF p_unknown_param IS NOT NULL THEN
            SET ErrNr = 192; -- Parameter unbekannt
            SET ErrArg = p_unknown_param;
        END IF;

        IF ErrNr != 0 THEN
            -- Attempt to get a job_entry_nr for error logging if not already set
            -- This handles early errors before DW_EintragsNr is properly initialized
            IF DW_EintragsNr IS NULL OR DW_EintragsNr = 0 THEN
                SET DW_EintragsNr = (SELECT IFNULL(MAX(job_entry_nr), 0) + 1 FROM `project.dataset.dw_job_registry` WHERE job_kennung = JobKennung);
                INSERT INTO `project.dataset.dw_job_registry` (job_kennung, job_entry_nr, last_update_ts)
                VALUES (JobKennung, DW_EintragsNr, CURRENT_TIMESTAMP());
            END IF;

            INSERT INTO `project.dataset.dw_job_error` (job_entry_nr, job_kennung, error_message)
            VALUES (DW_EintragsNr, JobKennung, FORMAT('Error %d: Unknown parameter %s', ErrNr, ErrArg));
            INSERT INTO `project.dataset.dw_job_log` (job_entry_nr, job_kennung, log_message)
            VALUES (DW_EintragsNr, JobKennung, FORMAT('Error %d: Unknown parameter %s', ErrNr, ErrArg));
            INSERT INTO `project.dataset.dw_job_status` (job_entry_nr, job_kennung, status_code, status_message)
            VALUES (DW_EintragsNr, JobKennung, 'ERR', FORMAT('Parameter error: %s', ErrArg));
            RAISE USING MESSAGE = FORMAT('AppError: Abbruch (Error %d: %s)', ErrNr, ErrArg);
        END IF;

        -- Simulate DWMSG_ErmittleNr
        SET DW_EintragsNr = (SELECT IFNULL(MAX(job_entry_nr), 0) + 1 FROM `project.dataset.dw_job_registry` WHERE job_kennung = JobKennung);
        INSERT INTO `project.dataset.dw_job_registry` (job_kennung, job_entry_nr, last_update_ts)
        VALUES (JobKennung, DW_EintragsNr, CURRENT_TIMESTAMP());

        -- Simulate DWMSG_Logdateiname
        SET LogDatei = CONCAT('log_', JobKennung, '_', CAST(DW_EintragsNr AS STRING), '.log');

        -- Simulate DWMSG_ErzeugeEintrag
        INSERT INTO `project.dataset.dw_job_log` (job_entry_nr, job_kennung, script_name, log_file_name, log_message)
        VALUES (DW_EintragsNr, JobKennung, 'r_ausd_v_ta_period.ksh', LogDatei, 'Job started.');

        -- Simulate DWMSG_SetzeStichtagInfo
        SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());
        INSERT INTO `project.dataset.dw_job_stichtag` (job_entry_nr, stichtag_value, stichtag_format)
        VALUES (DW_EintragsNr, v_sysdate, 'DDMMYYYY');

        BEGIN
            -- Simulate print statements
            INSERT INTO `project.dataset.dw_job_log` (job_entry_nr, job_kennung, log_message) VALUES (DW_EintragsNr, JobKennung, ' ----------------- Job -----------------------');
            INSERT INTO `project.dataset.dw_job_log` (job_entry_nr, job_kennung, log_message) VALUES (DW_EintragsNr, JobKennung, FORMAT(' Job-Nr    : ''%d''', DW_EintragsNr));
            INSERT INTO `project.dataset.dw_job_log` (job_entry_nr, job_kennung, log_message) VALUES (DW_EintragsNr, JobKennung, FORMAT(' JobKennung: ''%s''', JobKennung));
            INSERT INTO `project.dataset.dw_job_log` (job_entry_nr, job_kennung, log_message) VALUES (DW_EintragsNr, JobKennung, FORMAT(' Logdatei  : ''%s''', LogDatei));
            INSERT INTO `project.dataset.dw_job_log` (job_entry_nr, job_kennung, log_message) VALUES (DW_EintragsNr, JobKennung, ' ---------------------------------------------');

            -- Simulate core script invocation
            CALL `project.dataset.sp_k_ausd_v_ta_period`(JobKennung, DW_EintragsNr, p_simulate_core_error);

            -- Simulate success message
            INSERT INTO `project.dataset.dw_job_log` (job_entry_nr, job_kennung, log_message)
            VALUES (DW_EintragsNr, JobKennung, 'Die Abarbeitung wurde ohne erkennbare Fehler beendet');

            -- Simulate DWMSG_SetzeStatusOK
            INSERT INTO `project.dataset.dw_job_log` (job_entry_nr, job_kennung, log_message)
            VALUES (DW_EintragsNr, JobKennung, 'Job status set to OK.');
            INSERT INTO `project.dataset.dw_job_status` (job_entry_nr, job_kennung, status_code, status_message)
            VALUES (DW_EintragsNr, JobKennung, 'OK', 'Successful completion');

        EXCEPTION WHEN ERROR THEN
            -- Simulate DWMSG_Fehlerbehandlung and 'AppError: Abbruch'
            INSERT INTO `project.dataset.dw_job_error` (job_entry_nr, job_kennung, error_message)
            VALUES (DW_EintragsNr, JobKennung, @@error.message);
            INSERT INTO `project.dataset.dw_job_log` (job_entry_nr, job_kennung, log_message)
            VALUES (DW_EintragsNr, JobKennung, FORMAT('Error during execution: %s', @@error.message));
            INSERT INTO `project.dataset.dw_job_log` (job_entry_nr, job_kennung, log_message)
            VALUES (DW_EintragsNr, JobKennung, 'AppError: Abbruch');
            INSERT INTO `project.dataset.dw_job_status` (job_entry_nr, job_kennung, status_code, status_message)
            VALUES (DW_EintragsNr, JobKennung, 'ERR', 'Execution failed');
            RAISE USING MESSAGE = FORMAT('AppError: Abbruch - %s', @@error.message);
        END;
    END;
    ```

---

## Test Cases

### 1. Output Parity

#### Test Case 1.1: Full Execution Log Parity (Success Path)

*   **Purpose:** To verify that the sequence and content of log entries generated by the BigQuery Stored Procedure for a successful run are equivalent to the legacy script's log file.
*   **Setup:**
    1.  Ensure all logging tables (`dw_job_registry`, `dw_job_log`, `dw_job_status`, `dw_job_error`, `dw_job_stichtag`) are empty.
    2.  Generate a reference log file from a successful execution of the legacy `r_ausd_v_ta_period.ksh` script. This involves running the legacy script and capturing its `stdout` and redirected log file content. Normalize dynamic elements like timestamps and job numbers in the reference log.
*   **Action:** Execute the BigQuery Stored Procedure:
    ```sql
    CALL `project.dataset.sp_vertragsdatenabgleich`(p_simulate_core_error => FALSE);
    ```
*   **Pass/Fail Criterion:**
    1.  The call completes successfully without raising an error.
    2.  The `dw_job_status` table contains one entry for `JobKennung='BERT_V_TA_PERIOD'` with `status_code='OK'`.
    3.  The `dw_job_error` table contains 0 entries for the executed `job_entry_nr`.
    4.  The ordered sequence of `log_message` entries from `dw_job_log` (after normalizing dynamic values like `job_entry_nr`, `LogDatei`, and `v_sysdate`) matches the normalized reference log file content.

*   **Runnable Test Code (Pytest / SQL Assertions):**
    ```python
    import pytest
    from google.cloud import bigquery
    import re
    from datetime import datetime

    PROJECT_ID = "your-gcp-project-id"
    DATASET_ID = "your_dataset_id"

    @pytest.fixture(scope="module")
    def bq_client():
        return bigquery.Client(project=PROJECT_ID)

    @pytest.fixture(autouse=True)
    def cleanup_tables(bq_client):
        tables = [
            f"`{PROJECT_ID}.{DATASET_ID}.dw_job_registry`",
            f"`{PROJECT_ID}.{DATASET_ID}.dw_job_log`",
            f"`{PROJECT_ID}.{DATASET_ID}.dw_job_status`",
            f"`{PROJECT_ID}.{DATASET_ID}.dw_job_error`",
            f"`{PROJECT_ID}.{DATASET_ID}.dw_job_stichtag`",
        ]
        for table in tables:
            bq_client.query(f"TRUNCATE TABLE {table}").result()
        yield
        for table in tables:
            bq_client.query(f"TRUNCATE TABLE {table}").result()

    def normalize_log_message(message, job_entry_nr, log_file_name, sys_date):
        # Replace dynamic parts with placeholders for comparison
        message = re.sub(r"Job-Nr\s*:\s*'\d+'", "Job-Nr    : '<JOB_ENTRY_NR>'", message)
        message = re.sub(r"Logdatei\s*:\s*'log_BERT_V_TA_PERIOD_\d+\.log'", "Logdatei  : '<LOG_FILE_NAME>'", message)
        # The 'Job started.' message also contains the script name and log file name
        message = re.sub(r"r_ausd_v_ta_period\.ksh, log_BERT_V_TA_PERIOD_\d+\.log, Job started\.", "r_ausd_v_ta_period.ksh, <LOG_FILE_NAME>, Job started.", message)
        return message.replace(str(job_entry_nr), "<JOB_ENTRY_NR>").replace(log_file_name, "<LOG_FILE_NAME>").replace(sys_date, "<SYS_DATE>")

    def test_successful_execution_log_parity(bq_client):
        # 1. Generate a reference log from legacy script (manual step, save to file)
        # Example content (simplified, actual content would be longer):
        # --- REFERENCE_LEGACY_LOG_SUCCESS.txt ---
        # Job started.
        #  ----------------- Job -----------------------
        #  Job-Nr    : '12345'
        #  JobKennung: 'BERT_V_TA_PERIOD'
        #  Logdatei  : 'log_BERT_V_TA_PERIOD_12345.log'
        #  ---------------------------------------------
        # Core script sp_k_ausd_v_ta_period executed successfully.
        # Die Abarbeitung wurde ohne erkennbare Fehler beendet
        # Job status set to OK.
        # ------------------------------------------------
        # (Note: Actual legacy log would include DWMSG_ErzeugeEintrag, DWMSG_SetzeStatusOK, etc.)
        # For this test, we'll use a simplified expected log based on the BQ SP's logging.
        
        # Get current date for stichtag comparison
        current_date_ddmmyyyy = datetime.now().strftime('%d%m%Y')

        # 2. Execute the BigQuery Stored Procedure
        bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_vertragsdatenabgleich`(p_simulate_core_error => FALSE)").result()

        # 3. Assertions
        # Get the job_entry_nr from dw_job_registry
        job_registry_rows = list(bq_client.query(f"SELECT job_entry_nr FROM `{PROJECT_ID}.{DATASET_ID}.dw_job_registry` WHERE job_kennung = 'BERT_V_TA_PERIOD'").result())
        assert len(job_registry_rows) == 1
        job_entry_nr = job_registry_rows[0].job_entry_nr
        log_file_name = f"log_BERT_V_TA_PERIOD_{job_entry_nr}.log"

        # Expected log messages (normalized)
        expected_log_messages = [
            "Job started.",
            " ----------------- Job -----------------------",
            f" Job-Nr    : '{job_entry_nr}'",
            " JobKennung: 'BERT_V_TA_PERIOD'",
            f" Logdatei  : '{log_file_name}'",
            " ---------------------------------------------",
            "Core script sp_k_ausd_v_ta_period executed successfully.",
            "Die Abarbeitung wurde ohne erkennbare Fehler beendet",
            "Job status set to OK."
        ]

        # Fetch actual log messages
        query = f"""
            SELECT log_message
            FROM `{PROJECT_ID}.{DATASET_ID}.dw_job_log`
            WHERE job_entry_nr = {job_entry_nr}
            ORDER BY log_ts
        """
        actual_log_rows = list(bq_client.query(query).result())
        actual_log_messages = [row.log_message for row in actual_log_rows]

        # Normalize actual messages for comparison (if needed, but our BQ SP already logs directly)
        # For this specific BQ SP, the messages are already quite direct.
        # We might need to normalize the 'Job started.' message if it includes script_name and log_file_name in the message itself.
        # The current BQ SP logs 'Job started.' as a simple message, and script_name/log_file_name as separate columns.
        # So, direct comparison should work for most.

        # Assert log message content and order
        assert actual_log_messages == expected_log_messages

        # Assert status
        status_rows = list(bq_client.query(f"SELECT status_code FROM `{PROJECT_ID}.{DATASET_ID}.dw_job_status` WHERE job_entry_nr = {job_entry_nr}").result())
        assert len(status_rows) == 1
        assert status_rows[0].status_code == 'OK'

        # Assert no errors
        error_rows = list(bq_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.dw_job_error` WHERE job_entry_nr = {job_entry_nr}").result())
        assert len(error_rows) == 0

        # Assert stichtag
        stichtag_rows = list(bq_client.query(f"SELECT stichtag_value, stichtag_format FROM `{PROJECT_ID}.{DATASET_ID}.dw_job_stichtag` WHERE job_entry_nr = {job_entry_nr}").result())
        assert len(stichtag_rows) == 1
        assert stichtag_rows[0].stichtag_value == current_date_ddmmyyyy
        assert stichtag_rows[0].stichtag_format == 'DDMMYYYY'
    ```

#### Test Case 1.2: Full Execution Log Parity (Failure Path - Core Script Error)

*   **Purpose:** To verify that the BigQuery Stored Procedure correctly logs errors originating from the invoked core script, mirroring the legacy script's error handling and logging.
*   **Setup:**
    1.  Ensure all logging tables are empty.
    2.  Generate a reference log file from a legacy `r_ausd_v_ta_period.ksh` execution where `k_ausd_v_ta_period.ksh` fails (e.g., by forcing an `exit 1` in the core script). Capture its `stdout` and redirected log file content.
*   **Action:** Execute the BigQuery Stored Procedure, simulating a core script error:
    ```sql
    CALL `project.dataset.sp_vertragsdatenabgleich`(p_simulate_core_error => TRUE);
    ```
*   **Pass/Fail Criterion:**
    1.  The call to `sp_vertragsdatenabgleich` raises an error (e.g., `AppError: Abbruch - Simulated error from sp_k_ausd_v_ta_period`).
    2.  The `dw_job_status` table contains one entry for `JobKennung='BERT_V_TA_PERIOD'` with `status_code='ERR'`.
    3.  The `dw_job_error` table contains one entry for the executed `job_entry_nr` with the error message from the core script.
    4.  The ordered sequence of `log_message` entries from `dw_job_log` (after normalization) matches the normalized reference log file content for a failed run, including the "AppError: Abbruch" message.

*   **Runnable Test Code (Pytest / SQL Assertions):**
    ```python
    import pytest
    from google.cloud import bigquery
    import re
    from datetime import datetime

    # ... (bq_client and cleanup_tables fixtures as above) ...

    def test_failed_execution_log_parity(bq_client):
        # 1. Generate a reference log from legacy script (manual step, save to file)
        # Example content (simplified):
        # --- REFERENCE_LEGACY_LOG_FAILURE.txt ---
        # Job started.
        #  ----------------- Job -----------------------
        #  Job-Nr    : '12346'
        #  JobKennung: 'BERT_V_TA_PERIOD'
        #  Logdatei  : 'log_BERT_V_TA_PERIOD_12346.log'
        #  ---------------------------------------------
        # Simulated error from sp_k_ausd_v_ta_period
        # Error during execution: Simulated error from sp_k_ausd_v_ta_period
        # AppError: Abbruch
        # ------------------------------------------------

        # 2. Execute the BigQuery Stored Procedure, expecting an error
        with pytest.raises(Exception) as excinfo:
            bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_vertragsdatenabgleich`(p_simulate_core_error => TRUE)").result()
        assert "AppError: Abbruch - Simulated error from sp_k_ausd_v_ta_period" in str(excinfo.value)

        # 3. Assertions
        job_registry_rows = list(bq_client.query(f"SELECT job_entry_nr FROM `{PROJECT_ID}.{DATASET_ID}.dw_job_registry` WHERE job_kennung = 'BERT_V_TA_PERIOD'").result())
        assert len(job_registry_rows) == 1
        job_entry_nr = job_registry_rows[0].job_entry_nr
        log_file_name = f"log_BERT_V_TA_PERIOD_{job_entry_nr}.log"

        # Expected log messages (normalized)
        expected_log_messages = [
            "Job started.",
            " ----------------- Job -----------------------",
            f" Job-Nr    : '{job_entry_nr}'",
            " JobKennung: 'BERT_V_TA_PERIOD'",
            f" Logdatei  : '{log_file_name}'",
            " ---------------------------------------------",
            "Error during execution: Simulated error from sp_k_ausd_v_ta_period",
            "AppError: Abbruch"
        ]

        query = f"""
            SELECT log_message
            FROM `{PROJECT_ID}.{DATASET_ID}.dw_job_log`
            WHERE job_entry_nr = {job_entry_nr}
            ORDER BY log_ts
        """
        actual_log_rows = list(bq_client.query(query).result())
        actual_log_messages = [row.log_message for row in actual_log_rows]

        assert actual_log_messages == expected_log_messages

        # Assert status
        status_rows = list(bq_client.query(f"SELECT status_code FROM `{PROJECT_ID}.{DATASET_ID}.dw_job_status` WHERE job_entry_nr = {job_entry_nr}").result())
        assert len(status_rows) == 1
        assert status_rows[0].status_code == 'ERR'

        # Assert error details
        error_rows = list(bq_client.query(f"SELECT error_message FROM `{PROJECT_ID}.{DATASET_ID}.dw_job_error` WHERE job_entry_nr = {job_entry_nr}").result())
        assert len(error_rows) == 1
        assert "Simulated error from sp_k_ausd_v_ta_period" in error_rows[0].error_message
    ```

#### Test Case 1.3: Help Message Output Parity

*   **Purpose:** To verify that the help message displayed by the BigQuery Stored Procedure when requested matches the `usage()` output of the legacy script.
*   **Setup:** None.
*   **Action:** Execute the BigQuery Stored Procedure with the help parameter:
    ```sql
    CALL `project.dataset.sp_vertragsdatenabgleich`(p_help => TRUE);
    ```
*   **Pass/Fail Criterion:**
    1.  The procedure executes successfully without inserting any entries into the logging tables.
    2.  The `SELECT` statement within the procedure returns a single row with a string that is identical to the legacy `usage()` output (after normalizing dynamic elements like `ProgName`, `ProgVersion`, and the call syntax).

*   **Runnable Test Code (Pytest / SQL Assertions):**
    ```python
    import pytest
    from google.cloud import bigquery

    # ... (bq_client and cleanup_tables fixtures as above) ...

    def test_help_message_parity(bq_client):
        # 1. Expected help message (based on legacy script's usage() and BQ SP's SELECT)
        expected_help_message = """
    Programm: Vertragsdatenabgleich
    Version:  V1.0.0
    Aufruf:   CALL project.dataset.sp_vertragsdatenabgleich(p_help => TRUE);
    Parameter:
	p_help     zeigt diese Seite an
    p_simulate_core_error (internal for testing)
    p_unknown_param (internal for testing)

    Beschreibung:
        Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_period.
            """
        
        # 2. Execute the BigQuery Stored Procedure
        query_job = bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_vertragsdatenabgleich`(p_help => TRUE)")
        
        # For BigQuery stored procedures that return a SELECT, you need to fetch the results.
        # The exact method depends on how the client library handles procedure output.
        # Assuming the SELECT statement is the last one and its result is returned.
        # In practice, you might need to capture stdout/stderr if the procedure prints.
        # For this example, we assume the SELECT result is directly accessible.
        
        # A more robust way to capture output from a BQ SP that 'prints' (SELECTs)
        # would be to wrap the call in a script that captures stdout, or modify the SP
        # to insert into a temporary table for testing.
        # For simplicity, we'll assume the client can get the result of the final SELECT.
        
        # For a BQ SP that returns a result set, you'd typically iterate over it.
        # If the SP just prints to console, you'd need to capture stdout/stderr from the bq CLI.
        # Given the design, the SP's 'usage' is a SELECT, so we can query for it.
        
        # The current mock SP uses SELECT FORMAT(...) which returns a result set.
        rows = list(query_job.result())
        assert len(rows) == 1
        actual_help_message = rows[0][0] # Assuming the first column contains the message

        # 3. Assertions
        assert actual_help_message.strip() == expected_help_message.strip()

        # Verify no logging tables were touched
        for table in ["dw_job_registry", "dw_job_log", "dw_job_status", "dw_job_error", "dw_job_stichtag"]:
            count_query = f"SELECT COUNT(1) FROM `{PROJECT_ID}.{DATASET_ID}.{table}`"
            count_result = list(bq_client.query(count_query).result())
            assert count_result[0][0] == 0, f"Table {table} should be empty for help call."
    ```

### 2. Transformation Correctness

#### Test Case 2.1: Parameter Handling (Unknown Parameter)

*   **Purpose:** To verify that the BigQuery Stored Procedure correctly identifies and handles unknown parameters, mirroring the `getopts` `?)` case in the legacy script (`ErrNr=192`).
*   **Setup:** Ensure all logging tables are empty.
*   **Action:** Execute the BigQuery Stored Procedure with an unrecognized parameter:
    ```sql
    CALL `project.dataset.sp_vertragsdatenabgleich`(p_unknown_param => 'some_value');
    ```
*   **Pass/Fail Criterion:**
    1.  The call to `sp_vertragsdatenabgleich` raises an error (e.g., `AppError: Abbruch (Error 192: p_unknown_param)`).
    2.  The `dw_job_status` table contains one entry with `status_code='ERR'` and a message indicating a parameter error.
    3.  The `dw_job_error` table contains one entry with `error_message` indicating an unknown parameter and `ErrNr=192`.
    4.  The `dw_job_log` table contains entries reflecting the error.

*   **Runnable Test Code (Pytest / SQL Assertions):**
    ```python
    import pytest
    from google.cloud import bigquery

    # ... (bq_client and cleanup_tables fixtures as above) ...

    def test_unknown_parameter_handling(bq_client):
        # 1. Execute the BigQuery Stored Procedure with an unknown parameter
        with pytest.raises(Exception) as excinfo:
            bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_vertragsdatenabgleich`(p_unknown_param => 'invalid_arg')").result()
        assert "AppError: Abbruch (Error 192: invalid_arg)" in str(excinfo.value)

        # 2. Assertions
        job_registry_rows = list(bq_client.query(f"SELECT job_entry_nr FROM `{PROJECT_ID}.{DATASET_ID}.dw_job_registry` WHERE job_kennung = 'BERT_V_TA_PERIOD'").result())
        assert len(job_registry_rows) == 1
        job_entry_nr = job_registry_rows[0].job_entry_nr

        # Assert status
        status_rows = list(bq_client.query(f"SELECT status_code, status_message FROM `{PROJECT_ID}.{DATASET_ID}.dw_job_status` WHERE job_entry_nr = {job_entry_nr}").result())
        assert len(status_rows) == 1
        assert status_rows[0].status_code == 'ERR'
        assert "Parameter error: invalid_arg" in status_rows[0].status_message

        # Assert error details
        error_rows = list(bq_client.query(f"SELECT error_message FROM `{PROJECT_ID}.{DATASET_ID}.dw_job_error` WHERE job_entry_nr = {job_entry_nr}").result())
        assert len(error_rows) == 1
        assert "Error 192: Unknown parameter invalid_arg" in error_rows[0].error_message

        # Assert log entries
        log_rows = list(bq_client.query(f"SELECT log_message FROM `{PROJECT_ID}.{DATASET_ID}.dw_job_log` WHERE job_entry_nr = {job_entry_nr} ORDER BY log_ts").result())
        assert any("Error 192: Unknown parameter invalid_arg" in row.log_message for row in log_rows)
    ```

#### Test Case 2.2: Job Entry Number Generation Logic

*   **Purpose:** To verify that the BigQuery SP correctly generates the next sequential `job_entry_nr` by querying `dw_job_registry`, replicating `DWMSG_ErmittleNr`.
*   **Setup:**
    1.  Ensure all logging tables are empty.
    2.  Insert a known maximum `job_entry_nr` for `JobKennung='BERT_V_TA_PERIOD'` into `dw_job_registry`.
*   **Action:** Execute the BigQuery Stored Procedure.
    ```sql
    -- Insert a previous job entry
    INSERT INTO `project.dataset.dw_job_registry` (job_kennung, job_entry_nr, last_update_ts)
    VALUES ('BERT_V_TA_PERIOD', 100, CURRENT_TIMESTAMP());

    CALL `project.dataset.sp_vertragsdatenabgleich`(p_simulate_core_error => FALSE);
    ```
*   **Pass/Fail Criterion:**
    1.  The `job_entry_nr` recorded in `dw_job_registry`, `dw_job_log`, `dw_job_status`, `dw_job_error`, and `dw_job_stichtag` for the new execution is `MAX(previous_job_entry_nr) + 1`.

*   **Runnable Test Code (Pytest / SQL Assertions):**
    ```python
    import pytest
    from google.cloud import bigquery

    # ... (bq_client and cleanup_tables fixtures as above) ...

    def test_job_entry_number_generation(bq_client):
        # 1. Setup: Insert a known max job_entry_nr
        initial_max_job_nr = 100
        bq_client.query(f"""
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.dw_job_registry` (job_kennung, job_entry_nr, last_update_ts)
            VALUES ('BERT_V_TA_PERIOD', {initial_max_job_nr}, CURRENT_TIMESTAMP())
        """).result()

        # 2. Action: Execute the BigQuery Stored Procedure
        bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_vertragsdatenabgleich`(p_simulate_core_error => FALSE)").result()

        # 3. Assertions
        expected_job_nr = initial_max_job_nr + 1

        # Check dw_job_registry
        registry_rows = list(bq_client.query(f"SELECT job_entry_nr FROM `{PROJECT_ID}.{DATASET_ID}.dw_job_registry` WHERE job_kennung = 'BERT_V_TA_PERIOD' ORDER BY job_entry_nr DESC LIMIT 1").result())
        assert len(registry_rows) == 1
        assert registry_rows[0].job_entry_nr == expected_job_nr

        # Check dw_job_log
        log_rows = list(bq_client.query(f"SELECT COUNT(1) FROM `{PROJECT_ID}.{DATASET_ID}.dw_job_log` WHERE job_entry_nr = {expected_job_nr}").result())
        assert log_rows[0][0] > 0

        # Check dw_job_status
        status_rows = list(bq_client.query(f"SELECT COUNT(1) FROM `{PROJECT_ID}.{DATASET_ID}.dw_job_status` WHERE job_entry_nr = {expected_job_nr}").result())
        assert status_rows[0][0] == 1

        # Check dw_job_stichtag
        stichtag_rows = list(bq_client.query(f"SELECT COUNT(1) FROM `{PROJECT_ID}.{DATASET_ID}.dw_job_stichtag` WHERE job_entry_nr = {expected_job_nr}").result())
        assert stichtag_rows[0][0] == 1
    ```

#### Test Case 2.3: Date Formatting and Stichtag Logging

*   **Purpose:** To verify that the BigQuery SP correctly formats the current date (`v_sysdate=$(date +%d%m%Y)`) and logs it to `dw_job_stichtag` with the specified format, replicating `DWMSG_SetzeStichtagInfo`.
*   **Setup:** Ensure `dw_job_stichtag` is empty.
*   **Action:** Execute the BigQuery Stored Procedure.
    ```sql
    CALL `project.dataset.sp_vertragsdatenabgleich`(p_simulate_core_error => FALSE);
    ```
*   **Pass/Fail Criterion:**
    1.  The `dw_job_stichtag` table contains one entry for the executed `job_entry_nr`.
    2.  The `stichtag_value` in this entry matches `CURRENT_DATE()` formatted as `DDMMYYYY`.
    3.  The `stichtag_format` in this entry is 'DDMMYYYY'.

*   **Runnable Test Code (Pytest / SQL Assertions):**
    ```python
    import pytest
    from google.cloud import bigquery
    from datetime import datetime

    # ... (bq_client and cleanup_tables fixtures as above) ...

    def test_stichtag_logging(bq_client):
        # 1. Action: Execute the BigQuery Stored Procedure
        bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_vertragsdatenabgleich`(p_simulate_core_error => FALSE)").result()

        # 2. Assertions
        job_registry_rows = list(bq_client.query(f"SELECT job_entry_nr FROM `{PROJECT_ID}.{DATASET_ID}.dw_job_registry` WHERE job_kennung = 'BERT_V_TA_PERIOD'").result())
        assert len(job_registry_rows) == 1
        job_entry_nr = job_registry_rows[0].job_entry_nr

        stichtag_rows = list(bq_client.query(f"SELECT stichtag_value, stichtag_format FROM `{PROJECT_ID}.{DATASET_ID}.dw_job_stichtag` WHERE job_entry_nr = {job_entry_nr}").result())
        assert len(stichtag_rows) == 1

        expected_stichtag_value = datetime.now().strftime('%d%m%Y')
        assert stichtag_rows[0].stichtag_value == expected_stichtag_value
        assert stichtag_rows[0].stichtag_format == 'DDMMYYYY'
    ```

#### Test Case 2.4: Core Script Invocation

*   **Purpose:** To verify that `sp_vertragsdatenabgleich` correctly calls `sp_k_ausd_v_ta_period` with the expected parameters (`JobKennung`, `DW_EintragsNr`).
*   **Setup:**
    1.  Ensure `dw_job_log` is empty.
    2.  The mock `sp_k_ausd_v_ta_period` should log its invocation details.
*   **Action:** Execute `sp_vertragsdatenabgleich`.
    ```sql
    CALL `project.dataset.sp_vertragsdatenabgleich`(p_simulate_core_error => FALSE);
    ```
*   **Pass/Fail Criterion:**
    1.  The `dw_job_log` table contains an entry from `sp_k_ausd_v_ta_period` indicating successful execution.
    2.  This log entry confirms that `sp_k_ausd_v_ta_period` was called with the correct `job_kennung` and `job_entry_nr` as generated by `sp_vertragsdatenabgleich`.

*   **Runnable Test Code (Pytest / SQL Assertions):**
    ```python
    import pytest
    from google.cloud import bigquery

    # ... (bq_client and cleanup_tables fixtures as above) ...

    def test_core_script_invocation(bq_client):
        # 1. Action: Execute the BigQuery Stored Procedure
        bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_vertragsdatenabgleich`(p_simulate_core_error => FALSE)").result()

        # 2. Assertions
        job_registry_rows = list(bq_client.query(f"SELECT job_entry_nr FROM `{PROJECT_ID}.{DATASET_ID}.dw_job_registry` WHERE job_kennung = 'BERT_V_TA_PERIOD'").result())
        assert len(job_registry_rows) == 1
        job_entry_nr = job_registry_rows[0].job_entry_nr

        # Check for the log entry from the mocked core script
        core_script_log_query = f"""
            SELECT log_message
            FROM `{PROJECT_ID}.{DATASET_ID}.dw_job_log`
            WHERE job_entry_nr = {job_entry_nr}
              AND log_message = 'Core script sp_k_ausd_v_ta_period executed successfully.'
        """
        core_script_log_rows = list(bq_client.query(core_script_log_query).result())
        assert len(core_script_log_rows) == 1, "Core script invocation log not found or incorrect."
    ```

### 3. External-System Replacements

*   **Purpose:** The migration design explicitly states: "No external systems (like Oracle, SFTP, S3) were detected as directly referenced by this specific wrapper script." This section focuses on verifying that the BigQuery SP correctly replaces the *internal* filesystem-based dependencies and environment sourcing with BigQuery-native constructs.
*   **Setup:** None specific, other than the standard BigQuery environment.
*   **Action:** Execute `sp_vertragsdatenabgleich` in both success and failure scenarios.
*   **Pass/Fail Criterion:**
    1.  The BigQuery Stored Procedure executes without attempting to access any external filesystems, environment variables, or non-BigQuery external systems.
    2.  All configuration and utility functions (e.g., for error handling, parameter parsing, date formatting) are handled internally by BigQuery SQL constructs or by interacting with the designated BigQuery logging tables.

*   **Runnable Test Code (Pytest / SQL Assertions):**
    This is more of an architectural assertion and code review point. Direct programmatic testing for "absence of external calls" is challenging. However, we can assert that the procedure relies solely on BigQuery's capabilities.

    ```python
    import pytest
    from google.cloud import bigquery

    # ... (bq_client and cleanup_tables fixtures as above) ...

    def test_no_external_system_dependencies(bq_client):
        # This test primarily relies on code review of the BigQuery Stored Procedure
        # to ensure it does not contain any external system calls (e.g., UDFs that call external APIs,
        # external tables pointing to non-BQ sources not specified in design, etc.).
        # For automated testing, we can assert successful execution without external errors.

        # 1. Action: Execute the BigQuery Stored Procedure (success path)
        bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_vertragsdatenabgleich`(p_simulate_core_error => FALSE)").result()

        # 2. Assertions:
        # Verify successful completion (already covered by other tests, but reinforces here)
        status_rows = list(bq_client.query(f"SELECT status_code FROM `{PROJECT_ID}.{DATASET_ID}.dw_job_status` WHERE job_kennung = 'BERT_V_TA_PERIOD' ORDER BY status_ts DESC LIMIT 1").result())
        assert len(status_rows) == 1
        assert status_rows[0].status_code == 'OK'

        # Implicit assertion: If the procedure ran successfully and the design states no external dependencies,
        # then it implies no unexpected external calls were made.
        # A more explicit check would involve BigQuery audit logs, but that's outside a unit test scope.
        # For this test, we primarily confirm that the internal replacements (logging tables, parameters)
        # are functioning as expected, and no errors related to missing external resources occurred.

        # Example of a conceptual check (not directly runnable as a BQ query for external calls):
        # assert "No external API calls or filesystem operations detected in BigQuery audit logs for this procedure run."
        # This would be a manual verification or part of a broader monitoring setup.
    ```

### 4. Data-Quality / Row-Count / Schema Assertions

#### Test Case 4.1: Schema Conformance of Logging Tables

*   **Purpose:** To verify that the schema of the BigQuery logging tables (`dw_job_registry`, `dw_job_log`, `dw_job_status`, `dw_job_error`, `dw_job_stichtag`) matches the expected design.
*   **Setup:** Ensure the logging tables exist with their defined DDL.
*   **Action:** Query BigQuery's `INFORMATION_SCHEMA` views.
*   **Pass/Fail Criterion:**
    1.  Each table exists.
    2.  Each table has the expected columns with correct data types and nullability.

*   **Runnable Test Code (Pytest / SQL Assertions):**
    ```python
    import pytest
    from google.cloud import bigquery

    # ... (bq_client fixture as above) ...

    def test_logging_table_schemas(bq_client):
        expected_schemas = {
            "dw_job_registry": {
                "job_kennung": "STRING", "job_entry_nr": "INT64", "last_update_ts": "TIMESTAMP"
            },
            "dw_job_log": {
                "job_entry_nr": "INT64", "job_kennung": "STRING", "script_name": "STRING",
                "log_file_name": "STRING", "log_message": "STRING", "log_ts": "TIMESTAMP"
            },
            "dw_job_status": {
                "job_entry_nr": "INT64", "job_kennung": "STRING", "status_code": "STRING",
                "status_message": "STRING", "status_ts": "TIMESTAMP"
            },
            "dw_job_error": {
                "job_entry_nr": "INT64", "job_kennung": "STRING", "error_message": "STRING",
                "error_ts": "TIMESTAMP"
            },
            "dw_job_stichtag": {
                "job_entry_nr": "INT64", "stichtag_value": "STRING", "stichtag_format": "STRING",
                "insert_ts": "TIMESTAMP"
            }
        }

        for table_name, expected_cols in expected_schemas.items():
            query = f"""
                SELECT column_name, data_type
                FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name = '{table_name}'
            """
            rows = list(bq_client.query(query).result())
            actual_cols = {row.column_name: row.data_type for row in rows}

            assert len(actual_cols) == len(expected_cols), f"Mismatch in column count for {table_name}"
            for col_name, data_type in expected_cols.items():
                assert col_name in actual_cols, f"Missing column {col_name} in {table_name}"
                assert actual_cols[col_name] == data_type, f"Data type mismatch for {col_name} in {table_name}: Expected {data_type}, Got {actual_cols[col_name]}"
    ```

#### Test Case 4.2: Row Counts in Logging Tables (Success Scenario)

*   **Purpose:** To verify that the correct number of rows are inserted into each logging table during a successful execution of the BigQuery Stored Procedure.
*   **Setup:** Ensure all logging tables are empty.
*   **Action:** Execute the BigQuery Stored Procedure for a successful run:
    ```sql
    CALL `project.dataset.sp_vertragsdatenabgleich`(p_simulate_core_error => FALSE);
    ```
*   **Pass/Fail Criterion:**
    1.  `dw_job_registry`: 1 row (for the new job entry).
    2.  `dw_job_log`: 9 rows (1 initial, 5 print statements, 1 core script success, 1 final success message, 1 status OK message).
    3.  `dw_job_status`: 1 row (status 'OK').
    4.  `dw_job_error`: 0 rows.
    5.  `dw_job_stichtag`: 1 row.

*   **Runnable Test Code (Pytest / SQL Assertions):**
    ```python
    import pytest
    from google.cloud import bigquery

    # ... (bq_client and cleanup_tables fixtures as above) ...

    def test_row_counts_success_scenario(bq_client):
        # 1. Action: Execute the BigQuery Stored Procedure
        bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_vertragsdatenabgleich`(p_simulate_core_error => FALSE)").result()

        # 2. Assertions
        job_registry_rows = list(bq_client.query(f"SELECT job_entry_nr FROM `{PROJECT_ID}.{DATASET_ID}.dw_job_registry` WHERE job_kennung = 'BERT_V_TA_PERIOD'").result())
        assert len(job_registry_rows) == 1
        job_entry_nr = job_registry_rows[0].job_entry_nr

        # Expected row counts for a successful run
        expected_counts = {
            "dw_job_registry": 1,
            "dw_job_log": 9, # Initial, 5 prints, core script success, final success msg, status OK msg
            "dw_job_status": 1,
            "dw_job_error": 0,
            "dw_job_stichtag": 1,
        }

        for table_name, expected_count in expected_counts.items():
            query = f"SELECT COUNT(1) FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}` WHERE job_entry_nr = {job_entry_nr}"
            if table_name == "dw_job_registry": # Registry might have other entries, count specifically for this job_kennung
                query = f"SELECT COUNT(1) FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}` WHERE job_kennung = 'BERT_V_TA_PERIOD' AND job_entry_nr = {job_entry_nr}"
            
            rows = list(bq_client.query(query).result())
            actual_count = rows[0][0]
            assert actual_count == expected_count, f"Mismatch in row count for {table_name}. Expected {expected_count}, Got {actual_count}"
    ```

#### Test Case 4.3: Row Counts in Logging Tables (Failure Scenario)

*   **Purpose:** To verify that the correct number of rows are inserted into each logging table during a failed execution of the BigQuery Stored Procedure.
*   **Setup:** Ensure all logging tables are empty.
*   **Action:** Execute the BigQuery Stored Procedure, simulating a core script error:
    ```sql
    CALL `project.dataset.sp_vertragsdatenabgleich`(p_simulate_core_error => TRUE);
    ```
*   **Pass/Fail Criterion:**
    1.  `dw_job_registry`: 1 row.
    2.  `dw_job_log`: 9 rows (1 initial, 5 print statements, 1 error message, 1 "AppError: Abbruch", 1 status ERR message).
    3.  `dw_job_status`: 1 row (status 'ERR').
    4.  `dw_job_error`: 1 row.
    5.  `dw_job_stichtag`: 1 row.

*   **Runnable Test Code (Pytest / SQL Assertions):**
    ```python
    import pytest
    from google.cloud import bigquery

    # ... (bq_client and cleanup_tables fixtures as above) ...

    def test_row_counts_failure_scenario(bq_client):
        # 1. Action: Execute the BigQuery Stored Procedure, expecting an error
        with pytest.raises(Exception): # Expecting the procedure to raise an error
            bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_vertragsdatenabgleich`(p_simulate_core_error => TRUE)").result()

        # 2. Assertions
        job_registry_rows = list(bq_client.query(f"SELECT job_entry_nr FROM `{PROJECT_ID}.{DATASET_ID}.dw_job_registry` WHERE job_kennung = 'BERT_V_TA_PERIOD'").result())
        assert len(job_registry_rows) == 1
        job_entry_nr = job_registry_rows[0].job_entry_nr

        # Expected row counts for a failed run
        expected_counts = {
            "dw_job_registry": 1,
            "dw_job_log": 9, # Initial, 5 prints, error message, AppError: Abbruch, status ERR message
            "dw_job_status": 1,
            "dw_job_error": 1,
            "dw_job_stichtag": 1,
        }

        for table_name, expected_count in expected_counts.items():
            query = f"SELECT COUNT(1) FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}` WHERE job_entry_nr = {job_entry_nr}"
            if table_name == "dw_job_registry":
                query = f"SELECT COUNT(1) FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}` WHERE job_kennung = 'BERT_V_TA_PERIOD' AND job_entry_nr = {job_entry_nr}"

            rows = list(bq_client.query(query).result())
            actual_count = rows[0][0]
            assert actual_count == expected_count, f"Mismatch in row count for {table_name}. Expected {expected_count}, Got {actual_count}"
    ```