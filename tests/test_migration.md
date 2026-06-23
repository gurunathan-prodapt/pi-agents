As a senior data-migration QA engineer, I've analyzed the migration design for `r_ausd_v_ta_cntrct_crs.ksh` to a BigQuery Stored Procedure (`sp_vertragsdatenabgleich`). The legacy script is an orchestration layer, meaning its primary function is parameter handling, logging, error management, and invoking a core processing script. It does not directly manipulate business data.

The tests below focus on validating the behavioral equivalence of this orchestration logic, particularly concerning parameter parsing, logging to BigQuery audit tables (replacing filesystem logs), and robust error handling.

---

## Migration Validation Tests for `sp_vertragsdatenabgleich`

**Assumptions for Test Execution:**

*   A BigQuery project (`my_project`) and dataset (`my_dataset`) are configured.
*   The following BigQuery audit tables exist with the specified schemas:
    *   `my_project.my_dataset.job_control`
    *   `my_project.my_dataset.job_audit_log`
    *   `my_project.my_dataset.job_error_log`
    *   `my_project.my_dataset.job_status`
*   The migrated stored procedure `my_project.my_dataset.sp_vertragsdatenabgleich` is deployed.
*   A mock version of the core processing stored procedure `my_project.my_dataset.sp_ausd_v_ta_cntrct_crs` is available and can be replaced for specific test scenarios.
*   A `bq_client` object (e.g., from `google.cloud.bigquery`) is available in the `pytest` environment for executing SQL queries.

---

### Pre-requisite BigQuery Objects (Setup for Testing)

```sql
-- Create/Replace Audit Tables
CREATE OR REPLACE TABLE `my_project.my_dataset.job_control` (
    job_entry_nr INT64 NOT NULL,
    job_kennung STRING NOT NULL,
    program_name STRING,
    log_file_name STRING,
    start_timestamp TIMESTAMP,
    end_timestamp TIMESTAMP,
    status STRING, -- e.g., 'RUNNING', 'OK', 'ERROR'
    stichtag_info STRING,
    stichtag_format STRING
);

CREATE OR REPLACE TABLE `my_project.my_dataset.job_audit_log` (
    log_timestamp TIMESTAMP NOT NULL,
    job_entry_nr INT64 NOT NULL,
    job_kennung STRING NOT NULL,
    log_level STRING, -- e.g., 'INFO', 'WARNING', 'ERROR'
    message STRING
);

CREATE OR REPLACE TABLE `my_project.my_dataset.job_error_log` (
    error_timestamp TIMESTAMP NOT NULL,
    job_entry_nr INT64 NOT NULL,
    job_kennung STRING NOT NULL,
    error_code INT64,
    error_argument STRING,
    error_message STRING
);

CREATE OR REPLACE TABLE `my_project.my_dataset.job_status` (
    job_entry_nr INT64 NOT NULL,
    job_kennung STRING NOT NULL,
    status STRING, -- e.g., 'OK', 'ERROR'
    status_timestamp TIMESTAMP
);

-- Mock Core Stored Procedure (Default: Success)
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.sp_ausd_v_ta_cntrct_crs`(
    IN p_job_kennung STRING,
    IN p_eintragsnr INT64
)
BEGIN
    INSERT INTO `my_project.my_dataset.job_audit_log` (log_timestamp, job_entry_nr, job_kennung, log_level, message)
    VALUES (CURRENT_TIMESTAMP(), p_eintragsnr, p_job_kennung, 'INFO', 'Core script sp_ausd_v_ta_cntrct_crs called and succeeded.');
END;

-- Migrated Orchestration Stored Procedure (sp_vertragsdatenabgleich)
-- This is the code under test, based on the design document.
-- Note: Parameter parsing is simplified to handle only -h, unknown, and missing argument for -s.
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.sp_vertragsdatenabgleich`(
    IN p_raw_params STRING -- e.g., "-h", "-s value", "-x"
)
BEGIN
    DECLARE v_prog_name STRING DEFAULT 'Vertragsdatenabgleich';
    DECLARE v_prog_version STRING DEFAULT 'V1.0.0';
    DECLARE v_job_kennung STRING DEFAULT 'BERT_V_TA_CNTRCT_CRS';
    DECLARE v_eintragsnr INT64;
    DECLARE v_log_file_name STRING; -- Placeholder for legacy log file name
    DECLARE v_err_nr INT64 DEFAULT 0;
    DECLARE v_err_arg STRING DEFAULT '';
    DECLARE v_stichtag_info STRING DEFAULT FORMAT_DATE('%d%m%Y', CURRENT_DATE());

    -- Determine next job entry number (simplified for testing, real might use sequence)
    SET v_eintragsnr = (SELECT IFNULL(MAX(job_entry_nr), 0) + 1 FROM `my_project.my_dataset.job_control`);
    SET v_log_file_name = 'log_' || v_job_kennung || '_' || CAST(v_eintragsnr AS STRING) || '.log';

    -- Simplified parameter parsing (mimicking getopts for -h, unknown, missing arg)
    IF p_raw_params = '-h' THEN
        INSERT INTO `my_project.my_dataset.job_audit_log` (log_timestamp, job_entry_nr, job_kennung, log_level, message)
        VALUES (CURRENT_TIMESTAMP(), v_eintragsnr, v_job_kennung, 'INFO',
            'Programm: ' || v_prog_name || '\nVersion: ' || v_prog_version || '\nBeschreibung: Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_cntrct_crs.');
        RETURN; -- Exit gracefully for help
    ELSEIF STARTS_WITH(p_raw_params, '-') AND LENGTH(p_raw_params) = 2 THEN -- e.g., "-x"
        SET v_err_nr = 192; -- Parameter unbekannt
        SET v_err_arg = SUBSTR(p_raw_params, 2, 1);
    ELSEIF p_raw_params = '-s' THEN -- e.g., "-s" (missing argument for 's:')
        SET v_err_nr = 193; -- Notwendiges Argument fehlt
        SET v_err_arg = 's';
    END IF;

    -- Error handling for parameter parsing (legacy: if [ ! $ErrNr -eq 0 ])
    IF v_err_nr != 0 THEN
        INSERT INTO `my_project.my_dataset.job_error_log` (error_timestamp, job_entry_nr, job_kennung, error_code, error_argument, error_message)
        VALUES (CURRENT_TIMESTAMP(), v_eintragsnr, v_job_kennung, v_err_nr, v_err_arg, 'Parameter parsing error');
        -- Log usage message as well, as per legacy script
        INSERT INTO `my_project.my_dataset.job_audit_log` (log_timestamp, job_entry_nr, job_kennung, log_level, message)
        VALUES (CURRENT_TIMESTAMP(), v_eintragsnr, v_job_kennung, 'INFO',
            'Programm: ' || v_prog_name || '\nVersion: ' || v_prog_version || '\nBeschreibung: Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_cntrct_crs.');
        RAISE USING MESSAGE 'Parameter error: ' || CAST(v_err_nr AS STRING) || ' - ' || v_err_arg;
    END IF;

    -- Main logic block with error handling (mimicking trap ERR)
    BEGIN
        -- DWMSG_ErmittleNr, DWMSG_Logdateiname, DWMSG_ErzeugeEintrag
        INSERT INTO `my_project.my_dataset.job_control` (job_entry_nr, job_kennung, program_name, log_file_name, start_timestamp, status)
        VALUES (v_eintragsnr, v_job_kennung, v_prog_name, v_log_file_name, CURRENT_TIMESTAMP(), 'RUNNING');

        INSERT INTO `my_project.my_dataset.job_audit_log` (log_timestamp, job_entry_nr, job_kennung, log_level, message)
        VALUES (CURRENT_TIMESTAMP(), v_eintragsnr, v_job_kennung, 'INFO', 'Job started. Job-Nr: ' || CAST(v_eintragsnr AS STRING) || ', JobKennung: ' || v_job_kennung || ', Logdatei: ' || v_log_file_name);

        -- DWMSG_SetzeStichtagInfo
        UPDATE `my_project.my_dataset.job_control`
        SET stichtag_info = v_stichtag_info, stichtag_format = 'DDMMYYYY'
        WHERE job_entry_nr = v_eintragsnr AND job_kennung = v_job_kennung;

        -- Call core script (sp_ausd_v_ta_cntrct_crs)
        CALL `my_project.my_dataset.sp_ausd_v_ta_cntrct_crs`(v_job_kennung, v_eintragsnr);

        -- DWMSG_SetzeStatusOK
        UPDATE `my_project.my_dataset.job_control`
        SET status = 'OK', end_timestamp = CURRENT_TIMESTAMP()
        WHERE job_entry_nr = v_eintragsnr AND job_kennung = v_job_kennung;

        INSERT INTO `my_project.my_dataset.job_status` (job_entry_nr, job_kennung, status, status_timestamp)
        VALUES (v_eintragsnr, v_job_kennung, 'OK', CURRENT_TIMESTAMP());

        INSERT INTO `my_project.my_dataset.job_audit_log` (log_timestamp, job_entry_nr, job_kennung, log_level, message)
        VALUES (CURRENT_TIMESTAMP(), v_eintragsnr, v_job_kennung, 'INFO', 'Die Abarbeitung wurde ohne erkennbare Fehler beendet');

    EXCEPTION WHEN ERROR THEN
        -- DWMSG_Fehlerbehandlung (mimicking ERR trap)
        DECLARE error_message STRING;
        DECLARE error_stack STRING;
        DECLARE error_line INT64;
        DECLARE error_routine STRING;

        SET error_message = @@error.message;
        SET error_stack = @@error.stack_trace;
        SET error_line = @@error.line;
        SET error_routine = @@error.routine;

        UPDATE `my_project.my_dataset.job_control`
        SET status = 'ERROR', end_timestamp = CURRENT_TIMESTAMP()
        WHERE job_entry_nr = v_eintragsnr AND job_kennung = v_job_kennung;

        INSERT INTO `my_project.my_dataset.job_error_log` (error_timestamp, job_entry_nr, job_kennung, error_code, error_argument, error_message)
        VALUES (CURRENT_TIMESTAMP(), v_eintragsnr, v_job_kennung, 999, 'CORE_SCRIPT_FAILURE', error_message); -- Using 999 for generic internal error

        INSERT INTO `my_project.my_dataset.job_audit_log` (log_timestamp, job_entry_nr, job_kennung, log_level, message)
        VALUES (CURRENT_TIMESTAMP(), v_eintragsnr, v_job_kennung, 'ERROR', 'AppError: Abbruch. Details: ' || error_message);

        INSERT INTO `my_project.my_dataset.job_status` (job_entry_nr, job_kennung, status, status_timestamp)
        VALUES (v_eintragsnr, v_job_kennung, 'ERROR', CURRENT_TIMESTAMP());

        RAISE; -- Re-raise the error to the caller
    END;
END;
```

---

### Test Suite: `test_sp_vertragsdatenabgleich.py`

```python
import pytest
from google.cloud import bigquery
import time
import datetime

# --- Configuration ---
PROJECT_ID = "my_project"
DATASET_ID = "my_dataset"
SP_NAME = "sp_vertragsdatenabgleich"
CORE_SP_NAME = "sp_ausd_v_ta_cntrct_crs"

# --- BigQuery Client Fixture (assuming this is in conftest.py or similar) ---
@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client(project=PROJECT_ID)

# --- Helper to clear audit tables ---
def clear_audit_tables(client):
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_control`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_audit_log`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_error_log`").result()
    client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_status`").result()

# --- Test Class ---
class TestSpVertragsdatenabgleich:

    @pytest.fixture(autouse=True)
    def setup_method(self, bq_client):
        """Clear audit tables before each test and ensure default mock core SP."""
        clear_audit_tables(bq_client)
        # Ensure core SP is in success mode for most tests
        bq_client.query(f"""
            CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.{CORE_SP_NAME}`(
                IN p_job_kennung STRING,
                IN p_eintragsnr INT64
            )
            BEGIN
                INSERT INTO `{PROJECT_ID}.{DATASET_ID}.job_audit_log` (log_timestamp, job_entry_nr, job_kennung, log_level, message)
                VALUES (CURRENT_TIMESTAMP(), p_eintragsnr, p_job_kennung, 'INFO', 'Core script {CORE_SP_NAME} called and succeeded.');
            END;
        """).result()
        time.sleep(1) # Give BigQuery time to propagate procedure changes

    # --- Test Cases ---

    def test_1_successful_execution_audit_log_content(self, bq_client):
        """
        Purpose: Verify that a successful execution of the migrated stored procedure
                 correctly logs all expected events into the `job_audit_log` table,
                 mirroring the information that would have been in the legacy log file.
        """
        # Action
        bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.{SP_NAME}`('')").result()

        # Pass/Fail Criterion
        # Assert specific entries exist in job_audit_log with correct job ID, status, and messages.
        audit_logs = list(bq_client.query(f"SELECT message FROM `{PROJECT_ID}.{DATASET_ID}.job_audit_log` ORDER BY log_timestamp").result())
        job_control = list(bq_client.query(f"SELECT job_entry_nr, job_kennung, status FROM `{PROJECT_ID}.{DATASET_ID}.job_control`").result())
        job_status = list(bq_client.query(f"SELECT job_entry_nr, job_kennung, status FROM `{PROJECT_ID}.{DATASET_ID}.job_status`").result())

        assert len(job_control) == 1
        assert job_control[0].status == 'OK'
        assert job_control[0].job_kennung == 'BERT_V_TA_CNTRCT_CRS'

        assert len(job_status) == 1
        assert job_status[0].status == 'OK'
        assert job_status[0].job_kennung == 'BERT_V_TA_CNTRCT_CRS'

        messages = [log.message for log in audit_logs]
        assert any("Job started." in msg for msg in messages)
        assert any(f"Core script {CORE_SP_NAME} called and succeeded." in msg for msg in messages)
        assert any("Die Abarbeitung wurde ohne erkennbare Fehler beendet" in msg for msg in messages)
        assert len(messages) >= 3 # At least start, core call, end messages

    def test_2_successful_execution_job_control_entry(self, bq_client):
        """
        Purpose: Verify that a successful execution correctly creates/updates an entry
                 in `job_control` with the correct job number and metadata.
        """
        # Action
        bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.{SP_NAME}`('')").result()

        # Pass/Fail Criterion
        job_control_entry = list(bq_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.job_control`").result())
        assert len(job_control_entry) == 1
        entry = job_control_entry[0]
        assert entry.job_kennung == 'BERT_V_TA_CNTRCT_CRS'
        assert entry.program_name == 'Vertragsdatenabgleich'
        assert entry.status == 'OK'
        assert entry.start_timestamp is not None
        assert entry.end_timestamp is not None
        assert entry.stichtag_info == datetime.date.today().strftime('%d%m%Y')
        assert entry.stichtag_format == 'DDMMYYYY'
        assert entry.log_file_name.startswith('log_BERT_V_TA_CNTRCT_CRS_')
        assert entry.log_file_name.endswith('.log')

    def test_3_invalid_parameter_unknown_option(self, bq_client):
        """
        Purpose: Verify that the stored procedure handles unknown command-line options
                 (e.g., `-x`) as per legacy `getopts` behavior (ErrNr=192).
        """
        # Action
        with pytest.raises(Exception) as excinfo:
            bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.{SP_NAME}`('-x')").result()

        # Pass/Fail Criterion
        # Assert job_error_log contains an entry with error code 192, and the procedure raises an error.
        assert "Parameter error: 192 - x" in str(excinfo.value)

        error_logs = list(bq_client.query(f"SELECT error_code, error_argument FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`").result())
        assert len(error_logs) == 1
        assert error_logs[0].error_code == 192
        assert error_logs[0].error_argument == 'x'

        audit_logs = list(bq_client.query(f"SELECT message FROM `{PROJECT_ID}.{DATASET_ID}.job_audit_log` WHERE log_level = 'INFO'").result())
        messages = [log.message for log in audit_logs]
        assert any("Programm: Vertragsdatenabgleich" in msg for msg in messages) # Usage message should be logged

        job_status = list(bq_client.query(f"SELECT status FROM `{PROJECT_ID}.{DATASET_ID}.job_status`").result())
        assert len(job_status) == 1
        assert job_status[0].status == 'ERROR'

    def test_4_invalid_parameter_missing_argument(self, bq_client):
        """
        Purpose: Verify that the stored procedure handles missing arguments for options
                 requiring them (e.g., `-s` without a value) as per legacy `getopts`
                 behavior (ErrNr=193).
        """
        # Action
        with pytest.raises(Exception) as excinfo:
            bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.{SP_NAME}`('-s')").result()

        # Pass/Fail Criterion
        # Assert job_error_log contains an entry with error code 193, and the procedure raises an error.
        assert "Parameter error: 193 - s" in str(excinfo.value)

        error_logs = list(bq_client.query(f"SELECT error_code, error_argument FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`").result())
        assert len(error_logs) == 1
        assert error_logs[0].error_code == 193
        assert error_logs[0].error_argument == 's'

        audit_logs = list(bq_client.query(f"SELECT message FROM `{PROJECT_ID}.{DATASET_ID}.job_audit_log` WHERE log_level = 'INFO'").result())
        messages = [log.message for log in audit_logs]
        assert any("Programm: Vertragsdatenabgleich" in msg for msg in messages) # Usage message should be logged

        job_status = list(bq_client.query(f"SELECT status FROM `{PROJECT_ID}.{DATASET_ID}.job_status`").result())
        assert len(job_status) == 1
        assert job_status[0].status == 'ERROR'

    def test_5_help_option(self, bq_client):
        """
        Purpose: Verify that calling the procedure with the help option (`-h`) results
                 in the usage message being logged and the procedure exiting gracefully
                 without processing core logic.
        """
        # Action
        bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.{SP_NAME}`('-h')").result()

        # Pass/Fail Criterion
        # Assert job_audit_log contains the usage message, and no core logic is executed.
        # The procedure should exit without error.
        audit_logs = list(bq_client.query(f"SELECT message FROM `{PROJECT_ID}.{DATASET_ID}.job_audit_log`").result())
        messages = [log.message for log in audit_logs]

        assert any("Programm: Vertragsdatenabgleich" in msg for msg in messages)
        assert any("Beschreibung: Rahmenskript" in msg for msg in messages)
        assert not any(f"Core script {CORE_SP_NAME} called" in msg for msg in messages)
        assert not any("Job started." in msg for msg in messages) # Should not start full job logging

        job_control_count = bq_client.query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_control`").result().total_rows
        assert job_control_count == 0 # No job entry created for help option

        job_error_count = bq_client.query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`").result().total_rows
        assert job_error_count == 0 # No errors logged

        job_status_count = bq_client.query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_status`").result().total_rows
        assert job_status_count == 0 # No job status recorded

    def test_6_core_script_failure_simulation(self, bq_client):
        """
        Purpose: Verify that if the invoked core stored procedure (`sp_ausd_v_ta_cntrct_crs`)
                 fails, the orchestration procedure catches the error, logs it, and updates
                 the job status accordingly, similar to the `ERR` trap in the legacy script.
        """
        # Setup: Replace core SP with a failing version
        bq_client.query(f"""
            CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.{CORE_SP_NAME}`(
                IN p_job_kennung STRING,
                IN p_eintragsnr INT64
            )
            BEGIN
                INSERT INTO `{PROJECT_ID}.{DATASET_ID}.job_audit_log` (log_timestamp, job_entry_nr, job_kennung, log_level, message)
                VALUES (CURRENT_TIMESTAMP(), p_eintragsnr, p_job_kennung, 'INFO', 'Core script {CORE_SP_NAME} called and is about to fail.');
                RAISE USING MESSAGE 'Simulated core script failure for job ' || p_job_kennung || ' entry ' || CAST(p_eintragsnr AS STRING);
            END;
        """).result()
        time.sleep(1) # Give BigQuery time to propagate procedure changes

        # Action
        with pytest.raises(Exception) as excinfo:
            bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.{SP_NAME}`('')").result()

        # Pass/Fail Criterion
        # Assert job_error_log contains an entry for the core script failure,
        # job_audit_log reflects the error, and job_status is updated to 'ERROR'.
        assert "Simulated core script failure" in str(excinfo.value)
        assert "AppError: Abbruch" in str(excinfo.value) # Orchestration SP re-raises with its own message

        error_logs = list(bq_client.query(f"SELECT error_code, error_argument, error_message FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`").result())
        assert len(error_logs) == 1
        assert error_logs[0].error_code == 999 # Generic internal error code
        assert error_logs[0].error_argument == 'CORE_SCRIPT_FAILURE'
        assert "Simulated core script failure" in error_logs[0].error_message

        audit_logs = list(bq_client.query(f"SELECT message, log_level FROM `{PROJECT_ID}.{DATASET_ID}.job_audit_log` ORDER BY log_timestamp").result())
        messages_with_level = [(log.message, log.log_level) for log in audit_logs]
        assert any("Core script sp_ausd_v_ta_cntrct_crs called and is about to fail." in msg for msg, _ in messages_with_level)
        assert any("AppError: Abbruch. Details: Simulated core script failure" in msg and level == 'ERROR' for msg, level in messages_with_level)

        job_status = list(bq_client.query(f"SELECT status FROM `{PROJECT_ID}.{DATASET_ID}.job_status`").result())
        assert len(job_status) == 1
        assert job_status[0].status == 'ERROR'

        job_control = list(bq_client.query(f"SELECT status FROM `{PROJECT_ID}.{DATASET_ID}.job_control`").result())
        assert len(job_control) == 1
        assert job_control[0].status == 'ERROR'

    def test_7_audit_table_schema_and_data_types(self, bq_client):
        """
        Purpose: Verify that the schemas of the `job_error_log`, `job_control`,
                 `job_audit_log`, and `job_status` tables match the expected design
                 and data types.
        """
        # Action: Query BigQuery's INFORMATION_SCHEMA
        expected_schemas = {
            "job_control": {
                "job_entry_nr": "INT64", "job_kennung": "STRING", "program_name": "STRING",
                "log_file_name": "STRING", "start_timestamp": "TIMESTAMP", "end_timestamp": "TIMESTAMP",
                "status": "STRING", "stichtag_info": "STRING", "stichtag_format": "STRING"
            },
            "job_audit_log": {
                "log_timestamp": "TIMESTAMP", "job_entry_nr": "INT64", "job_kennung": "STRING",
                "log_level": "STRING", "message": "STRING"
            },
            "job_error_log": {
                "error_timestamp": "TIMESTAMP", "job_entry_nr": "INT64", "job_kennung": "STRING",
                "error_code": "INT64", "error_argument": "STRING", "error_message": "STRING"
            },
            "job_status": {
                "job_entry_nr": "INT64", "job_kennung": "STRING", "status": "STRING",
                "status_timestamp": "TIMESTAMP"
            }
        }

        # Pass/Fail Criterion
        for table_name, expected_cols in expected_schemas.items():
            query = f"""
                SELECT column_name, data_type
                FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name = '{table_name}'
            """
            schema_rows = list(bq_client.query(query).result())
            actual_cols = {row.column_name: row.data_type for row in schema_rows}

            assert len(actual_cols) == len(expected_cols), f"Schema mismatch for {table_name}: column count differs."
            for col_name, data_type in expected_cols.items():
                assert col_name in actual_cols, f"Schema mismatch for {table_name}: Missing column {col_name}."
                assert actual_cols[col_name] == data_type, f"Schema mismatch for {table_name}: Column {col_name} has unexpected type {actual_cols[col_name]}, expected {data_type}."

    def test_8_row_count_consistency_audit_tables(self, bq_client):
        """
        Purpose: Verify that the number of entries in audit tables is consistent with
                 the number of job executions and logged events (success and failure).
        """
        # Action
        # First run: Success
        bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.{SP_NAME}`('')").result()

        # Second run: Failure (by replacing core SP)
        bq_client.query(f"""
            CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.{DATASET_ID}.{CORE_SP_NAME}`(
                IN p_job_kennung STRING,
                IN p_eintragsnr INT64
            )
            BEGIN
                RAISE USING MESSAGE 'Simulated failure for second run.';
            END;
        """).result()
        time.sleep(1)
        with pytest.raises(Exception):
            bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.{SP_NAME}`('')").result()

        # Pass/Fail Criterion
        job_control_count = bq_client.query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_control`").result().total_rows
        job_audit_log_count = bq_client.query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_audit_log`").result().total_rows
        job_error_log_count = bq_client.query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`").result().total_rows
        job_status_count = bq_client.query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_status`").result().total_rows

        assert job_control_count == 2, "Expected 2 entries in job_control (1 success, 1 error)"
        assert job_status_count == 2, "Expected 2 entries in job_status (1 OK, 1 ERROR)"
        assert job_error_log_count == 1, "Expected 1 entry in job_error_log (from the failed run)"
        assert job_audit_log_count >= 6, "Expected at least 6 audit log entries (3+ for success, 3+ for failure)"
        # (Success: start, core_call, end; Failure: start, core_call_attempt, error_message)

        success_status = bq_client.query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_status` WHERE status = 'OK'").result().total_rows
        error_status = bq_client.query(f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.job_status` WHERE status = 'ERROR'").result().total_rows
        assert success_status == 1
        assert error_status == 1

```