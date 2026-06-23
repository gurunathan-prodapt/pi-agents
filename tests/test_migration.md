As a senior data-migration QA engineer, I've analyzed the migration design and the generated code for `r_ausd_adressen.ksh`. The migration re-platforms the KornShell orchestrator to an Airflow DAG and the core processing (`k_ausd_adressen.ksh`) to BigQuery SQL.

The tests below are designed to ensure behavioral equivalence, covering output parity, transformation correctness, external system replacements, and data quality assertions.

---

## Migration Validation Tests for `r_ausd_adressen.ksh`

### Test Setup Prerequisites

Before running these tests, ensure the following environment is prepared:

1.  **GCP Project & BigQuery:**
    *   A GCP project (`your-gcp-project`) is set up.
    *   A BigQuery source dataset (`your_source_dataset`) exists.
    *   A BigQuery target dataset (`your_target_dataset`) exists.
    *   The `crs_source_addresses` table in `your_source_dataset` is created and populated with diverse test data, including edge cases for dates, NULLs, and various address formats.
    *   The `dwh_target_addresses` table in `your_target_dataset` is created with the schema defined in `k_ausd_adressen_logic.sql`.
2.  **Airflow Composer Environment:**
    *   An Airflow Composer environment is deployed and accessible.
    *   The `dags/r_ausd_adressen_ksh_migration.py` DAG is deployed to the Airflow environment.
    *   The `dwh_util/utils.py` module is available on the Airflow worker's Python path (e.g., in the `plugins` folder or as a deployed Python package).
    *   The `sql/k_ausd_adressen_logic.sql` file is accessible by the `BigQueryOperator` (e.g., in the `dags/sql` folder).
3.  **Legacy Environment:**
    *   The original `r_ausd_adressen.ksh` and `k_ausd_adressen.ksh` scripts are available and runnable in a controlled test environment.
    *   Access to the legacy CRS system (or a snapshot/mock of it) is available to generate baseline data.
    *   A mechanism to capture the output (log files, console output) and the resulting data from the legacy `k_ausd_adressen.ksh` run is in place.

---

### 1. Test Case: Default Parameter Handling

**Purpose:** Verify that the Airflow DAG correctly defaults `stichtag` to the current system date and `wiederanlaufwert` to `0` when no parameters are explicitly provided, mirroring the legacy script's behavior.

**Setup:**
*   Ensure the `dwh_target_addresses` table is empty or truncated before running.
*   Note the current system date (DDMMYYYY format) on the Airflow worker at the time of DAG execution. This will be the expected `stichtag`.

**Action:**
1.  Trigger the `r_ausd_adressen_ksh_migration` DAG in Airflow without providing any DAG run configuration parameters.
    ```bash
    # Example Airflow CLI command (replace with your Composer environment's method)
    airflow dags trigger r_ausd_adressen_ksh_migration
    ```
2.  Monitor the DAG run for successful completion.
3.  Inspect the Airflow task logs for `prepare_parameters_and_env` and `initialize_job_logging` tasks.
4.  Query the `dwh_target_addresses` table in BigQuery.

**Pass/Fail Criterion:**
*   The DAG completes successfully.
*   The `prepare_parameters_and_env` task log shows `stichtag` as the current system date (DDMMYYYY) and `wiederanlaufwert` as `0`.
*   The `initialize_job_logging` task log shows `stichtag` as the current system date.
*   The `dwh_target_addresses` table contains data where `extraction_stichtag` matches the system date and `job_entry_number` corresponds to the DAG run's `entry_nr`.
*   **SQL Assertion (after DAG run):**
    ```sql
    SELECT
        COUNT(1)
    FROM
        `your-gcp-project.your_target_dataset.dwh_target_addresses`
    WHERE
        extraction_stichtag = PARSE_DATE('%d%m%Y', CURRENT_DATE('%d%m%Y')) -- Assuming current date is used for stichtag
        AND job_entry_number IS NOT NULL
        AND job_identifier = 'BERT_P_ADRESSEN';
    -- Expected: Count > 0 (if source data exists for current date)
    ```

### 2. Test Case: Explicit Parameter Handling

**Purpose:** Verify that the Airflow DAG correctly parses and uses explicitly provided `stichtag` and `wiederanlaufwert` parameters.

**Setup:**
*   Choose a specific `stichtag` (e.g., `01012023`) and `wiederanlaufwert` (e.g., `100`).
*   Ensure the `dwh_target_addresses` table is empty or truncated before running.
*   Populate `crs_source_addresses` with data relevant to `01012023`.

**Action:**
1.  Trigger the `r_ausd_adressen_ksh_migration` DAG with the chosen parameters.
    ```bash
    # Example Airflow CLI command
    airflow dags trigger r_ausd_adressen_ksh_migration -c '{"stichtag": "01012023", "wiederanlaufwert": 100}'
    ```
2.  Monitor the DAG run for successful completion.
3.  Inspect the Airflow task logs for `prepare_parameters_and_env` and `initialize_job_logging` tasks.
4.  Query the `dwh_target_addresses` table in BigQuery.

**Pass/Fail Criterion:**
*   The DAG completes successfully.
*   The `prepare_parameters_and_env` task log shows `stichtag` as `01012023` and `wiederanlaufwert` as `100`.
*   The `initialize_job_logging` task log shows `stichtag` as `01012023`.
*   The `execute_core_processing_task` (BigQueryOperator) logs confirm that the SQL was executed with `stichtag='01012023'` and `wiederanlaufwert=100`.
*   The `dwh_target_addresses` table contains data where `extraction_stichtag` is `2023-01-01` and `job_entry_number` corresponds to the DAG run's `entry_nr`.
*   **SQL Assertion (after DAG run):**
    ```sql
    SELECT
        COUNT(1)
    FROM
        `your-gcp-project.your_target_dataset.dwh_target_addresses`
    WHERE
        extraction_stichtag = PARSE_DATE('%d%m%Y', '01012023')
        AND job_entry_number IS NOT NULL
        AND job_identifier = 'BERT_P_ADRESSEN';
    -- Expected: Count > 0 (if source data exists for 01012023)
    ```

### 3. Test Case: `stichtag` Validation (Missing Parameter)

**Purpose:** Verify that the DAG fails gracefully when a mandatory parameter like `stichtag` is missing, mimicking the `pruefeParameterGesetzt` behavior.

**Setup:**
*   No specific data setup needed, as this tests early validation.

**Action:**
1.  Trigger the `r_ausd_adressen_ksh_migration` DAG with an empty `stichtag` and an explicit `wiederanlaufwert`.
    ```bash
    # Example Airflow CLI command
    airflow dags trigger r_ausd_adressen_ksh_migration -c '{"stichtag": "", "wiederanlaufwert": 50}'
    ```
2.  Monitor the DAG run.

**Pass/Fail Criterion:**
*   The `validate_parameters` task fails.
*   The `validate_parameters` task log contains an error message similar to "Mandatory parameter 'Stichtag' is not set or empty." (from `dwh_util.utils.log_error_and_exit`).
*   The overall DAG run status is "failed".

### 4. Test Case: Logging and Job Tracking

**Purpose:** Verify that the DAG correctly initializes job tracking, logs key information, and updates the final status, replacing the `DWMSG_` functions.

**Setup:**
*   Ensure the `dwh_target_addresses` table is empty or truncated.
*   Populate `crs_source_addresses` with valid test data.

**Action:**
1.  Trigger the `r_ausd_adressen_ksh_migration` DAG with valid parameters (e.g., `stichtag: 15062023`).
2.  Monitor the DAG run for successful completion.
3.  Inspect the logs of `initialize_job_logging` and `update_final_status` tasks.

**Pass/Fail Criterion:**
*   The `initialize_job_logging` task log contains messages similar to:
    *   `DWMSG_ INFO: Job started. EntryNr: <some_number>, JobKennung: BERT_P_ADRESSEN, LogFile: log/BERT_P_ADRESSEN_<some_number>.log`
    *   `DWMSG_ INFO: EntryNr <some_number>: Stichtag set to 15062023`
    *   `Zeitraum dates derived: Start=15062023, End=15062023` (or whatever `get_zeitraum_dates` returns based on its implementation).
*   The `update_final_status` task log contains a message similar to:
    *   `DWMSG_ INFO: EntryNr <some_number>: Job finished with status OK`
*   The `job_identifier` and `job_entry_number` columns in the `dwh_target_addresses` table are correctly populated.
*   **SQL Assertion (after DAG run):**
    ```sql
    SELECT
        job_identifier,
        job_entry_number,
        COUNT(1) AS row_count
    FROM
        `your-gcp-project.your_target_dataset.dwh_target_addresses`
    WHERE
        extraction_stichtag = PARSE_DATE('%d%m%Y', '15062023')
    GROUP BY 1, 2;
    -- Expected: One row with job_identifier='BERT_P_ADRESSEN', a specific job_entry_number, and row_count > 0.
    ```

### 5. Test Case: Transformation Correctness - Standard Data

**Purpose:** Verify that the `k_ausd_adressen_logic.sql` correctly transforms standard, valid input data from `crs_source_addresses` into `dwh_target_addresses`. This covers column mapping, date parsing, and `FARM_FINGERPRINT` generation.

**Setup:**
*   Populate `crs_source_addresses` with a set of diverse, valid address records.
*   Run the legacy `r_ausd_adressen.ksh` (which calls `k_ausd_adressen.ksh`) with a specific `stichtag` (e.g., `01012023`) and capture its output data (e.g., into a temporary file or table). This will serve as the "golden standard."
*   Ensure the `dwh_target_addresses` table is empty or truncated.

**Action:**
1.  Trigger the `r_ausd_adressen_ksh_migration` DAG with the same `stichtag` (`01012023`) as the legacy run.
2.  Monitor the DAG run for successful completion.
3.  Query the `dwh_target_addresses` table in BigQuery to retrieve the migrated data.

**Pass/Fail Criterion:**
*   The DAG completes successfully.
*   The data in `your-gcp-project.your_target_dataset.dwh_target_addresses` is identical to the "golden standard" output from the legacy `k_ausd_adressen.ksh` run for the same `stichtag`. This includes:
    *   **Row Count:** The number of rows must match.
    *   **Column Values:** All mapped columns (`address_key`, `business_partner_id`, `address_line_1`, `city`, `postal_code`, `country_code`, `valid_from_date`, `valid_to_date`, `effective_start_date`, `extraction_stichtag`) must have identical values.
    *   `effective_end_date` should be `9999-12-31`.
    *   `load_timestamp` will differ but should be recent.
*   **SQL Assertion (after DAG run, comparing with a baseline table `legacy_output_addresses`):**
    ```sql
    -- Compare row counts
    SELECT
        (SELECT COUNT(1) FROM `your-gcp-project.your_target_dataset.dwh_target_addresses` WHERE extraction_stichtag = PARSE_DATE('%d%m%Y', '01012023')) AS migrated_count,
        (SELECT COUNT(1) FROM `your-gcp-project.your_baseline_dataset.legacy_output_addresses` WHERE extraction_stichtag = PARSE_DATE('%d%m%Y', '01012023')) AS legacy_count;
    -- Expected: migrated_count = legacy_count

    -- Compare data content (example for a few columns)
    SELECT
        COUNT(1)
    FROM
        `your-gcp-project.your_target_dataset.dwh_target_addresses` AS migrated
    FULL OUTER JOIN
        `your-gcp-project.your_baseline_dataset.legacy_output_addresses` AS legacy
    ON
        migrated.address_key = legacy.address_key
        AND migrated.business_partner_id = legacy.business_partner_id
        AND migrated.address_line_1 = legacy.address_line_1
        AND migrated.city = legacy.city
        AND migrated.postal_code = legacy.postal_code
        AND migrated.country_code = legacy.country_code
        AND migrated.valid_from_date = legacy.valid_from_date
        AND migrated.valid_to_date = legacy.valid_to_date
        AND migrated.effective_start_date = legacy.effective_start_date
        AND migrated.extraction_stichtag = legacy.extraction_stichtag
    WHERE
        migrated.address_key IS NULL OR legacy.address_key IS NULL;
    -- Expected: 0 (no mismatches or missing rows)
    ```

### 6. Test Case: Transformation Correctness - Edge Cases (NULLs, Invalid Dates, `wiederanlaufwert`)

**Purpose:** Verify robust handling of edge cases, including NULL values in source columns, invalid date formats (if `PARSE_DATE` can handle them or if they cause errors), and the specific logic for `wiederanlaufwert`.

**Setup:**
*   Populate `crs_source_addresses` with:
    *   Rows where `address_id`, `business_partner_id`, or `valid_from_date_str` are NULL (to test `FARM_FINGERPRINT` with `IFNULL`).
    *   Rows with `valid_from_date_str` or `valid_to_date_str` that are malformed or outside typical ranges (e.g., '99999999', '00000000').
    *   Rows that should be filtered out by the `stichtag` condition (e.g., `valid_to_date_str` before `stichtag`).
    *   Rows that should be affected by a non-zero `wiederanlaufwert` (if the commented-out logic in `k_ausd_adressen_logic.sql` is implemented). For example, if `wiederanlaufwert` filters `DWH_VERTRAG_ID`, ensure test data covers this.
*   Run the legacy `r_ausd_adressen.ksh` with a specific `stichtag` and `wiederanlaufwert` and capture its output as the "golden standard."
*   Ensure the `dwh_target_addresses` table is empty or truncated.

**Action:**
1.  Trigger the `r_ausd_adressen_ksh_migration` DAG with the same `stichtag` and `wiederanlaufwert` as the legacy run.
2.  Monitor the DAG run for successful completion.
3.  Query the `dwh_target_addresses` table in BigQuery.

**Pass/Fail Criterion:**
*   The DAG completes successfully (unless an invalid date format in source is expected to cause a BigQuery error, which should then be handled by the DAG).
*   The data in `your-gcp-project.your_target_dataset.dwh_target_addresses` is identical to the "golden standard" output from the legacy `k_ausd_adressen.ksh` run for the same parameters.
*   Specifically:
    *   Rows with NULLs in `address_id`, `business_partner_id`, `valid_from_date_str` should have `address_key` generated correctly (e.g., `IFNULL` should prevent errors).
    *   Rows with dates outside the `stichtag` range are correctly filtered out.
    *   If `wiederanlaufwert` logic is implemented, only the expected subset of data is processed/inserted.
*   **SQL Assertion (after DAG run, comparing with a baseline table `legacy_output_addresses`):**
    ```sql
    -- Compare row counts for the specific stichtag and wiederanlaufwert
    SELECT
        (SELECT COUNT(1) FROM `your-gcp-project.your_target_dataset.dwh_target_addresses` WHERE extraction_stichtag = PARSE_DATE('%d%m%Y', '01012023') AND wiederanlaufwert_used = 100) AS migrated_count,
        (SELECT COUNT(1) FROM `your-gcp-project.your_baseline_dataset.legacy_output_addresses` WHERE extraction_stichtag = PARSE_DATE('%d%m%Y', '01012023') AND wiederanlaufwert_used = 100) AS legacy_count;
    -- Expected: migrated_count = legacy_count

    -- Detailed comparison for data content, similar to Test Case 5.
    -- Ensure specific edge case rows are handled as expected.
    ```

### 7. Test Case: Schema and Data Type Assertions

**Purpose:** Verify that the `dwh_target_addresses` table schema and data types match expectations and are consistent with the legacy output.

**Setup:**
*   Ensure the `dwh_target_addresses` table is created.
*   Run the DAG with valid parameters to populate the table.

**Action:**
1.  After a successful DAG run, inspect the schema of `dwh_target_addresses` in BigQuery.

**Pass/Fail Criterion:**
*   The schema of `dwh_target_addresses` matches the expected schema (as defined in `k_ausd_adressen_logic.sql` and derived from the legacy system).
*   All columns have the correct BigQuery data types (e.g., `address_key` as `INT64`, `business_partner_id` as `STRING`, `valid_from_date` as `DATE`, `load_timestamp` as `TIMESTAMP`).
*   **SQL Assertion (can be run manually or via a separate Airflow task):**
    ```sql
    SELECT
        column_name,
        data_type
    FROM
        `your-gcp-project.your_target_dataset.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = 'dwh_target_addresses'
    ORDER BY
        ordinal_position;
    /*
    Expected Output (example):
    column_name           data_type
    --------------------------------
    address_key           INT64
    business_partner_id   STRING
    address_line_1        STRING
    ...
    valid_from_date       DATE
    valid_to_date         DATE
    effective_start_date  DATE
    effective_end_date    DATE
    extraction_stichtag   DATE
    load_timestamp        TIMESTAMP
    job_identifier        STRING
    job_entry_number      INT64
    */
    ```

### 8. Test Case: External System Replacement - BigQuery Operator Invocation

**Purpose:** Verify that the `BigQueryOperator` correctly invokes the `k_ausd_adressen_logic.sql` script with all necessary parameters. This replaces the shell script's direct invocation of `k_ausd_adressen.ksh`.

**Setup:**
*   Ensure `sql/k_ausd_adressen_logic.sql` is correctly placed and accessible.
*   Populate `crs_source_addresses` with some test data.

**Action:**
1.  Trigger the `r_ausd_adressen_ksh_migration` DAG with valid parameters.
2.  Monitor the `execute_core_processing_task` logs in Airflow.

**Pass/Fail Criterion:**
*   The `execute_core_processing_task` completes successfully.
*   The task logs show the BigQuery job being initiated and completed.
*   The logs explicitly show the templated parameters (`job_kennung`, `stichtag`, `entry_nr`, `wiederanlaufwert`, `start_date`, `end_date`) being passed correctly to the BigQuery SQL query.
*   The final `SELECT` statement in `k_ausd_adressen_logic.sql` (returning `status` and `processed_rows`) is executed, and its results are visible in the task logs or XComs (if configured).
    ```python
    # Example of checking XComs in a downstream PythonOperator if needed
    def check_bq_output(**context):
        bq_result = context['ti'].xcom_pull(task_ids='execute_core_processing_task', key='return_value')
        # Assuming BigQueryOperator returns the last query result
        assert bq_result[0]['status'] == 'k_ausd_adressen_bq_logic_executed_successfully'
        assert bq_result[0]['processed_rows'] > 0
    ```

### 9. Test Case: Utility Functions Unit Tests (`dwh_util/utils.py`)

**Purpose:** Ensure the refactored Python utility functions (e.g., `generate_new_entry_number`, `log_error_and_exit`, `pruefe_parameter_gesetzt`, `get_zeitraum_dates`) behave as expected.

**Setup:**
*   A Python test environment with `pytest` installed.

**Action:**
1.  Run `pytest` on the `dwh_util/utils.py` module.

**Pass/Fail Criterion:**
*   All unit tests pass.
*   **Example `pytest` code for `dwh_util/utils.py`:**
    ```python
    # test_utils.py
    import pytest
    from datetime import datetime
    from dwh_util import utils

    def test_generate_new_entry_number():
        num1 = utils.generate_new_entry_number()
        num2 = utils.generate_new_entry_number()
        assert isinstance(num1, int)
        assert num1 != num2 # Should be unique enough for testing

    def test_generate_log_filename():
        filename = utils.generate_log_filename("TEST_JOB", 12345)
        assert filename == "log/TEST_JOB_12345.log"

    def test_pruefe_parameter_gesetzt_success():
        try:
            utils.pruefe_parameter_gesetzt("Stichtag", "01012023", 1)
            assert True # No exception means success
        except ValueError:
            pytest.fail("pruefe_parameter_gesetzt raised ValueError unexpectedly")

    def test_pruefe_parameter_gesetzt_failure():
        with pytest.raises(ValueError, match="Mandatory parameter 'Stichtag' is not set or empty."):
            utils.pruefe_parameter_gesetzt("Stichtag", "", 1)
        with pytest.raises(ValueError, match="Mandatory parameter 'Stichtag' is not set or empty."):
            utils.pruefe_parameter_gesetzt("Stichtag", None, 1)

    def test_get_zeitraum_dates_valid():
        start, end = utils.get_zeitraum_dates("01012023", '%d%m%Y')
        assert start == "01012023"
        assert end == "01012023"
        # Add more complex date logic tests if get_zeitraum_dates is more sophisticated

    def test_get_zeitraum_dates_invalid_format():
        # Should log a warning and return the input as is
        with pytest.raises(ValueError): # The current implementation raises ValueError
            utils.get_zeitraum_dates("2023-01-01", '%d%m%Y')
        # If the utility was designed to handle and return input as is on error:
        # start, end = utils.get_zeitraum_dates("2023-01-01", '%d%m%Y')
        # assert start == "2023-01-01"
        # assert end == "2023-01-01"
    ```

---