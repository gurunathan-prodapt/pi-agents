As a senior data-migration QA engineer, I've analyzed the provided migration design and generated code for `k_ausd_v_ta_vvl_dwh.ksh`. The migration targets BigQuery for data processing and Cloud Composer (Airflow) for orchestration.

The core logic involves a `DELETE` (truncate equivalent) followed by an `INSERT` into `SOF$TA_VVL_DWH` based on data from `DWH$TA_F_VVL_EREIGNISSE`, filtered by `vvl_aendgrund_id`. The legacy script also handled parameter parsing, environment setup, and error reporting.

A critical observation is the "TO DO" comment regarding the `DWPA_UTIL_SKRIPT` PL/SQL package and the potential omission of `DWTK_MELDUNGEN` and `VIA` table interactions from the transformed SQL. These are significant risks and will be highlighted in the test cases.

Below are the migration validation tests, organized by category, with a focus on proving behavioral equivalence and identifying potential gaps.

---

## Migration Validation Tests for `k_ausd_v_ta_vvl_dwh.ksh`

### 1. Output Parity & Transformation Correctness

These tests aim to ensure that for identical inputs, the migrated BigQuery SQL produces the exact same output data in `SOF$TA_VVL_DWH` as the legacy Oracle SQL.

#### Test Case 1.1: Full Data Content Parity (End-to-End)

*   **Purpose**: To verify that the entire data content of the target table `SOF$TA_VVL_DWH` is identical between the legacy and migrated systems when processing the same input data. This is the ultimate output parity check.
*   **Setup**:
    1.  **Legacy Environment**:
        *   Ensure the legacy Oracle database is accessible.
        *   Populate the legacy `DWH$TA_F_VVL_EREIGNISSE` table with a comprehensive set of test data, including various `vvl_aendgrund_id` values (in-list, between-range, and outside), NULLs, and boundary values for all relevant columns.
        *   Ensure the legacy `SOF$TA_VVL_DWH` table is empty or contains data that will be truncated by the legacy script.
    2.  **GCP Environment**:
        *   Ensure BigQuery tables `my-gcp-project.isbert_source_dataset.dwh_ta_f_vvl_ereignisse` and `my-gcp-project.target_dataset.sof_ta_vvl_dwh` exist with the correct schemas.
        *   Populate `my-gcp-project.isbert_source_dataset.dwh_ta_f_vvl_ereignisse` with data **identical** to the legacy `DWH$TA_F_VVL_EREIGNISSE` table.
        *   Ensure `my-gcp-project.target_dataset.sof_ta_vvl_dwh` is empty or contains data that will be deleted by the BigQuery SQL.
        *   Airflow DAG `bert_ausd_v_ta_vvl_dwh` is deployed and configured with appropriate `gcp_project_id`, `isbert_source_dataset`, and `target_dataset` parameters.
*   **Action**:
    1.  Execute the legacy KornShell script:
        ```bash
        ./k_ausd_v_ta_vvl_dwh.ksh -j "TEST_JOB_LEGACY" -f "TEST_ENTRY_LEGACY"
        ```
    2.  Trigger the Airflow DAG:
        ```python
        # Example using Airflow CLI (replace with your Airflow environment details)
        airflow dags trigger bert_ausd_v_ta_vvl_dwh \
            -c '{"p_jobkennung": "TEST_JOB_MIGRATED", "p_eintragsnr": "TEST_ENTRY_MIGRATED", "gcp_project_id": "my-gcp-project", "isbert_source_dataset": "isbert_source_dataset", "target_dataset": "target_dataset"}'
        ```
    3.  After both executions complete, extract all data from `SOF$TA_VVL_DWH` in the legacy Oracle database and from `my-gcp-project.target_dataset.sof_ta_vvl_dwh` in BigQuery.
*   **Pass/Fail Criterion**:
    *   The extracted datasets from both the legacy Oracle `SOF$TA_VVL_DWH` and BigQuery `my-gcp-project.target_dataset.sof_ta_vvl_dwh` are **byte-for-byte identical** (after accounting for potential column order differences, which should be normalized before comparison).
    *   Specifically, the row count, column values, and data types for each corresponding column must match exactly.

#### Test Case 1.2: Filter Logic Correctness

*   **Purpose**: To specifically verify that the `WHERE` clause in the BigQuery SQL (`vvl.vvl_aendgrund_id IN (...) OR vvl.vvl_aendgrund_id BETWEEN ...`) correctly filters data, producing the same subset of rows as the legacy system.
*   **Setup**:
    1.  **Legacy Environment**: Populate `DWH$TA_F_VVL_EREIGNISSE` with rows where `vvl_aendgrund_id` covers all conditions:
        *   Values in the `IN` list (`-3, 6, 7, 12, 13, 14, 15, 16, 17, 22, 80`).
        *   Values within the `BETWEEN` range (`24` to `60`).
        *   Values outside both conditions (e.g., `0`, `5`, `23`, `61`, `100`).
        *   NULL values for `vvl_aendgrund_id`.
    2.  **GCP Environment**: Populate `my-gcp-project.isbert_source_dataset.dwh_ta_f_vvl_ereignisse` with data **identical** to the legacy source.
*   **Action**:
    1.  Execute the `SELECT` portion of the legacy `d_ausd_v_ta_vvl_dwh.sql` against the legacy `DWH$TA_F_VVL_EREIGNISSE` table. Capture the unique identifiers (e.g., `vertrags_id`) of the resulting rows.
    2.  Execute the `SELECT` portion of the transformed BigQuery SQL against `my-gcp-project.isbert_source_dataset.dwh_ta_f_vvl_ereignisse`. Capture the unique identifiers of the resulting rows.
    ```sql
    -- BigQuery SQL to capture filtered rows for comparison
    SELECT
        vertrags_id,
        vvl_aendgrund_id,
        -- Include other key columns for detailed comparison if needed
    FROM `my-gcp-project.isbert_source_dataset.dwh_ta_f_vvl_ereignisse` AS vvl
    WHERE
        (
            vvl.vvl_aendgrund_id IN ( -3, 6, 7, 12, 13, 14, 15, 16, 17, 22, 80)
            OR vvl.vvl_aendgrund_id BETWEEN 24 AND 60
        );
    ```
*   **Pass/Fail Criterion**:
    *   The set of unique identifiers (e.g., `vertrags_id`) returned by both queries is identical.
    *   The total row count returned by both queries is identical.

#### Test Case 1.3: Data Type and NULL Handling

*   **Purpose**: To verify that all column data types are correctly mapped and handled, and NULL values are preserved consistently across the migration, preventing data loss or corruption.
*   **Setup**:
    1.  **Legacy Environment**: Populate `DWH$TA_F_VVL_EREIGNISSE` with test data that specifically targets each column's data type, including:
        *   Maximum and minimum values for numeric types.
        *   Long strings for string types.
        *   Edge dates (e.g., 1900-01-01, 2099-12-31) for date/timestamp types.
        *   Explicit `NULL` values for all nullable columns.
        *   Empty strings vs. NULLs (if applicable in Oracle).
    2.  **GCP Environment**: Populate `my-gcp-project.isbert_source_dataset.dwh_ta_f_vvl_ereignisse` with data **identical** to the legacy source.
*   **Action**:
    1.  Execute the legacy `k_ausd_v_ta_vvl_dwh.ksh` script.
    2.  Trigger the Airflow DAG `bert_ausd_v_ta_vvl_dwh`.
    3.  Query `SOF$TA_VVL_DWH` in both systems, focusing on specific rows and columns with the edge case data.
*   **Pass/Fail Criterion**:
    *   All column values, including NULLs, for the selected test rows in `SOF$TA_VVL_DWH` match exactly between the legacy Oracle and BigQuery tables.
    *   No data truncation, unexpected type conversions, or NULL/empty string mismatches are observed.

### 2. External System Replacements

These tests verify that the new GCP components correctly replace the functionality of legacy external systems and utilities.

#### Test Case 2.1: Airflow Orchestration and Parameter Passing

*   **Purpose**: To verify that the Airflow DAG correctly orchestrates the BigQuery job and passes parameters (`p_jobkennung`, `p_eintragsnr`) as intended, replacing the KornShell script's control flow and parameter parsing.
*   **Setup**:
    1.  Airflow DAG `bert_ausd_v_ta_vvl_dwh` is deployed.
    2.  BigQuery tables are set up as per Test Case 1.1.
*   **Action**:
    1.  Trigger the Airflow DAG with specific parameter values:
        ```python
        airflow dags trigger bert_ausd_v_ta_vvl_dwh \
            -c '{"p_jobkennung": "MY_CUSTOM_JOB", "p_eintragsnr": "12345", "gcp_project_id": "my-gcp-project", "isbert_source_dataset": "isbert_source_dataset", "target_dataset": "target_dataset"}'
        ```
    2.  Review the Airflow task logs for the `log_parameters` task and the `execute_d_ausd_v_ta_vvl_dwh_sql` task.
*   **Pass/Fail Criterion**:
    *   The `log_parameters` task logs show the exact `p_jobkennung` and `p_eintragsnr` values passed during the trigger.
    *   The `execute_d_ausd_v_ta_vvl_dwh_sql` task completes successfully, indicating that the BigQuery query was templated and executed correctly with the provided parameters.
    *   No errors related to parameter parsing or environment setup are observed in the logs.

#### Test Case 2.2: BigQuery as Data Source and Sink

*   **Purpose**: To verify that the Airflow DAG successfully connects to and utilizes BigQuery for both reading from source tables and writing to target tables, replacing the legacy Oracle database interactions.
*   **Setup**:
    1.  BigQuery source (`dwh_ta_f_vvl_ereignisse`) and target (`sof_ta_vvl_dwh`) tables exist and contain test data.
    2.  The Airflow service account has `BigQuery Data Editor` role (or equivalent) on the datasets.
    3.  `gcp_connection_id` is correctly configured in Airflow.
*   **Action**:
    1.  Trigger the Airflow DAG `bert_ausd_v_ta_vvl_dwh`.
    2.  Monitor the BigQuery job history in the GCP Console for the project.
    3.  Check Airflow task logs for the `execute_d_ausd_v_ta_vvl_dwh_sql` task.
*   **Pass/Fail Criterion**:
    *   A BigQuery job is successfully initiated and completed by the Airflow DAG.
    *   The BigQuery job details confirm that it read from `my-gcp-project.isbert_source_dataset.dwh_ta_f_vvl_ereignisse` and wrote to `my-gcp-project.target_dataset.sof_ta_vvl_dwh`.
    *   No authentication or authorization errors are reported in Airflow or BigQuery logs.

#### Test Case 2.3: Airflow Logging and Error Reporting

*   **Purpose**: To verify that Airflow's native logging and Cloud Logging integration effectively replace the legacy `DWMSG_MeldeFehler` and `echo` statements for operational visibility and error handling.
*   **Setup**:
    1.  Airflow environment is configured to send logs to Cloud Logging.
    2.  BigQuery tables are set up.
*   **Action**:
    1.  **Successful Run**: Trigger the Airflow DAG with valid parameters.
    2.  **Simulated Failure**: Modify the BigQuery SQL in the DAG (e.g., introduce a syntax error, reference a non-existent table) and trigger the DAG.
    3.  Review Airflow task logs and Cloud Logging for both successful and failed runs.
*   **Pass/Fail Criterion**:
    *   **Successful Run**: Airflow logs show all task execution details, including the output of `log_parameters` and successful completion messages for `execute_d_ausd_v_ta_vvl_dwh_sql`. These logs are visible in Cloud Logging.
    *   **Simulated Failure**: The `execute_d_ausd_v_ta_vvl_dwh_sql` task fails, and the error message (e.g., BigQuery syntax error) is clearly captured in the Airflow task logs and propagated to Cloud Logging. This demonstrates that operational issues are detectable and traceable.

### 3. Data Quality / Row Count / Schema Assertions

These tests focus on the structural integrity and volume of the migrated data.

#### Test Case 3.1: Row Count Parity

*   **Purpose**: To verify that the total number of rows inserted into `SOF$TA_VVL_DWH` is identical between the legacy and migrated systems for the same input, replacing the `tmpFile` record count capture.
*   **Setup**: Same as Test Case 1.1 (Full Data Content Parity).
*   **Action**:
    1.  Execute the legacy KornShell script.
    2.  Trigger the Airflow DAG `bert_ausd_v_ta_vvl_dwh`.
    3.  Count rows in `SOF$TA_VVL_DWH` in the legacy Oracle database.
    4.  Count rows in `my-gcp-project.target_dataset.sof_ta_vvl_dwh` in BigQuery.
    ```sql
    -- BigQuery SQL to count rows
    SELECT COUNT(*) FROM `my-gcp-project.target_dataset.sof_ta_vvl_dwh`;
    ```
*   **Pass/Fail Criterion**:
    *   The `COUNT(*)` from legacy Oracle `SOF$TA_VVL_DWH` equals the `COUNT(*)` from BigQuery `my-gcp-project.target_dataset.sof_ta_vvl_dwh`.
    *   **Note on `tmpFile`**: The original script captured `v_records` from `$tmpFile`. If this count is critical for downstream processes, a PythonOperator should be added to the Airflow DAG to query `my-gcp-project.target_dataset.sof_ta_vvl_dwh` for its row count and push it to XComs or a monitoring system. The current DAG does not explicitly do this.

#### Test Case 3.2: Schema Conformance

*   **Purpose**: To verify that the BigQuery target table `my-gcp-project.target_dataset.sof_ta_vvl_dwh` has the expected schema (column names, data types, nullability) as defined in the DDL and consistent with the legacy schema.
*   **Setup**: The BigQuery table `my-gcp-project.target_dataset.sof_ta_vvl_dwh` exists.
*   **Action**: Query BigQuery's `INFORMATION_SCHEMA` for the table schema.
    ```sql
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM
        `my-gcp-project.target_dataset.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = 'sof_ta_vvl_dwh'
    ORDER BY
        ordinal_position;
    ```
*   **Pass/Fail Criterion**:
    *   The retrieved schema (column names, data types, and nullability) matches the DDL provided in `sql/ddl/sof_ta_vvl_dwh.sql` and is functionally equivalent to the legacy Oracle schema.
    *   No unexpected data type conversions or nullability changes that could lead to data integrity issues are present.

### 4. Migration Gap Analysis & Edge Cases

These tests specifically address the "Unresolved / Risks" identified in the migration design and the discrepancies observed in the transformed code.

#### Test Case 4.1: `DWPA_UTIL_SKRIPT` Functionality (Beyond TRUNCATE)

*   **Purpose**: To investigate and confirm if the `DWPA_UTIL_SKRIPT` PL/SQL package had any functionality beyond the `TRUNCATE TABLE SOF$TA_VVL_DWH` equivalent, and if so, whether that functionality is correctly migrated or explicitly deemed out of scope.
*   **Setup**:
    1.  Access to the original `d_ausd_v_ta_vvl_dwh.sql` and the `DWPA_UTIL_SKRIPT` PL/SQL package definition in the legacy Oracle environment.
    2.  Test data in legacy `DWH$TA_F_VVL_EREIGNISSE` and `SOF$TA_VVL_DWH`.
*   **Action**:
    1.  **Manual Code Review**: Analyze the full `d_ausd_v_ta_vvl_dwh.sql` and the `DWPA_UTIL_SKRIPT` package. Identify all calls to `DWPA_UTIL_SKRIPT` and their parameters. Determine if any calls perform:
        *   Complex data transformations not covered by the `INSERT` statement.
        *   Updates/inserts to other tables (e.g., `VIA`).
        *   Specific logging or auditing actions.
        *   Error handling logic.
    2.  **Behavioral Test (if additional logic found)**: If additional critical logic is found, design specific tests to compare its behavior in the legacy system versus the migrated system (or confirm its absence).
*   **Pass/Fail Criterion**:
    *   **Pass**: A detailed analysis confirms that `DWPA_UTIL_SKRIPT`'s only relevant interaction with `SOF$TA_VVL_DWH` was the `TRUNCATE` operation, and any other functionalities were either non-critical, handled by Airflow's native features, or explicitly documented as out-of-scope with business approval.
    *   **Fail**: The analysis reveals critical data processing or side-effect logic from `DWPA_UTIL_SKRIPT` that is missing from the migrated BigQuery SQL or Airflow DAG, and this omission is not an approved design decision. This indicates a migration gap.

#### Test Case 4.2: `DWTK_MELDUNGEN` and `v_datum` Usage

*   **Purpose**: To determine if the `DWTK_MELDUNGEN` table was used to derive a `v_datum` variable in the legacy script, and if this `v_datum` was critical for the `SOF$TA_VVL_DWH` population. The transformed SQL comments out this logic.
*   **Setup**:
    1.  Access to the original `d_ausd_v_ta_vvl_dwh.sql` in the legacy environment.
    2.  Test data in legacy `DWTK_MELDUNGEN` that would influence `v_datum`.
*   **Action**:
    1.  **Manual Code Review**: Examine the original `d_ausd_v_ta_vvl_dwh.sql` to see how `v_datum` was calculated (if at all) and where it was used in the `INSERT` statement or other critical logic affecting `SOF$TA_VVL_DWH`.
    2.  **Behavioral Test (if critical)**: If `v_datum` was used in the `WHERE` clause or other transformations for `SOF$TA_VVL_DWH`, then Test Case 1.1 and 1.2 would need to be re-evaluated with this in mind, ensuring the migrated logic either correctly derives/uses `v_datum` or that its removal is justified.
*   **Pass/Fail Criterion**:
    *   **Pass**: `v_datum` was determined to be non-critical for the final data in `SOF$TA_VVL_DWH` (e.g., only used for logging, or its logic was implicitly handled by other means).
    *   **Fail**: `v_datum` was critical for filtering or transforming data inserted into `SOF$TA_VVL_DWH`, and its logic is missing or incorrectly implemented in the migrated solution. This indicates a migration gap.

#### Test Case 4.3: `VIA` Table Interaction

*   **Purpose**: To confirm if the `VIA` table was indeed a target for data modification by the legacy `d_ausd_v_ta_vvl_dwh.sql` script, as implied by the design document, and if its absence in the transformed BigQuery SQL is a migration gap.
*   **Setup**:
    1.  Access to the original `d_ausd_v_ta_vvl_dwh.sql` in the legacy environment.
    2.  Test data in the legacy `VIA` table.
*   **Action**:
    1.  **Manual Code Review**: Examine the original `d_ausd_v_ta_vvl_dwh.sql` for any `INSERT`, `UPDATE`, or `DELETE` statements targeting the `VIA` table.
    2.  **Behavioral Test**: If `VIA` was modified, run the legacy `k_ausd_v_ta_vvl_dwh.ksh` script and then query the legacy `VIA` table to observe changes.
*   **Pass/Fail Criterion**:
    *   **Pass**: The original `d_ausd_v_ta_vvl_dwh.sql` did *not* modify the `VIA` table, making its absence in the migrated SQL acceptable.
    *   **Fail**: The original `d_ausd_v_ta_vvl_dwh.sql` *did* modify the `VIA` table, and the migrated BigQuery SQL does not include equivalent operations on `my-gcp-project.target_dataset.via`. This is a critical migration gap.

#### Test Case 4.4: Parameter Validation (Missing Parameters - Behavioral Difference)

*   **Purpose**: To highlight and document the behavioral difference in parameter validation between the legacy KornShell script (which explicitly fails for missing critical parameters) and the migrated Airflow DAG (which uses default values if parameters are not explicitly overridden).
*   **Setup**: None.
*   **Action**:
    1.  **Legacy Script**: Execute `k_ausd_v_ta_vvl_dwh.ksh` *without* providing `-j` or `-f` parameters. Capture the exit code and error message.
        ```bash
        ./k_ausd_v_ta_vvl_dwh.ksh
        echo $? # Capture exit code
        ```
    2.  **Airflow DAG**: Trigger the `bert_ausd_v_ta_vvl_dwh` DAG *without* overriding `p_jobkennung` or `p_eintragsnr` in the trigger configuration (i.e., let it use the defaults defined in the DAG). Observe the DAG run status and logs.
        ```python
        # Example Airflow CLI trigger without overriding params
        airflow dags trigger bert_ausd_v_ta_vvl_dwh
        ```
*   **Pass/Fail Criterion**:
    *   **Legacy Script**: Exits with a non-zero error code (e.g., `193`) and prints an error message indicating missing parameters.
    *   **Airflow DAG**: Completes successfully, using the default values (`DEFAULT_JOB`, `DEFAULT_ENTRY`) for `p_jobkennung` and `p_eintragsnr` as defined in the DAG's `params`.
    *   This test **passes** if the observed behavior matches this expected difference, confirming that the design decision to use defaults in Airflow (instead of failing) is understood and accepted. If the Airflow DAG were *expected* to fail for missing parameters, then this would be a failure.

---