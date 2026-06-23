As a senior data-migration QA engineer, I've analyzed the migration design and the provided BigQuery code for `r_ausd_v_ta_inv_acc.ksh`. The core reconciliation logic (`D_AUSD_V_TA_INV_ACC.SQL`) is currently a placeholder in the migrated `k_ausd_v_ta_inv_acc` stored procedure. Therefore, the tests will primarily focus on the wrapper's behavior, parameter handling, logging, error handling, and the correct invocation of the core logic placeholder.

A critical observation is the discrepancy in date formatting: the legacy script uses `date +%d%m%Y` (DDMMYYYY), while the migrated BigQuery code uses `FORMAT_DATE('%Y%m%d', CURRENT_DATE())` (YYYYMMDD) for `v_date_format`. This is a behavioral difference that needs to be explicitly addressed, either by aligning the formats or by documenting this intentional change. For the purpose of these tests, I will highlight this.

All tests assume the BigQuery logging tables (`job_audit`, `job_error_log`, `job_log`) and the stored procedures (`Vertragsdatenabgleich`, `k_ausd_v_ta_inv_acc`) have been deployed to `my_project.my_dataset`.

---

## Migration Validation Tests for `r_ausd_v_ta_inv_acc.ksh`

### Test Case 1: Help Message Display (`p_h=TRUE`)

*   **Purpose**: Verify that the migrated wrapper stored procedure correctly handles the `p_h=TRUE` parameter, displays the help message, logs the event, and exits cleanly without invoking the core reconciliation logic. This tests output parity for the help functionality and correct job status updates.

*   **Setup**:
    1.  Ensure the BigQuery logging tables (`my_project.my_dataset.job_audit`, `my_project.my_dataset.job_error_log`, `my_project.my_dataset.job_log`) are empty.
    2.  The `my_project.my_dataset.Vertragsdatenabgleich` stored procedure is deployed.

*   **Action**:
    Execute the `Vertragsdatenabgleich` stored procedure with the help parameter set to `TRUE`.

    ```sql
    CALL `my_project.my_dataset.Vertragsdatenabgleich`(TRUE, NULL, NULL);
    ```

*   **Pass/Fail Criterion**:
    1.  The execution returns a result set containing the help message.
    2.  Exactly one record exists in `my_project.my_dataset.job_audit` with `status = 'SUCCESS'` and `message` indicating help was displayed.
    3.  Multiple records exist in `my_project.my_dataset.job_log` detailing the job start and help message display, but no log entries related to `k_ausd_v_ta_inv_acc` (core logic) should be present.
    4.  No records exist in `my_project.my_dataset.job_error_log`.

    ```python
    # Example pytest assertion (using a BigQuery client library)
    def test_help_message_display(bq_client):
        # Clear logging tables
        bq_client.query("TRUNCATE TABLE `my_project.my_dataset.job_audit`").result()
        bq_client.query("TRUNCATE TABLE `my_project.my_dataset.job_log`").result()
        bq_client.query("TRUNCATE TABLE `my_project.my_dataset.job_error_log`").result()

        # Action: Call the stored procedure
        query_job = bq_client.query("CALL `my_project.my_dataset.Vertragsdatenabgleich`(TRUE, NULL, NULL);")
        results = query_job.result()

        # 1. Verify help message output
        help_output = [row for row in results]
        assert len(help_output) == 1
        assert "Help: project.dataset.Vertragsdatenabgleich" in help_output[0].help_message
        assert "Parameters:" in help_output[0].help_message

        # 2. Verify job_audit table
        audit_rows = list(bq_client.query("SELECT status, message FROM `my_project.my_dataset.job_audit`").result())
        assert len(audit_rows) == 1
        assert audit_rows[0].status == 'SUCCESS'
        assert "Help message displayed" in audit_rows[0].message

        # 3. Verify job_log table
        log_rows = list(bq_client.query("SELECT message FROM `my_project.my_dataset.job_log` ORDER BY log_timestamp").result())
        assert any("Job started" in row.message for row in log_rows)
        assert any("Help message displayed" in row.message for row in log_rows)
        assert not any("Invoked procedure project.dataset.k_ausd_v_ta_inv_acc" in row.message for row in log_rows)
        assert not any("Calling project.dataset.k_ausd_v_ta_inv_acc" in row.message for row in log_rows)

        # 4. Verify job_error_log table
        error_rows = list(bq_client.query("SELECT * FROM `my_project.my_dataset.job_error_log`").result())
        assert len(error_rows) == 0
    ```

### Test Case 2: Successful Execution of Wrapper and Core Logic

*   **Purpose**: Verify that the wrapper orchestrates a successful job run, correctly initializes logging, calls the core logic (placeholder), and updates the job status to `SUCCESS`. This tests the main execution path, logging, and orchestration.

*   **Setup**:
    1.  Ensure the BigQuery logging tables are empty.
    2.  The `my_project.my_dataset.Vertragsdatenabgleich` and `my_project.my_dataset.k_ausd_v_ta_inv_acc` stored procedures are deployed.

*   **Action**:
    Execute the `Vertragsdatenabgleich` stored procedure with default parameters (no help, no unused parameters).

    ```sql
    CALL `my_project.my_dataset.Vertragsdatenabgleich`(FALSE, NULL, NULL);
    ```

*   **Pass/Fail Criterion**:
    1.  The stored procedure completes without raising an error.
    2.  Exactly one record exists in `my_project.my_dataset.job_audit` with `status = 'SUCCESS'`, `start_timestamp` and `end_timestamp` populated, and `message = 'Job completed successfully'`.
    3.  `job_audit.parameters` JSON accurately reflects the input parameters (`p_h=FALSE`, `p_s=NULL`, `p_l=NULL`).
    4.  `my_project.my_dataset.job_log` contains entries for:
        *   Job start (`Job started: project.dataset.Vertragsdatenabgleich`)
        *   Environment setup (`Environment setup started...`)
        *   Date format (`v_date_format set to YYYYMMDD`) - *Note the YYYYMMDD format, which differs from legacy DDMMYYYY.*
        *   Core logic invocation (`Calling project.dataset.k_ausd_v_ta_inv_acc`)
        *   Core logic start (`Starting core reconciliation logic...`)
        *   Core logic completion (`Core reconciliation logic completed successfully...`)
        *   Core logic return (`Returned from project.dataset.k_ausd_v_ta_inv_acc successfully`)
        *   Job completion (`Job completed successfully`)
    5.  No records exist in `my_project.my_dataset.job_error_log`.

    ```python
    def test_successful_execution(bq_client):
        # Clear logging tables
        bq_client.query("TRUNCATE TABLE `my_project.my_dataset.job_audit`").result()
        bq_client.query("TRUNCATE TABLE `my_project.my_dataset.job_log`").result()
        bq_client.query("TRUNCATE TABLE `my_project.my_dataset.job_error_log`").result()

        # Action: Call the stored procedure
        bq_client.query("CALL `my_project.my_dataset.Vertragsdatenabgleich`(FALSE, NULL, NULL);").result()

        # 1. Verify job_audit table
        audit_rows = list(bq_client.query("SELECT status, message, parameters FROM `my_project.my_dataset.job_audit`").result())
        assert len(audit_rows) == 1
        assert audit_rows[0].status == 'SUCCESS'
        assert audit_rows[0].message == 'Job completed successfully'
        assert '"p_h":false' in audit_rows[0].parameters
        assert '"p_s":null' in audit_rows[0].parameters
        assert '"p_l":null' in audit_rows[0].parameters

        # 2. Verify job_log table content and order (simplified check)
        log_messages = [row.message for row in bq_client.query("SELECT message FROM `my_project.my_dataset.job_log` ORDER BY log_timestamp").result()]
        assert "Job started: project.dataset.Vertragsdatenabgleich" in log_messages
        assert "Environment setup started" in log_messages
        assert f"v_date_format set to {datetime.now().strftime('%Y%m%d')}" in log_messages # Check current date format
        assert "Calling project.dataset.k_ausd_v_ta_inv_acc" in log_messages
        assert "Starting core reconciliation logic" in log_messages
        assert "Core reconciliation logic completed successfully" in log_messages
        assert "Returned from project.dataset.k_ausd_v_ta_inv_acc successfully" in log_messages
        assert "Job completed successfully" in log_messages

        # Ensure core logic logs are present
        assert any("core_logic" in row.component for row in bq_client.query("SELECT component FROM `my_project.my_dataset.job_log`").result())

        # 3. Verify job_error_log table
        error_rows = list(bq_client.query("SELECT * FROM `my_project.my_dataset.job_error_log`").result())
        assert len(error_rows) == 0
    ```

### Test Case 3: Error Handling in Core Logic

*   **Purpose**: Verify that if the core reconciliation logic (`k_ausd_v_ta_inv_acc`) encounters an error, the wrapper (`Vertragsdatenabgleich`) correctly catches it, logs the error details, and updates the job status to `FAILED`. This tests the `EXCEPTION WHEN ERROR` mechanism.

*   **Setup**:
    1.  Ensure the BigQuery logging tables are empty.
    2.  Modify `my_project.my_dataset.k_ausd_v_ta_inv_acc` to intentionally raise an error. For example, by attempting an invalid SQL operation.
        ```sql
        -- Temporarily modify k_ausd_v_ta_inv_acc to raise an error
        CREATE OR REPLACE PROCEDURE `my_project.my_dataset.k_ausd_v_ta_inv_acc`(
          p_job_id STRING,
          p_date_format STRING,
          p_stichtag_info STRING
        )
        BEGIN
          INSERT INTO `my_project.my_dataset.job_log` (job_id, log_level, message, log_timestamp, component)
          VALUES (p_job_id, 'INFO', 'Simulating an error in core logic.', CURRENT_TIMESTAMP(), 'core_logic');
          -- Intentional error: Divide by zero
          SELECT 1 / 0;
        EXCEPTION WHEN ERROR THEN
          INSERT INTO `my_project.my_dataset.job_error_log` (job_id, component, error_message, error_details, error_timestamp)
          VALUES (p_job_id, 'core_logic', @@error.message, @@error.stack_trace, CURRENT_TIMESTAMP());
          RAISE; -- Re-raises the caught exception to propagate to the wrapper
        END;
        ```
    3.  The `my_project.my_dataset.Vertragsdatenabgleich` stored procedure is deployed.

*   **Action**:
    Execute the `Vertragsdatenabgleich` stored procedure with default parameters.

    ```sql
    CALL `my_project.my_dataset.Vertragsdatenabgleich`(FALSE, NULL, NULL);
    ```

*   **Pass/Fail Criterion**:
    1.  The `CALL` statement for `Vertragsdatenabgleich` should fail and return an error message (e.g., "Division by zero").
    2.  Exactly one record exists in `my_project.my_dataset.job_audit` with `status = 'FAILED'`, `start_timestamp` and `end_timestamp` populated, and `message` indicating job failure with the error.
    3.  At least one record exists in `my_project.my_dataset.job_error_log` with `job_id` matching the `job_audit` entry, `component = 'core_logic'`, and `error_message` containing details of the simulated error (e.g., "Division by zero").
    4.  `my_project.my_dataset.job_log` contains entries up to the point of failure in the core logic, and then an entry from the wrapper indicating the core logic call failed.

    ```python
    def test_error_handling_in_core_logic(bq_client):
        # Setup: Modify k_ausd_v_ta_inv_acc to raise an error (as described above)
        # ... (code to deploy the modified k_ausd_v_ta_inv_acc) ...

        # Clear logging tables
        bq_client.query("TRUNCATE TABLE `my_project.my_dataset.job_audit`").result()
        bq_client.query("TRUNCATE TABLE `my_project.my_dataset.job_log`").result()
        bq_client.query("TRUNCATE TABLE `my_project.my_dataset.job_error_log`").result()

        # Action: Call the stored procedure, expecting it to fail
        with pytest.raises(Exception) as excinfo:
            bq_client.query("CALL `my_project.my_dataset.Vertragsdatenabgleich`(FALSE, NULL, NULL);").result()
        assert "Division by zero" in str(excinfo.value) # Verify the specific error

        # 1. Verify job_audit table
        audit_rows = list(bq_client.query("SELECT status, message FROM `my_project.my_dataset.job_audit`").result())
        assert len(audit_rows) == 1
        assert audit_rows[0].status == 'FAILED'
        assert "Job failed: Division by zero" in audit_rows[0].message

        # 2. Verify job_error_log table
        error_rows = list(bq_client.query("SELECT job_id, component, error_message FROM `my_project.my_dataset.job_error_log`").result())
        assert len(error_rows) >= 1 # Could be one from core, one from wrapper if wrapper also logs
        assert any("core_logic" in row.component and "Division by zero" in row.error_message for row in error_rows)
        
        # Get the job_id from audit table to verify error logs
        job_id = audit_rows[0].job_id
        assert any(row.job_id == job_id for row in error_rows)

        # 3. Verify job_log table (contains logs up to failure point)
        log_messages = [row.message for row in bq_client.query("SELECT message FROM `my_project.my_dataset.job_log` ORDER BY log_timestamp").result()]
        assert "Job started" in log_messages
        assert "Calling project.dataset.k_ausd_v_ta_inv_acc" in log_messages
        assert "Simulating an error in core logic." in log_messages
        assert "Job completed successfully" not in log_messages # Should not be present
        assert "Returned from project.dataset.k_ausd_v_ta_inv_acc successfully" not in log_messages # Should not be present

        # Teardown: Restore k_ausd_v_ta_inv_acc to its original placeholder state
        # ... (code to deploy the original k_ausd_v_ta_inv_acc) ...
    ```

### Test Case 4: Unused Parameters (`p_s`, `p_l`) Handling

*   **Purpose**: Verify that the migrated stored procedure correctly accepts the `p_s` and `p_l` parameters, even though they are unused, without causing errors or altering the execution flow. This confirms the behavioral equivalence for these vestigial parameters.

*   **Setup**:
    1.  Ensure the BigQuery logging tables are empty.
    2.  The `my_project.my_dataset.Vertragsdatenabgleich` and `my_project.my_dataset.k_ausd_v_ta_inv_acc` stored procedures are deployed in their default, non-erroring state.

*   **Action**:
    Execute the `Vertragsdatenabgleich` stored procedure, providing values for `p_s` and `p_l`.

    ```sql
    CALL `my_project.my_dataset.Vertragsdatenabgleich`(FALSE, 'some_system', 'some_log_level');
    ```

*   **Pass/Fail Criterion**:
    1.  The stored procedure completes successfully without raising an error.
    2.  Exactly one record exists in `my_project.my_dataset.job_audit` with `status = 'SUCCESS'`.
    3.  `job_audit.parameters` JSON accurately reflects the input parameters, including the provided values for `p_s` and `p_l`.
    4.  The sequence of log messages in `my_project.my_dataset.job_log` is identical to a successful run without these parameters (Test Case 2), confirming they did not alter the flow.
    5.  No records exist in `my_project.my_dataset.job_error_log`.

    ```python
    def test_unused_parameters_handling(bq_client):
        # Clear logging tables
        bq_client.query("TRUNCATE TABLE `my_project.my_dataset.job_audit`").result()
        bq_client.query("TRUNCATE TABLE `my_project.my_dataset.job_log`").result()
        bq_client.query("TRUNCATE TABLE `my_project.my_dataset.job_error_log`").result()

        # Action: Call the stored procedure with unused parameters
        bq_client.query("CALL `my_project.my_dataset.Vertragsdatenabgleich`(FALSE, 'some_system', 'some_log_level');").result()

        # 1. Verify job_audit table
        audit_rows = list(bq_client.query("SELECT status, parameters FROM `my_project.my_dataset.job_audit`").result())
        assert len(audit_rows) == 1
        assert audit_rows[0].status == 'SUCCESS'
        assert '"p_h":false' in audit_rows[0].parameters
        assert '"p_s":"some_system"' in audit_rows[0].parameters
        assert '"p_l":"some_log_level"' in audit_rows[0].parameters

        # 2. Verify job_log table (should be same as successful run, no extra processing for p_s, p_l)
        log_messages = [row.message for row in bq_client.query("SELECT message FROM `my_project.my_dataset.job_log` ORDER BY log_timestamp").result()]
        assert "Job started" in log_messages
        assert "Calling project.dataset.k_ausd_v_ta_inv_acc" in log_messages
        assert "Job completed successfully" in log_messages
        # No specific log messages should indicate processing of 'some_system' or 'some_log_level' beyond parameter capture

        # 3. Verify job_error_log table
        error_rows = list(bq_client.query("SELECT * FROM `my_project.my_dataset.job_error_log`").result())
        assert len(error_rows) == 0
    ```

### Test Case 5: Date Formatting and Environment Variable Replacement

*   **Purpose**: Verify how the legacy `date +%d%m%Y` and `typeset -u JobKennung` are replaced in the BigQuery environment. Specifically, confirm the format of `v_date_format` and `v_stichtag_info` and the `JobKennung` equivalent. This tests transformation correctness for environment handling.

*   **Setup**:
    1.  Ensure the BigQuery logging tables are empty.
    2.  The `my_project.my_dataset.Vertragsdatenabgleich` and `my_project.my_dataset.k_ausd_v_ta_inv_acc` stored procedures are deployed.

*   **Action**:
    Execute the `Vertragsdatenabgleich` stored procedure.

    ```sql
    CALL `my_project.my_dataset.Vertragsdatenabgleich`(FALSE, NULL, NULL);
    ```

*   **Pass/Fail Criterion**:
    1.  The `job_log` table contains an entry from the wrapper component with a message like `v_date_format set to YYYYMMDD` where `YYYYMMDD` matches the current date in `YYYYMMDD` format.
        *   **CRITICAL NOTE**: The legacy script uses `DDMMYYYY`. The migrated code uses `YYYYMMDD`. This is a functional difference. The test should pass if `YYYYMMDD` is observed, but this difference *must be explicitly documented and approved* by the business/stakeholders. If `DDMMYYYY` was required, the BigQuery code needs correction (`FORMAT_DATE('%d%m%Y', CURRENT_DATE())`).
    2.  The `job_log` table contains an entry from the wrapper component with a message like `v_stichtag_info set to DEFAULT_STICHTAG_INFO`. This confirms the placeholder is used.
    3.  The `job_audit` table's `source_job_name` is `r_ausd_v_ta_inv_acc.ksh`, and the `job_log` entries for the wrapper component use `wrapper` and for the core component use `core_logic`, effectively replacing the `JobKennung` and script names in logs.

    ```python
    def test_date_and_env_replacement(bq_client):
        # Clear logging tables
        bq_client.query("TRUNCATE TABLE `my_project.my_dataset.job_audit`").result()
        bq_client.query("TRUNCATE TABLE `my_project.my_dataset.job_log`").result()
        bq_client.query("TRUNCATE TABLE `my_project.my_dataset.job_error_log`").result()

        # Action: Call the stored procedure
        bq_client.query("CALL `my_project.my_dataset.Vertragsdatenabgleich`(FALSE, NULL, NULL);").result()

        # 1. Verify v_date_format
        log_rows = list(bq_client.query("SELECT message FROM `my_project.my_dataset.job_log` WHERE component = 'wrapper' ORDER BY log_timestamp").result())
        current_date_yyyymmdd = datetime.now().strftime('%Y%m%d')
        assert any(f"v_date_format set to {current_date_yyyymmdd}" in row.message for row in log_rows)
        # Highlight the discrepancy: Legacy was DDMMYYYY, migrated is YYYYMMDD. This is a pass for the current code, but a flag for review.

        # 2. Verify v_stichtag_info
        assert any("v_stichtag_info set to DEFAULT_STICHTAG_INFO" in row.message for row in log_rows)

        # 3. Verify JobKennung replacement (implicit in log messages and audit table)
        audit_rows = list(bq_client.query("SELECT source_job_name FROM `my_project.my_dataset.job_audit`").result())
        assert len(audit_rows) == 1
        assert audit_rows[0].source_job_name == 'r_ausd_v_ta_inv_acc.ksh' # This is the equivalent of ProgName/JobKennung for audit

        log_components = list(bq_client.query("SELECT DISTINCT component FROM `my_project.my_dataset.job_log`").result())
        assert any(row.component == 'wrapper' for row in log_components)
        assert any(row.component == 'core_logic' for row in log_components)
    ```

### Test Case 6: Logging Content and Structure

*   **Purpose**: Verify that the `job_audit` and `job_log` tables capture comprehensive and structured information about the job's execution, replacing the custom `DWMSG_*` calls and `print` statements of the legacy script. This tests data quality and schema assertions for logging.

*   **Setup**:
    1.  Ensure the BigQuery logging tables are empty.
    2.  The `my_project.my_dataset.Vertragsdatenabgleich` and `my_project.my_dataset.k_ausd_v_ta_inv_acc` stored procedures are deployed.

*   **Action**:
    Execute the `Vertragsdatenabgleich` stored procedure.

    ```sql
    CALL `my_project.my_dataset.Vertragsdatenabgleich`(FALSE, 'test_s_param', 'test_l_param');
    ```

*   **Pass/Fail Criterion**:
    1.  **`job_audit` table**:
        *   Contains exactly one record.
        *   `job_id` is a valid UUID.
        *   `start_timestamp` and `end_timestamp` are populated and `end_timestamp` is after `start_timestamp`.
        *   `status` is 'SUCCESS'.
        *   `source_job_name` is 'r_ausd_v_ta_inv_acc.ksh'.
        *   `parameters` is a valid JSON string containing `p_h=FALSE`, `p_s='test_s_param'`, `p_l='test_l_param'`.
    2.  **`job_log` table**:
        *   Contains multiple records (at least 10-15 for a full run).
        *   All records have a `job_id` matching the one in `job_audit`.
        *   `log_timestamp` is populated for all records.
        *   `log_level` is 'INFO' for all standard messages.
        *   `component` is either 'wrapper' or 'core_logic'.
        *   Messages are descriptive and cover all stages of execution (start, env setup, core call, core start, core end, core return, job end).
    3.  **`job_error_log` table**:
        *   Contains zero records.

    ```python
    def test_logging_content_and_structure(bq_client):
        # Clear logging tables
        bq_client.query("TRUNCATE TABLE `my_project.my_dataset.job_audit`").result()
        bq_client.query("TRUNCATE TABLE `my_project.my_dataset.job_log`").result()
        bq_client.query("TRUNCATE TABLE `my_project.my_dataset.job_error_log`").result()

        # Action: Call the stored procedure
        bq_client.query("CALL `my_project.my_dataset.Vertragsdatenabgleich`(FALSE, 'test_s_param', 'test_l_param');").result()

        # 1. Verify job_audit table structure and content
        audit_rows = list(bq_client.query("SELECT job_id, start_timestamp, end_timestamp, status, source_job_name, parameters FROM `my_project.my_dataset.job_audit`").result())
        assert len(audit_rows) == 1
        audit_entry = audit_rows[0]
        assert re.match(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", audit_entry.job_id) # Valid UUID
        assert audit_entry.start_timestamp is not None
        assert audit_entry.end_timestamp is not None
        assert audit_entry.end_timestamp > audit_entry.start_timestamp
        assert audit_entry.status == 'SUCCESS'
        assert audit_entry.source_job_name == 'r_ausd_v_ta_inv_acc.ksh'
        assert '"p_h":false' in audit_entry.parameters
        assert '"p_s":"test_s_param"' in audit_entry.parameters
        assert '"p_l":"test_l_param"' in audit_entry.parameters

        # 2. Verify job_log table structure and content
        log_rows = list(bq_client.query("SELECT job_id, log_timestamp, log_level, message, component FROM `my_project.my_dataset.job_log` ORDER BY log_timestamp").result())
        assert len(log_rows) >= 10 # Expect a reasonable number of log entries
        for log_entry in log_rows:
            assert log_entry.job_id == audit_entry.job_id # All logs linked to the same job_id
            assert log_entry.log_timestamp is not None
            assert log_entry.log_level == 'INFO'
            assert log_entry.message is not None and len(log_entry.message) > 0
            assert log_entry.component in ['wrapper', 'core_logic']

        # Check for key messages
        log_messages = [row.message for row in log_rows]
        assert "Job started" in log_messages[0]
        assert "Calling project.dataset.k_ausd_v_ta_inv_acc" in log_messages
        assert "Starting core reconciliation logic" in log_messages
        assert "Core reconciliation logic completed successfully" in log_messages
        assert "Job completed successfully" in log_messages[-1]

        # 3. Verify job_error_log table
        error_rows = list(bq_client.query("SELECT * FROM `my_project.my_dataset.job_error_log`").result())
        assert len(error_rows) == 0
    ```

### Test Case 7: External System Replacements (Implicit)

*   **Purpose**: Verify that the BigQuery solution correctly replaces the legacy shell utilities and custom frameworks (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) with BigQuery-native constructs and logging tables. This is primarily an architectural verification.

*   **Setup**:
    1.  The BigQuery stored procedures and logging tables are deployed.
    2.  No legacy shell scripts or environment files are present or accessible in the BigQuery execution environment.

*   **Action**:
    Execute the `Vertragsdatenabgleich` stored procedure.

    ```sql
    CALL `my_project.my_dataset.Vertragsdatenabgleich`(FALSE, NULL, NULL);
    ```

*   **Pass/Fail Criterion**:
    1.  The job completes successfully (as per Test Case 2).
    2.  The `job_log` table contains the message "Environment setup started (mock placeholder for .dw_init and shell utilities)." indicating the replacement strategy.
    3.  No errors related to missing shell scripts or environment variables occur during execution.
    4.  The logging and error handling mechanisms (BigQuery tables) function as expected, demonstrating the replacement of `DWMSG_*` functions and `trap` commands.

    ```python
    def test_external_system_replacements(bq_client):
        # Clear logging tables
        bq_client.query("TRUNCATE TABLE `my_project.my_dataset.job_audit`").result()
        bq_client.query("TRUNCATE TABLE `my_project.my_dataset.job_log`").result()
        bq_client.query("TRUNCATE TABLE `my_project.my_dataset.job_error_log`").result()

        # Action: Call the stored procedure
        bq_client.query("CALL `my_project.my_dataset.Vertragsdatenabgleich`(FALSE, NULL, NULL);").result()

        # 1. Verify successful completion (implicitly checked by no exception)
        audit_rows = list(bq_client.query("SELECT status FROM `my_project.my_dataset.job_audit`").result())
        assert len(audit_rows) == 1
        assert audit_rows[0].status == 'SUCCESS'

        # 2. Verify log message indicating environment replacement
        log_messages = [row.message for row in bq_client.query("SELECT message FROM `my_project.my_dataset.job_log`").result()]
        assert any("Environment setup started (mock placeholder for .dw_init and shell utilities)." in msg for msg in log_messages)

        # 3. Verify logging and error handling (already covered by other tests, but confirm no new errors)
        error_rows = list(bq_client.query("SELECT * FROM `my_project.my_dataset.job_error_log`").result())
        assert len(error_rows) == 0
    ```

---
**Important Considerations for Future Development and Testing:**

*   **Core Logic Implementation**: Once `D_AUSD_V_TA_INV_ACC.SQL` is fully migrated into `my_project.my_dataset.k_ausd_v_ta_inv_acc`, extensive data validation tests will be required. These tests will need to:
    *   Compare row counts and checksums of the `ta_inv_acc` table (and any intermediate tables) against the legacy system's output for various input scenarios.
    *   Verify transformation correctness for joins, aggregations, filters, data types, and NULL handling by comparing specific data points.
    *   Test edge cases identified in the original SQL (e.g., empty source tables, all NULL values, specific date ranges).
*   **Performance**: Compare execution times of the BigQuery solution against the legacy KornShell script, especially for large datasets.
*   **Resource Usage**: Monitor BigQuery slot consumption and storage costs.
*   **Orchestration (Cloud Composer)**: If Cloud Composer is used, tests will be needed to verify DAG scheduling, task dependencies, and error handling within the Airflow environment.
*   **Date Format Discrepancy**: The `DDMMYYYY` vs `YYYYMMDD` difference for `v_date_format` must be formally reviewed and either corrected in the BigQuery code or explicitly accepted as a change.