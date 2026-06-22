The migration of `r_ausd_v_ta_bp_ref.ksh` to an Airflow DAG primarily involves re-platforming its orchestration logic. The core data processing logic, encapsulated in `k_ausd_v_ta_bp_ref.ksh`, is a critical dependency whose detailed migration is outside the scope of this document. Therefore, these validation tests focus on ensuring the Airflow DAG correctly replicates the *wrapper's* behavior, including environment setup, parameter passing, logging, and error handling, particularly concerning its interaction with the (migrated) core script.

For these tests, we will assume the `k_ausd_v_ta_bp_ref.ksh` script has been migrated to a shell script (`k_ausd_v_ta_bp_ref.sh`) that can be executed by the `BashOperator`. We will use mock versions of this script to simulate its behavior.

---

## Migration Validation Tests for `r_ausd_v_ta_bp_ref_dag.py`

### Mock Script Setup

Before running any tests, create a temporary directory structure to simulate `BERT_DIR_ROOT` and place mock versions of `k_ausd_v_ta_bp_ref.sh` within it.

**1. Mock `k_ausd_v_ta_bp_ref.sh` (Success Version)**
This script will log all received arguments and exit successfully.

```bash
#!/bin/bash
# File: /tmp/bert_root/aufbereitung/bin/k_ausd_v_ta_bp_ref.sh (for BERT_DIR_ROOT=/tmp/bert_root)
echo "--- Mock Core Script Output (Success) ---"
echo "Mock k_ausd_v_ta_bp_ref.sh invoked with parameters: $@"
# Simulate some work
sleep 0.1
echo "--- Mock Core Script Finished (Success) ---"
exit 0
```
Make it executable: `chmod +x /tmp/bert_root/aufbereitung/bin/k_ausd_v_ta_bp_ref.sh`

**2. Mock `k_ausd_v_ta_bp_ref.sh` (Failure Version)**
This script will log an error and exit with a non-zero status.

```bash
#!/bin/bash
# File: /tmp/bert_root/aufbereitung/bin/k_ausd_v_ta_bp_ref.sh (for BERT_DIR_ROOT=/tmp/bert_root)
echo "--- Mock Core Script Output (Failure) ---"
echo "Mock k_ausd_v_ta_bp_ref.sh invoked with parameters: $@"
echo "Simulating an error in core logic." >&2 # Send to stderr
sleep 0.1
echo "--- Mock Core Script Finished (Failure) ---"
exit 1
```
Make it executable: `chmod +x /tmp/bert_root/aufbereitung/bin/k_ausd_v_ta_bp_ref.sh`

---

### Test Case 1: Basic Successful Execution and Core Script Invocation

*   **Purpose**: Verify that the Airflow DAG successfully orchestrates the job, logs its start and end, and correctly invokes the (mocked) core script with default parameters. This covers **output parity** for basic success and **external-system replacement** for core script invocation.

*   **Setup**:
    1.  Ensure the mock `k_ausd_v_ta_bp_ref.sh` (Success Version) is in place at `/tmp/bert_root/aufbereitung/bin/k_ausd_v_ta_bp_ref.sh`.
    2.  In Airflow, set an Airflow Variable named `BERT_DIR_ROOT` with the value `/tmp/bert_root`.
    3.  Ensure the DAG's `schedule_interval` is `None` for manual triggering.

*   **Action**:
    1.  Trigger the `r_ausd_v_ta_bp_ref_dag` manually from the Airflow UI or CLI, without providing any `conf` parameters.

*   **Pass/Fail Criterion**:
    1.  All tasks (`start_job_logging`, `execute_core_logic`, `end_job_logging`) complete successfully.
    2.  The `start_job_logging` task logs contain messages similar to:
        ```
        [INFO] Job 'BERT_V_TA_BP_REF' started.
        [INFO] DW_EintragsNr for this run: ... (e.g., manual__2023-10-27T10_00_00_00_00_00)
        [INFO] Execution date: ...
        ```
    3.  The `execute_core_logic` task logs contain output from the mock script, specifically:
        ```
        Invoking core logic script: /tmp/bert_root/aufbereitung/bin/k_ausd_v_ta_bp_ref.sh
        --- Mock Core Script Output (Success) ---
        Mock k_ausd_v_ta_bp_ref.sh invoked with parameters: -j "BERT_V_TA_BP_REF" -f "..."
        --- Mock Core Script Finished (Success) ---
        ```
        (The `...` for `-f` should match the `DW_EintragsNr` from `start_job_logging`.)
    4.  The `end_job_logging` task logs contain a message similar to:
        ```
        [INFO] Job 'BERT_V_TA_BP_REF' completed successfully.
        ```

### Test Case 2: Parameter Passing (`-s` and `-l`)

*   **Purpose**: Verify that the Airflow DAG correctly parses and passes the `-s` and `-l` parameters to the core script, both from DAG `params` and `dag_run.conf`. This covers **transformation correctness** for parameter handling.

*   **Setup**:
    1.  Ensure the mock `k_ausd_v_ta_bp_ref.sh` (Success Version) is in place.
    2.  Airflow Variable `BERT_DIR_ROOT` is set to `/tmp/bert_root`.

*   **Action**:
    1.  **Sub-test 2a (DAG Params)**: Modify the `r_ausd_v_ta_bp_ref_dag.py` to set default `params` for testing:
        ```python
        params={
            'job_kennung': 'BERT_V_TA_BP_REF',
            'param_s': 'test_s_val',
            'param_l': 'test_l_val',
        },
        ```
        Then, trigger the DAG manually without `conf`.
    2.  **Sub-test 2b (dag_run.conf)**: Trigger the DAG manually with the following `conf` payload:
        ```json
        {
            "param_s": "conf_s_val",
            "param_l": "conf_l_val"
        }
        ```
        (Ensure DAG `params` are reset to `None` for `param_s` and `param_l` for this sub-test, or that `conf` overrides are correctly applied).

*   **Pass/Fail Criterion**:
    1.  All tasks complete successfully for both sub-tests.
    2.  For **Sub-test 2a**, the `execute_core_logic` task logs show the mock script invoked with:
        ```
        Mock k_ausd_v_ta_bp_ref.sh invoked with parameters: -j "BERT_V_TA_BP_REF" -f "..." -s "test_s_val" -l "test_l_val"
        ```
    3.  For **Sub-test 2b**, the `execute_core_logic` task logs show the mock script invoked with:
        ```
        Mock k_ausd_v_ta_bp_ref.sh invoked with parameters: -j "BERT_V_TA_BP_REF" -f "..." -s "conf_s_val" -l "conf_l_val"
        ```

### Test Case 3: Core Script Failure Handling

*   **Purpose**: Verify that the Airflow DAG correctly handles a failure in the core script, marking the `execute_core_logic` task as failed and preventing subsequent tasks from running. This covers **error handling**.

*   **Setup**:
    1.  Replace the mock `k_ausd_v_ta_bp_ref.sh` (Success Version) with the **Failure Version** at `/tmp/bert_root/aufbereitung/bin/k_ausd_v_ta_bp_ref.sh`.
    2.  Airflow Variable `BERT_DIR_ROOT` is set to `/tmp/bert_root`.

*   **Action**:
    1.  Trigger the `r_ausd_v_ta_bp_ref_dag` manually.

*   **Pass/Fail Criterion**:
    1.  The `start_job_logging` task completes successfully.
    2.  The `execute_core_logic` task fails.
    3.  The `end_job_logging` task is skipped or not run (due to task dependency failure).
    4.  The `execute_core_logic` task logs contain the error message from the mock script and indicate a non-zero exit:
        ```
        --- Mock Core Script Output (Failure) ---
        Mock k_ausd_v_ta_bp_ref.sh invoked with parameters: -j "BERT_V_TA_BP_REF" -f "..."
        Simulating an error in core logic.
        --- Mock Core Script Finished (Failure) ---
        ...
        Bash command failed. The command returned a non-zero exit code 1.
        ```
    5.  The overall DAG run is marked as failed.

### Test Case 4: `BERT_DIR_ROOT` Configuration

*   **Purpose**: Verify that the `BERT_DIR_ROOT` Airflow Variable is correctly used to locate the core script. This covers **external-system replacement** for environment variables.

*   **Setup**:
    1.  Create a *new* temporary directory, e.g., `/opt/airflow_bert_test/`.
    2.  Create the necessary subdirectory: `mkdir -p /opt/airflow_bert_test/aufbereitung/bin/`.
    3.  Place the mock `k_ausd_v_ta_bp_ref.sh` (Success Version) in `/opt/airflow_bert_test/aufbereitung/bin/k_ausd_v_ta_bp_ref.sh`. Make it executable.
    4.  In Airflow, update the Airflow Variable `BERT_DIR_ROOT` to `/opt/airflow_bert_test`.

*   **Action**:
    1.  Trigger the `r_ausd_v_ta_bp_ref_dag` manually.

*   **Pass/Fail Criterion**:
    1.  All tasks complete successfully.
    2.  The `execute_core_logic` task logs show the mock script invoked from the *new* `BERT_DIR_ROOT` path:
        ```
        Invoking core logic script: /opt/airflow_bert_test/aufbereitung/bin/k_ausd_v_ta_bp_ref.sh
        ```

### Test Case 5: `DW_EintragsNr` Generation and XCom Usage

*   **Purpose**: Verify that `DW_EintragsNr` is correctly generated, pushed to XCom, and pulled by the `execute_core_logic` task. This covers **transformation correctness** for internal data flow.

*   **Setup**:
    1.  Ensure the mock `k_ausd_v_ta_bp_ref.sh` (Success Version) is in place.
    2.  Airflow Variable `BERT_DIR_ROOT` is set to `/tmp/bert_root`.

*   **Action**:
    1.  Trigger the `r_ausd_v_ta_bp_ref_dag` manually.

*   **Pass/Fail Criterion**:
    1.  All tasks complete successfully.
    2.  The `start_job_logging` task logs show a generated `DW_EintragsNr` (e.g., `manual__2023-10-27T10_00_00_00_00_00`).
    3.  The `execute_core_logic` task logs show the mock script being called with `-f "..."` where `...` exactly matches the `DW_EintragsNr` logged by `start_job_logging`.
    4.  (Optional, using Airflow UI/API): Verify that the XCom value for `dw_eintrags_nr` from the `start_job_logging` task instance matches the value passed to `execute_core_logic`.

### Test Case 6: `job_kennung` Override

*   **Purpose**: Verify that the `job_kennung` can be overridden via `dag_run.conf`, similar to how `JobKennung` might be dynamically set in a shell script. This covers **output parity** and **transformation correctness** for job identification.

*   **Setup**:
    1.  Ensure the mock `k_ausd_v_ta_bp_ref.sh` (Success Version) is in place.
    2.  Airflow Variable `BERT_DIR_ROOT` is set to `/tmp/bert_root`.

*   **Action**:
    1.  Trigger the `r_ausd_v_ta_bp_ref_dag` manually with the following `conf` payload:
        ```json
        {
            "job_kennung": "CUSTOM_JOB_ID_FOR_TEST"
        }
        ```

*   **Pass/Fail Criterion**:
    1.  All tasks complete successfully.
    2.  The `start_job_logging` task logs show:
        ```
        [INFO] Job 'CUSTOM_JOB_ID_FOR_TEST' started.
        ```
    3.  The `execute_core_logic` task logs show the mock script invoked with:
        ```
        Mock k_ausd_v_ta_bp_ref.sh invoked with parameters: -j "CUSTOM_JOB_ID_FOR_TEST" -f "..."
        ```
    4.  The `end_job_logging` task logs show:
        ```
        [INFO] Job 'CUSTOM_JOB_ID_FOR_TEST' completed successfully.
        ```

### Test Case 7: Missing Core Script (External System Replacement Failure)

*   **Purpose**: Verify that if the migrated core script is not found or not executable, the `execute_core_logic` task fails gracefully with an informative error. This tests a critical **external-system replacement** dependency.

*   **Setup**:
    1.  Set Airflow Variable `BERT_DIR_ROOT` to a directory where `k_ausd_v_ta_bp_ref.sh` *does not exist* (e.g., `/tmp/non_existent_bert_root`).
    2.  Ensure no mock script is present at the path derived from this `BERT_DIR_ROOT`.

*   **Action**:
    1.  Trigger the `r_ausd_v_ta_bp_ref_dag` manually.

*   **Pass/Fail Criterion**:
    1.  The `start_job_logging` task completes successfully.
    2.  The `execute_core_logic` task fails.
    3.  The `execute_core_logic` task logs contain the error message:
        ```
        ERROR: Core logic script '/tmp/non_existent_bert_root/aufbereitung/bin/k_ausd_v_ta_bp_ref.sh' not found or not executable. This task is a placeholder.
        Bash command failed. The command returned a non-zero exit code 1.
        ```
    4.  The overall DAG run is marked as failed.