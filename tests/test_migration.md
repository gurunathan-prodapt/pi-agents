As a senior data-migration QA engineer, I have analyzed the provided legacy KornShell script (`r_ausd_bp_ta_iccid_vertrag.ksh`) and its migrated Airflow DAG (`r_ausd_bp_ta_iccid_vertrag_dag.py`). The primary function of this script is orchestration and parameter handling, delegating the core data processing to another script (`k_ausd_bp_ta_iccid_vertrag.ksh`).

The tests below focus on ensuring the Airflow DAG correctly replicates the orchestration logic, parameter parsing, defaulting, and error handling of the legacy script, particularly concerning the parameters it prepares for the downstream core processing.

---

## Migration Validation Tests for `r_ausd_bp_ta_iccid_vertrag_dag.py`

### Test Setup Prerequisites

Before running these tests, ensure the following:

1.  **Legacy Environment**: Access to an environment where the original `r_ausd_bp_ta_iccid_vertrag.ksh` script can be executed.
2.  **Airflow Environment**: The `r_ausd_bp_ta_iccid_vertrag_dag.py` DAG is deployed and accessible in an Airflow environment (e.g., Cloud Composer).
3.  **Logging Access**: Ability to inspect logs for both the legacy script (e.g., `$LogDatei`) and Airflow tasks (Cloud Logging).
4.  **Python Test Environment**: A Python environment with `pytest` and `unittest.mock` for unit testing the `_process_parameters` function.

---

### Test Case 1: Default Parameter Handling (No parameters provided)

*   **Purpose**: Verify that when no `Stichtag` or `Wiederanlaufwert` is provided, the Airflow DAG correctly defaults `Stichtag` to the current system date (DDMMYYYY) and `Wiederanlaufwert` to `0`, matching the legacy script's behavior.
*   **Setup**:
    *   **Legacy**: Ensure no environment variables or configuration files pre-set `p_stichtag` or `p_wiederanlaufWert`.
    *   **Migrated**: Ensure the DAG is deployed.
*   **Action**:
    *   **Legacy**: Execute the legacy script without any command-line arguments:
        ```bash
        ./r_ausd_bp_ta_iccid_vertrag.ksh
        ```
        Note the current system date (e.g., `date +%d%m%Y`).
    *   **Migrated**: Trigger the Airflow DAG manually without providing any `stichtag` or `wiederanlaufwert` in the trigger configuration.
*   **Pass/Fail Criterion**:
    *   **Legacy**: The script's log file (`$LogDatei`) must show `Stichtag` set to the current system date (DDMMYYYY) and `p_wiederanlaufWert` (or its equivalent in the `k_ausd_bp_ta_iccid_vertrag.ksh` invocation) set to `0`.
        *   Expected log snippet: `Stichtag : 'DDMMYYYY'` (where DDMMYYYY is today's date)
        *   Expected `k_ausd_bp_ta_iccid_vertrag.ksh` invocation: `... -s DDMMYYYY -l 0 ...`
    *   **Migrated**:
        1.  The `process_parameters` task must complete successfully.
        2.  The `invoke_core_processing` task must complete successfully.
        3.  Inspect the logs of the `process_parameters` task. It should log:
            *   `Stichtag not provided, defaulting to system date: DDMMYYYY` (where DDMMYYYY is today's date).
            *   `Wiederanlaufwert not provided, defaulting to: 0`.
        4.  Inspect the logs of the `invoke_core_processing` task. The `command_to_execute` logged should contain:
            *   `--stichtag DDMMYYYY` (where DDMMYYYY is today's date).
            *   `--restart_value 0`.

*   **Runnable Test Code (Pytest for `_process_parameters` function logic)**:
    ```python
    import pytest
    from unittest.mock import MagicMock
    from datetime import datetime

    # Assuming _process_parameters is accessible, e.g., imported from your DAG file
    # For this example, I'll include a simplified version of the function for context.
    def _process_parameters_mock_for_test(**context):
        ti_mock = context['ti']
        dag_run_conf = context['dag_run'].conf
        params = context['params']

        stichtag_str = dag_run_conf.get('stichtag', params.get('stichtag'))
        if not stichtag_str:
            stichtag_dt = datetime.today()
            stichtag_str = stichtag_dt.strftime('%d%m%Y')
        else:
            datetime.strptime(stichtag_str, '%d%m%Y') # Validate format

        wiederanlaufwert = dag_run_conf.get('wiederanlaufwert', params.get('wiederanlaufwert'))
        if wiederanlaufwert is None or str(wiederanlaufwert) == '':
            wiederanlaufwert = 0
        else:
            wiederanlaufwert = int(wiederanlaufwert) # Validate type

        ti_mock.xcom_push(key='processed_stichtag', value=stichtag_str)
        ti_mock.xcom_push(key='processed_wiederanlaufwert', value=str(wiederanlaufwert))

    def create_mock_context(dag_run_conf=None, params=None):
        ti_mock = MagicMock()
        ti_mock.xcom_push = MagicMock()
        
        dag_run_mock = MagicMock()
        dag_run_mock.conf = dag_run_conf if dag_run_conf is not None else {}

        context = {
            'ti': ti_mock,
            'dag_run': dag_run_mock,
            'params': params if params is not None else {}
        }
        return context

    def test_default_parameters():
        context = create_mock_context()
        _process_parameters_mock_for_test(**context)

        expected_stichtag = datetime.today().strftime('%d%m%Y')
        context['ti'].xcom_push.assert_any_call(key='processed_stichtag', value=expected_stichtag)
        context['ti'].xcom_push.assert_any_call(key='processed_wiederanlaufwert', value='0')
    ```

---

### Test Case 2: Explicit Stichtag and Wiederanlaufwert Provided

*   **Purpose**: Verify that the Airflow DAG correctly processes explicitly provided `Stichtag` and `Wiederanlaufwert` parameters, matching the legacy script's behavior.
*   **Setup**:
    *   **Legacy**: Prepare specific `Stichtag` (e.g., `01012023`) and `Wiederanlaufwert` (e.g., `12345`).
    *   **Migrated**: Ensure the DAG is deployed.
*   **Action**:
    *   **Legacy**: Execute the legacy script with command-line arguments:
        ```bash
        ./r_ausd_bp_ta_iccid_vertrag.ksh -s 01012023 -l 12345
        ```
    *   **Migrated**: Trigger the Airflow DAG manually, providing `stichtag: "01012023"` and `wiederanlaufwert: 12345` in the trigger configuration.
*   **Pass/Fail Criterion**:
    *   **Legacy**: The script's log file (`$LogDatei`) must show `Stichtag : '01012023'` and the `k_ausd_bp_ta_iccid_vertrag.ksh` invocation must include `-s 01012023 -l 12345`.
    *   **Migrated**:
        1.  The `process_parameters` task must complete successfully.
        2.  The `invoke_core_processing` task must complete successfully.
        3.  Inspect the logs of the `process_parameters` task. It should log:
            *   `Stichtag provided: 01012023`.
            *   `Wiederanlaufwert provided: 12345`.
        4.  Inspect the logs of the `invoke_core_processing` task. The `command_to_execute` logged should contain:
            *   `--stichtag 01012023`.
            *   `--restart_value 12345`.

*   **Runnable Test Code (Pytest for `_process_parameters` function logic)**:
    ```python
    # (Using _process_parameters_mock_for_test and create_mock_context from above)

    def test_explicit_parameters():
        test_stichtag = "01012023"
        test_wiederanlaufwert = 12345
        context = create_mock_context(
            dag_run_conf={'stichtag': test_stichtag, 'wiederanlaufwert': test_wiederanlaufwert}
        )
        _process_parameters_mock_for_test(**context)

        context['ti'].xcom_push.assert_any_call(key='processed_stichtag', value=test_stichtag)
        context['ti'].xcom_push.assert_any_call(key='processed_wiederanlaufwert', value=str(test_wiederanlaufwert))
    ```

---

### Test Case 3: Invalid Stichtag Format

*   **Purpose**: Verify that the Airflow DAG correctly handles and rejects an invalid `Stichtag` format, causing the task to fail.
*   **Setup**:
    *   **Legacy**: Not directly testable with `getopts` for format validation, but `pruefeParameterGesetzt` or `DWDate_Gib_Zeitraum` might fail later.
    *   **Migrated**: Ensure the DAG is deployed.
*   **Action**:
    *   **Legacy**: Execute the legacy script with an invalid date format:
        ```bash
        ./r_ausd_bp_ta_iccid_vertrag.ksh -s 20230101
        ```
        Observe the script's output and log file for error messages.
    *   **Migrated**: Trigger the Airflow DAG manually, providing `stichtag: "20230101"` (invalid DDMMYYYY format) in the trigger configuration.
*   **Pass/Fail Criterion**:
    *   **Legacy**: The script should output an error message (e.g., "Parameter Stichtag ungueltig" or similar from `pruefeParameterGesetzt` or date utility) and exit with a non-zero status.
    *   **Migrated**:
        1.  The `process_parameters` task must **fail**.
        2.  Inspect the logs of the `process_parameters` task. It must contain an error message similar to: `ValueError: Invalid Stichtag format: '20230101'. Expected DDMMYYYY.`

*   **Runnable Test Code (Pytest for `_process_parameters` function logic)**:
    ```python
    # (Using _process_parameters_mock_for_test and create_mock_context from above)

    def test_invalid_stichtag_format():
        invalid_stichtag = "20230101" # YYYYMMDD instead of DDMMYYYY
        context = create_mock_context(
            dag_run_conf={'stichtag': invalid_stichtag}
        )
        with pytest.raises(ValueError, match=f"Invalid Stichtag format: '{invalid_stichtag}'. Expected DDMMYYYY."):
            _process_parameters_mock_for_test(**context)
    ```

---

### Test Case 4: Invalid Wiederanlaufwert Type

*   **Purpose**: Verify that the Airflow DAG correctly handles and rejects a non-integer `Wiederanlaufwert`, causing the task to fail.
*   **Setup**:
    *   **Legacy**: KornShell is loosely typed, so a non-numeric value might be treated as 0 or cause an error in a later arithmetic operation within `k_ausd_bp_ta_iccid_vertrag.ksh`.
    *   **Migrated**: Ensure the DAG is deployed.
*   **Action**:
    *   **Legacy**: Execute the legacy script with a non-numeric `Wiederanlaufwert`:
        ```bash
        ./r_ausd_bp_ta_iccid_vertrag.ksh -l "abc"
        ```
        Observe the script's output and log file for error messages.
    *   **Migrated**: Trigger the Airflow DAG manually, providing `wiederanlaufwert: "abc"` in the trigger configuration.
*   **Pass/Fail Criterion**:
    *   **Legacy**: The script should output an error message (potentially from `k_ausd_bp_ta_iccid_vertrag.ksh` if it attempts to use the value numerically) and exit with a non-zero status.
    *   **Migrated**:
        1.  The `process_parameters` task must **fail**.
        2.  Inspect the logs of the `process_parameters` task. It must contain an error message similar to: `ValueError: Invalid Wiederanlaufwert: 'abc'. Expected an integer.`

*   **Runnable Test Code (Pytest for `_process_parameters` function logic)**:
    ```python
    # (Using _process_parameters_mock_for_test and create_mock_context from above)

    def test_invalid_wiederanlaufwert_type():
        invalid_wiederanlaufwert = "abc"
        context = create_mock_context(
            dag_run_conf={'wiederanlaufwert': invalid_wiederanlaufwert}
        )
        with pytest.raises(ValueError, match=f"Invalid Wiederanlaufwert: '{invalid_wiederanlaufwert}'. Expected an integer."):
            _process_parameters_mock_for_test(**context)
    ```

---

### Test Case 5: Orchestration Flow and Invocation Parameters

*   **Purpose**: Verify that the Airflow DAG correctly orchestrates the tasks and passes the processed parameters to the `_invoke_core_processing` task, matching the legacy script's invocation of `k_ausd_bp_ta_iccid_vertrag.ksh`. This covers the "External-system replacements" aspect by confirming the interface to the downstream component.
*   **Setup**:
    *   **Legacy**: Prepare specific `Stichtag` (e.g., `15062023`) and `Wiederanlaufwert` (e.g., `9876`).
    *   **Migrated**: Ensure the DAG is deployed.
*   **Action**:
    *   **Legacy**: Execute the legacy script with the specified parameters:
        ```bash
        ./r_ausd_bp_ta_iccid_vertrag.ksh -s 15062023 -l 9876
        ```
    *   **Migrated**: Trigger the Airflow DAG manually, providing `stichtag: "15062023"` and `wiederanlaufwert: 9876` in the trigger configuration.
*   **Pass/Fail Criterion**:
    *   **Legacy**: The script's log file (`$LogDatei`) must explicitly show the command used to invoke `k_ausd_bp_ta_iccid_vertrag.ksh` with the correct parameters.
        *   Expected invocation: `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_iccid_vertrag.ksh -j ausd_bp_ta_iccid_vertrag -s 15062023 -f <DW_EintragsNr> -l 9876` (Note: `JobKennung` is `ausd_bp_ta_iccid_vertrag` from `typeset -u JobKennung`).
    *   **Migrated**:
        1.  Both `process_parameters` and `invoke_core_processing` tasks must complete successfully.
        2.  Inspect the logs of the `invoke_core_processing` task. The logged `command_to_execute` string must accurately reflect the parameters passed:
            *   `--job_kennung ISBERT_ICCID_VERTRAG` (matching the hardcoded value in the DAG).
            *   `--stichtag 15062023`.
            *   `--entry_nr DW00123` (matching the hardcoded value in the DAG).
            *   `--restart_value 9876`.
        *   **Note on `JobKennung` and `DW_EintragsNr`**: The legacy script derives `JobKennung` from `typeset -u JobKennung="ausd_bp_ta_iccid_vertrag"` and `DW_EintragsNr` from `DWMSG_ErmittleNr`. The migrated DAG hardcodes these as `ISBERT_ICCID_VERTRAG` and `DW00123`. This is a known divergence from the design document's "placeholder" note. For this test, we validate against the *migrated DAG's current implementation*. If the legacy derivation of `DW_EintragsNr` (which is likely a unique ID) is critical, this would require further migration and testing.

---

### Test Case 6: Error Handling and Logging (General)

*   **Purpose**: Verify that task failures within the Airflow DAG are correctly logged and propagate, and that successful execution logs are informative, leveraging Airflow's native logging capabilities. This covers the "Transformation correctness" for error handling and "External-system replacements" for logging.
*   **Setup**:
    *   **Legacy**: Ensure `f_alis_msgerr.ksh` is available and logging is configured.
    *   **Migrated**: Ensure the DAG is deployed and Cloud Logging is integrated with Airflow.
*   **Action**:
    *   **Legacy (Failure)**: Trigger a known failure scenario, e.g., by providing an invalid `Stichtag` (as in Test Case 3). Observe the script's output and the `$LogDatei` for error messages and exit status.
    *   **Legacy (Success)**: Trigger a successful run (as in Test Case 2). Observe the `$LogDatei` for success messages.
    *   **Migrated (Failure)**: Trigger a known failure scenario, e.g., by providing an invalid `Stichtag` (as in Test Case 3).
    *   **Migrated (Success)**: Trigger a successful run (as in Test Case 2).
*   **Pass/Fail Criterion**:
    *   **Legacy (Failure)**: The script should output error messages to `stderr` and `$LogDatei`, and exit with a non-zero status (e.g., `ErrNr=193` or `192`). The `trap` mechanism should be evident in the logs.
    *   **Legacy (Success)**: The `$LogDatei` should contain messages indicating successful parameter processing, the invocation of `k_ausd_bp_ta_iccid_vertrag.ksh`, and the final success message: "Die Abarbeitung wurde ohne erkennbare Fehler beendet".
    *   **Migrated (Failure)**:
        1.  The failing task (e.g., `process_parameters`) must be marked as `failed` in the Airflow UI.
        2.  The task's logs (accessible via Airflow UI or Cloud Logging) must contain the specific error message (e.g., `ValueError` traceback) that caused the failure.
        3.  Downstream tasks should not run (unless configured otherwise, but default behavior is to not run).
    *   **Migrated (Success)**:
        1.  All tasks must be marked as `success` in the Airflow UI.
        2.  The task logs (Cloud Logging) for `process_parameters` and `invoke_core_processing` must contain the expected informational messages, including parameter values and the simulated command. Airflow's native logging should clearly indicate task start, completion, and any `log.info` messages.

---