The migration of `r_ausd_bp_ta_bpr_apn.ksh` to an Airflow DAG primarily involves re-implementing orchestration logic, parameter handling, and the invocation of a core processing script. The tests below focus on ensuring the migrated DAG behaves identically to the legacy KornShell script in these aspects, particularly concerning parameter resolution and the interface to the downstream core logic.

---

## Migration Validation Tests for `r_ausd_bp_ta_bpr_apn_dag.py`

### 1. Output Parity & Transformation Correctness: Default Parameters

*   **Purpose:** Verify that when no `stichtag` or `wiederanlaufwert` parameters are provided, the migrated Airflow DAG correctly defaults `p_stichtag` to the current system date and `p_wiederanlaufwert` to `0`, matching the legacy script's behavior.
*   **Setup:**
    1.  Ensure the Airflow environment is running and the `r_ausd_bp_ta_bpr_apn_dag.py` DAG is deployed.
    2.  Note the current UTC date (e.g., `YYYY-MM-DD`).
*   **Action:**
    1.  **Legacy:** Execute the KornShell script without any arguments:
        ```bash
        # Assume BERT_DIR_ROOT and HOME are set for the legacy environment
        # Simulate execution in a controlled environment to capture logs
        export BERT_DIR_ROOT="/path/to/legacy/bert"
        export HOME="/path/to/legacy/home"
        /path/to/vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_apn.ksh
        # Inspect the generated LogDatei for Stichtag and Wiederanlaufwert
        ```
    2.  **Migrated:** Trigger the Airflow DAG `r_ausd_bp_ta_bpr_apn_dag` without providing any `dag_run.conf` parameters.
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   The legacy script's `LogDatei` shows `Stichtag` as the current system date (DDMMYYYY format) and `Wiederanlaufwert` as `0`.
        *   The Airflow task logs for `process_parameters_task` and `invoke_core_script_task` show `Resolved Stichtag` as the current UTC date (YYYY-MM-DD format) and `Resolved Wiederanlaufwert` as `0`.
        *   The `invoke_core_script_task` logs confirm these values are prepared for the core script.
    *   **Fail:** Any deviation from the expected default values.

### 2. Output Parity & Transformation Correctness: Valid Parameters Provided

*   **Purpose:** Verify that the migrated Airflow DAG correctly parses and uses explicitly provided `stichtag` and `wiederanlaufwert` parameters, matching the legacy script's behavior.
*   **Setup:**
    1.  Ensure the Airflow environment is running and the `r_ausd_bp_ta_bpr_apn_dag.py` DAG is deployed.
    2.  Choose a specific `stichtag` (e.g., `2023-10-26`) and `wiederanlaufwert` (e.g., `12345`).
*   **Action:**
    1.  **Legacy:** Execute the KornShell script with the chosen parameters:
        ```bash
        /path/to/vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_apn.ksh -s 26102023 -l 12345
        # Inspect the generated LogDatei for Stichtag and Wiederanlaufwert
        ```
    2.  **Migrated:** Trigger the Airflow DAG `r_ausd_bp_ta_bpr_apn_dag` with the following `dag_run.conf`:
        ```json
        {
          "stichtag": "2222-02-22",
          "wiederanlaufwert": 54321
        }
        ```
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   The legacy script's `LogDatei` shows `Stichtag` as `26102023` and `Wiederanlaufwert` as `12345`.
        *   The Airflow task logs for `process_parameters_task` and `invoke_core_script_task` show `Resolved Stichtag` as `2222-02-22` and `Resolved Wiederanlaufwert` as `54321`.
        *   The `invoke_core_script_task` logs confirm these values are prepared for the core script.
    *   **Fail:** Any deviation from the expected parsed values.

### 3. Transformation Correctness: Invalid Stichtag Format Handling

*   **Purpose:** Verify that the migrated Airflow DAG correctly handles an invalid `stichtag` format by defaulting to the current date, and logs an appropriate error/warning, matching the spirit of robust error handling in the legacy script.
*   **Setup:**
    1.  Ensure the Airflow environment is running and the `r_ausd_bp_ta_bpr_apn_dag.py` DAG is deployed.
    2.  Note the current UTC date (e.g., `YYYY-MM-DD`).
*   **Action:**
    1.  **Legacy:** The legacy script expects `DDMMYYYY`. Providing an invalid format (e.g., `YYYY-MM-DD`) would likely result in `pruefeParameterGesetzt` failing or the date being misinterpreted. For this test, we'll focus on the migrated behavior.
    2.  **Migrated:** Trigger the Airflow DAG `r_ausd_bp_ta_bpr_apn_dag` with an invalid `stichtag` format:
        ```json
        {
          "stichtag": "26.10.2023",
          "wiederanlaufwert": 100
        }
        ```
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   The `process_parameters_task` logs an `ERROR` message similar to `Invalid stichtag format: 26.10.2023. Expected YYYY-MM-DD. Using today's date.`.
        *   The `invoke_core_script_task` logs `Stichtag` as the current UTC date (YYYY-MM-DD format) and `Wiederanlaufwert` as `100`.
        *   The DAG run completes successfully (as the script defaults rather than failing).
    *   **Fail:** The DAG fails, or the `stichtag` is not defaulted to the current date, or no error/warning is logged.

### 4. Transformation Correctness: Invalid Wiederanlaufwert Type Handling

*   **Purpose:** Verify that the migrated Airflow DAG correctly handles a non-integer `wiederanlaufwert` by defaulting to `0` and logging a warning, matching the robust handling expected from the legacy script's `getopts` and `if [[ -z ... ]]` logic.
*   **Setup:** Ensure the Airflow environment is running and the `r_ausd_bp_ta_bpr_apn_dag.py` DAG is deployed.
*   **Action:**
    1.  **Legacy:** The `getopts` mechanism in ksh would treat a non-numeric value for `-l` as a string. The `if [[ -z "$p_wiederanlaufWert" ]]` check would not trigger if a value was provided, but subsequent operations expecting an integer might fail. For this test, we focus on the explicit defaulting in the migrated code.
    2.  **Migrated:** Trigger the Airflow DAG `r_ausd_bp_ta_bpr_apn_dag` with a non-integer `wiederanlaufwert`:
        ```json
        {
          "stichtag": "2023-10-26",
          "wiederanlaufwert": "abc"
        }
        ```
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   The `process_parameters_task` logs a `WARNING` message similar to `Invalid wiederanlaufwert: abc. Defaulting to 0.`.
        *   The `invoke_core_script_task` logs `Stichtag` as `2023-10-26` and `Wiederanlaufwert` as `0`.
        *   The DAG run completes successfully.
    *   **Fail:** The DAG fails, or the `wiederanlaufwert` is not defaulted to `0`, or no warning is logged.

### 5. Transformation Correctness: `p_stichtag` Determination (Legacy `FOSHoleLadedatum` vs. `v_sysdate`)

*   **Purpose:** Confirm that the migrated DAG's `p_stichtag` determination logic aligns with the *active* logic in the legacy script, specifically addressing the commented-out `FOSHoleLadedatum` section.
*   **Setup:**
    1.  Ensure the Airflow environment is running and the `r_ausd_bp_ta_bpr_apn_dag.py` DAG is deployed.
    2.  Identify the current system date for both legacy and migrated environments.
*   **Action:**
    1.  **Legacy:** Execute the KornShell script without the `-s` parameter.
        ```bash
        /path/to/vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_apn.ksh
        # Inspect the LogDatei for the resolved Stichtag.
        # It should be the system date due to 'p_stichtag=$v_sysdate;'
        ```
    2.  **Migrated:** Trigger the Airflow DAG `r_ausd_bp_ta_bpr_apn_dag` without providing a `stichtag` parameter.
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   The legacy script's `LogDatei` shows `Stichtag` as the current system date (DDMMYYYY).
        *   The Airflow task logs for `process_parameters_task` show `Resolved Stichtag` as the current UTC date (YYYY-MM-DD).
        *   This confirms that the migrated DAG correctly implements the *active* legacy logic (defaulting to `sysdate`) and does not attempt to re-implement the commented-out `FOSHoleLadedatum` functionality, which was explicitly noted as an unresolved risk in the design.
    *   **Fail:** The migrated DAG's `stichtag` differs from the current system date when no input is provided, or if it attempts to fetch `maxladedatum` from a source table.

### 6. External-System Replacements: Core Script Invocation Parameters

*   **Purpose:** Verify that the parameters prepared for the downstream core processing script (`k_ausd_bp_ta_bpr_apn.ksh` in legacy, its migrated equivalent in Airflow) are identical between the legacy and migrated orchestrators.
*   **Setup:**
    1.  Ensure both legacy and Airflow environments are set up.
    2.  Choose specific valid parameters: `stichtag="2024-01-15"` (legacy: `15012024`), `wiederanlaufwert=999`.
*   **Action:**
    1.  **Legacy:** Execute the KornShell script with the chosen parameters. Capture the exact command-line arguments passed to `k_ausd_bp_ta_bpr_apn.ksh` (e.g., by adding an `echo` before the invocation or inspecting the log file).
        ```bash
        # Modify legacy script temporarily for testing to echo the command
        # before execution, or capture from LogDatei if it logs the full command.
        # Example:
        # echo "${Name_Kernskript} -j $JobKennung -s $p_stichtag -f ${DW_EintragsNr} -l ${p_wiederanlaufWert}" >> $LogDatei
        /path/to/vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_apn.ksh -s 15012024 -l 999
        # Expected output in LogDatei (or stdout):
        # ... k_ausd_bp_ta_bpr_apn.ksh -j ausd_bp_ta_bpr_apn -s 15012024 -f <DW_EintragsNr> -l 999 ...
        ```
    2.  **Migrated:** Trigger the Airflow DAG `r_ausd_bp_ta_bpr_apn_dag` with `dag_run.conf`:
        ```json
        {
          "stichtag": "2024-01-15",
          "wiederanlaufwert": 999
        }
        ```
        Inspect the logs of the `invoke_core_script_task`.
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   The `Stichtag` and `Wiederanlaufwert` logged by the `invoke_core_script_task` in Airflow (`2024-01-15` and `999`) precisely match the values passed to the core script in the legacy execution (`15012024` and `999`).
        *   **Note:** The `JobKennung` and `DW_EintragsNr` are placeholders in the current migrated DAG's `_invoke_core_processing` function. This test confirms the *parameter values* are correct, acknowledging that the *mechanism* for passing `JobKennung` and `DW_EintragsNr` will be defined during the core script's migration.
    *   **Fail:** Any mismatch in the `Stichtag` or `Wiederanlaufwert` values prepared for the core script.

### 7. Data Quality / Row Count / Schema Assertions (Not Applicable to Orchestrator)

*   **Purpose:** Document that data-specific assertions are not applicable to this orchestrator DAG, as it does not directly interact with data sources or targets.
*   **Setup:** N/A
*   **Action:** N/A
*   **Pass/Fail Criterion:**
    *   **Pass:** This test case serves as a documentation point. It confirms that the `r_ausd_bp_ta_bpr_apn_dag.py` DAG's role is purely orchestration and parameter passing. All data quality, row count, and schema assertions will be covered in the dedicated migration tests for the *core processing script* (`k_ausd_bp_ta_bpr_apn.ksh`) and its migrated BigQuery/Python components.
    *   **Fail:** If the orchestrator DAG were found to perform direct data manipulation or assertions, contradicting its design as a pure orchestrator.