The migration of `r_ausd_v_ta_cntrct_crs3.ksh` primarily involves re-implementing its orchestration, parameter handling, logging, and error management in an Airflow DAG and Python utility modules. The core data processing logic (`k_ausd_v_ta_cntrct_crs3.ksh`) is still a placeholder. Therefore, these tests focus on validating the behavior of the *migrated wrapper and utility components*, ensuring they are functionally equivalent to the legacy KornShell script's control flow.

We will use `pytest` for unit and integration testing of the Python modules and Airflow DAG. For BigQuery schema assertions and Cloud Logging integration, we'll use Google Cloud client libraries.

---

## Test Case 1: Airflow DAG - Successful Execution Flow

*   **Purpose:** Verify the Airflow DAG successfully orchestrates the job, initializes logging, calls the core processing task (placeholder), and reports overall success, mirroring the happy path of the legacy wrapper script. This covers **Output parity** for job status and **External-system replacements** for logging.
*   **Setup:**
    *   An Airflow environment with the `r_ausd_v_ta_cntrct_crs3_orchestration` DAG deployed.
    *   Mock the `execute_core_data_processing` task to simulate a successful run.
    *   Mock the `dwmsg_*` logging functions to assert their calls.
*   **Action:** Trigger the `r_ausd_v_ta_cntrct_crs3_orchestration` DAG with default parameters (e.g., `job_kennung="TEST_SUCCESS_JOB"`).
*   **Pass/Fail Criterion:**
    *   The Airflow DAG run completes with a `success` status.
    *   The `initialize_job_entry` task completes successfully.
    *   The `execute_core_data_processing` task (mocked) completes successfully.
    *   The `finalize_job_status` task completes successfully.
    *   The mocked `dwmsg_ermittle_nr`, `dwmsg_erzeuge_eintrag`, and `dwmsg_setze_status_ok` functions are called exactly once with expected arguments.
    *   The mocked `dwmsg_melde_fehler` and `dwmsg_setze_status_abbruch` functions are *not* called.

```python
# tests/integration/test_dag_success.py
import pytest
from airflow.models.dagrun import DagRun
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from datetime import datetime
from unittest.mock import patch, MagicMock

# Assuming the DAG is available in the Airflow context
from dags.r_ausd_v_ta_cntrct_crs3_dag import dag as r_ausd_v_ta_cntrct_crs3_dag

@pytest.fixture
def mock_logging_utils():
    """Mocks logging utility functions to capture calls."""
    with patch('dags.r_ausd_v_ta_cntrct_crs3_dag.dwmsg_ermittle_nr', return_value="TEST_ENTRY_NR") as mock_ermittle_nr, \
         patch('dags.r_ausd_v_ta_cntrct_crs3_dag.dwmsg_erzeuge_eintrag') as mock_erzeuge_eintrag, \
         patch('dags.r_ausd_v_ta_cntrct_crs3_dag.dwmsg_setze_status_ok') as mock_setze_status_ok, \
         patch('dags.r_ausd_v_ta_cntrct_crs3_dag.dwmsg_melde_fehler') as mock_melde_fehler, \
         patch('dags.r_ausd_v_ta_cntrct_crs3_dag.dwmsg_setze_status_abbruch') as mock_setze_status_abbruch, \
         patch('dags.r_ausd_v_ta_cntrct_crs3_dag.dwmsg_logdateiname', return_value="/tmp/test.log") as mock_logdateiname:
        yield {
            "ermittle_nr": mock_ermittle_nr,
            "erzeuge_eintrag": mock_erzeuge_eintrag,
            "setze_status_ok": mock_setze_status_ok,
            "melde_fehler": mock_melde_fehler,
            "setze_status_abbruch": mock_setze_status_abbruch,
            "logdateiname": mock_logdateiname,
        }

@pytest.fixture
def mock_core_processing():
    """Mocks the core data processing task to always succeed."""
    with patch('dags.r_ausd_v_ta_cntrct_crs3_dag.execute_core_data_processing') as mock_exec:
        mock_exec.return_value = None # Simulate success
        yield mock_exec

def test_dag_successful_execution(mock_logging_utils, mock_core_processing):
    """
    Tests the happy path of the Airflow DAG, ensuring correct task execution and logging.
    """
    dag = r_ausd_v_ta_cntrct_crs3_dag
    dag.clear() # Clear previous runs for a clean test state

    # Create a DagRun
    dr = dag.create_dagrun(
        run_id=DagRunType.MANUAL.value + "_" + datetime.now().isoformat(),
        state=State.RUNNING,
        execution_date=datetime.now(),
        start_date=datetime.now(),
        conf={"job_kennung": "TEST_SUCCESS_JOB"}
    )

    # Run tasks sequentially
    ti_init = dr.get_task_instance(task_id="initialize_job_entry")
    ti_init.run(ignore_ti_state=True)
    assert ti_init.current_state() == State.SUCCESS

    ti_core = dr.get_task_instance(task_id="execute_core_data_processing")
    ti_core.run(ignore_ti_state=True)
    assert ti_core.current_state() == State.SUCCESS
    
    ti_finalize = dr.get_task_instance(task_id="finalize_job_status")
    ti_finalize.run(ignore_ti_state=True)
    assert ti_finalize.current_state() == State.SUCCESS

    # Assertions on logging calls
    mock_logging_utils["ermittle_nr"].assert_called_once()
    mock_logging_utils["erzeuge_eintrag"].assert_called_once_with(
        "TEST_ENTRY_NR", "TEST_SUCCESS_JOB", "r_ausd_v_ta_cntrct_crs3_dag.py", "/tmp/test.log"
    )
    mock_logging_utils["setze_status_ok"].assert_called_once_with("TEST_ENTRY_NR")
    mock_logging_utils["melde_fehler"].assert_not_called()
    mock_logging_utils["setze_status_abbruch"].assert_not_called()

    # Assert core processing was called
    mock_core_processing.assert_called_once()
```

---

## Test Case 2: Airflow DAG - Parameter Error Handling

*   **Purpose:** Verify the Airflow DAG correctly handles parameter validation errors, mirroring the legacy script's `getopts` error handling and `DWMSG_MeldeFehler` call, leading to job abortion. This covers **Output parity** for error status and **Transformation correctness** for parameter handling.
*   **Setup:**
    *   An Airflow environment with the DAG deployed.
    *   Mock the `dwmsg_*` logging functions to assert their calls.
*   **Action:** Trigger the DAG with an invalid parameter (e.g., `job_kennung=None`, which `pruefe_parameter_gesetzt` would catch).
*   **Pass/Fail Criterion:**
    *   The `initialize_job_entry` task fails.
    *   The DAG run completes with a `failed` status.
    *   The mocked `dwmsg_melde_fehler` is called with `FATAL` level, error code `194` (for missing parameter), and `arg_info="JobKennung"`.
    *   The mocked `dwmsg_setze_status_abbruch` is called by the `finalize_job_status` task (due to `trigger_rule="all_done"`).
    *   The mocked `dwmsg_setze_status_ok` is *not* called.
    *   The `execute_core_data_processing` task is skipped or not executed.

```python
# tests/integration/test_dag_param_error.py
import pytest
from airflow.models.dagrun import DagRun
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from datetime import datetime
from unittest.mock import patch, MagicMock

from dags.r_ausd_v_ta_cntrct_crs3_dag import dag as r_ausd_v_ta_cntrct_crs3_dag
from utils.logging_utils import FATAL
from utils.parameter_utils import ParameterError

@pytest.fixture
def mock_logging_utils_for_error():
    """Mocks logging utility functions for error scenarios."""
    with patch('dags.r_ausd_v_ta_cntrct_crs3_dag.dwmsg_ermittle_nr', return_value="TEST_ERROR_ENTRY_NR") as mock_ermittle_nr, \
         patch('dags.r_ausd_v_ta_cntrct_crs3_dag.dwmsg_erzeuge_eintrag') as mock_erzeuge_eintrag, \
         patch('dags.r_ausd_v_ta_cntrct_crs3_dag.dwmsg_setze_status_ok') as mock_setze_status_ok, \
         patch('dags.r_ausd_v_ta_cntrct_crs3_dag.dwmsg_melde_fehler') as mock_melde_fehler, \
         patch('dags.r_ausd_v_ta_cntrct_crs3_dag.dwmsg_setze_status_abbruch') as mock_setze_status_abbruch, \
         patch('dags.r_ausd_v_ta_cntrct_crs3_dag.dwmsg_logdateiname', return_value="/tmp/test_error.log") as mock_logdateiname:
        yield {
            "ermittle_nr": mock_ermittle_nr,
            "erzeuge_eintrag": mock_erzeuge_eintrag,
            "setze_status_ok": mock_setze_status_ok,
            "melde_fehler": mock_melde_fehler,
            "setze_status_abbruch": mock_setze_status_abbruch,
            "logdateiname": mock_logdateiname,
        }

def test_dag_parameter_error_handling(mock_logging_utils_for_error):
    """
    Tests that the DAG correctly handles a parameter error during initialization,
    leading to task failure and appropriate logging.
    """
    dag = r_ausd_v_ta_cntrct_crs3_dag
    dag.clear()

    # Simulate a missing job_kennung, which should trigger ParameterError
    dr = dag.create_dagrun(
        run_id=DagRunType.MANUAL.value + "_" + datetime.now().isoformat(),
        state=State.RUNNING,
        execution_date=datetime.now(),
        start_date=datetime.now(),
        conf={"job_kennung": None} # This should cause pruefe_parameter_gesetzt to fail
    )

    ti_init = dr.get_task_instance(task_id="initialize_job_entry")
    
    # Expect the task to fail by raising ParameterError
    with pytest.raises(ParameterError):
        ti_init.run(ignore_ti_state=True)
    
    assert ti_init.current_state() == State.FAILED

    # The core processing task should not run (default trigger_rule is all_success)
    ti_core = dr.get_task_instance(task_id="execute_core_data_processing")
    assert ti_core.current_state() == State.SKIPPED

    # Finalize task should run due to trigger_rule="all_done"
    ti_finalize = dr.get_task_instance(task_id="finalize_job_status")
    ti_finalize.run(ignore_ti_state=True)
    assert ti_finalize.current_state() == State.SUCCESS # Task itself succeeds, but logs ABORT

    # Assertions on logging calls
    # dwmsg_ermittle_nr might be called before the parameter error, depending on exact flow
    mock_logging_utils_for_error["melde_fehler"].assert_called_once_with(
        "UNKNOWN", FATAL, 194, "JobKennung", # Assuming 194 is the error code for missing param
    )
    mock_logging_utils_for_error["setze_status_abbruch"].assert_called_once_with("TEST_ERROR_ENTRY_NR")
    mock_logging_utils_for_error["setze_status_ok"].assert_not_called()
```

---

## Test Case 3: Airflow DAG - Core Script Failure Handling

*   **Purpose:** Verify the Airflow DAG correctly handles failures in the core data processing task, mirroring the legacy script's `trap ERR` behavior, leading to job abortion. This covers **Output parity** for error status and **External-system replacements** for error logging.
*   **Setup:**
    *   An Airflow environment with the DAG deployed.
    *   Mock `execute_core_data_processing` to raise an exception, simulating a failure in the core logic.
    *   Mock the `dwmsg_*` logging functions to assert their calls.
*   **Action:** Trigger the DAG.
*   **Pass/Fail Criterion:**
    *   The `initialize_job_entry` task completes successfully.
    *   The `execute_core_data_processing` task fails.
    *   The DAG run completes with a `failed` status.
    *   The mocked `dwmsg_melde_fehler` is called with `ERROR` level (or `FATAL` if the core script's error is considered fatal) and an appropriate error code (e.g., `500`).
    *   The `finalize_job_status` task runs (due to `trigger_rule="all_done"`) and calls `dwmsg_setze_status_abbruch`.
    *   The mocked `dwmsg_setze_status_ok` is *not* called.

```python
# tests/integration/test_dag_core_failure.py
import pytest
from airflow.models.dagrun import DagRun
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from datetime import datetime
from unittest.mock import patch, MagicMock

from dags.r_ausd_v_ta_cntrct_crs3_dag import dag as r_ausd_v_ta_cntrct_crs3_dag
from utils.logging_utils import ERROR

@pytest.fixture
def mock_logging_utils_for_core_error():
    """Mocks logging utility functions for core script error scenarios."""
    with patch('dags.r_ausd_v_ta_cntrct_crs3_dag.dwmsg_ermittle_nr', return_value="TEST_CORE_ERROR_ENTRY_NR") as mock_ermittle_nr, \
         patch('dags.r_ausd_v_ta_cntrct_crs3_dag.dwmsg_erzeuge_eintrag') as mock_erzeuge_eintrag, \
         patch('dags.r_ausd_v_ta_cntrct_crs3_dag.dwmsg_setze_status_ok') as mock_setze_status_ok, \
         patch('dags.r_ausd_v_ta_cntrct_crs3_dag.dwmsg_melde_fehler') as mock_melde_fehler, \
         patch('dags.r_ausd_v_ta_cntrct_crs3_dag.dwmsg_setze_status_abbruch') as mock_setze_status_abbruch, \
         patch('dags.r_ausd_v_ta_cntrct_crs3_dag.dwmsg_logdateiname', return_value="/tmp/test_core_error.log") as mock_logdateiname:
        yield {
            "ermittle_nr": mock_ermittle_nr,
            "erzeuge_eintrag": mock_erzeuge_eintrag,
            "setze_status_ok": mock_setze_status_ok,
            "melde_fehler": mock_melde_fehler,
            "setze_status_abbruch": mock_setze_status_abbruch,
            "logdateiname": mock_logdateiname,
        }

@pytest.fixture
def mock_core_processing_failure():
    """Mocks the core data processing task to raise an exception."""
    with patch('dags.r_ausd_v_ta_cntrct_crs3_dag.execute_core_data_processing') as mock_exec:
        mock_exec.side_effect = Exception("Simulated core script failure")
        yield mock_exec

def test_dag_core_script_failure_handling(mock_logging_utils_for_core_error, mock_core_processing_failure):
    """
    Tests that the DAG correctly handles a failure in the core data processing task,
    ensuring appropriate logging and job abortion.
    """
    dag = r_ausd_v_ta_cntrct_crs3_dag
    dag.clear()

    dr = dag.create_dagrun(
        run_id=DagRunType.MANUAL.value + "_" + datetime.now().isoformat(),
        state=State.RUNNING,
        execution_date=datetime.now(),
        start_date=datetime.now(),
        conf={"job_kennung": "TEST_CORE_FAIL_JOB"}
    )

    ti_init = dr.get_task_instance(task_id="initialize_job_entry")
    ti_init.run(ignore_ti_state=True)
    assert ti_init.current_state() == State.SUCCESS

    ti_core = dr.get_task_instance(task_id="execute_core_data_processing")
    with pytest.raises(Exception, match="Simulated core script failure"):
        ti_core.run(ignore_ti_state=True)
    assert ti_core.current_state() == State.FAILED
    
    ti_finalize = dr.get_task_instance(task_id="finalize_job_status")
    ti_finalize.run(ignore_ti_state=True)
    assert ti_finalize.current_state() == State.SUCCESS # Finalize task runs, but logs ABORT

    # Assertions on logging calls
    mock_logging_utils_for_core_error["ermittle_nr"].assert_called_once()
    mock_logging_utils_for_core_error["erzeuge_eintrag"].assert_called_once()
    mock_logging_utils_for_core_error["melde_fehler"].assert_called_once_with(
        "TEST_CORE_ERROR_ENTRY_NR", ERROR, 500, "Core script execution failed", "Simulated core script failure"
    )
    mock_logging_utils_for_core_error["setze_status_abbruch"].assert_called_once_with("TEST_CORE_ERROR_ENTRY_NR")
    mock_logging_utils_for_core_error["setze_status_ok"].assert_not_called()
```

---

## Test Case 4: Airflow DAG - Help Message (`-h`) Behavior

*   **Purpose:** Verify that the Airflow DAG can simulate the `-h` (help) parameter behavior of the legacy script. In the legacy script, this would print usage and exit successfully. In Airflow, this means logging the usage message and then short-circuiting the DAG to complete successfully without executing the core logic. This covers **Output parity** for the help message and **Transformation correctness** for parameter handling.
*   **Setup:**
    *   An Airflow environment with the DAG deployed.
    *   The DAG's `initialize_job_entry` task is modified to check for a `show_help` parameter. If `True`, it logs the usage message and sets an XCom to skip downstream tasks.
    *   The `execute_core_data_processing` task checks this XCom and returns early if `show_help` is true.
    *   The `finalize_job_status` task also checks this XCom and calls `dwmsg_setze_status_ok`.
    *   Mock the `dwmsg_*` logging functions and `execute_core_data_processing`.
*   **Action:** Trigger the DAG with `conf={"show_help": True}`.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG run completes with a `success` status.
    *   The `initialize_job_entry` task completes successfully.
    *   The `execute_core_data_processing` task is effectively skipped (it runs but returns early, so its state is `success`).
    *   The `finalize_job_status` task completes successfully.
    *   The Airflow logs contain the expected usage message.
    *   The mocked `execute_core_data_processing` is *not* called (or called but immediately returns if the mock is inside the task).
    *   The mocked `dwmsg_setze_status_ok` is called.
    *   The mocked `dwmsg_melde_fehler` and `dwmsg_setze_status_abbruch` are *not* called.

```python
# NOTE: This test requires modifications to the DAG code as described in the setup.
# dags/r_ausd_v_ta_cntrct_crs3_dag.py (modifications for this test)
# Add to DAG params:
# "show_help": False,

# Modify initialize_job_entry:
# ...
#         show_help = dag_run_conf.get("show_help", dag.params["show_help"])
#
#         if show_help:
#             usage_message = """
#     Programm: Vertragsdatenabgleich
#     Version:  V1.0.0
#     Aufruf:   r_ausd_v_ta_cntrct_crs3_dag.py Parameter
#     Parameter:
#         -h     zeigt diese Seite an
#
#     Beschreibung:
#         Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_cntrct_crs3.
# """
#             airflow_logger.info(usage_message)
#             ti.xcom_push(key="skip_core_processing", value=True)
#             ti.xcom_push(key="dw_eintrags_nr", value="HELP_MODE") # Dummy entry for finalize
#             ti.xcom_push(key="job_kennung", value="HELP_MODE")
#             ti.xcom_push(key="log_file_name", value="N/A")
#             return # Exit early
#
#         try:
#             # ... rest of original initialize_job_entry logic ...
#             ti.xcom_push(key="skip_core_processing", value=False)
#             # ...
#         except Exception as e:
#             # ... error handling ...

# Modify execute_core_data_processing:
# ...
#         skip_core_processing = ti.xcom_pull(key="skip_core_processing", task_ids="initialize_job_entry")
#         if skip_core_processing:
#             airflow_logger.info("Help mode activated. Skipping core data processing.")
#             return # Short-circuit this task
#
#         # ... rest of original execute_core_data_processing logic ...

# Modify finalize_job_status:
# ...
#         skip_core_processing = ti.xcom_pull(key="skip_core_processing", task_ids="initialize_job_entry")
#
#         if skip_core_processing:
#             airflow_logger.info("Help mode activated. Finalizing with success status.")
#             dwmsg_setze_status_ok(dw_eintrags_nr) # Mark as success for the help mode
#             return
#
#         core_processing_status = ti.xcom_pull(key="core_processing_status", task_ids="execute_core_data_processing")
#         # ... rest of original finalize_job_status logic ...

# tests/integration/test_dag_help_mode.py
import pytest
from airflow.models.dagrun import DagRun
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from datetime import datetime
from unittest.mock import patch, MagicMock
import logging

from dags.r_ausd_v_ta_cntrct_crs3_dag import dag as r_ausd_v_ta_cntrct_crs3_dag
from utils.logging_utils import INFO

@pytest.fixture
def mock_logging_utils_for_help():
    """Mocks logging utility functions for help mode scenarios."""
    with patch('dags.r_ausd_v_ta_cntrct_crs3_dag.dwmsg_ermittle_nr', return_value="HELP_MODE_ENTRY_NR") as mock_ermittle_nr, \
         patch('dags.r_ausd_v_ta_cntrct_crs3_dag.dwmsg_erzeuge_eintrag') as mock_erzeuge_eintrag, \
         patch('dags.r_ausd_v_ta_cntrct_crs3_dag.dwmsg_setze_status_ok') as mock_setze_status_ok, \
         patch('dags.r_ausd_v_ta_cntrct_crs3_dag.dwmsg_melde_fehler') as mock_melde_fehler, \
         patch('dags.r_ausd_v_ta_cntrct_crs3_dag.dwmsg_setze_status_abbruch') as mock_setze_status_abbruch, \
         patch('dags.r_ausd_v_ta_cntrct_crs3_dag.dwmsg_logdateiname', return_value="N/A") as mock_logdateiname:
        yield {
            "ermittle_nr": mock_ermittle_nr,
            "erzeuge_eintrag": mock_erzeuge_eintrag,
            "setze_status_ok": mock_setze_status_ok,
            "melde_fehler": mock_melde_fehler,
            "setze_status_abbruch": mock_setze_status_abbruch,
            "logdateiname": mock_logdateiname,
        }

@pytest.fixture
def mock_core_processing_for_help():
    """Mocks the core data processing task for help mode."""
    with patch('dags.r_ausd_v_ta_cntrct_crs3_dag.execute_core_data_processing') as mock_exec:
        yield mock_exec

def test_dag_help_mode_execution(mock_logging_utils_for_help, mock_core_processing_for_help, caplog):
    """
    Tests that the DAG correctly handles the 'show_help' parameter,
    logging usage and skipping core processing.
    """
    dag = r_ausd_v_ta_cntrct_crs3_dag
    dag.clear()

    with caplog.at_level(INFO): # Capture logs for assertion
        dr = dag.create_dagrun(
            run_id=DagRunType.MANUAL.value + "_" + datetime.now().isoformat(),
            state=State.RUNNING,
            execution_date=datetime.now(),
            start_date=datetime.now(),
            conf={"show_help": True, "job_kennung": "HELP_JOB"}
        )

        ti_init = dr.get_task_instance(task_id="initialize_job_entry")
        ti_init.run(ignore_ti_state=True)
        assert ti_init.current_state() == State.SUCCESS

        ti_core = dr.get_task_instance(task_id="execute_core_data_processing")
        ti_core.run(ignore_ti_state=True)
        assert ti_core.current_state() == State.SUCCESS # Task itself succeeds by short-circuiting

        ti_finalize = dr.get_task_instance(task_id="finalize_job_status")
        ti_finalize.run(ignore_ti_state=True)
        assert ti_finalize.current_state() == State.SUCCESS

    # Assertions on logged messages
    assert "Programm: Vertragsdatenabgleich" in caplog.text
    assert "Aufruf:   r_ausd_v_ta_cntrct_crs3_dag.py Parameter" in caplog.text
    assert "Help mode activated. Skipping core data processing." in caplog.text
    assert "Help mode activated. Finalizing with success status." in caplog.text

    # Assertions on mocked logging calls
    mock_logging_utils_for_help["ermittle_nr"].assert_not_called()
    mock_logging_utils_for_help["erzeuge_eintrag"].assert_not_called()
    mock_logging_utils_for_help["setze_status_ok"].assert_called_once_with("HELP_MODE")
    mock_logging_utils_for_help["melde_fehler"].assert_not_called()
    mock_logging_utils_for_help["setze_status_abbruch"].assert_not_called()

    # Assert core processing was effectively skipped
    mock_core_processing_for_help.assert_not_called()
```

---

## Test Case 5: Utility - `env_config.py` (Environment Variables)

*   **Purpose:** Verify `env_config.py` correctly loads default values and overrides them with environment variables, mimicking the behavior of sourcing `.dw_init`. This covers **Transformation correctness** for environment setup.
*   **Setup:** Unit test environment.
*   **Action:**
    1.  Instantiate `EnvConfig` and check its default attribute values.
    2.  Set specific environment variables (e.g., `BERT_DIR_ROOT`, `GCP_PROJECT_ID`).
    3.  Re-evaluate `EnvConfig` attributes (or re-import if necessary) and check if they reflect the new environment variable values.
*   **Pass/Fail Criterion:** All assertions in `tests/test_env_config.py` pass.

```python
# Provided in the generated code: tests/test_env_config.py
# This test is already well-defined and runnable.
```

---

## Test Case 6: Utility - `logging_utils.py` (Logging and Error Handling)

*   **Purpose:** Verify `logging_utils.py` functions correctly generate log messages, handle different error levels (INFO, WARNING, ERROR, FATAL), and raise `DWMSGError` when appropriate, mimicking `f_alis_msgerr.ksh`. This covers **Transformation correctness** for logging and error handling.
*   **Setup:** Unit test environment with mocked `datetime` for predictable log file names and a captured log stream.
*   **Action:**
    1.  Call `dwmsg_ermittle_nr` and assert a unique string is returned.
    2.  Call `dwmsg_erzeuge_eintrag`, `dwmsg_setze_status_ok`, `dwmsg_setze_status_abbruch`, `dwmsg_setze_stichtag_info`, `dwmsg_append_timing_infos` and assert that corresponding log messages are captured with the correct level and content.
    3.  Call `dwmsg_melde_fehler` with `WARNING`, `ERROR`, and `FATAL` levels. Assert that `WARNING` and `ERROR` logs are captured, and `FATAL` raises a `DWMSGError`.
    4.  Call `dwmsg_logdateiname` and assert the generated filename format.
*   **Pass/Fail Criterion:** All assertions in `tests/test_logging_utils.py` pass.

```python
# Provided in the generated code: tests/test_logging_utils.py
# This test is already well-defined and runnable.
```

---

## Test Case 7: Utility - `parameter_utils.py` (Parameter Validation and Conversion)

*   **Purpose:** Verify `parameter_utils.py` functions correctly validate parameters, convert descriptive codes to abbreviations, and check date ranges, mimicking `h_alis_parameter.ksh`. This covers **Transformation correctness** for parameter handling, type handling, and edge cases (invalid inputs).
*   **Setup:** Unit test environment.
*   **Action:**
    1.  Test `pruefe_parameter_gesetzt` with `None`, empty string, and valid string inputs.
    2.  Test `konvertiere_kennzahl`, `konvertiere_system`, `konvertiere_sd_name`, `konvertiere_aufbau_stufe_xtra` with valid and invalid (unknown) descriptive inputs.
    3.  Test `pruefe_system_kennzahl` with known valid and invalid combinations of system and kennzahl.
    4.  Test `gib_bereich` and `gib_intervall` with various kennzahlen to ensure correct categorization.
    5.  Test `pruefe_zeitraum` with valid date ranges, invalid formats, and start date after end date.
    6.  Test `pruefe_zahl_positiv` with positive, zero, negative, and non-numeric inputs.
    7.  Test `pruefe_zeit_parameter` with valid combinations (only offset, only dates) and invalid combinations (mix, missing all).
    8.  Test `konvertiere_zeitspanne` with different kennzahlen (e.g., 'zug' for days, 'bst' for months) and offsets, asserting the correct date range calculation (mocking `dwdate_gib_zeitraum`).
*   **Pass/Fail Criterion:** All assertions in `tests/test_parameter_utils.py` pass.

```python
# tests/test_parameter_utils.py
import pytest
import logging
from unittest.mock import patch
from utils.parameter_utils import (
    pruefe_parameter_gesetzt, konvertiere_kennzahl, konvertiere_system,
    pruefe_system_kennzahl, gib_bereich, gib_intervall, pruefe_zeitraum,
    pruefe_zahl_positiv, pruefe_zeit_parameter, konvertiere_zeitspanne,
    ParameterError
)
from utils.date_utils import DWDateError # For konvertiere_zeitspanne

logging.basicConfig(level=logging.DEBUG) # Set logging level for tests

class TestParameterUtils:

    # Test pruefe_parameter_gesetzt
    def test_pruefe_parameter_gesetzt_valid(self):
        pruefe_parameter_gesetzt("test_param", "some_value") # Should not raise

    def test_pruefe_parameter_gesetzt_none(self):
        with pytest.raises(ParameterError, match="Parameter 'test_param' is not set.") as excinfo:
            pruefe_parameter_gesetzt("test_param", None)
        assert excinfo.value.error_code == 194

    def test_pruefe_parameter_gesetzt_empty_string(self):
        with pytest.raises(ParameterError, match="Parameter 'test_param' is not set.") as excinfo:
            pruefe_parameter_gesetzt("test_param", "")
        assert excinfo.value.error_code == 194

    # Test konvertiere_kennzahl
    @pytest.mark.parametrize("input_val, expected_val", [
        ("Zugang", "zug"), ("BESTAND", "bst"), ("tarifwechsel", "twe")
    ])
    def test_konvertiere_kennzahl_valid(self, input_val, expected_val):
        assert konvertiere_kennzahl(input_val) == expected_val

    def test_konvertiere_kennzahl_invalid(self):
        with pytest.raises(ParameterError, match="Unknown Kennzahl description: 'invalid_kennzahl'") as excinfo:
            konvertiere_kennzahl("invalid_kennzahl")
        assert excinfo.value.error_code == 198

    # Test konvertiere_system
    @pytest.mark.parametrize("input_val, expected_val", [
        ("SAP", "sap"), ("carmen", "carmen"), ("DPPS", "dpps")
    ])
    def test_konvertiere_system_valid(self, input_val, expected_val):
        assert konvertiere_system(input_val) == expected_val

    def test_konvertiere_system_invalid(self):
        with pytest.raises(ParameterError, match="Unknown System description: 'invalid_system'") as excinfo:
            konvertiere_system("invalid_system")
        assert excinfo.value.error_code == 195

    # Test pruefe_system_kennzahl
    @pytest.mark.parametrize("system, kennzahl", [
        ("SAP", "srs"), ("Carmen", "bst"), ("NNV", "tvd"), ("DWH", "mds")
    ])
    def test_pruefe_system_kennzahl_valid(self, system, kennzahl):
        pruefe_system_kennzahl(system, kennzahl) # Should not raise

    @pytest.mark.parametrize("system, kennzahl", [
        ("SAP", "zug"), ("Carmen", "twe"), ("NNV", "bst"), ("DWH", "zug")
    ])
    def test_pruefe_system_kennzahl_invalid(self, system, kennzahl):
        with pytest.raises(ParameterError, match="Ungueltige Kombination") as excinfo:
            pruefe_system_kennzahl(system, kennzahl)
        assert excinfo.value.error_code == 195

    # Test gib_bereich
    @pytest.mark.parametrize("kennzahl, expected_bereich", [
        ("zug", "tn"), ("bst", "tn"), ("gut", "us"), ("tvd", "gd"), ("ksd", "sd"), ("mds", "md")
    ])
    def test_gib_bereich_valid(self, kennzahl, expected_bereich):
        assert gib_bereich(kennzahl) == expected_bereich

    def test_gib_bereich_invalid(self):
        with pytest.raises(ParameterError, match="Kennzahl 'unknown' unknown for Bereich determination.") as excinfo:
            gib_bereich("unknown")
        assert excinfo.value.error_code == 196

    # Test gib_intervall
    @pytest.mark.parametrize("kennzahl, expected_intervall", [
        ("zug", "t"), ("bst", "m"), ("gut", "t"), ("tvd", "m")
    ])
    def test_gib_intervall_valid(self, kennzahl, expected_intervall):
        assert gib_intervall(kennzahl) == expected_intervall

    def test_gib_intervall_invalid(self):
        with pytest.raises(ParameterError, match="Kennzahl 'unknown' unknown for Intervall determination.") as excinfo:
            gib_intervall("unknown")
        assert excinfo.value.error_code == 196

    # Test pruefe_zeitraum
    def test_pruefe_zeitraum_valid(self):
        pruefe_zeitraum("20230101", "20230131")
        pruefe_zeitraum("20230101", "20230101")

    def test_pruefe_zeitraum_invalid_format(self):
        with pytest.raises(ParameterError, match="Start date '2023-01-01' does not match format '%Y%m%d'.") as excinfo:
            pruefe_zeitraum("2023-01-01", "20230131")
        assert excinfo.value.error_code == 195

    def test_pruefe_zeitraum_start_after_end(self):
        with pytest.raises(ParameterError, match="Start date '20230131' is after end date '20230101'.") as excinfo:
            pruefe_zeitraum("20230131", "20230101")
        assert excinfo.value.error_code == 195

    # Test pruefe_zahl_positiv
    def test_pruefe_zahl_positiv_valid(self):
        pruefe_zahl_positiv("0", "offset")
        pruefe_zahl_positiv("10", "offset")

    def test_pruefe_zahl_positiv_negative(self):
        with pytest.raises(ParameterError, match="Parameter 'offset' must be greater than or equal to 0.") as excinfo:
            pruefe_zahl_positiv("-5", "offset")
        assert excinfo.value.error_code == 195

    def test_pruefe_zahl_positiv_non_numeric(self):
        with pytest.raises(ParameterError, match="Parameter 'offset' is not a numeric value.") as excinfo:
            pruefe_zahl_positiv("abc", "offset")
        assert excinfo.value.error_code == 195

    # Test pruefe_zeit_parameter
    def test_pruefe_zeit_parameter_valid_offset(self):
        pruefe_zeit_parameter(None, None, "10") # Should not raise

    def test_pruefe_zeit_parameter_valid_dates(self):
        pruefe_zeit_parameter("20230101", "20230131", None) # Should not raise

    def test_pruefe_zeit_parameter_mix_offset_and_dates(self):
        with pytest.raises(ParameterError, match="Only a time offset OR both start and end dates can be set, not a mix.") as excinfo:
            pruefe_zeit_parameter("20230101", None, "10")
        assert excinfo.value.error_code == 195

    def test_pruefe_zeit_parameter_missing_dates_and_offset(self):
        with pytest.raises(ParameterError, match="Date values or time offset are missing.") as excinfo:
            pruefe_zeit_parameter(None, None, None)
        assert excinfo.value.error_code == 195

    def test_pruefe_zeit_parameter_one_date_missing(self):
        with pytest.raises(ParameterError, match="Both start and end dates must be provided.") as excinfo:
            pruefe_zeit_parameter("20230101", None, None)
        assert excinfo.value.error_code == 195

    # Test konvertiere_zeitspanne (relies on date_utils.dwdate_gib_zeitraum)
    @patch('utils.parameter_utils.dwdate_gib_zeitraum')
    def test_konvertiere_zeitspanne_success(self, mock_dwdate_gib_zeitraum):
        mock_dwdate_gib_zeitraum.return_value = ("20230101", "20230131")
        start, end = konvertiere_zeitspanne("ANF", "END", 30, "zug")
        assert start == "20230101"
        assert end == "20230131"
        mock_dwdate_gib_zeitraum.assert_called_once_with(offset=-30, unit='D', result_format="%Y%m%d")

    @patch('utils.parameter_utils.dwdate_gib_zeitraum')
    def test_konvertiere_zeitspanne_bst_kennzahl(self, mock_dwdate_gib_zeitraum):
        mock_dwdate_gib_zeitraum.return_value = ("20230101", "20230131")
        start, end = konvertiere_zeitspanne("ANF", "END", 2, "bst")
        assert start == "20230101"
        assert end == "20230131"
        mock_dwdate_gib_zeitraum.assert_called_once_with(offset=-2, unit='M', result_format="%Y%m%d")

    @patch('utils.parameter_utils.dwdate_gib_zeitraum')
    def test_konvertiere_zeitspanne_dwdate_error(self, mock_dwdate_gib_zeitraum):
        mock_dwdate_gib_zeitraum.side_effect = DWDateError("Simulated date error")
        with pytest.raises(ParameterError, match="Error calculating time span: Simulated date error") as excinfo:
            konvertiere_zeitspanne("ANF", "END", 30, "zug")
        assert excinfo.value.error_code == 85
```

---

## Test Case 8: Utility - `date_utils.py` (Date Calculations)

*   **Purpose:** Verify `date_utils.py` functions correctly perform date calculations and checks, mimicking `h_alis_date.ksh`. This is crucial as the legacy script often used `sqlplus` for date functions, and these Python replacements must be behaviorally equivalent. This covers **Transformation correctness** for date handling and edge cases (leap years, month boundaries).
*   **Setup:** Unit test environment with mocked `datetime.now()` for predictable "today" values.
*   **Action:**
    1.  Test `dwdate_vormonat` for various "today" dates (e.g., beginning of month, beginning of year).
    2.  Test `dwdate_datum_check` with valid/invalid dates and formats.
    3.  Test `dwdate_datum_le` with various date comparisons (equal, less than, greater than, invalid format).
    4.  Test `dwdate_gib_zeitraum` for 'D' (Day), 'M' (Month), 'Y' (Year) units and positive/negative offsets, asserting the correct start and end dates.
    5.  Test `letzter_tag_des_monats` for various dates, including end of month, mid-month, and leap year February.
    6.  Test `tage_im_monat` for different months and years (including leap years).
    7.  Test `addiere_datum` with positive and negative days, crossing month/year boundaries.
*   **Pass/Fail Criterion:** All assertions in `tests/test_date_utils.py` pass.

```python
# tests/test_date_utils.py
import pytest
import logging
from datetime import datetime, timedelta
from unittest.mock import patch

from utils.date_utils import (
    dwdate_vormonat, dwdate_datum_check, dwdate_datum_le,
    dwdate_gib_zeitraum, letzter_tag_des_monats, tage_im_monat,
    addiere_datum, DWDateError
)

logging.basicConfig(level=logging.DEBUG) # Set logging level for tests

class TestDateUtils:

    @patch('utils.date_utils.datetime')
    def test_dwdate_vormonat(self, mock_datetime):
        mock_datetime.now.return_value = datetime(2023, 3, 15) # Today is March 15, 2023
        mock_datetime.strptime = datetime.strptime # Keep original strptime
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw) # Allow datetime() calls
        
        # Expected: First day of previous month (February 2023)
        assert dwdate_vormonat() == "20230201"
        
        mock_datetime.now.return_value = datetime(2023, 1, 10) # Today is Jan 10, 2023
        assert dwdate_vormonat() == "20221201"

    @pytest.mark.parametrize("date_str, date_format, expected", [
        ("20231026", "%Y%m%d", True),
        ("2023-10-26", "%Y-%m-%d", True),
        ("26.10.2023", "%d.%m.%Y", True),
        ("20231026", "%Y-%m-%d", False), # Mismatch
        ("invalid", "%Y%m%d", False),
    ])
    def test_dwdate_datum_check(self, date_str, date_format, expected):
        assert dwdate_datum_check(date_str, date_format) == expected

    @pytest.mark.parametrize("date1, date2, expected", [
        ("20230101", "20230101", True),
        ("20230101", "20230102", True),
        ("20230102", "20230101", False),
    ])
    def test_dwdate_datum_le_valid(self, date1, date2, expected):
        assert dwdate_datum_le(date1, date2) == expected

    def test_dwdate_datum_le_invalid_format(self):
        with pytest.raises(DWDateError, match="Invalid date format or value"):
            dwdate_datum_le("2023-01-01", "20230101")

    @patch('utils.date_utils.datetime')
    @pytest.mark.parametrize("offset, unit, expected_start_date, expected_end_date", [
        (5, 'D', datetime(2023, 3, 15), datetime(2023, 3, 20)), # Today 2023-03-15, +5 days
        (-5, 'D', datetime(2023, 3, 15), datetime(2023, 3, 10)), # Today 2023-03-15, -5 days (end_date is 10th, start is today)
        (1, 'M', datetime(2023, 3, 1), datetime(2023, 4, 30)), # Today 2023-03-15, +1 month (start of current month to end of next month)
        (-1, 'M', datetime(2023, 2, 1), datetime(2023, 3, 31)), # Today 2023-03-15, -1 month (start of prev month to end of current month)
        (0, 'M', datetime(2023, 3, 1), datetime(2023, 3, 31)), # Today 2023-03-15, 0 months (start of current month to end of current month)
        (1, 'Y', datetime(2023, 1, 1), datetime(2024, 12, 31)), # Today 2023-03-15, +1 year (start of current year to end of next year)
        (-1, 'Y', datetime(2022, 1, 1), datetime(2023, 12, 31)), # Today 2023-03-15, -1 year (start of prev year to end of current year)
    ])
    def test_dwdate_gib_zeitraum(self, mock_datetime, offset, unit, expected_start_date, expected_end_date):
        mock_datetime.now.return_value = datetime(2023, 3, 15)
        mock_datetime.strptime = datetime.strptime
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
        
        start, end = dwdate_gib_zeitraum(offset, unit)
        
        assert start == expected_start_date.strftime("%Y%m%d")
        assert end == expected_end_date.strftime("%Y%m%d")

    def test_dwdate_gib_zeitraum_unsupported_unit(self):
        with pytest.raises(DWDateError, match="Unsupported unit"):
            dwdate_gib_zeitraum(1, 'X')

    @pytest.mark.parametrize("date_str, expected", [
        ("20230131", True),
        ("20230228", True), # Non-leap year
        ("20240229", True), # Leap year
        ("20230315", False),
    ])
    def test_letzter_tag_des_monats(self, date_str, expected):
        assert letzter_tag_des_monats(date_str) == expected

    @pytest.mark.parametrize("year, month, expected", [
        (2023, 1, 31),
        (2023, 2, 28),
        (2024, 2, 29), # Leap year
        (2023, 4, 30),
    ])
    def test_tage_im_monat(self, year, month, expected):
        assert tage_im_monat(year, month) == expected

    def test_tage_im_monat_invalid_month(self):
        with pytest.raises(DWDateError, match="Invalid month"):
            tage_im_monat(2023, 13)

    @pytest.mark.parametrize("date_str, days_to_add, expected", [
        ("20230101", 5, "20230106"),
        ("20230131", 1, "20230201"),
        ("20240229", 1, "20240301"),
        ("20230105", -3, "20230102"),
    ])
    def test_addiere_datum(self, date_str, days_to_add, expected):
        assert addiere_datum(date_str, days_to_add) == expected
```

---

## Test Case 9: BigQuery Target Table Schema Assertion

*   **Purpose:** Assert that the target BigQuery table `ta_cntrct_crs3` exists and its schema matches the expected structure derived from the legacy system. This covers **Data-quality / row-count / schema assertions** for the target table.
*   **Setup:**
    *   A Google Cloud Project and BigQuery dataset are configured.
    *   The DDL for `ta_cntrct_crs3` (`ddl/ta_cntrct_crs3.sql`) has been executed in the target BigQuery environment.
    *   The `EnvConfig` module is correctly configured with `GCP_PROJECT_ID` and `BQ_DATASET`.
    *   The `EXPECTED_SCHEMA` in the test code is accurately defined based on the legacy `ta_cntrct_crs3` table.
*   **Action:** Use the `google-cloud-bigquery` client library to connect to BigQuery and retrieve the schema of the `ta_cntrct_crs3` table.
*   **Pass/Fail Criterion:**
    *   The table `<your-gcp-project-id>.<your-bigquery-dataset>.ta_cntrct_crs3` exists.
    *   The table's column names, data types, and nullability modes match the `EXPECTED_SCHEMA` definition.

```python
# tests/integration/test_bigquery_schema.py
import pytest
from google.cloud import bigquery
from utils.env_config import EnvConfig # To get project_id and dataset

# Define the expected schema based on legacy system (this needs to be filled in accurately)
# This is a placeholder; replace with the actual schema from the legacy ta_cntrct_crs3 table.
EXPECTED_SCHEMA = [
    bigquery.SchemaField("contract_id", "STRING", mode="REQUIRED", description="Unique identifier for the contract"),
    bigquery.SchemaField("customer_id", "STRING", mode="NULLABLE", description="Associated customer ID"),
    bigquery.SchemaField("start_date", "DATE", mode="NULLABLE", description="Contract start date"),
    bigquery.SchemaField("end_date", "DATE", mode="NULLABLE", description="Contract end date"),
    bigquery.SchemaField("contract_amount", "NUMERIC", mode="NULLABLE", description="Monetary value of the contract"),
    bigquery.SchemaField("currency", "STRING", mode="NULLABLE", description="Currency of the contract amount"),
    bigquery.SchemaField("status", "STRING", mode="NULLABLE", description="Current status of the contract (e.g., Active, Expired)"),
    bigquery.SchemaField("last_updated_ts", "TIMESTAMP", mode="NULLABLE", description="Timestamp of the last update"),
    # Add all other expected fields here, ensuring names, types, and modes match.
]

@pytest.fixture(scope="module")
def bq_client():
    """Provides a BigQuery client for the test module."""
    # Ensure GCP_PROJECT_ID is set in your test environment or EnvConfig
    if not EnvConfig.GCP_PROJECT_ID or EnvConfig.GCP_PROJECT_ID == "<your-gcp-project-id>":
        pytest.skip("GCP_PROJECT_ID not configured in EnvConfig for BigQuery tests.")
    return bigquery.Client(project=EnvConfig.GCP_PROJECT_ID)

def test_ta_cntrct_crs3_table_exists(bq_client):
    """Verify that the ta_cntrct_crs3 table exists in BigQuery."""
    table_id = f"{EnvConfig.GCP_PROJECT_ID}.{EnvConfig.BQ_DATASET}.ta_cntrct_crs3"
    try:
        bq_client.get_table(table_id)
    except Exception as e:
        pytest.fail(f"Table {table_id} does not exist or is inaccessible: {e}")

def test_ta_cntrct_crs3_schema_matches_expected(bq_client):
    """Verify that the schema of ta_cntrct_crs3 matches the expected schema."""
    table_id = f"{EnvConfig.GCP_PROJECT_ID}.{EnvConfig.BQ_DATASET}.ta_cntrct_crs3"
    table = bq_client.get_table(table_id)
    
    actual_schema_fields = sorted(table.schema, key=lambda f: f.name)
    expected_schema_fields = sorted(EXPECTED_SCHEMA, key=lambda f: f.name)

    assert len(actual_schema_fields) == len(expected_schema_fields), \
        f"Schema field count mismatch. Expected {len(expected_schema_fields)}, got {len(actual_schema_fields)}. " \
        f"Actual fields: {[f.name for f in actual_schema_fields]}, Expected fields: {[f.name for f in expected_schema_fields]}"

    for i, (actual_field, expected_field) in enumerate(zip(actual_schema_fields, expected_schema_fields)):
        assert actual_field.name == expected_field.name, \
            f"Field name mismatch at index {i}. Expected '{expected_field.name}', got '{actual_field.name}'"
        assert actual_field.field_type == expected_field.field_type, \
            f"Field type mismatch for '{actual_field.name}'. Expected '{expected_field.field_type}', got '{actual_field.field_type}'"
        assert actual_field.mode == expected_field.mode, \
            f"Field mode mismatch for '{actual_field.name}'. Expected '{expected_field.mode}', got '{actual_field.mode}'"
        # Optionally, assert descriptions or other properties if they are part of the migration contract
        # assert actual_field.description == expected_field.description, \
        #     f"Field description mismatch for '{actual_field.name}'"
```

---

## Test Case 10: External System - Cloud Logging Integration

*   **Purpose:** Verify that log messages generated by the Airflow DAG and utility functions are correctly ingested into Google Cloud Logging, replacing the legacy file-based logging. This covers **External-system replacements** for logging.
*   **Setup:**
    *   An Airflow DAG run (e.g., the successful execution from Test Case 1) has recently completed in a Cloud Composer environment.
    *   The Cloud Composer environment is configured to send logs to Google Cloud Logging.
    *   The `EnvConfig` module is configured with `GCP_PROJECT_ID` and the Airflow environment name (`AIRFLOW_ENV_NAME`).
*   **Action:** Use the `google-cloud-logging` client library to query Cloud Logging for specific log entries generated by the DAG within a recent time window.
*   **Pass/Fail Criterion:**
    *   Cloud Logging contains entries with `resource.type="cloud_composer_environment"` (or `airflow_task` depending on specific setup) and `resource.labels.dag_id="r_ausd_v_ta_cntrct_crs3_orchestration"`.
    *   Specific log messages (e.g., "Initialized job entry", "Job completed successfully") are found with the correct severity levels (INFO, ERROR, CRITICAL).
    *   Structured log data (e.g., `jsonPayload.job_id`, `jsonPayload.error_code`) is present and correct if the logging setup uses structured logging.

```python
# tests/integration/test_cloud_logging.py
import pytest
from google.cloud import logging_v2
from datetime import datetime, timedelta
import time
from utils.env_config import EnvConfig

@pytest.fixture(scope="module")
def logging_client():
    """Provides a Cloud Logging client for the test module."""
    # Ensure GCP_PROJECT_ID is set in your test environment or EnvConfig
    if not EnvConfig.GCP_PROJECT_ID or EnvConfig.GCP_PROJECT_ID == "<your-gcp-project-id>":
        pytest.skip("GCP_PROJECT_ID not configured in EnvConfig for Cloud Logging tests.")
    return logging_v2.LoggingServiceV2Client()

def get_log_entries(client, project_id, dag_id, task_id, min_timestamp, max_timestamp, text_filter=None, severity=None):
    """Helper to query Cloud Logging for specific entries."""
    # Adjust resource type and labels based on your Cloud Composer/Airflow logging setup
    filter_str = (
        f'resource.type="cloud_composer_environment" ' # Common for Cloud Composer
        f'resource.labels.environment_name="{EnvConfig.get_config("AIRFLOW_ENV_NAME", "your-airflow-env")}" '
        f'resource.labels.dag_id="{dag_id}" '
        f'resource.labels.task_id="{task_id}" '
        f'timestamp>="{min_timestamp.isoformat("T")}Z" '
        f'timestamp<="{max_timestamp.isoformat("T")}Z" '
    )
    if text_filter:
        filter_str += f'textPayload:"{text_filter}" ' # Or jsonPayload.message if structured
    if severity:
        filter_str += f'severity="{severity}" '

    entries = []
    for entry in client.list_log_entries(
        resource_names=[f"projects/{project_id}"],
        filter=filter_str.strip(),
        order_by="timestamp asc",
    ):
        entries.append(entry)
    return entries

def test_cloud_logging_successful_dag_run(logging_client):
    """
    Verify that logs from a successful DAG run are correctly sent to Cloud Logging.
    Assumes a successful DAG run (e.g., from Test Case 1) has just completed.
    """
    project_id = EnvConfig.GCP_PROJECT_ID
    dag_id = "r_ausd_v_ta_cntrct_crs3_orchestration"
    
    # Define a time window for logs (e.g., last 15 minutes)
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(minutes=15) # Adjust as needed for log propagation

    # Wait a bit for logs to propagate to Cloud Logging
    time.sleep(30) 

    # Check for initialization logs
    init_logs = get_log_entries(logging_client, project_id, dag_id, "initialize_job_entry", start_time, end_time, "Initialized job entry", severity="INFO")
    assert len(init_logs) >= 1, "Expected at least one 'Initialized job entry' log in Cloud Logging."
    
    # Check for success logs
    success_logs = get_log_entries(logging_client, project_id, dag_id, "finalize_job_status", start_time, end_time, "Job completed successfully", severity="INFO")
    assert len(success_logs) >= 1, "Expected at least one 'Job completed successfully' log in Cloud Logging."

    # Example: Check for structured log data (if your logging setup uses it)
    # If using default Airflow logging to Cloud Logging, it often appears in textPayload.
    # If using a custom Cloud Logging handler, it might be in jsonPayload.
    for log_entry in success_logs:
        if log_entry.json_payload:
            assert log_entry.json_payload.get("job_entry_number") is not None
            assert log_entry.json_payload.get("error_type") == "I" # INFO
        else:
            assert "Job completed successfully" in log_entry.text_payload
            assert "EntryNr:" in log_entry.text_payload # Check for key info in text

def test_cloud_logging_failed_dag_run(logging_client):
    """
    Verify that logs from a failed DAG run are correctly sent to Cloud Logging.
    Assumes a failed DAG run (e.g., from Test Case 2 or 3) has just completed.
    """
    project_id = EnvConfig.GCP_PROJECT_ID
    dag_id = "r_ausd_v_ta_cntrct_crs3_orchestration"
    
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(minutes=15)
    time.sleep(30) # Wait for logs

    # Check for error logs from initialize_job_entry (e.g., parameter error)
    error_init_logs = get_log_entries(logging_client, project_id, dag_id, "initialize_job_entry", start_time, end_time, "Reporting error", severity="CRITICAL")
    # Check for error logs from execute_core_data_processing (e.g., core script failure)
    error_core_logs = get_log_entries(logging_client, project_id, dag_id, "execute_core_data_processing", start_time, end_time, "Core data processing failed", severity="ERROR")
    
    assert len(error_init_logs) >= 1 or len(error_core_logs) >= 1, "Expected at least one error log from failed DAG run."

    # Check for job aborted status
    aborted_logs = get_log_entries(logging_client, project_id, dag_id, "finalize_job_status", start_time, end_time, "Job aborted", severity="ERROR")
    assert len(aborted_logs) >= 1, "Expected at least one 'Job aborted' log in Cloud Logging."
```

---

### Limitations and Future Work

The current migration design explicitly states that the core data processing logic of `k_ausd_v_ta_cntrct_crs3.ksh` is **unresolved** and will be migrated in a separate phase. Therefore, the following critical validation aspects cannot be covered by the current tests:

1.  **Output Parity (Data):** Once `k_ausd_v_ta_cntrct_crs3.ksh` is migrated to BigQuery SQL, Python, or Dataform, comprehensive data comparison tests will be required. This involves:
    *   Running both the legacy and migrated core logic with identical input data.
    *   Comparing the resulting `ta_cntrct_crs3` table (or intermediate outputs) for exact match in row counts, column values, and potentially checksums.
    *   This would typically involve a "diff" tool or SQL queries to identify discrepancies.

2.  **Transformation Correctness (Data):** Detailed tests for the actual data transformations performed by `k_ausd_v_ta_cntrct_crs3.ksh` will be needed. This includes:
    *   Specific test cases for joins, aggregations, filters, type conversions, and NULL handling within the core logic.
    *   Edge cases identified during the analysis of `k_ausd_v_ta_cntrct_crs3.ksh` (e.g., empty source data, malformed records, specific date ranges).

3.  **Data Quality / Row Count / Schema Assertions (Beyond Existence):** While Test Case 9 verifies the target table's schema existence, actual data quality and row count assertions can only be performed once the core data processing logic is in place and populating the table. This would involve:
    *   Asserting expected row counts after a full run.
    *   Validating data completeness, uniqueness, and referential integrity.
    *   Checking for data freshness and timeliness.

These tests provide a robust validation of the migrated wrapper script's behavioral equivalence, setting the stage for the subsequent, more complex data-centric validation once the core processing logic is migrated.