The migration of `r_ausd_bp_ta_bpr_bcp.ksh` to an Airflow DAG primarily involves re-implementing its orchestration, parameter handling, logging, and error management logic in Python. The core data processing logic (within `k_ausd_bp_ta_bpr_bcp.ksh`) is treated as an external dependency that will be migrated separately.

The following test cases are designed to validate the behavioral equivalence of the migrated Airflow DAG and its utility functions against the legacy KornShell script.

---

## Test Case 1: Default Parameter Handling - Stichtag and Wiederanlaufwert

*   **Purpose**: Verify that the Airflow DAG correctly applies default values for `stichtag` (current system date in `DDMMYYYY` format) and `wiederanlaufwert` (0) when these parameters are not explicitly provided in the DAG run configuration. This covers **Transformation correctness** (defaulting logic) and **Output parity** (parameters passed to the core logic).
*   **Setup**:
    1.  Ensure Airflow Variables `BERT_DIR_ROOT` and `LOG_BASE_DIR` are set in your Airflow environment (e.g., `BERT_DIR_ROOT=/tmp/bert_test`, `LOG_BASE_DIR=/tmp/airflow_logs_test`).
    2.  The `r_ausd_bp_ta_bpr_bcp_dag` DAG is deployed to Airflow.
*   **Action**:
    1.  Trigger the `r_ausd_bp_ta_bpr_bcp_dag` DAG via the Airflow UI or CLI.
    2.  Leave the `stichtag` and `wiederanlaufwert` parameters empty or set them to `None` in the DAG run configuration.
*   **Pass/Fail Criterion**:
    *   The DAG run completes successfully.
    *   Inspect the XComs of the `initialize_job_context` task:
        *   `stichtag` XCom value matches the current system date in `DDMMYYYY` format (e.g., `27102023`).
        *   `wiederanlaufwert` XCom value is `0`.
    *   Inspect the logs of the `invoke_core_logic` task:
        *   The log output should contain lines indicating the parameters passed to the simulated core logic, specifically:
            *   `-s <current_date_DDMMYYYY>`
            *   `-l 0`
    *   The `log_file` XCom value should reflect the `LOG_BASE_DIR` and include the current date in its filename (e.g., `/tmp/airflow_logs_test/R_AUSD_BP_TA_BPR_BCP_YYYYMMDD_<eintrags_nr>.log`).

```python
# Conceptual Pytest for Airflow DAG (requires Airflow test harness setup)
import pytest
from airflow.models.dag import DAG
from airflow.utils.state import State
from airflow.utils.session import provide_session
from airflow.models import DagRun, TaskInstance, Variable
from datetime import datetime
import pendulum
import os
import re

# Assuming the DAG file is in the dags folder and airflow_utils.py is accessible
from r_ausd_bp_ta_bpr_bcp_dag import dag as r_ausd_bp_ta_bpr_bcp_dag

@pytest.fixture(scope="module", autouse=True)
def setup_airflow_variables_default():
    Variable.set("BERT_DIR_ROOT", "/tmp/bert_test_default")
    Variable.set("LOG_BASE_DIR", "/tmp/airflow_logs_test_default")
    yield
    Variable.delete("BERT_DIR_ROOT")
    Variable.delete("LOG_BASE_DIR")
    # Clean up created log files
    log_dir = "/tmp/airflow_logs_test_default"
    if os.path.exists(log_dir):
        for f in os.listdir(log_dir):
            os.remove(os.path.join(log_dir, f))
        os.rmdir(log_dir)

@provide_session
def test_default_parameter_handling(session):
    dag = r_ausd_bp_ta_bpr_bcp_dag
    dag_id = dag.dag_id
    execution_date = pendulum.datetime(2023, 10, 27, tz="UTC")

    # Clear previous runs if any
    session.query(DagRun).filter(DagRun.dag_id == dag_id).delete()
    session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).delete()
    session.commit()

    # Trigger DAG without explicit parameters
    dag_run = dag.create_dagrun(
        state=State.RUNNING,
        execution_date=execution_date,
        start_date=execution_date,
        data_interval_start=execution_date,
        data_interval_end=execution_date,
        session=session,
        conf={} # No params provided
    )

    # Run tasks
    ti_init = TaskInstance(task=dag.get_task("initialize_job_context"), run_id=dag_run.run_id)
    ti_init.run(session=session)
    assert ti_init.current_state() == State.SUCCESS

    ti_validate = TaskInstance(task=dag.get_task("validate_parameters"), run_id=dag_run.run_id)
    ti_validate.run(session=session)
    assert ti_validate.current_state() == State.SUCCESS

    ti_invoke = TaskInstance(task=dag.get_task("invoke_core_logic"), run_id=dag_run.run_id)
    ti_invoke.run(session=session)
    assert ti_invoke.current_state() == State.SUCCESS

    ti_success = TaskInstance(task=dag.get_task("log_success"), run_id=dag_run.run_id)
    ti_success.run(session=session)
    assert ti_success.current_state() == State.SUCCESS

    # Assertions for default values
    stichtag_xcom = ti_init.xcom_pull(key="stichtag")
    wiederanlaufwert_xcom = ti_init.xcom_pull(key="wiederanlaufwert")
    log_file_xcom = ti_init.xcom_pull(key="log_file")

    expected_stichtag = datetime.now().strftime('%d%m%Y')
    assert stichtag_xcom == expected_stichtag
    assert wiederanlaufwert_xcom == 0

    # Verify log file content (simulated in BashOperator)
    rendered_command = ti_invoke.render_templates()
    assert f"-s {expected_stichtag}" in rendered_command
    assert "-l 0" in rendered_command
    
    # Verify log file name structure
    expected_log_file_pattern = re.compile(rf"\/tmp\/airflow_logs_test_default\/R_AUSD_BP_TA_BPR_BCP_{datetime.now().strftime('%Y%m%d')}_\d{{17}}\.log")
    assert expected_log_file_pattern.match(log_file_xcom)

    # Check if the DAG run itself is successful
    dag_run.update_state(session=session)
    assert dag_run.state == State.SUCCESS
```

---

## Test Case 2: Explicit Parameter Handling

*   **Purpose**: Verify that the Airflow DAG correctly processes explicitly provided `stichtag` and `wiederanlaufwert` parameters, overriding any defaults. This covers **Transformation correctness** and **Output parity**.
*   **Setup**:
    1.  Ensure Airflow Variables `BERT_DIR_ROOT` and `LOG_BASE_DIR` are set.
    2.  The `r_ausd_bp_ta_bpr_bcp_dag` DAG is deployed to Airflow.
*   **Action**:
    1.  Trigger the `r_ausd_bp_ta_bpr_bcp_dag` DAG.
    2.  Provide specific values in the DAG run configuration: `stichtag="15032023"`, `wiederanlaufwert=12345`.
*   **Pass/Fail Criterion**:
    *   The DAG run completes successfully.
    *   Inspect the XComs of the `initialize_job_context` task:
        *   `stichtag` XCom value is `"15032023"`.
        *   `wiederanlaufwert` XCom value is `12345`.
    *   Inspect the logs of the `invoke_core_logic` task:
        *   The log output should contain lines indicating the parameters passed to the simulated core logic, specifically:
            *   `-s 15032023`
            *   `-l 12345`
    *   The `log_file` XCom value should reflect the `LOG_BASE_DIR` and include `20230315` (derived from `stichtag`) in its filename.

```python
# Conceptual Pytest for Airflow DAG (requires Airflow test harness setup)
import pytest
from airflow.models.dag import DAG
from airflow.utils.state import State
from airflow.utils.session import provide_session
from airflow.models import DagRun, TaskInstance, Variable
import pendulum
import os
import re

from r_ausd_bp_ta_bpr_bcp_dag import dag as r_ausd_bp_ta_bpr_bcp_dag

@pytest.fixture(scope="module", autouse=True)
def setup_airflow_variables_explicit():
    Variable.set("BERT_DIR_ROOT", "/tmp/bert_test_explicit")
    Variable.set("LOG_BASE_DIR", "/tmp/airflow_logs_test_explicit")
    yield
    Variable.delete("BERT_DIR_ROOT")
    Variable.delete("LOG_BASE_DIR")
    log_dir = "/tmp/airflow_logs_test_explicit"
    if os.path.exists(log_dir):
        for f in os.listdir(log_dir):
            os.remove(os.path.join(log_dir, f))
        os.rmdir(log_dir)

@provide_session
def test_explicit_parameter_handling(session):
    dag = r_ausd_bp_ta_bpr_bcp_dag
    dag_id = dag.dag_id
    execution_date = pendulum.datetime(2023, 10, 27, tz="UTC")

    session.query(DagRun).filter(DagRun.dag_id == dag_id).delete()
    session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).delete()
    session.commit()

    # Trigger DAG with explicit parameters
    explicit_stichtag = "15032023"
    explicit_wiederanlaufwert = 12345
    dag_run = dag.create_dagrun(
        state=State.RUNNING,
        execution_date=execution_date,
        start_date=execution_date,
        data_interval_start=execution_date,
        data_interval_end=execution_date,
        session=session,
        conf={"stichtag": explicit_stichtag, "wiederanlaufwert": explicit_wiederanlaufwert}
    )

    # Run tasks
    ti_init = TaskInstance(task=dag.get_task("initialize_job_context"), run_id=dag_run.run_id)
    ti_init.run(session=session)
    assert ti_init.current_state() == State.SUCCESS

    ti_validate = TaskInstance(task=dag.get_task("validate_parameters"), run_id=dag_run.run_id)
    ti_validate.run(session=session)
    assert ti_validate.current_state() == State.SUCCESS

    ti_invoke = TaskInstance(task=dag.get_task("invoke_core_logic"), run_id=dag_run.run_id)
    ti_invoke.run(session=session)
    assert ti_invoke.current_state() == State.SUCCESS

    ti_success = TaskInstance(task=dag.get_task("log_success"), run_id=dag_run.run_id)
    ti_success.run(session=session)
    assert ti_success.current_state() == State.SUCCESS

    # Assertions for explicit values
    stichtag_xcom = ti_init.xcom_pull(key="stichtag")
    wiederanlaufwert_xcom = ti_init.xcom_pull(key="wiederanlaufwert")
    log_file_xcom = ti_init.xcom_pull(key="log_file")

    assert stichtag_xcom == explicit_stichtag
    assert wiederanlaufwert_xcom == explicit_wiederanlaufwert

    # Verify log file content (simulated in BashOperator)
    rendered_command = ti_invoke.render_templates()
    assert f"-s {explicit_stichtag}" in rendered_command
    assert f"-l {explicit_wiederanlaufwert}" in rendered_command
    
    # Verify log file name structure
    expected_log_file_pattern = re.compile(rf"\/tmp\/airflow_logs_test_explicit\/R_AUSD_BP_TA_BPR_BCP_20230315_\d{{17}}\.log")
    assert expected_log_file_pattern.match(log_file_xcom)

    dag_run.update_state(session=session)
    assert dag_run.state == State.SUCCESS
```

---

## Test Case 3: Parameter Validation Failure (Missing Stichtag)

*   **Purpose**: Verify that the DAG correctly handles scenarios where a required parameter (`stichtag`) is missing or empty, leading to a controlled failure, similar to the legacy script's `pruefeParameterGesetzt` and `DWMSG_MeldeFehler` logic. This covers **Transformation correctness** (validation logic) and **Output parity** (error handling).
*   **Setup**:
    1.  Ensure Airflow Variables `BERT_DIR_ROOT` and `LOG_BASE_DIR` are set.
    2.  The `r_ausd_bp_ta_bpr_bcp_dag` DAG is deployed to Airflow.
*   **Action**:
    1.  Trigger the `r_ausd_bp_ta_bpr_bcp_dag` DAG.
    2.  Provide `stichtag=""` or `stichtag=None` in the DAG run configuration.
*   **Pass/Fail Criterion**:
    *   The `validate_parameters` task fails.
    *   The overall DAG run enters a `failed` state.
    *   Inspect the logs of the `validate_parameters` task:
        *   It should contain an error message similar to "Parameter validation failed: ERROR: The following required parameters are missing or empty: stichtag".
    *   The `dag_failure_callback` should be invoked (indicated by logs from `DWMSG_Fehlerbehandlung` in the DAG run logs or the failed task's logs).

```python
# Conceptual Pytest for Airflow DAG (requires Airflow test harness setup)
import pytest
from airflow.models.dag import DAG
from airflow.utils.state import State
from airflow.utils.session import provide_session
from airflow.models import DagRun, TaskInstance, Variable
import pendulum
import logging
from unittest.mock import patch

from r_ausd_bp_ta_bpr_bcp_dag import dag as r_ausd_bp_ta_bpr_bcp_dag
from airflow_utils import DWMSG_Fehlerbehandlung # Import the actual function

@pytest.fixture(scope="module", autouse=True)
def setup_airflow_variables_validation():
    Variable.set("BERT_DIR_ROOT", "/tmp/bert_test_validation")
    Variable.set("LOG_BASE_DIR", "/tmp/airflow_logs_test_validation")
    yield
    Variable.delete("BERT_DIR_ROOT")
    Variable.delete("LOG_BASE_DIR")

@provide_session
def test_parameter_validation_failure(session, caplog):
    dag = r_ausd_bp_ta_bpr_bcp_dag
    dag_id = dag.dag_id
    execution_date = pendulum.datetime(2023, 10, 27, tz="UTC")

    session.query(DagRun).filter(DagRun.dag_id == dag_id).delete()
    session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).delete()
    session.commit()

    # Mock DWMSG_Fehlerbehandlung to check if it's called
    with patch('airflow_utils.DWMSG_Fehlerbehandlung') as mock_fehlerbehandlung:
        with caplog.at_level(logging.INFO): # Capture INFO and ERROR logs
            # Trigger DAG with missing stichtag
            dag_run = dag.create_dagrun(
                state=State.RUNNING,
                execution_date=execution_date,
                start_date=execution_date,
                data_interval_start=execution_date,
                data_interval_end=execution_date,
                session=session,
                conf={"stichtag": None} # Explicitly set to None to trigger validation failure
            )

            # Run initialize_job_context (should succeed)
            ti_init = TaskInstance(task=dag.get_task("initialize_job_context"), run_id=dag_run.run_id)
            ti_init.run(session=session)
            assert ti_init.current_state() == State.SUCCESS

            # Run validate_parameters (should fail)
            ti_validate = TaskInstance(task=dag.get_task("validate_parameters"), run_id=dag_run.run_id)
            with pytest.raises(ValueError, match="The following required parameters are missing or empty: stichtag"):
                ti_validate.run(session=session)
            
            # After the task fails, Airflow's on_failure_callback should be triggered
            dag_run.update_state(session=session)
            assert dag_run.state == State.FAILED
            
            # Check if DWMSG_Fehlerbehandlung was called (via dag_failure_callback)
            mock_fehlerbehandlung.assert_called_once()
            # Check for specific error message in logs
            assert any("Parameter validation failed: ERROR: The following required parameters are missing or empty: stichtag" in record.message for record in caplog.records)
            assert any("DWMSG_Fehlerbehandlung: Job failed with error" in record.message for record in caplog.records)
```

---

## Test Case 4: Core Logic Invocation Parameters

*   **Purpose**: Verify that the parameters passed to the *simulated* core logic (`k_ausd_bp_ta_bpr_bcp.ksh` equivalent) within the `invoke_core_logic` task match the expected parameters from the legacy script. This covers **Output parity** and **External-system replacements** (correct invocation of the downstream process).
*   **Setup**:
    1.  Ensure Airflow Variables `BERT_DIR_ROOT` and `LOG_BASE_DIR` are set.
    2.  The `r_ausd_bp_ta_bpr_bcp_dag` DAG is deployed to Airflow.
*   **Action**:
    1.  Trigger the DAG with `stichtag="01012024"` and `wiederanlaufwert=500`.
*   **Pass/Fail Criterion**:
    *   The `invoke_core_logic` task completes successfully.
    *   Inspect the logs of the `invoke_core_logic` task. The printed "Legacy command parameters" should exactly match the expected values derived from the DAG run:
        *   `-j R_AUSD_BP_TA_BPR_BCP_<run_id>` (JobKennung, dynamically generated)
        *   `-s 01012024` (Stichtag)
        *   `-f <eintrags_nr>` (DW_EintragsNr, dynamically generated)
        *   `-l 500` (Wiederanlaufwert)
    *   The `Output redirected to: <log_file_path>` line should show the correctly constructed log file path.

```python
# Conceptual Pytest for Airflow DAG (requires Airflow test harness setup)
import pytest
from airflow.models.dag import DAG
from airflow.utils.state import State
from airflow.utils.session import provide_session
from airflow.models import DagRun, TaskInstance, Variable
import pendulum
import re

from r_ausd_bp_ta_bpr_bcp_dag import dag as r_ausd_bp_ta_bpr_bcp_dag

@pytest.fixture(scope="module", autouse=True)
def setup_airflow_variables_core_logic():
    Variable.set("BERT_DIR_ROOT", "/tmp/bert_test_core")
    Variable.set("LOG_BASE_DIR", "/tmp/airflow_logs_test_core")
    yield
    Variable.delete("BERT_DIR_ROOT")
    Variable.delete("LOG_BASE_DIR")

@provide_session
def test_core_logic_invocation_parameters(session):
    dag = r_ausd_bp_ta_bpr_bcp_dag
    dag_id = dag.dag_id
    execution_date = pendulum.datetime(2023, 10, 27, tz="UTC")

    session.query(DagRun).filter(DagRun.dag_id == dag_id).delete()
    session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).delete()
    session.commit()

    explicit_stichtag = "01012024"
    explicit_wiederanlaufwert = 500
    dag_run = dag.create_dagrun(
        state=State.RUNNING,
        execution_date=execution_date,
        start_date=execution_date,
        data_interval_start=execution_date,
        data_interval_end=execution_date,
        session=session,
        conf={"stichtag": explicit_stichtag, "wiederanlaufwert": explicit_wiederanlaufwert}
    )

    # Run all tasks up to invoke_core_logic
    ti_init = TaskInstance(task=dag.get_task("initialize_job_context"), run_id=dag_run.run_id)
    ti_init.run(session=session)
    assert ti_init.current_state() == State.SUCCESS

    ti_validate = TaskInstance(task=dag.get_task("validate_parameters"), run_id=dag_run.run_id)
    ti_validate.run(session=session)
    assert ti_validate.current_state() == State.SUCCESS

    ti_invoke = TaskInstance(task=dag.get_task("invoke_core_logic"), run_id=dag_run.run_id)
    ti_invoke.run(session=session)
    assert ti_invoke.current_state() == State.SUCCESS

    # Extract XCom values
    job_kennung_xcom = ti_init.xcom_pull(key="job_kennung")
    eintrags_nr_xcom = ti_init.xcom_pull(key="eintrags_nr")
    log_file_xcom = ti_init.xcom_pull(key="log_file")

    # Get the rendered bash command to check parameters
    rendered_command = ti_invoke.render_templates()

    assert f"JOB_KENNUNG=\"{job_kennung_xcom}\"" in rendered_command
    assert f"STICHTAG=\"{explicit_stichtag}\"" in rendered_command
    assert f"EINTRAGS_NR=\"{eintrags_nr_xcom}\"" in rendered_command
    assert f"WIEDERANLAUFWERT=\"{explicit_wiederanlaufwert}\"" in rendered_command
    assert f"LOG_FILE=\"{log_file_xcom}\"" in rendered_command

    # Also check the printed "Legacy command parameters"
    assert f"-j {job_kennung_xcom}" in rendered_command
    assert f"-s {explicit_stichtag}" in rendered_command
    assert f"-f {eintrags_nr_xcom}" in rendered_command
    assert f"-l {explicit_wiederanlaufwert}" in rendered_command

    dag_run.update_state(session=session)
    assert dag_run.state == State.SUCCESS
```

---

## Test Case 5: Logging and Status Updates

*   **Purpose**: Verify that the custom logging functions (`DWMSG_*`) re-implemented in `airflow_utils.py` are correctly invoked at appropriate stages (initialization, parameter logging, success, failure), and that the overall DAG status accurately reflects the job outcome. This covers **External-system replacements** (logging framework) and **Data-quality / row-count / schema assertions** (in terms of logging fidelity and job status).
*   **Setup**:
    1.  Ensure Airflow Variables `BERT_DIR_ROOT` and `LOG_BASE_DIR` are set.
    2.  The `r_ausd_bp_ta_bpr_bcp_dag` DAG is deployed to Airflow.
*   **Action**:
    1.  **Successful Run**: Trigger a DAG run with valid parameters (e.g., `stichtag="20112023"`).
    2.  **Failed Run**: Trigger a DAG run with an invalid parameter (e.g., `stichtag=""`) to force a validation failure.
*   **Pass/Fail Criterion**:
    *   **Successful Run**:
        *   The DAG run completes with `SUCCESS` status.
        *   Inspect the Airflow task logs for the run. They should contain messages from:
            *   `DWMSG_init_job` (e.g., "Initializing job...")
            *   `DWMSG_log_message` (e.g., "Job Parameters: Stichtag=20112023...", "Parameter validation successful.")
            *   `DWMSG_SetzeStatusOK` (e.g., "Job completed successfully.")
    *   **Failed Run**:
        *   The DAG run completes with `FAILED` status.
        *   The `validate_parameters` task fails.
        *   Inspect the Airflow task logs for the failed run. They should contain:
            *   An error message from `validate_parameters` (e.g., "Parameter validation failed: ERROR: The following required parameters are missing or empty: stichtag").
            *   A message from `DWMSG_Fehlerbehandlung` (invoked by `dag_failure_callback`) indicating job failure.

```python
# Conceptual Pytest for Airflow DAG (requires Airflow test harness setup)
import pytest
from airflow.models.dag import DAG
from airflow.utils.state import State
from airflow.utils.session import provide_session
from airflow.models import DagRun, TaskInstance, Variable
import pendulum
import logging
from unittest.mock import patch

from r_ausd_bp_ta_bpr_bcp_dag import dag as r_ausd_bp_ta_bpr_bcp_dag
from airflow_utils import DWMSG_init_job, DWMSG_log_message, DWMSG_SetzeStatusOK, DWMSG_Fehlerbehandlung

@pytest.fixture(scope="module", autouse=True)
def setup_airflow_variables_logging():
    Variable.set("BERT_DIR_ROOT", "/tmp/bert_test_logging")
    Variable.set("LOG_BASE_DIR", "/tmp/airflow_logs_test_logging")
    yield
    Variable.delete("BERT_DIR_ROOT")
    Variable.delete("LOG_BASE_DIR")

@provide_session
def test_logging_and_status_success(session, caplog):
    dag = r_ausd_bp_ta_bpr_bcp_dag
    dag_id = dag.dag_id
    execution_date = pendulum.datetime(2023, 10, 27, tz="UTC")

    session.query(DagRun).filter(DagRun.dag_id == dag_id).delete()
    session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).delete()
    session.commit()

    with caplog.at_level(logging.INFO): # Capture INFO level logs
        dag_run = dag.create_dagrun(
            state=State.RUNNING,
            execution_date=execution_date,
            start_date=execution_date,
            data_interval_start=execution_date,
            data_interval_end=execution_date,
            session=session,
            conf={"stichtag": "20112023", "wiederanlaufwert": 100}
        )

        # Run all tasks
        for task_id in ["initialize_job_context", "validate_parameters", "invoke_core_logic", "log_success"]:
            ti = TaskInstance(task=dag.get_task(task_id), run_id=dag_run.run_id)
            ti.run(session=session)
            assert ti.current_state() == State.SUCCESS

        dag_run.update_state(session=session)
        assert dag_run.state == State.SUCCESS

        # Assert specific log messages
        assert any("DWMSG_init_job: Initializing job" in record.message for record in caplog.records)
        assert any("Job Parameters: Stichtag=20112023, Wiederanlaufwert=100" in record.message for record in caplog.records)
        assert any("Parameter validation successful." in record.message for record in caplog.records)
        assert any("DWMSG_SetzeStatusOK: Job completed successfully." in record.message for record in caplog.records)

@provide_session
def test_logging_and_status_failure(session, caplog):
    dag = r_ausd_bp_ta_bpr_bcp_dag
    dag_id = dag.dag_id
    execution_date = pendulum.datetime(2023, 10, 27, tz="UTC")

    session.query(DagRun).filter(DagRun.dag_id == dag_id).delete()
    session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).delete()
    session.commit()

    with caplog.at_level(logging.INFO): # Capture INFO and ERROR logs
        # Mock DWMSG_Fehlerbehandlung to ensure it's called
        with patch('airflow_utils.DWMSG_Fehlerbehandlung') as mock_fehlerbehandlung:
            dag_run = dag.create_dagrun(
                state=State.RUNNING,
                execution_date=execution_date,
                start_date=execution_date,
                data_interval_start=execution_date,
                data_interval_end=execution_date,
                session=session,
                conf={"stichtag": ""} # Trigger validation failure
            )

            ti_init = TaskInstance(task=dag.get_task("initialize_job_context"), run_id=dag_run.run_id)
            ti_init.run(session=session)
            assert ti_init.current_state() == State.SUCCESS

            ti_validate = TaskInstance(task=dag.get_task("validate_parameters"), run_id=dag_run.run_id)
            with pytest.raises(ValueError): # Expect task to raise ValueError
                ti_validate.run(session=session)
            
            # After the task fails, the DAG run state should be updated to FAILED
            dag_run.update_state(session=session)
            assert dag_run.state == State.FAILED
            
            # Check if DWMSG_Fehlerbehandlung was called by the dag_failure_callback
            mock_fehlerbehandlung.assert_called_once()
            # Check for specific error message in logs
            assert any("Parameter validation failed: ERROR: The following required parameters are missing or empty: stichtag" in record.message for record in caplog.records)
            assert any("DWMSG_Fehlerbehandlung: Job failed with error" in record.message for record in caplog.records)
```

---

## Test Case 6: `DWDate_Gib_Zeitraum` Functionality

*   **Purpose**: Verify that the `DWDate_Gib_Zeitraum` utility function in `airflow_utils.py` correctly returns the current system date in `DDMMYYYY` format, matching the behavior of the legacy KornShell utility. This covers **External-system replacements** and **Transformation correctness** (date formatting).
*   **Setup**:
    *   No specific Airflow setup is needed, this is a unit test for `airflow_utils.py`.
*   **Action**:
    *   Call the `DWDate_Gib_Zeitraum()` function directly.
*   **Pass/Fail Criterion**:
    *   The function returns a string that is exactly 8 characters long.
    *   The string consists only of digits.
    *   The string matches `datetime.now().strftime('%d%m%Y')`.

```python
# Pytest for airflow_utils.py
import pytest
from datetime import datetime
from airflow_utils import DWDate_Gib_Zeitraum

def test_dwdate_gib_zeitraum():
    expected_date = datetime.now().strftime('%d%m%Y')
    actual_date = DWDate_Gib_Zeitraum()
    assert actual_date == expected_date
    assert len(actual_date) == 8 # DDMMYYYY format
    assert actual_date.isdigit() # Ensure it's numeric
```

---

## Test Case 7: `pruefeParameterGesetzt` Functionality

*   **Purpose**: Verify that the `pruefeParameterGesetzt` utility function in `airflow_utils.py` correctly identifies missing or empty required parameters and raises a `ValueError`, replicating the behavior of the legacy `h_alis_parameter.ksh` script. This covers **External-system replacements** and **Transformation correctness** (parameter validation logic).
*   **Setup**:
    *   No specific Airflow setup is needed, this is a unit test for `airflow_utils.py`.
*   **Action**:
    1.  Call `pruefeParameterGesetzt` with all required parameters present and non-empty.
    2.  Call `pruefeParameterGesetzt` with a missing required parameter.
    3.  Call `pruefeParameterGesetzt` with an empty string as a required parameter.
    4.  Call `pruefeParameterGesetzt` with a `None` value as a required parameter.
*   **Pass/Fail Criterion**:
    *   **All present/valid**: The function executes without raising any exception.
    *   **Missing/Empty/None**: The function raises a `ValueError` with a message indicating the specific missing or empty parameter(s).

```python
# Pytest for airflow_utils.py
import pytest
from airflow_utils import pruefeParameterGesetzt

def test_pruefe_parameter_gesetzt_success():
    params = {"param1": "value1", "param2": 123}
    required = ["param1"]
    try:
        pruefeParameterGesetzt(params, required)
        assert True # Should pass without error
    except ValueError:
        pytest.fail("pruefeParameterGesetzt raised ValueError unexpectedly for valid parameters.")

def test_pruefe_parameter_gesetzt_missing():
    params = {"param1": "value1"}
    required = ["param1", "param2"]
    with pytest.raises(ValueError, match="The following required parameters are missing or empty: param2"):
        pruefeParameterGesetzt(params, required)

def test_pruefe_parameter_gesetzt_empty_string():
    params = {"param1": "value1", "param2": ""}
    required = ["param1", "param2"]
    with pytest.raises(ValueError, match="The following required parameters are missing or empty: param2"):
        pruefeParameterGesetzt(params, required)

def test_pruefe_parameter_gesetzt_none_value():
    params = {"param1": "value1", "param2": None}
    required = ["param1", "param2"]
    with pytest.raises(ValueError, match="The following required parameters are missing or empty: param2"):
        pruefeParameterGesetzt(params, required)

def test_pruefe_parameter_gesetzt_multiple_missing():
    params = {"param1": "value1"}
    required = ["param1", "param2", "param3"]
    with pytest.raises(ValueError, match="The following required parameters are missing or empty: param2, param3"):
        pruefeParameterGesetzt(params, required)
```