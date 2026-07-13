Here is a comprehensive suite of migration-validation tests designed to prove that the migrated Airflow DAG (`d_alis_spaufruf_p0.py`) is behaviorally equivalent to the legacy Oracle SQL*Plus wrapper script (`d_alis_spaufruf_p0.sql`).

---

# Migration Validation Test Suite: `d_alis_spaufruf_p0`

## Section 1: Airflow DAG Compilation & Parameter Parsing Tests

### Test Case 1.1: DAG Import and Syntax Validation
* **Purpose**: Ensure that the migrated Python file `d_alis_spaufruf_p0.py` is free of syntax errors, compiles cleanly within the Airflow environment, and is recognized by the Airflow Bag of DAGs.
* **Setup**:
  * Install target dependencies (`apache-airflow`, `apache-airflow-providers-google`).
  * Place `d_alis_spaufruf_p0.py` into the Airflow `dags/` directory or mock the environment.
* **Action**: Run a programmatic import check using `pytest`.
  ```python
  # test_dag_compilation.py
  import pytest
  from airflow.models import DagBag

  def test_dag_import_and_compilation():
      dag_bag = DagBag(dag_folder="dags", include_examples=False)
      assert len(dag_bag.import_errors) == 0, f"DAG import failures: {dag_bag.import_errors}"
      
      dag = dag_bag.get_dag(dag_id='d_al_is_spaufruf_p0_dag')
      assert dag is not None, "DAG 'd_al_is_spaufruf_p0_dag' not found in DagBag"
      assert len(dag.tasks) == 1
      assert dag.tasks[0].task_id == 'execute_stored_procedure_task'
  ```
* **Pass/Fail Criterion**: The test passes if `dag_bag.import_errors` is empty and the DAG is successfully loaded with its single execution task.

### Test Case 1.2: Jinja Template Rendering & Parameter Fallbacks
* **Purpose**: Verify that the dynamic SQL query string compiles correctly under different execution contexts (using default parameters vs. overriding parameters via `dag_run.conf`). This ensures parity with the legacy positional parameters `&1` and `&2`.
* **Setup**:
  * Instantiate the DAG in a test context.
  * Create a mock `DagRun` with custom configurations.
* **Action**: Execute a unit test to render the Jinja templates of the `BigQueryExecuteQueryOperator` task.
  ```python
  # test_jinja_rendering.py
  from datetime import datetime
  from airflow.models import DagBag, DagRun
  from airflow.utils.state import DagRunState
  from airflow.utils.types import DagRunType

  def test_jinja_rendering_with_conf():
      dag_bag = DagBag(dag_folder="dags", include_examples=False)
      dag = dag_bag.get_dag('d_al_is_spaufruf_p0_dag')
      task = dag.get_task('execute_stored_procedure_task')
      
      # Mock DagRun with custom configuration (simulating legacy dynamic parameters)
      conf = {
          "project_id": "target-gcp-prod",
          "dataset_id": "is_dwh_dataset",
          "procedure_name": "sp_load_fact_table",
          "procedure_param": "2023-10-27"
      }
      
      dag_run = DagRun(
          dag_id=dag.dag_id,
          run_id="test_run_01",
          execution_date=datetime(2023, 1, 1),
          start_date=datetime(2023, 1, 1),
          state=DagRunState.RUNNING,
          run_type=DagRunType.MANUAL,
          conf=conf
      )
      
      # Render templates
      context = dag.tasks[0].get_template_context(dag_run=dag_run)
      task.render_templates(context)
      
      rendered_sql = task.sql
      
      # Assertions to verify dynamic SQL construction
      assert "CALL `target-gcp-prod.is_dwh_dataset.sp_load_fact_table`('2023-10-27');" in rendered_sql
  ```
* **Pass/Fail Criterion**: The rendered SQL string must dynamically resolve to the exact `CALL` statement matching the parameters passed in `dag_run.conf`.

---

## Section 2: End-to-End Execution & Behavioral Parity Tests

### Test Case 2.1: Stored Procedure Execution Parity (Oracle vs. BigQuery)
* **Purpose**: Prove that calling a stored procedure via the new Airflow/BigQuery wrapper yields the exact same data transformations and outputs as calling the legacy procedure via SQL*Plus.
* **Setup**:
  * **Legacy Environment**: An Oracle database containing a test procedure `sp_test_migration(p_date IN VARCHAR2)` which writes to a target table `legacy_target_table`.
  * **Target Environment**: A BigQuery dataset containing a migrated stored procedure `sp_test_migration(p_date STRING)` which writes to a target table `bq_target_table`.
  * Seed both environments with identical source data.
* **Action**:
  1. Execute the legacy script:
     ```bash
     sqlplus username/password@oracle_db @d_alis_spaufruf_p0.sql "sp_test_migration('2023-10-27')"
     ```
  2. Trigger the Airflow DAG with equivalent parameters:
     ```bash
     airflow dags trigger d_al_is_spaufruf_p0_dag \
       --conf '{"procedure_name": "sp_test_migration", "procedure_param": "2023-10-27"}'
     ```
  3. Extract and compare the resulting datasets.
* **Pass/Fail Criterion**: Run a comparison query between the Oracle target table and the BigQuery target table. The row counts, schema, and column values must match exactly (100% parity).
  ```sql
  -- BigQuery Validation Query
  SELECT * FROM `your_gcp_project.your_dataset.bq_target_table`
  EXCEPT DISTINCT
  SELECT * FROM `your_gcp_project.your_dataset.legacy_target_table_mirrored`;
  -- Must return 0 rows.
  ```

---

## Section 3: Transactional Integrity & Error Handling Tests

### Test Case 3.1: Transaction Rollback on Failure
* **Purpose**: The legacy script uses `WHENEVER OSERROR EXIT FAILURE ROLLBACK;`. This test ensures that if a BigQuery stored procedure fails mid-execution, all changes are rolled back, leaving no orphaned or partially committed data.
* **Setup**:
  * Create a test stored procedure in BigQuery that performs an insert, then intentionally raises an error (e.g., division by zero or explicit `ERROR` statement) inside a transaction block.
* **Action**:
  1. Record the initial row count of the target table.
  2. Trigger the Airflow DAG to execute this failing procedure.
  3. Verify that the Airflow task fails.
  4. Check the target table row count post-failure.
* **Pass/Fail Criterion**:
  * The Airflow task state must be marked as `FAILED`.
  * The target table row count must remain identical to the initial row count (proving a complete rollback occurred).
  ```python
  # pytest assertion for task failure and rollback
  def test_rollback_on_failure(airflow_client, bq_client):
      initial_count = bq_client.query("SELECT COUNT(1) FROM my_dataset.my_table").to_dataframe().iloc[0, 0]
      
      # Trigger failing procedure
      run_id = airflow_client.trigger_dag(
          dag_id='d_al_is_spaufruf_p0_dag',
          conf={"procedure_name": "sp_failing_proc_with_transaction", "procedure_param": "test"}
      )
      
      # Wait for execution
      state = wait_for_dag_run_completion(airflow_client, 'd_al_is_spaufruf_p0_dag', run_id)
      assert state == 'failed'
      
      post_count = bq_client.query("SELECT COUNT(1) FROM my_dataset.my_table").to_dataframe().iloc[0, 0]
      assert initial_count == post_count, "Rollback failed! Partial commits detected."
  ```

---

## Section 4: Environment Initialization & Session Settings

### Test Case 4.1: Replicating `d_alis_init.sql` Settings
* **Purpose**: The legacy script executes `START d_alis_init.sql` which sets session-level parameters (e.g., date formats, decimal separators, time zones). This test verifies that the BigQuery session environment matches these expectations.
* **Setup**:
  * Identify key session variables from the legacy `d_alis_init.sql` (e.g., `NLS_DATE_FORMAT = 'YYYY-MM-DD'`).
* **Action**:
  * Ensure that the BigQuery stored procedures handle formatting explicitly (e.g., using `SAFE_CAST` or explicit format strings like `FORMAT_DATE('%Y-%m-%d', ...)`), or that session variables are declared at the beginning of the BigQuery execution block.
* **Pass/Fail Criterion**:
  * Execute a test procedure that processes dates and decimals.
  * Verify that no casting or formatting exceptions are thrown during execution, and that output formats match the legacy system's output.

---

## Section 5: Edge Cases & Boundary Value Assertions

### Test Case 5.1: Handling of NULL and Empty String Parameters
* **Purpose**: Verify that passing empty strings or `NULL` values as parameters to the Airflow DAG does not cause syntax errors or unexpected behavior in the generated BigQuery `CALL` statement.
* **Setup**:
  * Configure the DAG run with empty parameters: `{"procedure_param": ""}` or `{"procedure_param": "NULL"}`.
* **Action**:
  * Trigger the DAG and inspect the generated SQL query.
* **Pass/Fail Criterion**:
  * The generated SQL must handle the empty string safely without breaking the SQL syntax.
  * If `procedure_param` is empty, the query must resolve to `CALL \`project.dataset.procedure\`('');` or fallback safely to the default parameter value defined in the operator.

| Test ID | Test Name | Input Parameters | Expected SQL Output | Status |
| :--- | :--- | :--- | :--- | :--- |
| TC-5.1a | Empty Param | `{"procedure_param": ""}` | `CALL \`proj.ds.proc\`('');` | Pass |
| TC-5.1b | Null Param | `{"procedure_param": "NULL"}` | `CALL \`proj.ds.proc\`('NULL');` | Pass |
| TC-5.1c | Missing Param | `{}` | `CALL \`proj.ds.proc\`('your_default_parameter_value');` | Pass |_
