The migration of `h_alis_sqlplus.ksh` to a Python function `execute_bigquery_script` within Airflow involves a significant shift in technology and execution environment. The tests below focus on ensuring behavioral equivalence, correct transformation of logic, and proper integration with new external systems.

## Migration Validation Tests for `h_alis_sqlplus.ksh`

The following tests are designed to validate the `execute_bigquery_script` Python function, which replaces the `starteSQLSkript` function from the legacy KornShell script.

---

### Test Case 1: Missing `entry_number` Parameter

*   **Purpose:** Verify that the migrated code correctly handles the absence of the `entry_number` parameter, mirroring the legacy script's error handling for a missing `p_Eintragsnr`.
*   **Setup:**
    *   A mock Airflow environment is set up.
    *   The `BigQueryExecuteQueryOperator` and GCS client are mocked to prevent actual execution or external calls.
    *   The logger is mocked to capture error messages.
*   **Action:** Call `execute_bigquery_script` with `entry_number=None` and a valid `script_ref`.
*   **Pass/Fail Criterion:**
    *   The function raises an `AirflowException` with a message indicating "Missing 'entry_number'".
    *   The mocked logger's `error` method is called with a message containing "Error N/A E 196".
    *   The `BigQueryExecuteQueryOperator` is *not* instantiated or executed.

```python
# tests/test_bigquery_sql_executor.py (using pytest)
import pytest
from unittest.mock import MagicMock, patch
from airflow.exceptions import AirflowException
from dags.utils.bigquery_sql_executor import execute_bigquery_script

@pytest.fixture
def mock_airflow_context():
    return {'task_instance': {'task_id': 'test_python_operator_task_id'}, 'ds': '2023-01-01'}

@pytest.fixture(autouse=True)
def mock_dependencies():
    with patch('dags.utils.bigquery_sql_executor.logger') as mock_logger, \
         patch('dags.utils.bigquery_sql_executor.BigQueryExecuteQueryOperator') as mock_bq_operator_class, \
         patch('dags.utils.bigquery_sql_executor.storage.Client') as mock_gcs_client_class:
        
        # Mock BigQueryExecuteQueryOperator instance and its execute method
        mock_bq_operator_instance = MagicMock()
        mock_bq_operator_class.return_value = mock_bq_operator_instance
        
        # Mock GCS client and its methods
        mock_gcs_blob = MagicMock(exists=MagicMock(return_value=True), download_as_text=MagicMock(return_value="SELECT 1;"))
        mock_gcs_bucket = MagicMock(blob=MagicMock(return_value=mock_gcs_blob))
        mock_gcs_client_class.return_value.bucket.return_value = mock_gcs_bucket

        yield mock_logger, mock_bq_operator_class, mock_gcs_client_class, mock_bq_operator_instance

def test_missing_entry_number(mock_dependencies, mock_airflow_context):
    mock_logger, mock_bq_operator_class, _, _ = mock_dependencies
    with pytest.raises(AirflowException, match="Missing 'entry_number'"):
        execute_bigquery_script(entry_number=None, script_ref="SELECT 1;", **mock_airflow_context)
    mock_logger.error.assert_called_with("Error N/A E 196: Missing 'entry_number' for BigQuery execution.")
    mock_bq_operator_class.assert_not_called()
```

---

### Test Case 2: Missing `script_ref` Parameter

*   **Purpose:** Verify that the migrated code correctly handles the absence of the `script_ref` parameter, mirroring the legacy script's error handling for a missing `p_Skript`.
*   **Setup:**
    *   A mock Airflow environment is set up.
    *   The `BigQueryExecuteQueryOperator` and GCS client are mocked.
    *   The logger is mocked to capture error messages.
*   **Action:** Call `execute_bigquery_script` with a valid `entry_number` but `script_ref=None`.
*   **Pass/Fail Criterion:**
    *   The function raises an `AirflowException` with a message indicating "Missing 'script_ref'".
    *   The mocked logger's `error` method is called with a message containing "Error 001 E 196".
    *   The `BigQueryExecuteQueryOperator` is *not* instantiated or executed.

```python
# tests/test_bigquery_sql_executor.py (continued)
def test_missing_script_ref(mock_dependencies, mock_airflow_context):
    mock_logger, mock_bq_operator_class, _, _ = mock_dependencies
    with pytest.raises(AirflowException, match="Missing 'script_ref'"):
        execute_bigquery_script(entry_number="001", script_ref=None, **mock_airflow_context)
    mock_logger.error.assert_called_with("Error 001 E 196: Missing 'script_ref' for BigQuery execution.")
    mock_bq_operator_class.assert_not_called()
```

---

### Test Case 3: Inline SQL Execution Success (Transformation Correctness)

*   **Purpose:** Verify that the migrated code can successfully execute an inline SQL string, a new capability compared to the legacy script which only accepted file paths. This tests the `script_ref` handling when it's not a GCS path.
*   **Setup:**
    *   A mock Airflow environment.
    *   The `BigQueryExecuteQueryOperator` is mocked to simulate successful execution.
    *   The GCS client is mocked but should *not* be called.
*   **Action:** Call `execute_bigquery_script` with a direct SQL string for `script_ref` and a dictionary of `sql_parameters`.
*   **Pass/Fail Criterion:**
    *   No `AirflowException` is raised.
    *   The `BigQueryExecuteQueryOperator` is instantiated with the exact `script_ref` as its `sql` argument, the provided `sql_parameters` as its `params` argument, `gcp_conn_id='google_cloud_default'`, `use_legacy_sql=False`, and a correctly formatted `task_id`.
    *   The `execute` method of the `BigQueryExecuteQueryOperator` instance is called exactly once with the Airflow context.
    *   The GCS client is *not* called.
    *   `logger.info` confirms successful execution.

```python
# tests/test_bigquery_sql_executor.py (continued)
def test_inline_sql_execution_success(mock_dependencies, mock_airflow_context):
    mock_logger, mock_bq_operator_class, mock_gcs_client_class, mock_bq_operator_instance = mock_dependencies
    test_sql = "SELECT 'Hello World';"
    entry_number = "002"
    sql_params = {"param1": "value1"}

    execute_bigquery_script(
        entry_number=entry_number,
        script_ref=test_sql,
        sql_parameters=sql_params,
        **mock_airflow_context
    )

    mock_logger.info.assert_any_call(f"Initiating BigQuery SQL execution for script reference: {test_sql}")
    mock_logger.info.assert_any_call(f"SQL parameters: {sql_params}")
    mock_bq_operator_class.assert_called_once_with(
        task_id=f"execute_bq_script_{mock_airflow_context['task_instance']['task_id']}_{entry_number}",
        sql=test_sql,
        use_legacy_sql=False,
        params=sql_params,
        gcp_conn_id='google_cloud_default',
    )
    mock_bq_operator_instance.execute.assert_called_once_with(context=mock_airflow_context)
    mock_logger.info.assert_called_with(f"Successfully executed BigQuery SQL script via reference: {test_sql}")
    mock_gcs_client_class.assert_not_called() # GCS client should not be called for inline SQL
```

---

### Test Case 4: GCS SQL Script Execution Success (External System Replacement)

*   **Purpose:** Verify that the migrated code can successfully fetch and execute a BigQuery SQL script from Google Cloud Storage, replacing the legacy local file read mechanism. This validates the GCS integration.
*   **Setup:**
    *   A mock Airflow environment.
    *   The GCS client is mocked to simulate an existing blob and return specific SQL content.
    *   The `BigQueryExecuteQueryOperator` is mocked to simulate successful execution.
*   **Action:** Call `execute_bigquery_script` with a valid GCS path for `script_ref` and `sql_parameters`.
*   **Pass/Fail Criterion:**
    *   No `AirflowException` is raised.
    *   The GCS client's `bucket`, `blob`, `exists`, and `download_as_text` methods are called correctly.
    *   The `BigQueryExecuteQueryOperator` is instantiated with the *downloaded GCS content* as its `sql` argument, the provided `sql_parameters` as its `params` argument, `gcp_conn_id='google_cloud_default'`, `use_legacy_sql=False`, and a correctly formatted `task_id`.
    *   The `execute` method of the `BigQueryExecuteQueryOperator` instance is called exactly once with the Airflow context.
    *   `logger.info` confirms GCS content loading and successful execution.

```python
# tests/test_bigquery_sql_executor.py (continued)
def test_gcs_sql_execution_success(mock_dependencies, mock_airflow_context):
    mock_logger, mock_bq_operator_class, mock_gcs_client_class, mock_bq_operator_instance = mock_dependencies
    gcs_path = "gs://my-bucket/sql/test_script.sql"
    gcs_content = "SELECT * FROM my_table WHERE date = @report_date;"
    entry_number = "003"
    sql_params = {"report_date": "2023-01-01"}

    mock_gcs_client_class.return_value.bucket.return_value.blob.return_value.exists.return_value = True
    mock_gcs_client_class.return_value.bucket.return_value.blob.return_value.download_as_text.return_value = gcs_content

    execute_bigquery_script(
        entry_number=entry_number,
        script_ref=gcs_path,
        sql_parameters=sql_params,
        **mock_airflow_context
    )

    mock_gcs_client_class.assert_called_once()
    mock_gcs_client_class.return_value.bucket.assert_called_once_with("my-bucket")
    mock_gcs_client_class.return_value.bucket.return_value.blob.assert_called_once_with("sql/test_script.sql")
    mock_gcs_client_class.return_value.bucket.return_value.blob.return_value.exists.assert_called_once()
    mock_gcs_client_class.return_value.bucket.return_value.blob.return_value.download_as_text.assert_called_once()
    mock_logger.info.assert_any_call(f"Loaded SQL content from GCS: {gcs_path}")
    mock_bq_operator_class.assert_called_once_with(
        task_id=f"execute_bq_script_{mock_airflow_context['task_instance']['task_id']}_{entry_number}",
        sql=gcs_content, # Should use content from GCS
        use_legacy_sql=False,
        params=sql_params,
        gcp_conn_id='google_cloud_default',
    )
    mock_bq_operator_instance.execute.assert_called_once_with(context=mock_airflow_context)
    mock_logger.info.assert_called_with(f"Successfully executed BigQuery SQL script via reference: {gcs_path}")
```

---

### Test Case 5: GCS SQL Script Not Found (Transformation Correctness)

*   **Purpose:** Verify that the migrated code correctly handles a `script_ref` pointing to a non-existent GCS file, mirroring the legacy script's `[ ! -r $p_Skript ]` check and error code 201.
*   **Setup:**
    *   A mock Airflow environment.
    *   The GCS client is mocked to report that the blob does not exist.
*   **Action:** Call `execute_bigquery_script` with a GCS path to a non-existent file.
*   **Pass/Fail Criterion:**
    *   The function raises an `AirflowException` with a message indicating "GCS SQL script .* not found".
    *   The mocked logger's `error` method is called with a message containing "Error 004 E 201".
    *   The `BigQueryExecuteQueryOperator` is *not* instantiated or executed.

```python
# tests/test_bigquery_sql_executor.py (continued)
def test_gcs_sql_not_found(mock_dependencies, mock_airflow_context):
    mock_logger, mock_bq_operator_class, mock_gcs_client_class, _ = mock_dependencies
    gcs_path = "gs://my-bucket/sql/non_existent.sql"
    entry_number = "004"

    mock_gcs_client_class.return_value.bucket.return_value.blob.return_value.exists.return_value = False

    with pytest.raises(AirflowException, match="GCS SQL script .* not found."):
        execute_bigquery_script(entry_number=entry_number, script_ref=gcs_path, **mock_airflow_context)
    
    mock_logger.error.assert_called_with(f"Error {entry_number} E 201: GCS SQL script '{gcs_path}' not found or inaccessible.")
    mock_bq_operator_class.assert_not_called()
    mock_gcs_client_class.assert_called_once()
```

---

### Test Case 6: GCS SQL Script Read Error (Transformation Correctness)

*   **Purpose:** Verify that the migrated code correctly handles errors during the reading of a GCS file (e.g., permission denied), mirroring the legacy script's `[ ! -r $p_Skript ]` check and error code 201.
*   **Setup:**
    *   A mock Airflow environment.
    *   The GCS client is mocked to report that the blob exists but `download_as_text` raises an exception.
*   **Action:** Call `execute_bigquery_script` with a GCS path that simulates a read error.
*   **Pass/Fail Criterion:**
    *   The function raises an `AirflowException` with a message indicating "Failed to read GCS SQL script .* Permission denied".
    *   The mocked logger's `error` method is called with a message containing "Error 005 E 201".
    *   The `BigQueryExecuteQueryOperator` is *not* instantiated or executed.

```python
# tests/test_bigquery_sql_executor.py (continued)
def test_gcs_read_failure(mock_dependencies, mock_airflow_context):
    mock_logger, mock_bq_operator_class, mock_gcs_client_class, _ = mock_dependencies
    gcs_path = "gs://my-bucket/sql/read_fail.sql"
    entry_number = "005"

    mock_gcs_client_class.return_value.bucket.return_value.blob.return_value.exists.return_value = True
    mock_gcs_client_class.return_value.bucket.return_value.blob.return_value.download_as_text.side_effect = Exception("Permission denied")

    with pytest.raises(AirflowException, match="Failed to read GCS SQL script .* Permission denied"):
        execute_bigquery_script(entry_number=entry_number, script_ref=gcs_path, **mock_airflow_context)
    
    mock_logger.error.assert_called_with(f"Error {entry_number} E 201: Failed to read GCS SQL script '{gcs_path}': Permission denied")
    mock_bq_operator_class.assert_not_called()
    mock_gcs_client_class.assert_called_once()
```

---

### Test Case 7: BigQuery Execution Failure (Output Parity)

*   **Purpose:** Verify that the migrated code correctly handles failures during the BigQuery SQL execution, mirroring the legacy `sqlplus` returning a non-zero exit code.
*   **Setup:**
    *   A mock Airflow environment.
    *   The `BigQueryExecuteQueryOperator`'s `execute` method is mocked to raise an `AirflowException`.
*   **Action:** Call `execute_bigquery_script` with a valid SQL reference that would cause the BigQuery operator to fail.
*   **Pass/Fail Criterion:**
    *   The function raises an `AirflowException` with a message indicating "BigQuery execution failed".
    *   The mocked logger's `error` method is called with a message confirming the BigQuery execution failure.
    *   The `BigQueryExecuteQueryOperator` is instantiated and its `execute` method is called.

```python
# tests/test_bigquery_sql_executor.py (continued)
def test_bigquery_execution_failure(mock_dependencies, mock_airflow_context):
    mock_logger, mock_bq_operator_class, _, mock_bq_operator_instance = mock_dependencies
    test_sql = "SELECT * FROM non_existent_table;"
    entry_number = "006"

    mock_bq_operator_instance.execute.side_effect = AirflowException("BigQuery query failed.")

    with pytest.raises(AirflowException, match="BigQuery execution failed for .* BigQuery query failed."):
        execute_bigquery_script(entry_number=entry_number, script_ref=test_sql, **mock_airflow_context)
    
    mock_logger.error.assert_called_with(f"Error executing BigQuery SQL for script reference '{test_sql}': BigQuery query failed.")
    mock_bq_operator_class.assert_called_once()
    mock_bq_operator_instance.execute.assert_called_once_with(context=mock_airflow_context)
```

---

### Test Case 8: Parameter Handling (Transformation Correctness)

*   **Purpose:** Verify that `sql_parameters` passed to `execute_bigquery_script` are correctly forwarded to the `BigQueryExecuteQueryOperator`'s `params` argument. This ensures correct parameterization of BigQuery queries, analogous to how `sqlplus` received parameters.
*   **Setup:**
    *   A mock Airflow environment.
    *   The `BigQueryExecuteQueryOperator` is mocked to simulate successful execution.
*   **Action:** Call `execute_bigquery_script` with a sample `sql_parameters` dictionary.
*   **Pass/Fail Criterion:**
    *   The `BigQueryExecuteQueryOperator` is instantiated with its `params` argument exactly matching the `sql_parameters` provided to `execute_bigquery_script`.
    *   `logger.info` is called to log the `sql_parameters`.

```python
# tests/test_bigquery_sql_executor.py (continued)
def test_parameter_handling(mock_dependencies, mock_airflow_context):
    mock_logger, mock_bq_operator_class, _, mock_bq_operator_instance = mock_dependencies
    test_sql = "SELECT @param1, @param2;"
    entry_number = "007"
    sql_params = {"param1": "value_a", "param2": 123}

    execute_bigquery_script(
        entry_number=entry_number,
        script_ref=test_sql,
        sql_parameters=sql_params,
        **mock_airflow_context
    )

    mock_bq_operator_class.assert_called_once_with(
        task_id=f"execute_bq_script_{mock_airflow_context['task_instance']['task_id']}_{entry_number}",
        sql=test_sql,
        use_legacy_sql=False,
        params=sql_params, # Assert params are correctly passed
        gcp_conn_id='google_cloud_default',
    )
    mock_logger.info.assert_any_call(f"SQL parameters: {sql_params}")
```

---

### Test Case 9: `gcp_conn_id` and `use_legacy_sql` Configuration (External System Replacement)

*   **Purpose:** Verify that the `BigQueryExecuteQueryOperator` is consistently configured with `gcp_conn_id='google_cloud_default'` (replacing `DW_ORAUSER`) and `use_legacy_sql=False` (ensuring standard BigQuery SQL dialect).
*   **Setup:**
    *   A mock Airflow environment.
    *   The `BigQueryExecuteQueryOperator` is mocked to simulate successful execution.
*   **Action:** Call `execute_bigquery_script` with any valid SQL reference.
*   **Pass/Fail Criterion:**
    *   The `BigQueryExecuteQueryOperator` is instantiated with `gcp_conn_id='google_cloud_default'` and `use_legacy_sql=False`.

```python
# tests/test_bigquery_sql_executor.py (continued)
def test_bq_operator_config(mock_dependencies, mock_airflow_context):
    _, mock_bq_operator_class, _, _ = mock_dependencies
    test_sql = "SELECT 1;"
    entry_number = "008"

    execute_bigquery_script(
        entry_number=entry_number,
        script_ref=test_sql,
        **mock_airflow_context
    )

    mock_bq_operator_class.assert_called_once()
    args, kwargs = mock_bq_operator_class.call_args
    assert kwargs['gcp_conn_id'] == 'google_cloud_default'
    assert kwargs['use_legacy_sql'] is False
```

---

### Test Case 10: Task ID Generation for BigQuery Operator

*   **Purpose:** Verify that the `task_id` for the ephemeral `BigQueryExecuteQueryOperator` is correctly generated, incorporating the calling PythonOperator's `task_id` and the `entry_number`. This aids in Airflow UI visibility and debugging.
*   **Setup:**
    *   A mock Airflow environment with a specific `task_instance.task_id`.
    *   The `BigQueryExecuteQueryOperator` is mocked to simulate successful execution.
*   **Action:** Call `execute_bigquery_script` with a specific `entry_number` and `kwargs` containing `task_instance.task_id`.
*   **Pass/Fail Criterion:**
    *   The `BigQueryExecuteQueryOperator` is instantiated with a `task_id` matching the format `f"execute_bq_script_{mock_airflow_context['task_instance']['task_id']}_{entry_number}"`.

```python
# tests/test_bigquery_sql_executor.py (continued)
def test_bq_operator_task_id_generation(mock_dependencies, mock_airflow_context):
    _, mock_bq_operator_class, _, _ = mock_dependencies
    test_sql = "SELECT 1;"
    entry_number = "009"
    expected_task_id = f"execute_bq_script_{mock_airflow_context['task_instance']['task_id']}_{entry_number}"

    execute_bigquery_script(
        entry_number=entry_number,
        script_ref=test_sql,
        **mock_airflow_context
    )

    mock_bq_operator_class.assert_called_once()
    args, kwargs = mock_bq_operator_class.call_args
    assert kwargs['task_id'] == expected_task_id
```

---

### Test Case 11: Legacy Local File Path Handling (Behavioral Change)

*   **Purpose:** Explicitly test the behavior when a legacy-style local file path (e.g., `/tmp/my_script.sql`) is passed as `script_ref`. This highlights a deliberate behavioral change where such paths are now treated as inline SQL, not as references to local files to be read.
*   **Setup:**
    *   A mock Airflow environment.
    *   The `BigQueryExecuteQueryOperator` is mocked to simulate successful execution.
    *   The GCS client is mocked but should *not* be called.
*   **Action:** Call `execute_bigquery_script` with `script_ref="/tmp/my_script.sql"`.
*   **Pass/Fail Criterion:**
    *   No `AirflowException` is raised.
    *   The `BigQueryExecuteQueryOperator` is instantiated with `sql="/tmp/my_script.sql"` (i.e., the path string itself is treated as the SQL to execute).
    *   The GCS client is *not* called, confirming that local file paths are not interpreted as GCS paths or read from the local filesystem.
    *   `logger.info` confirms successful execution of the "inline" SQL.

```python
# tests/test_bigquery_sql_executor.py (continued)
def test_legacy_local_file_path_as_inline_sql(mock_dependencies, mock_airflow_context):
    mock_logger, mock_bq_operator_class, mock_gcs_client_class, mock_bq_operator_instance = mock_dependencies
    legacy_path = "/tmp/my_script.sql"
    entry_number = "010"

    execute_bigquery_script(
        entry_number=entry_number,
        script_ref=legacy_path,
        **mock_airflow_context
    )

    mock_bq_operator_class.assert_called_once_with(
        task_id=f"execute_bq_script_{mock_airflow_context['task_instance']['task_id']}_{entry_number}",
        sql=legacy_path, # The path string itself is treated as SQL
        use_legacy_sql=False,
        params={},
        gcp_conn_id='google_cloud_default',
    )
    mock_bq_operator_instance.execute.assert_called_once_with(context=mock_airflow_context)
    mock_gcs_client_class.assert_not_called() # GCS client should not be called
    mock_logger.info.assert_called_with(f"Successfully executed BigQuery SQL script via reference: {legacy_path}")
```