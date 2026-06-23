"""
Unit tests for the bigquery_sql_executor module.
"""
# Replaces legacy source vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh
import unittest
from unittest.mock import patch, MagicMock
from airflow.exceptions import AirflowException

# Import the function to be tested
from dags.utils.bigquery_sql_executor import execute_bigquery_script

class TestBigQuerySqlExecutor(unittest.TestCase):

    def setUp(self):
        # Common mocks for all tests
        self.mock_logger = MagicMock()
        # Patch the logger to capture its calls
        patcher = patch('dags.utils.bigquery_sql_executor.logger', self.mock_logger)
        patcher.start()
        self.addCleanup(patcher.stop)

        self.mock_bq_operator_execute = MagicMock()
        self.mock_bq_operator_class = MagicMock(return_value=MagicMock(execute=self.mock_bq_operator_execute))
        patcher = patch('dags.utils.bigquery_sql_executor.BigQueryExecuteQueryOperator', self.mock_bq_operator_class)
        patcher.start()
        self.addCleanup(patcher.stop)

        self.mock_gcs_blob_exists = MagicMock()
        self.mock_gcs_blob_download_as_text = MagicMock()
        self.mock_gcs_blob = MagicMock(exists=self.mock_gcs_blob_exists, download_as_text=self.mock_gcs_blob_download_as_text)
        self.mock_gcs_bucket = MagicMock(blob=MagicMock(return_value=self.mock_gcs_blob))
        self.mock_gcs_client_bucket = MagicMock(return_value=self.mock_gcs_bucket)
        self.mock_gcs_client_class = MagicMock(return_value=MagicMock(bucket=self.mock_gcs_client_bucket))
        patcher = patch('dags.utils.bigquery_sql_executor.storage.Client', self.mock_gcs_client_class)
        patcher.start()
        self.addCleanup(patcher.stop)

        self.mock_kwargs = {'task_instance': {'task_id': 'test_python_operator_task_id'}, 'ds': '2023-01-01'}


    def test_missing_entry_number(self):
        with self.assertRaisesRegex(AirflowException, "Missing 'entry_number'"):
            execute_bigquery_script(entry_number=None, script_ref="SELECT 1;", **self.mock_kwargs)
        self.mock_logger.error.assert_called_with("Error N/A E 196: Missing 'entry_number' for BigQuery execution.")
        self.mock_bq_operator_class.assert_not_called()

    def test_missing_script_ref(self):
        with self.assertRaisesRegex(AirflowException, "Missing 'script_ref'"):
            execute_bigquery_script(entry_number="001", script_ref=None, **self.mock_kwargs)
        self.mock_logger.error.assert_called_with("Error 001 E 196: Missing 'script_ref' for BigQuery execution.")
        self.mock_bq_operator_class.assert_not_called()

    def test_inline_sql_execution_success(self):
        test_sql = "SELECT 'Hello World';"
        entry_number = "002"
        sql_params = {"param1": "value1"}

        execute_bigquery_script(
            entry_number=entry_number,
            script_ref=test_sql,
            sql_parameters=sql_params,
            **self.mock_kwargs
        )

        self.mock_logger.info.assert_any_call(f"Initiating BigQuery SQL execution for script reference: {test_sql}")
        self.mock_logger.info.assert_any_call(f"SQL parameters: {sql_params}")
        self.mock_bq_operator_class.assert_called_once_with(
            task_id=f"execute_bq_script_{self.mock_kwargs['task_instance']['task_id']}_{entry_number}",
            sql=test_sql,
            use_legacy_sql=False,
            params=sql_params,
            gcp_conn_id='google_cloud_default',
        )
        self.mock_bq_operator_execute.assert_called_once_with(context=self.mock_kwargs)
        self.mock_logger.info.assert_called_with(f"Successfully executed BigQuery SQL script via reference: {test_sql}")
        self.mock_gcs_client_class.assert_not_called() # GCS client should not be called for inline SQL

    def test_gcs_sql_execution_success(self):
        gcs_path = "gs://my-bucket/sql/test_script.sql"
        gcs_content = "SELECT * FROM my_table WHERE date = @report_date;"
        entry_number = "003"
        sql_params = {"report_date": "2023-01-01"}

        self.mock_gcs_blob_exists.return_value = True
        self.mock_gcs_blob_download_as_text.return_value = gcs_content

        execute_bigquery_script(
            entry_number=entry_number,
            script_ref=gcs_path,
            sql_parameters=sql_params,
            **self.mock_kwargs
        )

        self.mock_gcs_client_class.assert_called_once()
        self.mock_gcs_client_class.return_value.bucket.assert_called_once_with("my-bucket")
        self.mock_gcs_bucket.blob.assert_called_once_with("sql/test_script.sql")
        self.mock_gcs_blob_exists.assert_called_once()
        self.mock_gcs_blob_download_as_text.assert_called_once()
        self.mock_logger.info.assert_any_call(f"Loaded SQL content from GCS: {gcs_path}")
        self.mock_bq_operator_class.assert_called_once_with(
            task_id=f"execute_bq_script_{self.mock_kwargs['task_instance']['task_id']}_{entry_number}",
            sql=gcs_content, # Should use content from GCS
            use_legacy_sql=False,
            params=sql_params,
            gcp_conn_id='google_cloud_default',
        )
        self.mock_bq_operator_execute.assert_called_once_with(context=self.mock_kwargs)
        self.mock_logger.info.assert_called_with(f"Successfully executed BigQuery SQL script via reference: {gcs_path}")


    def test_gcs_sql_not_found(self):
        gcs_path = "gs://my-bucket/sql/non_existent.sql"
        entry_number = "004"

        self.mock_gcs_blob_exists.return_value = False

        with self.assertRaisesRegex(AirflowException, "GCS SQL script .* not found."):
            execute_bigquery_script(entry_number=entry_number, script_ref=gcs_path, **self.mock_kwargs)
        
        self.mock_logger.error.assert_called_with(f"Error {entry_number} E 201: GCS SQL script '{gcs_path}' not found or inaccessible.")
        self.mock_bq_operator_class.assert_not_called()
        self.mock_gcs_client_class.assert_called_once()


    def test_gcs_read_failure(self):
        gcs_path = "gs://my-bucket/sql/read_fail.sql"
        entry_number = "005"

        self.mock_gcs_blob_exists.return_value = True
        self.mock_gcs_blob_download_as_text.side_effect = Exception("Permission denied")

        with self.assertRaisesRegex(AirflowException, "Failed to read GCS SQL script .* Permission denied"):
            execute_bigquery_script(entry_number=entry_number, script_ref=gcs_path, **self.mock_kwargs)
        
        self.mock_logger.error.assert_called_with(f"Error {entry_number} E 201: Failed to read GCS SQL script '{gcs_path}': Permission denied")
        self.mock_bq_operator_class.assert_not_called()
        self.mock_gcs_client_class.assert_called_once()

    def test_bigquery_execution_failure(self):
        test_sql = "SELECT * FROM non_existent_table;"
        entry_number = "006"

        self.mock_bq_operator_execute.side_effect = AirflowException("BigQuery query failed.")

        with self.assertRaisesRegex(AirflowException, "BigQuery execution failed for .* BigQuery query failed."):
            execute_bigquery_script(entry_number=entry_number, script_ref=test_sql, **self.mock_kwargs)
        
        self.mock_logger.error.assert_called_with(f"Error executing BigQuery SQL for script reference '{test_sql}': BigQuery query failed.")
        self.mock_bq_operator_class.assert_called_once()


if __name__ == '__main__':
    unittest.main()