import unittest
from unittest.mock import MagicMock, patch
import json
import requests
from dags.utils.h_alis_rest_metaauth import MetaAuthClient, MetaAuthException

class TestMetaAuthClient(unittest.TestCase):

    def setUp(self):
        self.client = MetaAuthClient(
            rest_endpoint="https://mock-auth.internal/v1",
            gcp_project_id="mock-project"
        )

    @patch('dags.utils.h_alis_rest_metaauth.HAS_SECRET_MANAGER', True)
    @patch('google.cloud.secretmanager.SecretManagerServiceClient')
    def test_get_from_secret_manager_json_payload(self, mock_sm_client_class):
        # Setup mocking instances
        mock_client = MagicMock()
        mock_sm_client_class.return_value = mock_client
        
        mock_response = MagicMock()
        mock_response.payload.data = b'{"secret_value": "super-secret-key", "credential_type": "api_key"}'
        mock_client.access_secret_version.return_value = mock_response

        result = self.client.get_credentials("my-secret", auth_method="secret_manager")
        
        self.assertEqual(result["status"], "SUCCESS")
        self.assertEqual(result["source"], "google_secret_manager")
        self.assertEqual(result["secret_value"], "super-secret-key")
        self.assertEqual(result["credential_type"], "api_key")

    @patch('dags.utils.h_alis_rest_metaauth.HAS_AIRFLOW', True)
    @patch('airflow.hooks.base.BaseHook.get_connection')
    def test_get_from_airflow_connection(self, mock_get_connection):
        # Setup mock Airflow Connection
        mock_conn = MagicMock()
        mock_conn.login = "admin_user"
        mock_conn.get_password.return_value = "password123"
        mock_conn.extra_dejson = {"credential_type": "db_auth", "expires_in": 600}
        mock_get_connection.return_value = mock_conn

        result = self.client.get_credentials("airflow_conn_id", auth_method="airflow")
        
        self.assertEqual(result["status"], "SUCCESS")
        self.assertEqual(result["source"], "airflow_connection")
        self.assertEqual(result["secret_value"], "password123")
        self.assertEqual(result["login"], "admin_user")

    @patch('requests.Session.post')
    def test_get_from_legacy_rest_success(self, mock_post):
        # Mocking REST endpoint post response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "secret_value": "rest-session-token",
            "credential_type": "bearer_token",
            "expires_in": 3600
        }
        mock_post.return_value = mock_response

        result = self.client.get_credentials("legacy-client-token", auth_method="rest")

        self.assertEqual(result["status"], "SUCCESS")
        self.assertEqual(result["source"], "legacy_rest_api")
        self.assertEqual(result["secret_value"], "rest-session-token")

    @patch('requests.Session.post')
    def test_get_from_legacy_rest_failure(self, mock_post):
        mock_post.side_effect = requests.exceptions.Timeout("Connection timed out")
        
        with self.assertRaises(MetaAuthException):
            self.client.get_credentials("legacy-client-token", auth_method="rest")

if __name__ == '__main__':
    unittest.main()