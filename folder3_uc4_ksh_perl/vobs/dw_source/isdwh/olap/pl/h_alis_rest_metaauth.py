"""
Module: h_alis_rest_metaauth
Description: A secure, modern, and pythonic utility to retrieve authentication 
             credentials. Provides unified access to Google Cloud Secret Manager, 
             Apache Airflow Connections, and legacy on-premises REST endpoints.
Migration Target: Google Cloud Composer (Apache Airflow) / Python 3.11+
"""

import os
import json
import logging
import datetime
from typing import Dict, Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# Attempt imports for optional external cloud/airflow libraries.
# This ensures standard environments without these libraries (e.g. basic tests) 
# can still import the client without raising immediate ModuleNotFoundErrors.
try:
    from google.cloud import secretmanager
    HAS_SECRET_MANAGER = True
except ImportError:
    HAS_SECRET_MANAGER = False

try:
    from airflow.hooks.base import BaseHook
    from airflow.models import Variable
    HAS_AIRFLOW = True
except ImportError:
    HAS_AIRFLOW = False


class MetaAuthException(Exception):
    """Custom exception raised for all metadata authentication failures."""
    pass


class MetaAuthClient:
    """
    A unified credentials resolution client providing fallback adapters
    across Secret Manager, Airflow Connections, and Legacy REST services.
    """

    def __init__(
        self, 
        rest_endpoint: Optional[str] = None, 
        gcp_project_id: Optional[str] = None,
        retries: int = 3,
        backoff_factor: float = 1.0,
        timeout: int = 10
    ):
        """
        Initializes the MetaAuthClient.
        
        Sourcing Strategy:
        - GCP Project ID: Parameter -> Airflow Variable -> Environment Variable -> Fallback Default.
        - REST Endpoint: Parameter -> Airflow Variable -> Environment Variable -> Fallback Default.
        """
        self.logger = logging.getLogger("h_alis_rest_metaauth")
        
        # Configure the GCP Project ID
        self.gcp_project_id = gcp_project_id
        if not self.gcp_project_id and HAS_AIRFLOW:
            self.gcp_project_id = Variable.get("GCP_PROJECT", default_var=None)
        if not self.gcp_project_id:
            self.gcp_project_id = os.environ.get("GCP_PROJECT", "gcp-prod-data-project")

        # Configure the REST Endpoint
        self.rest_endpoint = rest_endpoint
        if not self.rest_endpoint and HAS_AIRFLOW:
            self.rest_endpoint = Variable.get("RESTMETAAUTH_URI", default_var=None)
        if not self.rest_endpoint:
            self.rest_endpoint = os.environ.get(
                "RESTMETAAUTH_URI", "http://localhost:8080/metaAuth/rest"
            )

        self.timeout = timeout
        self.session = self._build_http_session(retries, backoff_factor)

    def _build_http_session(self, retries: int, backoff_factor: float) -> requests.Session:
        """Configures a thread-safe HTTP Session with connection pooling and retries."""
        session = requests.Session()
        retry_strategy = Retry(
            total=retries,
            backoff_factor=backoff_factor,
            status_forcelist=[500, 502, 503, 504],
            raise_on_status=False
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def get_credentials(self, secret_id: str, auth_method: str = "auto") -> Dict[str, Any]:
        """
        Resolves credential parameters using the requested auth mechanism.
        Executes an fallback hierarchy if 'auto' is specified.
        
        :param secret_id: Logical identifier/key of target secret.
        :param auth_method: Strategy to use ('secret_manager', 'airflow', 'rest', 'auto').
        :return: Standardized payload dictionary.
        """
        self.logger.info(
            f"Resolving credential payload for target: '{secret_id}' using strategy: '{auth_method}'"
        )
        normalized_secret_id = self._apply_environment_isolation(secret_id)

        if auth_method == "secret_manager":
            return self._get_from_secret_manager(normalized_secret_id)
        elif auth_method == "airflow":
            return self._get_from_airflow_connection(normalized_secret_id)
        elif auth_method == "rest":
            return self._get_from_legacy_rest(normalized_secret_id)
        elif auth_method == "auto":
            return self._fallback_chain(normalized_secret_id)
        else:
            raise MetaAuthException(f"Unsupported authentication method: '{auth_method}'")

    def _apply_environment_isolation(self, secret_id: str) -> str:
        """
        Rule 3: Environment Isolation
        Detects active environment and automatically appends suffixes/prefixes if necessary.
        """
        env = os.environ.get("ENVIRONMENT", "DEV").upper()
        if env in ["PROD", "PRODUCTION"] and not secret_id.endswith("-prod"):
            return f"{secret_id}-prod"
        elif env in ["STAGE", "STAGING"] and not secret_id.endswith("-stage"):
            return f"{secret_id}-stage"
        return secret_id

    def _fallback_chain(self, secret_id: str) -> Dict[str, Any]:
        """Rule 1: Fallback Authentication Chain implementation."""
        # 1. Try Google Cloud Secret Manager
        try:
            self.logger.info("Attempting resolution via Google Cloud Secret Manager...")
            return self._get_from_secret_manager(secret_id)
        except Exception as e:
            self.logger.warning(f"Secret Manager resolution bypassed or failed: {e}")

        # 2. Try Airflow Connections
        try:
            self.logger.info("Attempting resolution via local Airflow Connection Hook...")
            return self._get_from_airflow_connection(secret_id)
        except Exception as e:
            self.logger.warning(f"Airflow Connection resolution bypassed or failed: {e}")

        # 3. Fallback to Legacy REST API
        self.logger.info("Defaulting resolution to legacy REST endpoint...")
        return self._get_from_legacy_rest(secret_id)

    def _get_from_secret_manager(self, secret_id: str) -> Dict[str, Any]:
        """Interacts natively with Google Cloud Secret Manager via gRPC."""
        if not HAS_SECRET_MANAGER:
            raise MetaAuthException(
                "google-cloud-secret-manager client library is not installed in this environment."
            )

        try:
            client = secretmanager.SecretManagerServiceClient()
            resource_name = f"projects/{self.gcp_project_id}/secrets/{secret_id}/versions/latest"
            response = client.access_secret_version(request={"name": resource_name})
            payload = response.payload.data.decode("UTF-8")

            # Try to safely decode JSON, otherwise represent as raw string
            try:
                secret_data = json.loads(payload)
            except json.JSONDecodeError:
                secret_data = {"secret_value": payload}

            self._apply_token_expiration_policy(secret_data)

            return {
                "status": "SUCCESS",
                "source": "google_secret_manager",
                "credential_type": secret_data.get("credential_type", "generic_secret"),
                "secret_value": secret_data.get("secret_value", payload),
                "expires_in": secret_data.get("expires_in"),
                "retrieved_at": datetime.datetime.utcnow().isoformat() + "Z"
            }
        except Exception as e:
            self.logger.error(f"GCP Secret Manager connection failed: {str(e)}")
            raise MetaAuthException(e)

    def _get_from_airflow_connection(self, secret_id: str) -> Dict[str, Any]:
        """Interacts with Apache Airflow's encrypted Metadata DB backend."""
        if not HAS_AIRFLOW:
            raise MetaAuthException(
                "apache-airflow framework packages are not installed in this environment."
            )

        try:
            connection = BaseHook.get_connection(secret_id)
            extra_payload = connection.extra_dejson or {}
            
            # Reconstruct standardized dictionary matching response signature
            secret_value = connection.get_password() or extra_payload.get("secret_value")
            
            return {
                "status": "SUCCESS",
                "source": "airflow_connection",
                "credential_type": extra_payload.get("credential_type", "airflow_generic"),
                "secret_value": secret_value,
                "login": connection.login,
                "extra": extra_payload,
                "expires_in": extra_payload.get("expires_in"),
                "retrieved_at": datetime.datetime.utcnow().isoformat() + "Z"
            }
        except Exception as e:
            self.logger.error(f"Airflow connection retrieval failed: {str(e)}")
            raise MetaAuthException(e)

    def _get_from_legacy_rest(self, secret_id: str) -> Dict[str, Any]:
        """Interacts securely with Legacy rest authentication services."""
        url = f"{self.rest_endpoint}/auth/token"
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "h_alis_rest_metaauth/2.0 (Python/3.11)"
        }
        payload = {
            "client_id": "airflow-composer-service-account",
            "resource": secret_id
        }

        try:
            # Enforces secure SSL Verification by default
            response = self.session.post(
                url, 
                json=payload, 
                headers=headers, 
                timeout=self.timeout,
                verify=True
            )
            response.raise_for_status()
            response_json = response.json()
            
            self._apply_token_expiration_policy(response_json)

            return {
                "status": "SUCCESS",
                "source": "legacy_rest_api",
                "credential_type": response_json.get("credential_type", "bearer_token"),
                "secret_value": response_json.get("secret_value"),
                "expires_in": response_json.get("expires_in"),
                "retrieved_at": datetime.datetime.utcnow().isoformat() + "Z"
            }
        except requests.exceptions.RequestException as re:
            self.logger.error(f"HTTP connection handshake failed with Legacy REST: {str(re)}")
            raise MetaAuthException(re)

    def _apply_token_expiration_policy(self, data: Dict[str, Any]) -> None:
        """
        Rule 2: Token Expiry and Refresh Windows
        Aggressively intercepts and warns if token is active but nearing 5-minute expiry.
        """
        expires_in = data.get("expires_in")
        if expires_in is not None:
            try:
                seconds_remaining = int(expires_in)
                if seconds_remaining <= 300:
                    self.logger.warning(
                        f"Active Token is near expiry (Remaining: {seconds_remaining}s). "
                        "Aggressive refresh is recommended."
                    )
            except ValueError:
                pass