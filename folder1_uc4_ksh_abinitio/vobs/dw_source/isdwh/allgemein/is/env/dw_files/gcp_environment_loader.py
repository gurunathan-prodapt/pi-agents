"""
gcp_environment_loader.py

A unified cloud-native environment configuration utility replacing legacy
Ab Initio and Oracle DWH environment scripts (.dw_global, .dw_ai, .dw_db, .dw_init).
"""

import json
import logging
from typing import Any, Dict
from google.cloud import secretmanager
from google.api_core.exceptions import GoogleAPIError
from airflow.models import Variable

# Initialize structured logging
logger = logging.getLogger("gcp_environment_loader")


class GCPEnvironmentLoader:
    """
    Engine to dynamically resolve execution paths, fetch database connection parameters,
    manage cluster scaling thresholds, and handle lifecycle task locks.
    """

    def __init__(self, project_id: str, environment_phase: str):
        """
        Initializes the configuration loader.

        Args:
            project_id: The target GCP project identifier.
            environment_phase: Execution environment tier ('dev', 'qa', or 'prod').
        """
        if not project_id:
            raise ValueError("project_id must be a non-empty string.")
        if environment_phase.lower() not in ["dev", "qa", "prod"]:
            raise ValueError("environment_phase must be one of: 'dev', 'qa', 'prod'.")

        self.project_id = project_id
        self.env = environment_phase.lower()
        self._secret_client = None

    @property
    def secret_client(self) -> secretmanager.SecretManagerServiceClient:
        """Laidly instantiates and caches the Secret Manager Client."""
        if self._secret_client is None:
            self._secret_client = secretmanager.SecretManagerServiceClient()
        return self._secret_client

    def resolve_global_paths(self, legacy_var_name: str) -> str:
        """
        Replaces legacy '.dw_global' dynamic filesystem routing.
        Maps historical logical variables to physical Cloud Storage paths.

        Args:
            legacy_var_name: Legacy environment variable (e.g., 'AI_SERIAL').

        Returns:
            Resolved Google Cloud Storage URI string.
        """
        mapping = {
            "AI_SERIAL": f"gs://dwh-landing-zone-{self.env}/serial_staging/",
            "AI_TEMP": f"gs://dwh-transient-zone-{self.env}/temp_processing/",
            "AI_MFS": f"gs://dwh-mfs-partitioned-{self.env}/mfs_data/",
        }

        if legacy_var_name in mapping:
            resolved_path = mapping[legacy_var_name]
            logger.info(f"Resolved legacy path {legacy_var_name} -> {resolved_path}")
            return resolved_path

        # Standard Airflow Variable Fallback for non-hardcoded configs
        airflow_var_key = f"{legacy_var_name.lower()}_{self.env}"
        try:
            resolved_path = Variable.get(airflow_var_key)
            logger.info(f"Resolved legacy variable from Airflow Config {airflow_var_key} -> {resolved_path}")
            return resolved_path
        except KeyError as err:
            raise KeyError(
                f"Unable to resolve global variable destination: {legacy_var_name}. "
                f"Airflow variable '{airflow_var_key}' is not defined."
            ) from err

    def fetch_database_profile(self, target_schema: str) -> Dict[str, Any]:
        """
        Replaces legacy '.dw_db' Oracle connection profiles.
        Fetches decrypted database credentials securely from Google Secret Manager.

        Args:
            target_schema: The physical source schema target (e.g., 'dw_core').

        Returns:
            Dict containing database connection credentials.
        """
        secret_id = f"db-conn-{target_schema}-{self.env}"
        secret_name = f"projects/{self.project_id}/secrets/{secret_id}/versions/latest"

        try:
            logger.info(f"Fetching credentials from Secret Manager: {secret_name}")
            response = self.secret_client.access_secret_version(request={"name": secret_name})
            payload_str = response.payload.data.decode("UTF-8")
            return json.loads(payload_str)
        except GoogleAPIError as api_err:
            raise ConnectionError(
                f"Failed to access database credentials for schema: {target_schema} via Secret Manager path "
                f"[{secret_name}]. Details: {str(api_err)}"
            ) from api_err
        except json.JSONDecodeError as json_err:
            raise ValueError(
                f"Secret data payload retrieved for schema '{target_schema}' is not valid JSON. "
                f"Details: {str(json_err)}"
            ) from json_err

    def fetch_execution_parallelism(self) -> Dict[str, Any]:
        """
        Replaces legacy '.dw_ai' MFS layout parameters.
        Determines execution worker limits and resource scaling boundaries.

        Returns:
            Dict containing target execution parameters for PySpark engine scaling.
        """
        airflow_var_key = f"mfs_depth_{self.env}"
        parallel_depth = Variable.get(airflow_var_key, default_var="8")

        scaling_profiles = {
            "dev": {"min_workers": 2, "max_workers": 5, "machine_type": "n1-standard-4"},
            "qa": {"min_workers": 2, "max_workers": 10, "machine_type": "n1-standard-8"},
            "prod": {"min_workers": 4, "max_workers": 30, "machine_type": "n1-standard-16"},
        }

        profile = scaling_profiles.get(self.env, scaling_profiles["dev"])
        return {
            "do_p": int(parallel_depth),
            "scaling": profile,
        }

    def job_initialization_handshake(self, job_name: str) -> str:
        """
        Replaces legacy '.dw_init' preprocessing.
        Evaluates system execution safety, locks, and parameter configurations.

        Args:
            job_name: Identifier of the executing workflow context.

        Returns:
            Success acknowledgement code string.
        """
        lock_key = f"lock_{job_name}_{self.env}"
        # Preserve original logging formats without translation
        print(f"[INIT RUN] Executing initialization handshake for process: {job_name} in environment: {self.env}")

        is_locked = Variable.get(lock_key, default_var="FALSE")
        if is_locked.upper() == "TRUE":
            raise PermissionError(
                f"Concurrency Lock Violation: {job_name} is already active or locked in environment {self.env}."
            )

        # Apply exclusive run-lock to Airflow Configuration DB
        Variable.set(lock_key, "TRUE")
        print(f"[INIT SUCCESS] System locked. Proceeding with execution of {job_name}.")
        return "SUCCESS"

    def job_finalization_release(self, job_name: str, exit_status: str) -> None:
        """
        Safely releases active process run locks within the configuration system.

        Args:
            job_name: Identifier of the executing workflow context.
            exit_status: Pipeline outcome string.
        """
        lock_key = f"lock_{job_name}_{self.env}"
        print(f"[FINALIZE] Executing environment cleanup routines for {job_name}. Exit Status: {exit_status}")
        Variable.set(lock_key, "FALSE")