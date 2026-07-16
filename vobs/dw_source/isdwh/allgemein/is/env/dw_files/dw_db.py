"""
Migration Engine: Legacy Oracle `.dw_db` to GCP (Secret Manager & BigQuery)
Path: vobs/dw_source/isdwh/allgemein/is/env/dw_files/dw_db.py

This module provides reusable, modular, and secure utilities to:
1. Retrieve dynamic secrets from Google Secret Manager with retry capabilities.
2. Formulate and normalize localization configuration mappings (NLS translation).
3. Connect and execute safe logging / configuration persistence in BigQuery.
4. Provide a command-line interface and entry points for downstream orchestration pipelines.
"""

import os
import sys
import json
import logging
import uuid
from typing import Dict, Any, List, Optional
from datetime import datetime

# Import Google Cloud SDK Libraries
try:
    from google.cloud import secretmanager
    from google.cloud import bigquery
    from google.api_core.exceptions import GoogleAPICallError, RetryError
    from google.auth.exceptions import DefaultCredentialsError
except ImportError as e:
    raise ImportError(
        "Missing required Google Cloud client libraries. "
        "Please run: pip install google-cloud-secret-manager google-cloud-bigquery google-auth"
    ) from e

# Set up structured application logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("dw_db_migration_engine")


class SecretResolver:
    """Handles secure connection parameters retrieval from Google Secret Manager."""

    def __init__(self, project_id: str):
        self.project_id = project_id
        try:
            self.client = secretmanager.SecretManagerServiceClient()
        except DefaultCredentialsError as e:
            logger.critical("GCP Application Default Credentials (ADC) could not be resolved.")
            raise e

    def get_secret(self, secret_name: str, version: str = "latest") -> Dict[str, Any]:
        """
        Retrieves a secret payload from GCP Secret Manager and parses it as JSON.
        
        Args:
            secret_name (str): The logical name of the secret target.
            version (str): Target version. Defaults to "latest".
            
        Returns:
            Dict[str, Any]: Parsed JSON secret dictionary.
        """
        secret_path = f"projects/{self.project_id}/secrets/{secret_name}/versions/{version}"
        logger.info(f"Accessing Secret Manager payload: {secret_path}")
        
        try:
            response = self.client.access_secret_version(request={"name": secret_path})
            payload_str = response.payload.data.decode("UTF-8")
            return json.loads(payload_str)
        except GoogleAPICallError as e:
            logger.error(f"Failed to fetch secret from GCP API: {e}")
            raise RuntimeError(f"SecretVersionResolutionError: Could not resolve secret '{secret_name}'") from e
        except json.JSONDecodeError as e:
            logger.error("Secret payload retrieved successfully, but is not valid JSON.")
            raise ValueError("Secret payload must be structured as valid JSON string dictionary.") from e


class BigQueryIntegrationManager:
    """Manages operational configuration logging and analytical metrics updates in BigQuery."""

    def __init__(self, project_id: str, dataset_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        try:
            self.client = bigquery.Client(project=self.project_id)
        except DefaultCredentialsError as e:
            logger.critical("GCP Application Default Credentials (ADC) could not be resolved.")
            raise e

    def save_environment_config(self, table_name: str, configs: List[Dict[str, Any]]) -> bool:
        """
        Inserts operational environmental parameters into BQ registry table.
        
        Args:
            table_name (str): Target configuration table name.
            configs (List[Dict[str, Any]]): Records formatted to match schema.
        """
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_name}"
        logger.info(f"Persisting metadata block variables to target table: {table_ref}")
        
        try:
            errors = self.client.insert_rows_json(table_ref, configs)
            if not errors:
                logger.info("Successfully persisted environment settings mapping to BigQuery.")
                return True
            else:
                logger.error(f"Encountered errors inserting BQ settings records: {errors}")
                return False
        except Exception as e:
            logger.error(f"BigQueryJobError: Row insertion aborted. Stack trace: {e}")
            return False

    def write_audit_log(
        self,
        table_name: str,
        log_id: str,
        job_name: str,
        status: str,
        records_processed: int = 0,
        error_message: Optional[str] = None
    ) -> bool:
        """Writes execution runtime tracking and diagnostic outputs back to BigQuery audit tables."""
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_name}"
        log_record = {
            "log_id": log_id,
            "job_name": job_name,
            "execution_status": status,
            "records_processed": records_processed,
            "error_message": error_message,
            "started_at": datetime.utcnow().isoformat(),
            "finished_at": datetime.utcnow().isoformat() if status != "RUNNING" else None
        }
        
        try:
            errors = self.client.insert_rows_json(table_ref, [log_record])
            return not errors
        except Exception as e:
            logger.warning(f"Failed logging state checkpoint '{status}' to BigQuery: {e}")
            return False


class NLSNormalizer:
    """Translates Oracle regional and date configurations to GCP-compatible formats."""
    
    @staticmethod
    def map_character_set(legacy_nls_lang: str) -> str:
        """Maps legacy Oracle NLS_LANG inputs to standard GCP encoding patterns."""
        if "UTF8" in legacy_nls_lang.upper() or "UTF-8" in legacy_nls_lang.upper():
            return "UTF-8"
        # Default fallback standardized encoding target for GCP Streams
        return "UTF-8"

    @staticmethod
    def map_date_format(legacy_date_format: str) -> str:
        """Maps legacy Oracle date configuration patterns to standard formatting patterns."""
        # Mapping Oracle format standards to ISO Standard Parsing format
        mapping = {
            "YYYY-MM-DD HH24:MI:SS": "%Y-%m-%d %H:%M:%S",
            "YYYY-MM-DD": "%Y-%m-%d"
        }
        return mapping.get(legacy_date_format.upper().strip('"'), "%Y-%m-%d %H:%M:%S")


class MigrationEngine:
    """Coordinating Engine to securely build and resolve environment initialization properties."""

    def __init__(self, project_id: str, dataset_id: str, secret_name: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.secret_name = secret_name
        self.run_id = str(uuid.uuid4())
        
        self.secret_resolver = SecretResolver(self.project_id)
        self.bq_manager = BigQueryIntegrationManager(self.project_id, self.dataset_id)

    def initialize_environment(self) -> Dict[str, Any]:
        """
        Executes secure decoupled environmental loading.
        Replaces legacy sourcing of `.dw_db`.
        """
        self.bq_manager.write_audit_log(
            table_name="gcp_migration_audit_log",
            log_id=self.run_id,
            job_name="dw_db_migration_initialization",
            status="RUNNING"
        )
        
        try:
            # 1. Fetch parameters dynamically from GCP Secret Manager
            secret_payload = self.secret_resolver.get_secret(self.secret_name)
            
            # 2. Extract configuration contexts
            legacy_sid = secret_payload.get("ORACLE_SID", "DWPROD")
            legacy_nls_lang = secret_payload.get("NLS_LANG", "AMERICAN_AMERICA.AL32UTF8")
            legacy_date_format = secret_payload.get("NLS_DATE_FORMAT", "YYYY-MM-DD")
            
            # 3. Translate and map behaviors
            mapped_charset = NLSNormalizer.map_character_set(legacy_nls_lang)
            mapped_date_fmt = NLSNormalizer.map_date_format(legacy_date_format)
            
            resolved_env_map = {
                "config_id": self.run_id[:8],
                "legacy_sid": legacy_sid,
                "gcp_project_id": self.project_id,
                "bq_dataset_id": self.dataset_id,
                "nls_lang_mapped": mapped_charset,
                "secret_manager_path": f"projects/{self.project_id}/secrets/{self.secret_name}/versions/latest",
                "resolved_date_format": mapped_date_fmt,
                "db_execution_user": secret_payload.get("DB_USER_DWH", "sa-dwh-meyreis")
            }
            
            # 4. Save metadata settings verification to bigquery
            configs_to_save = [{
                "config_id": resolved_env_map["config_id"],
                "legacy_sid": resolved_env_map["legacy_sid"],
                "gcp_project_id": resolved_env_map["gcp_project_id"],
                "bq_dataset_id": resolved_env_map["bq_dataset_id"],
                "nls_lang_mapped": resolved_env_map["nls_lang_mapped"],
                "secret_manager_path": resolved_env_map["secret_manager_path"]
            }]
            
            bq_save_success = self.bq_manager.save_environment_config(
                table_name="dw_environment_configurations",
                configs=configs_to_save
            )
            
            if not bq_save_success:
                raise RuntimeError("Failed to log current environmental metrics configuration details to BigQuery.")
            
            # Complete execution successfully
            self.bq_manager.write_audit_log(
                table_name="gcp_migration_audit_log",
                log_id=self.run_id,
                job_name="dw_db_migration_initialization",
                status="SUCCESS",
                records_processed=1
            )
            
            return resolved_env_map

        except Exception as err:
            logger.critical(f"Aborting execution due to critical error: {err}")
            self.bq_manager.write_audit_log(
                table_name="gcp_migration_audit_log",
                log_id=self.run_id,
                job_name="dw_db_migration_initialization",
                status="FAILED",
                error_message=str(err)
            )
            raise err


def main() -> None:
    """Primary entrypoint. Validates local/Cloud environment variables, initializing the migration engine."""
    logger.info("Initializing configuration migration pipeline execution...")
    
    # Read systemic Environment variables injected by Cloud Composer / Airflow context
    project_id = os.environ.get("GCP_PROJECT_ID")
    dataset_id = os.environ.get("BQ_DATASET_ID", "migration_metadata")
    secret_name = os.environ.get("SECRET_NAME", "dw-database-access-credentials")

    if not project_id:
        logger.error("Required System Environment Variable 'GCP_PROJECT_ID' is missing.")
        sys.exit(1)

    try:
        engine = MigrationEngine(
            project_id=project_id,
            dataset_id=dataset_id,
            secret_name=secret_name
        )
        resolved_configs = engine.initialize_environment()
        
        # Output values for system integration processes / diagnostic validations
        logger.info("====================================================================")
        logger.info("ENVIRONMENT RESOLVED SUCCESSFULLY:")
        logger.info(json.dumps(resolved_configs, indent=2))
        logger.info("====================================================================")
        
        sys.exit(0)
    except Exception as run_error:
        logger.error(f"Execution failed with runtime exception trace: {run_error}")
        sys.exit(1)


if __name__ == "__main__":
    main()