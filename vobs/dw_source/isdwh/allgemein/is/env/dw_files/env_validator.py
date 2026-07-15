#!/usr/bin/env python3
"""
Target Migration Platform: Google Cloud Composer / Airflow Configuration Engine.
This module validates system-wide configurations, loads legacy path mappings, 
and resolves them to Target Google Cloud Storage URIs dynamically.
"""

import json
import logging
import os
from typing import Dict, Any, List, Optional

# Set up clean structured logging
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")
logger = logging.getLogger("EnvConfigurationEngine")


class ConfigurationEngine:
    """
    Handles loading, formatting, validation, and execution of runtime environment
    configurations for the migrated Information Service DWH platform.
    """

    MANDATORY_KEYS = [
        "DW_DIR_ROOT",
        "DW_DIR_PROT",
        "DW_DIR_CUBES",
        "DW_DIR_IMP_D1",
        "DW_DIR_IMP_XTRA",
        "DW_DIR_IMP_CTEL",
        "DW_DIR_IMP_VO",
        "DW_DIR_IMP_RV",
        "DW_DIR_IMP_IF",
        "DW_DIR_IMP_NNV"
    ]

    def __init__(self, gcs_bucket: Optional[str] = None):
        """
        Initializes the configuration engine.
        
        :param gcs_bucket: Target Google Cloud Storage bucket name (e.g. 'gs://dwh-isdwh-prod')
        """
        self.gcs_bucket = gcs_bucket or self._resolve_gcs_bucket_variable()
        self.resolved_paths: Dict[str, str] = {}
        self.ai_config: Dict[str, str] = {}

    def _resolve_gcs_bucket_variable(self) -> str:
        """
        Resolves GCS_BUCKET dynamically using either Cloud Composer (Airflow) variables,
        system environment variables, or fallback parameters.
        """
        # Try to resolve via Apache Airflow's model dynamically
        try:
            from airflow.models import Variable
            return Variable.get("GCS_BUCKET", default_var="gs://dwh-isdwh-fallback")
        except ImportError:
            # Fallback for localized validation tests outside Airflow
            return os.getenv("GCS_BUCKET", "gs://dwh-isdwh-local-dev")

    def load_path_config(self, filepath: str) -> Dict[str, str]:
        """
        Loads the POSIX-to-GCS target JSON mapping config and replaces dynamic placeholder values.
        
        :param filepath: Path to the gcs_paths.json file.
        :return: Resolved key-value dictionary containing target storage URIs.
        """
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Missing mandatory path map template: {filepath}")

        with open(filepath, 'r', encoding='utf-8') as file_handle:
            raw_paths: Dict[str, str] = json.load(file_handle)

        # Inject GCS_BUCKET variable dynamically across configuration maps
        self.resolved_paths = {
            key: val.replace("{GCS_BUCKET}", self.gcs_bucket) 
            for key, val in raw_paths.items()
        }
        
        logger.info("Successfully resolved %d runtime path configurations.", len(self.resolved_paths))
        return self.resolved_paths

    def load_ai_compat_config(self, filepath: str) -> Dict[str, str]:
        """
        Loads historical Ab Initio environmental configuration maps for downstream processes.
        
        :param filepath: Path to the env_ai_config.json configuration.
        """
        if not os.path.exists(filepath):
            logger.warning("Ab Initio compatibility configuration file not found at %s", filepath)
            return {}

        with open(filepath, 'r', encoding='utf-8') as file_handle:
            self.ai_config = json.load(file_handle)
            
        logger.info("Successfully loaded Ab Initio compatibility runtime parameters.")
        return self.ai_config

    def validate_runtime_environment(self) -> bool:
        """
        Validates whether all mandatory migration parameters are registered.
        Replaces legacy validation logic of .dw_global.
        
        :return: Boolean representing validation status.
        """
        validation_errors: List[str] = []

        # Validate existence of our primary target configuration paths
        for key in self.MANDATORY_KEYS:
            resolved_val = self.resolved_paths.get(key)
            if not resolved_val:
                validation_errors.append(key)

        if validation_errors:
            logger.error("Fehler in .dw_global:")
            for missing_key in validation_errors:
                logger.error("   Umgebungsvariable %s ist nicht gesetzt !", missing_key)
            return False

        logger.info("Configuration Validation successfully completed. All environment layers set.")
        return True

    def get_database_runtime_context(self) -> Dict[str, str]:
        """
        Returns BigQuery-specific global DB localization settings (replacing .dw_db & NLS configurations).
        """
        return {
            "NLS_LANG": "GERMAN_GERMANY.WE8ISO8859P1",
            "NLS_DATE_FORMAT": "DD.MM.YY",
            "NLS_DATE_LANGUAGE": "GERMAN_GERMANY.WE8ISO8859P1",
            "LANG": "de"
        }


# Modular Execution Example / Unit Test Verification
if __name__ == "__main__":
    # Simulate execution in a local workspace environment
    config_dir = os.path.join(os.path.dirname(__file__), "..", "config")
    paths_cfg_path = os.path.join(config_dir, "gcs_paths.json")
    ai_cfg_path = os.path.join(config_dir, "env_ai_config.json")

    # Initialize Engine (Injected GCS Bucket name simulating Airflow Runtime execution)
    engine = ConfigurationEngine(gcs_bucket="gs://dwh-isdwh-prod-migration")
    
    # 1. Resolve and Load POSIX to GCS Mapping Configurations (.dw_init migration)
    try:
        resolved_gcs_mapping = engine.load_path_config(paths_cfg_path)
        print("\n=== SAMPLE GCS RESOLVED PATHS ===")
        for path_key in list(resolved_gcs_mapping.keys())[:5]:
            print(f"{path_key} -> {resolved_gcs_mapping[path_key]}")
    except FileNotFoundError as err:
        logger.error("Skipping test run: %s", err)

    # 2. Load Ab Initio compatibility settings (.dw_ai migration)
    try:
        ai_parameters = engine.load_ai_compat_config(ai_cfg_path)
    except Exception as err:
         logger.error("Ab Initio config execution test error: %s", err)

    # 3. Perform Runtime Integrity Validations (.dw_global migration)
    validation_status = engine.validate_runtime_environment()
    print(f"\nEnvironment Validation Status Passed: {validation_status}")

    # 4. Fetch database context (.dw_db migration)
    db_ctx = engine.get_database_runtime_context()
    print(f"Database Local Context Target Maps: {db_ctx}")