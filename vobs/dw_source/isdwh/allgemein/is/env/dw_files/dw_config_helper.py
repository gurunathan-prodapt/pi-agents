#!/usr/bin/env python3
# -*- coding: utf-8(-*-
"""
Module Name: dw_config_helper
Description: Reusable utility class for managing environment validation,
             GCP/GCS environment mapping, and Secret Manager access.
"""

import os
import sys
import logging

# Setup standardized logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DWConfigHelper")

class GCPConfigHelper:
    """
    Utility helper to centralize GCP project parameters, environment variables,
    GCS pathing resolutions, and runtime configuration validation.
    """
    
    @staticmethod
    def get_gcp_variable(key: str, default_val: str = "") -> str:
        """
        Retrieves variable from OS Environment or Airflow Variables.
        """
        val = os.environ.get(key)
        if val:
            return val
            
        try:
            from airflow.models import Variable
            return Variable.get(key, default_val)
        except ImportError:
            return default_val

    @classmethod
    def get_gcs_root(cls) -> str:
        """Returns the logical GCS Target Bucket root URI."""
        bucket_name = cls.get_gcp_variable("GCS_BUCKET", "")
        if not bucket_name:
            return "gs://your_gcs_data_bucket"
        return f"gs://{bucket_name.replace('gs://', '')}"

    @classmethod
    def validate_environment(cls, required_vars: list) -> bool:
        """
        Validates the presence of required environment variables.
        Logs standard warnings and returns verification status.
        """
        missing_vars = [var for var in required_vars if not os.environ.get(var)]
        if missing_vars:
            print("Fehler in .dw_global:", file=sys.stderr)
            for varname in missing_vars:
                print(f"   Umgebungsvariable {varname} ist nicht gesetzt !", file=sys.stderr)
            return False
        return True

    @staticmethod
    def get_secret(secret_id: str, default: str = "") -> str:
        """
        Retrieves a credential/password securely from Google Secret Manager.
        Replaces legacy plain-text or m_password configurations.
        """
        project_id = os.environ.get("GCP_PROJECT")
        if not project_id:
            logger.warning("GCP_PROJECT not set. Cannot retrieve Secret: %s. Returning default.", secret_id)
            return default
            
        try:
            from google.cloud import secretmanager
            client = secretmanager.SecretManagerServiceClient()
            name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
            response = client.access_secret_version(request={"name": name})
            return response.payload.data.decode("UTF-8")
        except Exception as e:
            logger.error("Failed to retrieve secret %s: %s. Using fallback.", secret_id, str(e))
            return default