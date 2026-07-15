#!/usr/bin/env python3
"""
Shared utility helper routines for the Environment Configuration migration.
Provides parser functions and GCP Secret Manager integrations.
"""

import os
import logging
from typing import Dict, Optional

# Set up standard logger
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")
logger = logging.getLogger("dw_config_helpers")


def load_legacy_shell_exports(file_path: str, env_dict: Dict[str, str]) -> Dict[str, str]:
    """
    Parses a legacy POSIX profile file containing 'export KEY=VAL' or 'KEY=VAL'
    lines and merges them directly into the provided environment dictionary.
    
    :param file_path: Path to the physical profile configuration file.
    :param env_dict: Target dictionary to store extracted variables.
    :return: Updated environment dictionary.
    """
    if not os.path.exists(file_path):
        logger.warning(f"Sourced legacy configuration file was not found: {file_path}")
        return env_dict

    logger.info(f"Sourcing environment configurations from: {file_path}")
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                line = line.strip()
                # Skip comments and empty lines
                if not line or line.startswith('#'):
                    continue
                # Strip out POSIX shell prefix if it exists
                if line.startswith('export '):
                    line = line.replace('export ', '', 1)
                
                if '=' in line:
                    key, val = line.split('=', 1)
                    key = key.strip()
                    val = val.strip().strip('"').strip("'")
                    env_dict[key] = val
    except Exception as e:
        logger.error(f"Error parsing legacy configuration file '{file_path}': {str(e)}")
        
    return env_dict


def get_secret_from_gsm(secret_id: str, project_id: Optional[str] = None) -> Optional[str]:
    """
    Retrieves secret string values dynamically from GCP Secret Manager.
    Falls back gracefully to environmental variables or placeholders if not available.
    
    :param secret_id: Name/ID of the secret in GSM.
    :param project_id: Optional GCP Project ID. Resolves from environment if omitted.
    :return: Secret payload string or None.
    """
    resolved_project = project_id or os.environ.get("GCP_PROJECT")
    if not resolved_project:
        logger.warning(f"GCP_PROJECT not set. Cannot fetch secret '{secret_id}' from GSM.")
        return None

    try:
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{resolved_project}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except ImportError:
        logger.warning("google-cloud-secret-manager package not found. Skipping API fetch.")
        return None
    except Exception as e:
        logger.error(f"Failed to fetch secret '{secret_id}' from GCP Secret Manager: {str(e)}")
        return None