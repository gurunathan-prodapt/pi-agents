#!/usr/bin/env python3
"""
Database Credential and Session Localization Configurations.
Implements secure Secret Manager lookups with standard fallbacks.
"""

import os
import sys
from typing import Dict

# Append execution context's library path
sys.path.append(os.getenv('DIR_LIB_PY', ''))

from dags.config.helpers import get_secret_from_gsm

try:
    from framework.core.lib import script
except ImportError:
    script = None


def get_database_configs() -> Dict[str, str]:
    """
    Builds the database configuration environment parameters.
    Fetches credentials securely if GCP integrations are configured.
    """
    db_env = {}
    
    # Legacy connection parameters
    db_env['DB_TNS_NAME_DWH'] = "@eDWH3.devlab.de.tmo"
    db_env['DB_USER_DWH'] = "meyreis"
    
    # Fetch Password Securely from GCP Secret Manager (Fallback to legacy string)
    secret_pass = get_secret_from_gsm("DB_PASSWD_DWH")
    if secret_pass:
        db_env['DB_PASSWD_DWH'] = secret_pass
    else:
        db_env['DB_PASSWD_DWH'] = "<password encrypted with m_password>"
        
    # Oracle-specific localization configurations
    db_env['NLS_LANG'] = "GERMAN_GERMANY.WE8ISO8859P1"
    
    return db_env


def main():
    db_configs = get_database_configs()
    for key, value in db_configs.items():
        os.environ[key] = value
    print("[INFO] Database context configuration variables loaded.")


if __name__ == "__main__":
    main()