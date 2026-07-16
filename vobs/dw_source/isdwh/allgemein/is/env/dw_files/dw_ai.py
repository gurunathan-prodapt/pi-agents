"""
Module: dw_ai
Path: vobs/dw_source/isdwh/allgemein/is/env/dw_files/dw_ai.py

Migrates legacy Ab Initio environment setup variables (.dw_ai) into a structured
Python configuration module. Provides helper functions to inject configurations
directly into the execution environment or export them as standard formats for Airflow.
"""

import os
import sys
import logging

# Setup Logging
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s - %(message)s")
logger = logging.getLogger(__name__)

# Ensure library path for Horizon is included (as per design requirement)
DIR_LIB_PY = os.getenv('DIR_LIB_PY', '')
if DIR_LIB_PY and DIR_LIB_PY not in sys.path:
    sys.path.append(DIR_LIB_PY)


def get_default_env_config() -> dict:
    """
    Returns the legacy environment configuration mapping.
    
    Resolves dynamic paths (like HOME) using current environment defaults, 
    with fallbacks for transition-phase compatibility.
    """
    home_dir = os.getenv('HOME', '/home/user')
    
    return {
        "AB_HOME": "/appl/local/abinitio/abinitio",
        "AB_AIR_ROOT": "/appl/local/abinitio/TMD_EME/eme_dev/repo",
        "AB_AIR_HOME": "/appl/local/abinitio/abinitio-V2-14",
        "ETL_Host": "dxcsa4.bn.detemobil.de",
        "ETL_Projekt": "BHB",
        "AI_PRIV_SAND_ROOT": f"{home_dir}/abinitio",
        "AI_ENV_SAND_ROOT": "/appl/local/abinitio/sandboxes/DEV"
    }


def inject_variables_to_environ(config: dict) -> None:
    """
    Injects a dictionary of environment variables directly into the OS environment.
    """
    for key, val in config.items():
        os.environ[key] = str(val)
        logger.info("Configured Environment Variable: %s = %s", key, val)


def append_legacy_path(ab_home_path: str) -> None:
    """
    Appends the legacy Ab Initio binary path to the system execution PATH.
    """
    additional_path = f"{ab_home_path}/bin"
    current_path = os.environ.get("PATH", "")
    
    # Avoid duplicate appends
    if additional_path not in current_path:
        os.environ["PATH"] = f"{current_path}:{additional_path}".strip(":")
        logger.info("System PATH updated with legacy Ab Initio binary path: %s", additional_path)
    else:
        logger.info("Legacy Ab Initio binary path already present in system PATH.")


def get_airflow_variables(config: dict) -> dict:
    """
    Adapts the environment variables into Airflow Variable format structures.
    Useful for populating dag_run configurations or triggering downstream Airflow tasks.
    """
    return {
        "gcp_project_fallback_host": config.get("ETL_Host"),
        "etl_projekt": config.get("ETL_Projekt"),
        "legacy_ab_initio_config": {
            "AB_HOME": config.get("AB_HOME"),
            "AB_AIR_ROOT": config.get("AB_AIR_ROOT"),
            "AB_AIR_HOME": config.get("AB_AIR_HOME"),
            "AI_PRIV_SAND_ROOT": config.get("AI_PRIV_SAND_ROOT"),
            "AI_ENV_SAND_ROOT": config.get("AI_ENV_SAND_ROOT")
        }
    }


def main() -> None:
    """
    Main entry point for local execution/testing.
    Generates configuration, sets up the runtime environment, and updates pathing.
    """
    logger.info("Initializing migrated environment setup wrapper...")
    
    # 1. Fetch configurations
    legacy_config = get_default_env_config()
    
    # 2. Inject configurations into system variables
    inject_variables_to_environ(legacy_config)
    
    # 3. Handle specific path adjustments
    append_legacy_path(legacy_config["AB_HOME"])
    
    logger.info("Environment setup wrapper executed successfully.")


if __name__ == "__main__":
    main()