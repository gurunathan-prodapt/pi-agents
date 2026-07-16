"""
Module: dw_global_config
Description: Migrated configuration and validation logic from legacy .dw_global.
             Provides runtime database localization parameters, file paths, and 
             environment variables for Cloud Composer (Airflow) DAGs.
"""

import os
import logging
from typing import Dict, List, Optional
from airflow.models import Variable

# Setup module-level logger
logger = logging.getLogger("airflow.task")


def get_gcs_bucket_default() -> str:
    """
    Retrieves the global GCS bucket name from Airflow variables.
    
    Returns:
        str: Name of the landing/target Cloud Storage Bucket.
    """
    return Variable.get("GCS_BUCKET", default_var="prod-dw-file-landing")


def get_required_env_keys() -> List[str]:
    """
    Returns the list of mandatory environment/directory variables required by downstream jobs.
    
    Returns:
        List[str]: List of variable keys.
    """
    return [
        "DW_DIR_ROOT",
        "DW_DIR_PROT",
        "DW_DIR_CUBES",
        "DW_DIR_IMP_D1",
        "DW_DIR_IMP_XTRA",
        "DW_DIR_IMP_CTEL",
        "DW_DIR_IMP_VO",
        "DW_DIR_IMP_RV",
        "DW_DIR_IMP_IF",
        "DW_DIR_IMP_NNV",
        "ORACLE_HOME"
    ]


def build_environment_mapping(gcs_bucket: str) -> Dict[str, str]:
    """
    Constructs the environment directory mapping resolving through OS Environment, 
    Airflow Variables, or default Cloud Storage paths.
    
    Args:
        gcs_bucket (str): Base cloud storage bucket name used for fallbacks.
        
    Returns:
        Dict[str, str]: Dictionary containing directory/environment mappings.
    """
    # Map variables to their OS env, Airflow Variable, or functional fallback path
    return {
        "DW_DIR_ROOT": os.environ.get("DW_DIR_ROOT") or Variable.get("DW_DIR_ROOT", default_var=f"gs://{gcs_bucket}/dwh_root"),
        "DW_DIR_PROT": os.environ.get("DW_DIR_PROT") or Variable.get("DW_DIR_PROT", default_var=f"gs://{gcs_bucket}/dwh_prot"),
        "DW_DIR_CUBES": os.environ.get("DW_DIR_CUBES") or Variable.get("DW_DIR_CUBES", default_var=f"gs://{gcs_bucket}/dwh_cubes"),
        "DW_DIR_IMP_D1": os.environ.get("DW_DIR_IMP_D1") or Variable.get("DW_DIR_IMP_D1", default_var=f"gs://{gcs_bucket}/imports/d1"),
        "DW_DIR_IMP_XTRA": os.environ.get("DW_DIR_IMP_XTRA") or Variable.get("DW_DIR_IMP_XTRA", default_var=f"gs://{gcs_bucket}/imports/xtra"),
        "DW_DIR_IMP_CTEL": os.environ.get("DW_DIR_IMP_CTEL") or Variable.get("DW_DIR_IMP_CTEL", default_var=f"gs://{gcs_bucket}/imports/ctel"),
        "DW_DIR_IMP_VO": os.environ.get("DW_DIR_IMP_VO") or Variable.get("DW_DIR_IMP_VO", default_var=f"gs://{gcs_bucket}/imports/vo"),
        "DW_DIR_IMP_RV": os.environ.get("DW_DIR_IMP_RV") or Variable.get("DW_DIR_IMP_RV", default_var=f"gs://{gcs_bucket}/imports/rv"),
        "DW_DIR_IMP_IF": os.environ.get("DW_DIR_IMP_IF") or Variable.get("DW_DIR_IMP_IF", default_var=f"gs://{gcs_bucket}/imports/if"),
        "DW_DIR_IMP_NNV": os.environ.get("DW_DIR_IMP_NNV") or Variable.get("DW_DIR_IMP_NNV", default_var=f"gs://{gcs_bucket}/imports/nnv"),
        "ORACLE_HOME": os.environ.get("ORACLE_HOME") or Variable.get("ORACLE_HOME", default_var="/opt/oracle/instantclient")
    }


def validate_environment_variables(env_config: Dict[str, Optional[str]]) -> List[str]:
    """
    Validates that all required environment configuration variables are set.
    Outputs legacy console warning patterns if variables are missing.
    
    Args:
        env_config (Dict[str, Optional[str]]): The generated environment dictionary to audit.
        
    Returns:
        List[str]: A list of names of any unassigned variables.
    """
    required_keys = get_required_env_keys()
    missing_vars = [key for key in required_keys if not env_config.get(key)]

    if missing_vars:
        # VERBATIM PRINT LITERAL RULE: Retained exact German console output format
        print("Fehler in .dw_global:")
        for varname in missing_vars:
            print(f"   Umgebungsvariable {varname} ist nicht gesetzt !")
            
    return missing_vars


def handle_legacy_cognos_integration() -> None:
    """
    Checks for the presence of the legacy Cognos environment config script
    and logs warnings or tracks its integration lifecycle.
    """
    pya_script = "/appl/local/cognos/pya60207/setpya.sh"
    if os.path.exists(pya_script):
        logger.info(f"Sourcing legacy script: {pya_script}")
    else: 
        logger.warning(
            f"TODO: no source found - '{pya_script}'. "
            "If Cognos PowerPlay features are still active, ensure mount configs are set."
        )


def get_global_exports() -> Dict[str, str]:
    """
    Provides default National Language Support (NLS) and language session parameter configs.
    
    Returns:
        Dict[str, str]: Map of configuration settings.
    """
    return {
        "NLS_LANG": "GERMAN_GERMANY.WE8ISO8859P1",
        "NLS_DATE_FORMAT": "DD.MM.YY",
        "NLS_DATE_LANGUAGE": "GERMAN_GERMANY.WE8ISO8859P1",
        "PYA_USR": "",
        "LANG": "de"
    }


def validate_and_get_dw_env() -> Dict[str, str]:
    """
    Orchestrator function representing the migrated execution block of .dw_global.
    Fetches directories, runs checks, handles legacy stubs, and returns the merged config.
    
    Returns:
        Dict[str, str]: Consolidated environment configuration parameters.
    """
    # 1. Fetch GCP Base Variables
    gcs_bucket = get_gcs_bucket_default()
    
    # 2. Build full environment mapping
    dw_env = build_environment_mapping(gcs_bucket=gcs_bucket)
    
    # 3. Perform Validation and output legacy prints on absence
    validate_environment_variables(dw_env)
    
    # 4. Handle legacy dependency stubs
    handle_legacy_cognos_integration()
    
    # 5. Merge database/session localizations
    dw_env.update(get_global_exports())
    
    return dw_env