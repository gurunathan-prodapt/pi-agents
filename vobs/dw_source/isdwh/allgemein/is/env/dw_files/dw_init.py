#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Module Name: dw_init
Description: Core Orchestration and Mapping Hub. Replaces filesystem endpoints 
             with GCS URI definitions dynamically linked to Cloud Buckets.
"""

import os
import sys
import logging
from dw_config_helper import GCPConfigHelper

logger = logging.getLogger("DWInit")

def load_db_credentials():
    """
    Initializes database parameters. Obtains secret keys securely 
    from Secret Manager in place of legacy plain-text or obfuscated configurations.
    """
    os.environ['DB_TNS_NAME_DWH'] = "@eDWH3.devlab.de.tmo"
    os.environ['DB_USER_DWH'] = "meyreis"
    
    # Retrieve credential securely from Cloud Secret Manager
    db_password = GCPConfigHelper.get_secret("DB_PASSWD_DWH", "<password encrypted with m_password>")
    os.environ['DB_PASSWD_DWH'] = db_password

def initialize_paths():
    """
    Maps legacy Unix directory structures dynamically to GCS cloud storage URIs
    and exports variables safely for execution environments.
    """
    gcs_root = GCPConfigHelper.get_gcs_root()
    
    # Core directories mapped logically as GCS prefixes
    path_mappings = {
        "DW_DIR_ROOT": f"{gcs_root}/dwh/core",
        "DW_DIR_PROT": f"{gcs_root}/dwh/logs",
        "DW_DIR_CUBES": f"{gcs_root}/dwh/cubes",
        
        # Ingestion & Interface path mappings
        "DW_DIR_IMP_D1": f"{gcs_root}/dwh/imports/d1",
        "DW_DIR_IMP_BWA": f"{gcs_root}/dwh/imports/dpps/bwa",
        "DW_DIR_IMP_XTRA": f"{gcs_root}/dwh/imports/xtra",
        "DW_DIR_IMP_CTEL": f"{gcs_root}/dwh/imports/ctel",
        "DW_DIR_IMP_VO": f"{gcs_root}/dwh/imports/vo",
        "DW_DIR_IMP_RV": f"{gcs_root}/dwh/imports/rv",
        "DW_DIR_IMP_IF": f"{gcs_root}/dwh/imports/ees",
        "DW_DIR_IMP_NNV": f"{gcs_root}/dwh/imports/nnv",
        "DW_DIR_IMP_SIGMA": f"{gcs_root}/dwh/imports/gd/sigma",
        "DW_DIR_EXP_SIGMA": f"{gcs_root}/dwh/exports/gd/sigma",
        "DW_DIR_IMP_TRF": f"{gcs_root}/dwh/imports/trf",
        
        # Customer MD and SD Modules
        "DW_DIR_IMP_AUF": f"{gcs_root}/dwh/imports/sd/auf",
        "DW_DIR_IMP_GUT": f"{gcs_root}/dwh/imports/sd/gut",
        "DW_DIR_IMP_KDG": f"{gcs_root}/dwh/imports/sd/kdg",
        "DW_DIR_IMP_TS": f"{gcs_root}/dwh/imports/sd/ts",
        "DW_DIR_IMP_ZM": f"{gcs_root}/dwh/imports/sd/zm",
        
        # CARMEN & SAP Imports
        "DW_DIR_IMP_CARMEN": f"{gcs_root}/dwh/imports/carmen",
        "DW_DIR_IMP_SAP": f"{gcs_root}/dwh/imports/sap",
        "DW_DIR_IMP_SR_RV": f"{gcs_root}/dwh/imports/sap/sr_rv_dpps",
        
        # Legacy Host Reference
        "DW_HOST_CUSTOMER": "dxcst3.bn.detemobil.de"
    }

    # Dynamic Oracle Client Path Resolution (If local bin runtime execution is required)
    oracle_home = os.getenv("ORACLE_HOME")
    if not oracle_home:
        for candidate in ["/appl/local/oracle/12.2.0.1.0", "/appl/local/oracle/11.2.0"]:
            if os.path.isdir(candidate):
                oracle_home = candidate
                break
        else: 
            # Warn user as required by the source control
            print("Fehler in .dw_init:")
            print("   Konnte ORACLE_HOME nicht setzen !")
            oracle_home = ""
    
    path_mappings["ORACLE_HOME"] = oracle_home
    path_mappings["DW_DIR_UTL_FILE"] = f"/appl/local/oracle/admin/DEFAULT_SID/utl_file"

    # Export mapping values safely into system execution runtime
    for env_var, target_path in path_mappings.items():
        os.environ[env_var] = target_path

    logger.info("Horizon Environment Paths successfully mapped to Target GCS structures.")

def main():
    logger.info("Starting System Setup Initialization...")
    load_db_credentials()
    initialize_paths()
    
    # Source Global Config
    try:
        import dw_global
        dw_global.setup_global_environment()
    except ImportError:
        logger.warning("Could not import dw_global")

if __name__ == "__main__":
    main()