#!/usr/bin/env python3
"""
Module: dwh_env_config.py
Description: Migrated environment config module replacing legacy .dw_ai, .dw_db, 
             .dw_global, and .dw_init scripts. Integrates with Airflow/GCP or 
             local environments cleanly.
"""

import os
import sys
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Setup structured logger
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s - %(message)s")
logger = logging.getLogger("dwh_env_config")

# Ensure Horizon Library Path is appended if present
DIR_LIB_PY = os.getenv('DIR_LIB_PY')
if DIR_LIB_PY and DIR_LIB_PY not in sys.path:
    sys.path.append(DIR_LIB_PY)


class DWHConfigManager:
    """Manages the initialization, validation, and loading of DWH configuration profiles."""

    def __init__(self, gcs_bucket_override: Optional[str] = None):
        # Respect standard ENV VARIABLE POLICY: Source global configurations dynamically from runtime
        from airflow.models import Variable
        try:
            self.gcs_bucket = gcs_bucket_override or Variable.get("GCS_BUCKET")
        except Exception:
            self.gcs_bucket = gcs_bucket_override or os.environ.get("GCS_BUCKET", "gs://your_project_dw_bucket")
        self.home_dir = f"{self.gcs_bucket}/dwh"
        self.configs: Dict[str, str] = {}

    def load_ai_profile(self) -> Dict[str, str]:
        """
        Replicates `.dw_ai` legacy environment configurations.
        Maps Ab Initio and project variables to active execution space.
        """
        logger.info("Initializing Ab Initio legacy environment configuration (.dw_ai)...")
        home_path = os.path.expanduser('~')

        ai_vars = {
            "AB_HOME": "/appl/local/abinitio/abinitio",
            "AB_AIR_ROOT": "/appl/local/abinitio/TMD_EME/eme_dev/repo",
            "AB_AIR_HOME": "/appl/local/abinitio/abinitio-V2-14",
            "ETL_Host": "dxcsa4.bn.detemobil.de",
            "ETL_Projekt": "BHB",
            "AI_PRIV_SAND_ROOT": os.path.join(home_path, "abinitio"),
            "AI_ENV_SAND_ROOT": "/appl/local/abinitio/sandboxes/DEV"
        }

        # Update system path safely
        current_path = os.environ.get('PATH', '')
        ab_bin = f"{ai_vars['AB_HOME']}/bin"
        if ab_bin not in current_path:
            os.environ['PATH'] = f"{current_path}.:{ab_bin}"

        self._export_to_os(ai_vars)
        return ai_vars

    def load_db_profile(self, use_secret_manager: bool = False, secret_id: Optional[str] = None) -> Dict[str, str]:
        """
        Replicates `.dw_db` database connections.
        Integrates with local variables or Cloud Secret Manager depending on execution environment.
        """
        logger.info("Initializing database connection configuration (.dw_db)...")
        
        # Standard default variables
        db_vars = {
            "NLS_LANG": "GERMAN_GERMANY.WE8ISO8859P1",
            "DB_TNS_NAME_DWH": "@eDWH3.devlab.de.tmo",
            "DB_USER_DWH": "meyreis",
            "DB_PASSWD_DWH": "<password encrypted with m_password>" # Fallback/Legacy placeholder
        }

        if use_secret_manager and secret_id:
            try:
                # Dynamic imports to avoid forcing GCP client dependencies in local test spaces
                from google.cloud import secretmanager
                client = secretmanager.SecretManagerServiceClient()
                response = client.access_secret_version(request={"name": secret_id})
                db_vars["DB_PASSWD_DWH"] = response.payload.data.decode("UTF-8")
                logger.info("Successfully fetched database password from Secret Manager.")
            except Exception as e:
                logger.warning(f"Could not fetch password from Secret Manager: {e}. Using default configurations.")

        self._export_to_os(db_vars)
        return db_vars

    def validate_global_profile(self) -> Tuple[bool, List[str]]:
        """
        Replicates validations present in `.dw_global`.
        Verifies existence of directories and exports localized run-time configurations.
        """
        logger.info("Validating global environment dependencies (.dw_global)...")
        required_env_vars = [ 
            "DW_DIR_ROOT", "DW_DIR_PROT", "DW_DIR_CUBES", "DW_DIR_IMP_D1",
            "DW_DIR_IMP_XTRA", "DW_DIR_IMP_CTEL", "DW_DIR_IMP_VO", "DW_DIR_IMP_RV",
            "DW_DIR_IMP_IF", "DW_DIR_IMP_NNV", "ORACLE_HOME"
        ]
        
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]

        if missing_vars:
            # OUTPUT/PRINT LITERAL RULE: Must match legacy print logs exactly
            print("Fehler in .dw_global:", file=sys.stderr)
            for missing_var in missing_vars:
                print(f"   Umgebungsvariable {missing_var} ist nicht gesetzt !", file=sys.stderr)
        
        # Set localization configurations
        localization_vars = {
            "NLS_LANG": "GERMAN_GERMANY.WE8ISO8859P1",
            "NLS_DATE_FORMAT": "DD.MM.YY",
            "NLS_DATE_LANGUAGE": "GERMAN_GERMANY.WE8ISO8859P1",
            "LANG": "de"
        }
        self._export_to_os(localization_vars)

        # Legacy Cognos bypass notification
        pya_script_path = "/appl/local/cognos/pya60207/setpya.sh"
        if os.path.exists(pya_script_path):
            logger.info(f"Legacy setup script found at {pya_script_path}. (Execution bypassed in Cloud environment)")

        return (len(missing_vars) == 0, missing_vars)

    def load_init_profile(self) -> Dict[str, str]:
        """
        Replicates directory mappings from `.dw_init`.
        Maps standard directory structures to GCS URIs/prefixes or path mappings.
        """
        logger.info("Initializing directory maps and infrastructure targets (.dw_init)...")
        
        init_vars = {
            "DW_DIR_ROOT": f"{self.home_dir}/aktuell",
            "DW_DIR_PROT": f"{self.home_dir}/daten/logfiles",
            "DW_DIR_CUBES": f"{self.home_dir}/daten/cubes",
            "DW_DIR_IMP_D1": f"{self.home_dir}/daten/d1",
            "DW_DIR_IMP_BWA": f"{self.home_dir}/daten/dpps/bwa",
            "DW_DIR_IMP_XTRA": f"{self.home_dir}/daten/xtra",
            "DW_DIR_IMP_CTEL": f"{self.home_dir}/daten/ctel",
            "DW_DIR_IMP_VO": f"{self.home_dir}/daten/vo",
            "DW_DIR_IMP_RV": f"{self.home_dir}/daten/rv",
            "DW_DIR_IMP_IF": f"{self.home_dir}/daten/ees",
            "DW_DIR_IMP_NNV": f"{self.home_dir}/daten/nnv",
            "DW_DIR_IMP_SIGMA": f"{self.home_dir}/daten/gd/sigma",
            "DW_DIR_EXP_SIGMA": f"{self.home_dir}/daten/gd/sigma/export",
            "DW_DIR_IMP_TRF": f"{self.home_dir}/daten/trf",
            "DW_DIR_IMP_AUF": f"{self.home_dir}/daten/sd/auf",
            "DW_DIR_IMP_GUT": f"{self.home_dir}/daten/sd/gut",
            "DW_DIR_IMP_KDG": f"{self.home_dir}/daten/sd/kdg",
            "DW_DIR_IMP_MP_KDG": f"{self.home_dir}/daten/mp/kdg",
            "DW_DIR_IMP_MP_TS": f"{self.home_dir}/daten/mp/ts",
            "DW_DIR_IMP_MP_ZM": f"{self.home_dir}/daten/mp/zm",
            "DW_DIR_IMP_TS": f"{self.home_dir}/daten/sd/ts",
            "DW_DIR_IMP_ZM": f"{self.home_dir}/daten/sd/zm",
            "DW_DIR_EXP": f"{self.home_dir}/daten/exporter",
            "DW_DIR_IMP_BPM": f"{self.home_dir}/daten/bm",
            "DW_DIR_IMP_ZTS": f"{self.home_dir}/daten/zts",
            "DW_DIR_IMP_VRS": f"{self.home_dir}/daten/vrs",
            "DW_DIR_IMP_BRUNET": f"{self.home_dir}/daten/brunet",
            "DW_DIR_IMP_DWH": f"{self.home_dir}/daten/dwh",
            "DW_DIR_IMP_PLATO": f"{self.home_dir}/daten/dwh/plato",
            "DW_DIR_IMP_CARMEN": f"{self.home_dir}/daten/carmen",
            "DW_DIR_IMP_SAP": f"{self.home_dir}/daten/sap",
            "DW_DIR_IMP_SR_RV": f"{self.home_dir}/daten/sap/sr_rv_dpps",
            "DW_DIR_IMP_SAP_L_GUTGR": f"{self.home_dir}/daten/sap/sap_l_gutgr",
            "DW_DIR_IMP_L_MAHNSTYP_IST": f"{self.home_dir}/daten/sap/mahn",
            "DW_DIR_IMP_L_MAHNV_FI": f"{self.home_dir}/daten/sap/mahn",
            "DW_DIR_IMP_L_MAHNV_IST": f"{self.home_dir}/daten/sap/mahn",
            "DW_DIR_IMP_L_GUTGR": f"{self.home_dir}/daten/sd/l_gutschr",
            "DW_DIR_IMP_L_LEIST": f"{self.home_dir}/daten/sd/l_leist",
            "DW_DIR_IMP_L_PROD": f"{self.home_dir}/daten/sd/l_prod",
            "DW_DIR_IMP_LKODE": f"{self.home_dir}/daten/sd/lkode",
            "DW_DIR_IMP_SUBSE": f"{self.home_dir}/daten/subse",
            "DW_DIR_SMS_PRG": f"{self.home_dir}/aktuell/allgemein/is/util",
            "DW_DIR_SMS_ADR": f"{self.home_dir}/daten/sms/adressen",
            "DW_DIR_SMS_TMP": f"{self.home_dir}/daten/sms/tmp",
            "DW_DIR_IMP_DPPS": f"{self.home_dir}/daten/dpps",
            "DW_DIR_IMP_PLANF2": f"{self.home_dir}/daten/planf2",
            "DW_HOST_CUSTOMER": "dxcst3.bn.detemobil.de"
        }

        # Resolve ORACLE_HOME dynamic fallback values with exact German legacy warning
        oracle_home = os.getenv("ORACLE_HOME")
        if not oracle_home:
            opt1, opt2 = Path("/appl/local/oracle/12.2.0.1.0"), Path("/appl/local/oracle/11.2.0")
            if opt1.is_dir():
                oracle_home = str(opt1)
            elif opt2.is_dir():
                oracle_home = str(opt2)
            else:
                # OUTPUT/PRINT LITERAL RULE: Restore exact original German error message verbatim
                print("Fehler in .dw_init:", file=sys.stderr)
                print("   Konnte ORACLE_HOME nicht setzen !", file=sys.stderr)
        
        if oracle_home:
            init_vars["ORACLE_HOME"] = oracle_home

        # Dynamic construction of Utility directory via ORACLE_SID
        oracle_sid = os.getenv("ORACLE_SID", "DEFAULT_SID")
        init_vars["DW_DIR_UTL_FILE"] = f"/appl/local/oracle/admin/{oracle_sid}/utl_file"

        self._export_to_os(init_vars)
        return init_vars

    def _export_to_os(self, target_dict: Dict[str, str]) -> None:
        """Utility module to bind environment mappings back into active execution space (os.environ)."""
        for key, val in target_dict.items():
            if val is not None:
                os.environ[key] = str(val)
                self.configs[key] = str(val)


def initialize_all(secret_id: Optional[str] = None) -> DWHConfigManager:
    """
    Convenience initializer helper. Runs config initialization in standard order:
    1. dw_init -> 2. dw_ai -> 3. dw_db -> 4. dw_global (Validation check)
    """
    manager = DWHConfigManager()
    manager.load_init_profile()
    manager.load_ai_profile()
    
    # Check if Secret ID is defined for DB config loading
    use_secrets = secret_id is not None
    manager.load_db_profile(use_secret_manager=use_secrets, secret_id=secret_id)
    
    is_valid, missing = manager.validate_global_profile()
    if not is_valid:
        logger.warning(f"Profile initialization completed with warnings. Missing values: {missing}")
    else:
        logger.info("All environment profiles successfully initialized and validated.")
        
    return manager


if __name__ == "__main__":
    # If run as a standalone script, initialize environment configurations and print state
    print("=== Executing Horizon Environment Profiling Initialization ===")
    config_state = initialize_all()
    print(f"\nInitialized [{len(config_state.configs)}] variables successfully.")
    print(f"Sample - DW_DIR_PROT: {os.environ.get('DW_DIR_PROT')}")