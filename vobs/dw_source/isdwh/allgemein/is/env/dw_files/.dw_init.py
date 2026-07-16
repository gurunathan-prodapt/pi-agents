#!/usr/bin/env python3
"""
Migration of Environment and Variable Initialization Script.
Targeted for Google Cloud Composer / Cloud Run / BigQuery environments.

This script parses, maps, and initializes global and local configurations,
supporting both Google Cloud Storage (GCS) assets and local execution.
"""

import json
import os
import sys
import logging
from typing import Dict, Any, Optional

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("dw_env_init")

# Optional import of Airflow components for Composer environments
try:
    from airflow.models import Variable
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    logger.warning("Airflow models not available. Falling back to OS environment variables.")


class EnvInitializer:
    """
    Handles environment variable initialization, sourcing configuration parameters,
    and adapting directory structures for GCP/Cloud Composer execution.
    """

    def __init__(self, use_airflow_vars: bool = True):
        self.use_airflow_vars = use_airflow_vars
        self.home_dir = os.getenv("HOME", "/home/user")
        self.gcs_bucket = self._resolve_gcs_bucket()

    def _resolve_gcs_bucket(self) -> str:
        ""Resolves GCS bucket from Airflow Variables, OS environment, or defaults."""
        if self.use_airflow_vars and AIRFLOW_AVAILABLE:
            try:
                return Variable.get("GCS_BUCKET", default_var="your_project_id_bucket")
            except Exception as e:
                logger.debug(f"Could not fetch GCS_BUCKET from Airflow: {e}")
        
        return os.getenv("GCS_BUCKET_NAME", os.getenv("GCS_BUCKET", "your_project_id_bucket"))

    def resolve_placeholders(self, value: str) -> str:
        """
        Recursively replaces placeholder patterns such as $HOME or env variables in paths.
        """
        if not isinstance(value, str):
            return value
        
        # Replace legacy home indicators
        if "$HOME" in value:
            value = value.replace("$HOME", self.home_dir)
        if "${HOME}" in value:
            value = value.replace("${HOME}", self.home_dir)
            
        # Resolve any other environment variable references (e.g. $GCS_BUCKET)
        for key, val in os.environ.items():
            placeholder = f"${key}"
            placeholder_braces = f"${{{key}}}"
            if placeholder in value:
                value = value.replace(placeholder, val)
            if placeholder_braces in value:
                value = value.replace(placeholder_braces, val)
                
        return value

    def set_base_directories(self) -> Dict[str, str]:
        """
        Sets and registers base cloud directories mapping.
        """
        env_vars = {
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
            "DW_HOST_CUSTOMER": "dxcst3.bn.detemobil.de",
            "GCS_BUCKET": self.gcs_bucket
        }

        for var, path in env_vars.items():
            resolved_val = self.resolve_placeholders(path)
            os.environ[var] = resolved_val
        
        logger.info("Base environment variables registered.")
        return env_vars

    def check_oracle_home(self) -> Optional[str]:
        """
        Resolves the ORACLE_HOME location. Fallback logic matches legacy setup
        but logs warning configurations on serverless environments.
        """
        oracle_home = os.getenv("ORACLE_HOME")
        if not oracle_home:
            path_v1 = "/appl/local/oracle/12.2.0.1.0"
            path_v2 = "/appl/local/oracle/11.2.0"
            
            if os.path.isdir(path_v1):
                oracle_home = path_v1
            elif os.path.isdir(path_v2):
                oracle_home = path_v2
            else: 
                # Output identical warning text derived from original logic if applicable
                logger.warning(
                    "Fehler in .dw_init:\n"
                    "   Konnte ORACLE_HOME nicht setzen !"
                )
                return None
        
        os.environ["ORACLE_HOME"] = oracle_home
        logger.info(f"ORACLE_HOME set to: {oracle_home}")
        return oracle_home

    def load_env_from_json(self, filepath: str) -> bool:
        """
        Loads configuration key-value pairs from a JSON file.
        Provides modern alternative to parsing unstructured shell parameters.
        """
        if not os.path.exists(filepath):
            logger.debug(f"Config file not found: {filepath}")
            return False
            
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, dict):
                    for key, val in data.items():
                        resolved_val = self.resolve_placeholders(str(val))
                        os.environ[key] = resolved_val
                    logger.info(f"Successfully loaded variables from JSON: {filepath}")
                    return True
        except Exception as e:
            logger.error(f"Error reading JSON configuration file {filepath}: {e}")
            
        return False

    def load_legacy_env_file(self, filepath: str) -> bool:
        """
        Simulates sourcing a legacy shell script (.dw_global / .dw_lokal)
        by parsing exports and standard key=value definitions.
        """
        if not os.path.exists(filepath):
            logger.warning(f"Legacy script path does not exist: {filepath}")
            return False

        logger.info(f"Parsing legacy script file: {filepath}")
        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    
                    if line.startswith('export '):
                        line = line[7:]
                        
                    if '=' in line:
                        key, val = line.split('=', 1)
                        key = key.strip()
                        val = val.strip().strip('"\'')
                        
                        resolved_val = self.resolve_placeholders(val)
                        os.environ[key] = resolved_val
            return True
        except Exception as e:
            logger.error(f"Failed to parse legacy script {filepath}: {e}")
            return False

    def configure_oracle_sid(self) -> None:
        """Configures downstream path dependent on the selected ORACLE_SID."""
        oracle_sid = os.getenv("ORACLE_SID", "DEFAULT_SID")
        os.environ["DW_DIR_UTL_FILE"] = f"/appl/local/oracle/admin/{oracle_sid}/utl_file"
        logger.info(f"DW_DIR_UTL_FILE set for SID {oracle_sid}")


def initialize_environment(use_airflow: bool = True) -> EnvInitializer:
    """
    Standard Orchestrator Function to set up the entire environment state.
    """
    initializer = EnvInitializer(use_airflow_vars=use_airflow)
    
    # 1. Base Variables Mapping
    initializer.set_base_directories()
    
    # 2. Check Oracle Home configuration
    initializer.check_oracle_home()
    
    # 3. Source Global Configurations (Check for JSON variant first, then fall back to Shell)
    global_json = os.path.join(initializer.home_dir, ".dw_global.json")
    global_legacy = os.path.join(initializer.home_dir, ".dw_global")
    
    if not initializer.load_env_from_json(global_json):
        initializer.load_legacy_env_file(global_legacy)

    # 4. Source Local Configurations (Verify/Remediate .dw_lokal missing file risk)
    local_json = os.path.join(initializer.home_dir, ".dw_lokal.json")
    local_legacy = os.path.join(initializer.home_dir, ".dw_lokal")
    
    has_local = initializer.load_env_from_json(local_json) or initializer.load_legacy_env_file(local_legacy)
    if not has_local:
        logger.warning(
            "Local environment properties (.dw_lokal) could not be resolved. "
            "Please configure Airflow variables or establish a .dw_lokal.json file."
        )

    # 5. Build Dependent SID Variables
    initializer.configure_oracle_sid()
    
    return initializer


if __name__ == "__main__":
    # Execute as standalone process initializer for validation
    logger.info("Initializing environment migration setup...")
    init = initialize_environment(use_airflow=AIRFLOW_AVAILABLE)
    logger.info("Migration environment setup complete.")