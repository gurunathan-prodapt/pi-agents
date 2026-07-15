#!/usr/bin/env python3
"""
Master environment bootstrapper. Maps target configurations to GCS buckets,
resolves environment dependencies, and sources legacy profile overrides.
"""

import os
import sys
from typing import Dict

# Append execution context's library path
sys.path.append(os.getenv('DIR_LIB_PY', ''))

from dags.config.helpers import load_legacy_shell_exports
from dags.config.dw_ai import configure_ab_initio_env
from dags.config.dw_db import get_database_configs


def build_gcs_path_map(bucket_url: str) -> Dict[str, str]:
    """
    Maps legacy physical directories to modern Google Cloud Storage prefix patterns.
    """
    return {
        'DW_DIR_ROOT': f"{bucket_url}/aktuell",
        'DW_DIR_PROT': f"{bucket_url}/daten/logfiles",
        'DW_DIR_CUBES': f"{bucket_url}/daten/cubes",
        'DW_DIR_IMP_D1': f"{bucket_url}/daten/d1",
        'DW_DIR_IMP_BWA': f"{bucket_url}/daten/dpps/bwa",
        'DW_DIR_IMP_XTRA': f"{bucket_url}/daten/xtra",
        'DW_DIR_IMP_CTEL': f"{bucket_url}/daten/ctel",
        'DW_DIR_IMP_VO': f"{bucket_url}/daten/vo",
        'DW_DIR_IMP_RV': f"{bucket_url}/daten/rv",
        'DW_DIR_IMP_IF': f"{bucket_url}/daten/ees",
        'DW_DIR_IMP_NNV': f"{bucket_url}/daten/nnv",
        'DW_DIR_IMP_SIGMA': f"{bucket_url}/daten/gd/sigma",
        'DW_DIR_EXP_SIGMA': f"{bucket_url}/daten/gd/sigma/export",
        'DW_DIR_IMP_TRF': f"{bucket_url}/daten/trf",
        'DW_DIR_IMP_AUF': f"{bucket_url}/daten/sd/auf",
        'DW_DIR_IMP_GUT': f"{bucket_url}/daten/sd/gut",
        'DW_DIR_IMP_KDG': f"{bucket_url}/daten/sd/kdg",
        'DW_DIR_IMP_MP_KDG': f"{bucket_url}/daten/mp/kdg",
        'DW_DIR_IMP_MP_TS': f"{bucket_url}/daten/mp/ts",
        'DW_DIR_IMP_MP_ZM': f"{bucket_url}/daten/mp/zm",
        'DW_DIR_IMP_TS': f"{bucket_url}/daten/sd/ts",
        'DW_DIR_IMP_ZM': f"{bucket_url}/daten/sd/zm",
        'DW_DIR_EXP': f"{bucket_url}/daten/exporter",
        'DW_DIR_IMP_BPM': f"{bucket_url}/daten/bm",
        'DW_DIR_IMP_ZTS': f"{bucket_url}/daten/zts",
        'DW_DIR_IMP_VRS': f"{bucket_url}/daten/vrs",
        'DW_DIR_IMP_BRUNET': f"{bucket_url}/daten/brunet",
        'DW_DIR_IMP_DWH': f"{bucket_url}/daten/dwh",
        'DW_DIR_IMP_PLATO': f"{bucket_url}/daten/dwh/plato",
        'DW_DIR_IMP_CARMEN': f"{bucket_url}/daten/carmen",
        'DW_DIR_IMP_SAP': f"{bucket_url}/daten/sap",
        'DW_DIR_IMP_SR_RV': f"{bucket_url}/daten/sap/sr_rv_dpps",
        'DW_DIR_IMP_SAP_L_GUTGR': f"{bucket_url}/daten/sap/sap_l_gutgr",
        'DW_DIR_IMP_L_MAHNSTYP_IST': f"{bucket_url}/daten/sap/mahn",
        'DW_DIR_IMP_L_MAHNV_FI': f"{bucket_url}/daten/sap/mahn",
        'DW_DIR_IMP_L_MAHNV_IST': f"{bucket_url}/daten/sap/mahn",
        'DW_DIR_IMP_L_GUTGR': f"{bucket_url}/daten/sd/l_gutschr",
        'DW_DIR_IMP_L_LEIST': f"{bucket_url}/daten/sd/l_leist",
        'DW_DIR_IMP_L_PROD': f"{bucket_url}/daten/sd/l_prod",
        'DW_DIR_IMP_LKODE': f"{bucket_url}/daten/sd/lkode",
        'DW_DIR_IMP_SUBSE': f"{bucket_url}/daten/subse",
        'DW_DIR_SMS_PRG': f"{bucket_url}/aktuell/allgemein/is/util",
        'DW_DIR_SMS_ADR': f"{bucket_url}/daten/sms/adressen",
        'DW_DIR_SMS_TMP': f"{bucket_url}/daten/sms/tmp",
        'DW_DIR_IMP_DPPS': f"{bucket_url}/daten/dpps",
        'DW_DIR_IMP_PLANF2': f"{bucket_url}/daten/planf2",
    }


def resolve_oracle_home() -> str:
    """
    Checks physical installation paths on the filesystem to resolve ORACLE_HOME dynamically.
    """
    for candidate in ['/appl/local/oracle/12.2.0.1.0', '/appl/local/oracle/11.2.0']:
        if os.path.isdir(candidate):
            return candidate
    return ""


def bootstrap_environment() -> None:
    """
    Initializes path structures, executes configuration steps, and exports
    the resulting values directly to the process execution environment.
    """
    env_context = {}

    # 1. Resolve Global GCS Storage Variables
    gcs_project_bucket = os.environ.get("GCS_BUCKET") or "gs://your_project_id_gcs_bucket"
    env_context.update(build_gcs_path_map(gcs_project_bucket))
    
    # 2. Add System Infrastructure Metadata
    env_context['DW_HOST_CUSTOMER'] = "dxcst3.bn.detemobil.de"
    
    # 3. Resolve and Set ORACLE_HOME
    oracle_home = os.getenv('ORACLE_HOME', resolve_oracle_home())
    if oracle_home:
        env_context['ORACLE_HOME'] = oracle_home
    else:
        print("Fehler in .dw_init:")
        print("   Konnte ORACLE_HOME nicht setzen !")

    # 4. Parse Legacy User Overrides (simulating POSIX sourcing)
    user_home = os.path.expanduser('~')
    env_context = load_legacy_shell_exports(os.path.join(user_home, '.dw_global'), env_context)
    env_context = load_legacy_shell_exports(os.path.join(user_home, '.dw_lokal'), env_context)

    # 5. Populate Secondary Variables Depending on Sourced Contexts
    oracle_sid = env_context.get('ORACLE_SID', os.getenv('ORACLE_SID', 'DEFAULT_SID'))
    env_context['DW_DIR_UTL_FILE'] = f"/appl/local/oracle/admin/{oracle_sid}/utl_file"

    # 6. Apply database & localization contexts
    db_configs = get_database_configs()
    env_context.update(db_configs)

    # 7. Write runtime parameters back to the OS execution context
    for key, value in env_context.items():
        os.environ[key] = str(value)
        
    # 8. Setup Ab Initio variables
    configure_ab_initio_env()

    print("Environment setup completed successfully.")


def main():
    bootstrap_environment()


if __name__ == '__main__':
    main()