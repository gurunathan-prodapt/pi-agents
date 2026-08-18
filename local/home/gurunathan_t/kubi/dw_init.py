#!/usr/bin/env python3
import os
import sys
import logging
from pathlib import Path

# Configure logging for reporting setup errors
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def main():
    # Step 1: Establish base environment directories
    home = os.environ.get("HOME", "")
    if not home:
        home = str(Path.home())

    # Step 2: Initialize DW Directory Paths and populate os.environ
    os.environ["DW_DIR_ROOT"] = os.path.join(home, "aktuell")
    os.environ["DW_DIR_PROT"] = os.path.join(home, "daten/logfiles")
    os.environ["DW_DIR_CUBES"] = os.path.join(home, "daten/cubes")

    os.environ["DW_DIR_IMP_D1"] = os.path.join(home, "daten/d1")
    os.environ["DW_DIR_IMP_BWA"] = os.path.join(home, "daten/dpps/bwa")
    os.environ["DW_DIR_IMP_XTRA"] = os.path.join(home, "daten/xtra")
    os.environ["DW_DIR_IMP_CTEL"] = os.path.join(home, "daten/ctel")
    os.environ["DW_DIR_IMP_VO"] = os.path.join(home, "daten/vo")
    os.environ["DW_DIR_IMP_RV"] = os.path.join(home, "daten/rv")
    os.environ["DW_DIR_IMP_IF"] = os.path.join(home, "daten/ees")
    os.environ["DW_DIR_IMP_NNV"] = os.path.join(home, "daten/nnv")
    os.environ["DW_DIR_IMP_SIGMA"] = os.path.join(home, "daten/gd/sigma")
    os.environ["DW_DIR_EXP_SIGMA"] = os.path.join(home, "daten/gd/sigma/export")
    os.environ["DW_DIR_IMP_TRF"] = os.path.join(home, "daten/trf")
    os.environ["DW_DIR_IMP_AUF"] = os.path.join(home, "daten/sd/auf")
    os.environ["DW_DIR_IMP_GUT"] = os.path.join(home, "daten/sd/gut")
    os.environ["DW_DIR_IMP_KDG"] = os.path.join(home, "daten/sd/kdg")
    os.environ["DW_DIR_IMP_MP_KDG"] = os.path.join(home, "daten/mp/kdg")
    os.environ["DW_DIR_IMP_MP_TS"] = os.path.join(home, "daten/mp/ts")
    os.environ["DW_DIR_IMP_MP_ZM"] = os.path.join(home, "daten/mp/zm")
    os.environ["DW_DIR_IMP_TS"] = os.path.join(home, "daten/sd/ts")
    os.environ["DW_DIR_IMP_ZM"] = os.path.join(home, "daten/sd/zm")
    os.environ["DW_DIR_EXP"] = os.path.join(home, "daten/exporter")
    os.environ["DW_DIR_IMP_BPM"] = os.path.join(home, "daten/bm")
    os.environ["DW_DIR_IMP_ZTS"] = os.path.join(home, "daten/zts")
    os.environ["DW_DIR_IMP_VRS"] = os.path.join(home, "daten/vrs")

    os.environ["DW_DIR_IMP_BRUNET"] = os.path.join(home, "daten/brunet")
    os.environ["DW_DIR_IMP_DWH"] = os.path.join(home, "daten/dwh")
    os.environ["DW_DIR_IMP_PLATO"] = os.path.join(home, "daten/dwh/plato")

    os.environ["DW_DIR_IMP_CARMEN"] = os.path.join(home, "daten/carmen")
    os.environ["DW_DIR_IMP_SAP"] = os.path.join(home, "daten/sap")
    os.environ["DW_DIR_IMP_SR_RV"] = os.path.join(home, "daten/sap/sr_rv_dpps")

    # REVIEW: export DW_DIR_IMP_SAP_L does not match assigned variable name DW_DIR_IMP_SAP_L_GUTGR; check if this was a legacy typo.
    os.environ["DW_DIR_IMP_SAP_L_GUTGR"] = os.path.join(home, "daten/sap/sap_l_gutgr")
    os.environ["DW_DIR_IMP_SAP_L"] = os.environ["DW_DIR_IMP_SAP_L_GUTGR"]

    os.environ["DW_DIR_IMP_L_MAHNSTYP_IST"] = os.path.join(home, "daten/sap/mahn")
    os.environ["DW_DIR_IMP_L_MAHNV_FI"] = os.path.join(home, "daten/sap/mahn")
    os.environ["DW_DIR_IMP_L_MAHNV_IST"] = os.path.join(home, "daten/sap/mahn")
    os.environ["DW_DIR_IMP_L_GUTGR"] = os.path.join(home, "daten/sd/l_gutschr")
    os.environ["DW_DIR_IMP_L_LEIST"] = os.path.join(home, "daten/sd/l_leist")
    os.environ["DW_DIR_IMP_L_PROD"] = os.path.join(home, "daten/sd/l_prod")
    os.environ["DW_DIR_IMP_LKODE"] = os.path.join(home, "daten/sd/lkode")

    os.environ["DW_DIR_IMP_SUBSE"] = os.path.join(home, "daten/subse")

    os.environ["DW_DIR_SMS_PRG"] = os.path.join(home, "aktuell/allgemein/is/util")
    os.environ["DW_DIR_SMS_ADR"] = os.path.join(home, "daten/sms/adressen")
    os.environ["DW_DIR_SMS_TMP"] = os.path.join(home, "daten/sms/tmp")

    os.environ["DW_DIR_IMP_DPPS"] = os.path.join(home, "daten/dpps")
    os.environ["DW_DIR_IMP_PLANF2"] = os.path.join(home, "daten/planf2")

    # Step 3: Configure Remote Hosts
    os.environ["DW_HOST_CUSTOMER"] = "dxcst3.bn.detemobil.de"

    # Step 4: Resolve ORACLE_HOME path
    # Note: Since the target platform is BIGQUERY, Oracle settings are retained purely for legacy compatibility.
    if not os.environ.get("ORACLE_HOME"):
        if os.path.isdir("/appl/local/oracle/12.2.0.1.0"):
            os.environ["ORACLE_HOME"] = "/appl/local/oracle/12.2.0.1.0"
        elif os.path.isdir("/appl/local/oracle/11.2.0"):
            os.environ["ORACLE_HOME"] = "/appl/local/oracle/11.2.0"
        else:
            print("Fehler in .dw_init:", file=sys.stderr)
            print("   Konnte ORACLE_HOME nicht setzen !", file=sys.stderr)

    # Step 5: Load sibling environmental files
    # REVIEW-STRUCT: environment file $HOME/.dw_global not supplied — variables it sets are unknown; do not guess their names or values
    dw_global_path = os.path.join(home, ".dw_global")
    if os.path.exists(dw_global_path):
        # Load parameters from .dw_global into runtime context if possible
        pass

    # REVIEW-STRUCT: environment file $HOME/.dw_lokal not supplied — variables it sets are unknown; do not guess their names or values
    dw_lokal_path = os.path.join(home, ".dw_lokal")
    if os.path.exists(dw_lokal_path):
        # Load parameters from .dw_lokal into runtime context if possible
        pass

    # Step 6: Configure Database UTL_FILE directory path
    oracle_sid = os.environ.get("ORACLE_SID", "")
    os.environ["DW_DIR_UTL_FILE"] = f"/appl/local/oracle/admin/{oracle_sid}/utl_file"

    return 0

if __name__ == "__main__":
    sys.exit(main())