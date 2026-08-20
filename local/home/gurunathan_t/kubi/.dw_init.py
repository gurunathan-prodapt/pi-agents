#!/usr/bin/env python3
import os
import sys

# Global environment variables sourced at runtime
GCP_PROJECT = os.environ.get("GCP_PROJECT")
GCS_BUCKET = os.environ.get("GCS_BUCKET")

def main():
    # Step 1: Get base directory (prefer GCS_BUCKET for cloud storage, fallback to HOME)
    home_dir = GCS_BUCKET if GCS_BUCKET else os.environ.get("HOME", "")

    # Step 2: Set and export environmental directory configurations
    os.environ["DW_DIR_ROOT"] = os.path.join(home_dir, "aktuell")
    os.environ["DW_DIR_PROT"] = os.path.join(home_dir, "daten/logfiles")
    os.environ["DW_DIR_CUBES"] = os.path.join(home_dir, "daten/cubes")

    os.environ["DW_DIR_IMP_D1"] = os.path.join(home_dir, "daten/d1")
    os.environ["DW_DIR_IMP_BWA"] = os.path.join(home_dir, "daten/dpps/bwa")
    os.environ["DW_DIR_IMP_XTRA"] = os.path.join(home_dir, "daten/xtra")
    os.environ["DW_DIR_IMP_CTEL"] = os.path.join(home_dir, "daten/ctel")
    os.environ["DW_DIR_IMP_VO"] = os.path.join(home_dir, "daten/vo")
    os.environ["DW_DIR_IMP_RV"] = os.path.join(home_dir, "daten/rv")
    os.environ["DW_DIR_IMP_IF"] = os.path.join(home_dir, "daten/ees")
    os.environ["DW_DIR_IMP_NNV"] = os.path.join(home_dir, "daten/nnv")
    os.environ["DW_DIR_IMP_SIGMA"] = os.path.join(home_dir, "daten/gd/sigma")
    os.environ["DW_DIR_EXP_SIGMA"] = os.path.join(home_dir, "daten/gd/sigma/export")
    os.environ["DW_DIR_IMP_TRF"] = os.path.join(home_dir, "daten/trf")
    os.environ["DW_DIR_IMP_AUF"] = os.path.join(home_dir, "daten/sd/auf")
    os.environ["DW_DIR_IMP_GUT"] = os.path.join(home_dir, "daten/sd/gut")
    os.environ["DW_DIR_IMP_KDG"] = os.path.join(home_dir, "daten/sd/kdg")
    os.environ["DW_DIR_IMP_MP_KDG"] = os.path.join(home_dir, "daten/mp/kdg")
    os.environ["DW_DIR_IMP_MP_TS"] = os.path.join(home_dir, "daten/mp/ts")
    os.environ["DW_DIR_IMP_MP_ZM"] = os.path.join(home_dir, "daten/mp/zm")
    os.environ["DW_DIR_IMP_TS"] = os.path.join(home_dir, "daten/sd/ts")
    os.environ["DW_DIR_IMP_ZM"] = os.path.join(home_dir, "daten/sd/zm")
    os.environ["DW_DIR_EXP"] = os.path.join(home_dir, "daten/exporter")
    os.environ["DW_DIR_IMP_BPM"] = os.path.join(home_dir, "daten/bm")
    os.environ["DW_DIR_IMP_ZTS"] = os.path.join(home_dir, "daten/zts")
    os.environ["DW_DIR_IMP_VRS"] = os.path.join(home_dir, "daten/vrs")

    os.environ["DW_DIR_IMP_BRUNET"] = os.path.join(home_dir, "daten/brunet")
    os.environ["DW_DIR_IMP_DWH"] = os.path.join(home_dir, "daten/dwh")
    os.environ["DW_DIR_IMP_PLATO"] = os.path.join(home_dir, "daten/dwh/plato")

    os.environ["DW_DIR_IMP_CARMEN"] = os.path.join(home_dir, "daten/carmen")
    os.environ["DW_DIR_IMP_SAP"] = os.path.join(home_dir, "daten/sap")
    os.environ["DW_DIR_IMP_SR_RV"] = os.path.join(home_dir, "daten/sap/sr_rv_dpps")
    os.environ["DW_DIR_IMP_SAP_L"] = os.path.join(home_dir, "daten/sap/sap_l_gutgr")
    os.environ["DW_DIR_IMP_L_MAHNSTYP_IST"] = os.path.join(home_dir, "daten/sap/mahn")
    os.environ["DW_DIR_IMP_L_MAHNV_FI"] = os.path.join(home_dir, "daten/sap/mahn")
    os.environ["DW_DIR_IMP_L_MAHNV_IST"] = os.path.join(home_dir, "daten/sap/mahn")
    os.environ["DW_DIR_IMP_L_GUTGR"] = os.path.join(home_dir, "daten/sd/l_gutschr")
    os.environ["DW_DIR_IMP_L_LEIST"] = os.path.join(home_dir, "daten/sd/l_leist")
    os.environ["DW_DIR_IMP_L_PROD"] = os.path.join(home_dir, "daten/sd/l_prod")
    os.environ["DW_DIR_IMP_LKODE"] = os.path.join(home_dir, "daten/sd/lkode")

    os.environ["DW_DIR_IMP_SUBSE"] = os.path.join(home_dir, "daten/subse")

    os.environ["DW_DIR_SMS_PRG"] = os.path.join(home_dir, "aktuell/allgemein/is/util")
    os.environ["DW_DIR_SMS_ADR"] = os.path.join(home_dir, "daten/sms/adressen")
    os.environ["DW_DIR_SMS_TMP"] = os.path.join(home_dir, "daten/sms/tmp")

    os.environ["DW_DIR_IMP_DPPS"] = os.path.join(home_dir, "daten/dpps")
    os.environ["DW_DIR_IMP_PLANF2"] = os.path.join(home_dir, "daten/planf2")

    os.environ["DW_HOST_CUSTOMER"] = "dxcst3.bn.detemobil.de"

    # Step 3: Conditionally resolve ORACLE_HOME
    # Note: Target platform is confirmed as BigQuery. These Oracle directories and Oracle SID dependencies are likely obsolete in the target state.
    oracle_home = os.environ.get("ORACLE_HOME")
    if not oracle_home:
        if os.path.isdir("/appl/local/oracle/12.2.0.1.0"):
            os.environ["ORACLE_HOME"] = "/appl/local/oracle/12.2.0.1.0"
        elif os.path.isdir("/appl/local/oracle/11.2.0"):
            os.environ["ORACLE_HOME"] = "/appl/local/oracle/11.2.0"
        else:
            print("Fehler in .dw_init:", file=sys.stderr)
            print("   Konnte ORACLE_HOME nicht setzen !", file=sys.stderr)

    # Step 4: Resolve DW_DIR_UTL_FILE path
    oracle_sid = os.environ.get("ORACLE_SID", "")
    os.environ["DW_DIR_UTL_FILE"] = f"/appl/local/oracle/admin/{oracle_sid}/utl_file"

if __name__ == "__main__":
    sys.exit(main())