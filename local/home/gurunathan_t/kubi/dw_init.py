#!/usr/bin/env python3
import os
import sys

def init_environment():
    # Resolve HOME directory base
    home = os.environ.get("HOME", "")
    if not home:
        # Fallback to user home if HOME env var is completely missing
        home = os.path.expanduser("~")

    # Global cloud context: Support GCS mapping if GCS_BUCKET environment variable is present
    gcs_bucket = os.environ.get("GCS_BUCKET")

    def get_path(sub_path):
        if gcs_bucket:
            # If using Cloud Storage, map sub_path (e.g. "daten/logfiles") to GCS URI
            return f"gs://{gcs_bucket}/{sub_path}"
        else:
            return os.path.join(home, sub_path)

    # Define and export directory structures
    os.environ["DW_DIR_ROOT"] = get_path("aktuell")
    os.environ["DW_DIR_PROT"] = get_path("daten/logfiles")
    os.environ["DW_DIR_CUBES"] = get_path("daten/cubes")

    os.environ["DW_DIR_IMP_D1"] = get_path("daten/d1")
    os.environ["DW_DIR_IMP_BWA"] = get_path("daten/dpps/bwa")
    os.environ["DW_DIR_IMP_XTRA"] = get_path("daten/xtra")
    os.environ["DW_DIR_IMP_CTEL"] = get_path("daten/ctel")
    os.environ["DW_DIR_IMP_VO"] = get_path("daten/vo")
    os.environ["DW_DIR_IMP_RV"] = get_path("daten/rv")
    os.environ["DW_DIR_IMP_IF"] = get_path("daten/ees")
    os.environ["DW_DIR_IMP_NNV"] = get_path("daten/nnv")
    os.environ["DW_DIR_IMP_SIGMA"] = get_path("daten/gd/sigma")
    os.environ["DW_DIR_EXP_SIGMA"] = get_path("daten/gd/sigma/export")
    os.environ["DW_DIR_IMP_TRF"] = get_path("daten/trf")
    os.environ["DW_DIR_IMP_AUF"] = get_path("daten/sd/auf")
    os.environ["DW_DIR_IMP_GUT"] = get_path("daten/sd/gut")
    os.environ["DW_DIR_IMP_KDG"] = get_path("daten/sd/kdg")
    os.environ["DW_DIR_IMP_MP_KDG"] = get_path("daten/mp/kdg")
    os.environ["DW_DIR_IMP_MP_TS"] = get_path("daten/mp/ts")
    os.environ["DW_DIR_IMP_MP_ZM"] = get_path("daten/mp/zm")
    os.environ["DW_DIR_IMP_TS"] = get_path("daten/sd/ts")
    os.environ["DW_DIR_IMP_ZM"] = get_path("daten/sd/zm")
    os.environ["DW_DIR_EXP"] = get_path("daten/exporter")
    os.environ["DW_DIR_IMP_BPM"] = get_path("daten/bm")
    os.environ["DW_DIR_IMP_ZTS"] = get_path("daten/zts")
    os.environ["DW_DIR_IMP_VRS"] = get_path("daten/vrs")

    os.environ["DW_DIR_IMP_BRUNET"] = get_path("daten/brunet")
    os.environ["DW_DIR_IMP_DWH"] = get_path("daten/dwh")
    os.environ["DW_DIR_IMP_PLATO"] = get_path("daten/dwh/plato")
    os.environ["DW_DIR_IMP_CARMEN"] = get_path("daten/carmen")
    os.environ["DW_DIR_IMP_SAP"] = get_path("daten/sap")
    os.environ["DW_DIR_IMP_SR_RV"] = get_path("daten/sap/sr_rv_dpps")
    os.environ["DW_DIR_IMP_SAP_L_GUTGR"] = get_path("daten/sap/sap_l_gutgr")
    os.environ["DW_DIR_IMP_L_MAHNSTYP_IST"] = get_path("daten/sap/mahn")
    os.environ["DW_DIR_IMP_L_MAHNV_FI"] = get_path("daten/sap/mahn")
    os.environ["DW_DIR_IMP_L_MAHNV_IST"] = get_path("daten/sap/mahn")
    os.environ["DW_DIR_IMP_L_GUTGR"] = get_path("daten/sd/l_gutschr")
    os.environ["DW_DIR_IMP_L_LEIST"] = get_path("daten/sd/l_leist")
    os.environ["DW_DIR_IMP_L_PROD"] = get_path("daten/sd/l_prod")
    os.environ["DW_DIR_IMP_LKODE"] = get_path("daten/sd/lkode")

    os.environ["DW_DIR_IMP_SUBSE"] = get_path("daten/subse")

    os.environ["DW_DIR_SMS_PRG"] = get_path("aktuell/allgemein/is/util")
    os.environ["DW_DIR_SMS_ADR"] = get_path("daten/sms/adressen")
    os.environ["DW_DIR_SMS_TMP"] = get_path("daten/sms/tmp")

    os.environ["DW_DIR_IMP_DPPS"] = get_path("daten/dpps")
    os.environ["DW_DIR_IMP_PLANF2"] = get_path("daten/planf2")

    # Define Remote Host (sourced from environment if defined, otherwise defaulting to legacy value)
    os.environ["DW_HOST_CUSTOMER"] = os.environ.get("DW_HOST_CUSTOMER", "dxcst3.bn.detemobil.de")

    # Resolve ORACLE_HOME dynamically if not already populated
    if not os.environ.get("ORACLE_HOME"):
        if os.path.isdir("/appl/local/oracle/12.2.0.1.0"):
            os.environ["ORACLE_HOME"] = "/appl/local/oracle/12.2.0.1.0"
        elif os.path.isdir("/appl/local/oracle/11.2.0"):
            os.environ["ORACLE_HOME"] = "/appl/local/oracle/11.2.0"
        else:
            print("Fehler in .dw_init:", file=sys.stderr)
            print("   Konnte ORACLE_HOME nicht setzen !", file=sys.stderr)

    # Note: .dw_global and .dw_lokal resolved as "NO SOURCE NEEDED" and thus skipped.

    # Construct dynamic Oracle Admin UTL path
    oracle_sid = os.environ.get("ORACLE_SID", "")
    os.environ["DW_DIR_UTL_FILE"] = f"/appl/local/oracle/admin/{oracle_sid}/utl_file"

def main():
    init_environment()
    print("[INFO] Environment variables initialized successfully.")
    return 0

if __name__ == "__main__":
    sys.exit(main())