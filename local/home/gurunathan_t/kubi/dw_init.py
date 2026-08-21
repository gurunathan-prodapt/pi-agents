#!/usr/bin/env python3
import os
import sys
import posixpath

def initialize_environment():
    # Establish home directory base or GCS bucket base
    home = os.environ.get("HOME", "")
    gcs_bucket = os.environ.get("GCS_BUCKET")
    
    if gcs_bucket:
        base_dir = f"gs://{gcs_bucket}"
    else:
        base_dir = home
        if not home:
            print("Warning: HOME environment variable is not set.", file=sys.stderr)

    def join_path(base, *parts):
        if not base:
            return "/".join(parts)
        if base.startswith("gs://"):
            return "/".join([base.rstrip("/")] + [p.strip("/") for p in parts])
        return posixpath.join(base, *parts)

    # Define directory configurations
    os.environ["DW_DIR_ROOT"] = join_path(home, "aktuell")
    os.environ["DW_DIR_PROT"] = join_path(base_dir, "daten/logfiles")
    os.environ["DW_DIR_CUBES"] = join_path(base_dir, "daten/cubes")

    os.environ["DW_DIR_IMP_D1"] = join_path(base_dir, "daten/d1")
    os.environ["DW_DIR_IMP_BWA"] = join_path(base_dir, "daten/dpps/bwa")
    os.environ["DW_DIR_IMP_XTRA"] = join_path(base_dir, "daten/xtra")
    os.environ["DW_DIR_IMP_CTEL"] = join_path(base_dir, "daten/ctel")
    os.environ["DW_DIR_IMP_VO"] = join_path(base_dir, "daten/vo")
    os.environ["DW_DIR_IMP_RV"] = join_path(base_dir, "daten/rv")
    os.environ["DW_DIR_IMP_IF"] = join_path(base_dir, "daten/ees")
    os.environ["DW_DIR_IMP_NNV"] = join_path(base_dir, "daten/nnv")
    os.environ["DW_DIR_IMP_SIGMA"] = join_path(base_dir, "daten/gd/sigma")
    os.environ["DW_DIR_EXP_SIGMA"] = join_path(base_dir, "daten/gd/sigma/export")
    os.environ["DW_DIR_IMP_TRF"] = join_path(base_dir, "daten/trf")
    os.environ["DW_DIR_IMP_AUF"] = join_path(base_dir, "daten/sd/auf")
    os.environ["DW_DIR_IMP_GUT"] = join_path(base_dir, "daten/sd/gut")
    os.environ["DW_DIR_IMP_KDG"] = join_path(base_dir, "daten/sd/kdg")
    os.environ["DW_DIR_IMP_MP_KDG"] = join_path(base_dir, "daten/mp/kdg")
    os.environ["DW_DIR_IMP_MP_TS"] = join_path(base_dir, "daten/mp/ts")
    os.environ["DW_DIR_IMP_MP_ZM"] = join_path(base_dir, "daten/mp/zm")
    os.environ["DW_DIR_IMP_TS"] = join_path(base_dir, "daten/sd/ts")
    os.environ["DW_DIR_IMP_ZM"] = join_path(base_dir, "daten/sd/zm")
    os.environ["DW_DIR_EXP"] = join_path(base_dir, "daten/exporter")
    os.environ["DW_DIR_IMP_BPM"] = join_path(base_dir, "daten/bm")
    os.environ["DW_DIR_IMP_ZTS"] = join_path(base_dir, "daten/zts")
    os.environ["DW_DIR_IMP_VRS"] = join_path(base_dir, "daten/vrs")

    os.environ["DW_DIR_IMP_BRUNET"] = join_path(base_dir, "daten/brunet")
    os.environ["DW_DIR_IMP_DWH"] = join_path(base_dir, "daten/dwh")
    os.environ["DW_DIR_IMP_PLATO"] = join_path(base_dir, "daten/dwh/plato")
    os.environ["DW_DIR_IMP_CARMEN"] = join_path(base_dir, "daten/carmen")
    os.environ["DW_DIR_IMP_SAP"] = join_path(base_dir, "daten/sap")
    os.environ["DW_DIR_IMP_SR_RV"] = join_path(base_dir, "daten/sap/sr_rv_dpps")
    os.environ["DW_DIR_IMP_SAP_L"] = join_path(base_dir, "daten/sap/sap_l_gutgr")
    os.environ["DW_DIR_IMP_L_MAHNSTYP_IST"] = join_path(base_dir, "daten/sap/mahn")
    os.environ["DW_DIR_IMP_L_MAHNV_FI"] = join_path(base_dir, "daten/sap/mahn")
    os.environ["DW_DIR_IMP_L_MAHNV_IST"] = join_path(base_dir, "daten/sap/mahn")
    os.environ["DW_DIR_IMP_L_GUTGR"] = join_path(base_dir, "daten/sd/l_gutschr")
    os.environ["DW_DIR_IMP_L_LEIST"] = join_path(base_dir, "daten/sd/l_leist")
    os.environ["DW_DIR_IMP_L_PROD"] = join_path(base_dir, "daten/sd/l_prod")
    os.environ["DW_DIR_IMP_LKODE"] = join_path(base_dir, "daten/sd/lkode")

    os.environ["DW_DIR_IMP_SUBSE"] = join_path(base_dir, "daten/subse")

    os.environ["DW_DIR_SMS_PRG"] = join_path(home, "aktuell/allgemein/is/util")
    os.environ["DW_DIR_SMS_ADR"] = join_path(base_dir, "daten/sms/adressen")
    os.environ["DW_DIR_SMS_TMP"] = join_path(base_dir, "daten/sms/tmp")

    os.environ["DW_DIR_IMP_DPPS"] = join_path(base_dir, "daten/dpps")
    os.environ["DW_DIR_IMP_PLANF2"] = join_path(base_dir, "daten/planf2")

    os.environ["DW_HOST_CUSTOMER"] = os.environ.get("DW_HOST_CUSTOMER", "dxcst3.bn.detemobil.de")

    # Resolve ORACLE_HOME if empty
    if not os.environ.get("ORACLE_HOME"):
        if os.path.isdir("/appl/local/oracle/12.2.0.1.0"):
            os.environ["ORACLE_HOME"] = "/appl/local/oracle/12.2.0.1.0"
        elif os.path.isdir("/appl/local/oracle/11.2.0"):
            os.environ["ORACLE_HOME"] = "/appl/local/oracle/11.2.0"
        else:
            print("Fehler in .dw_init:")
            print("   Konnte ORACLE_HOME nicht setzen !")

    # Define and export DW_DIR_UTL_FILE
    oracle_sid = os.environ.get("ORACLE_SID", "")
    if gcs_bucket:
        os.environ["DW_DIR_UTL_FILE"] = f"gs://{gcs_bucket}/oracle/admin/{oracle_sid}/utl_file" if oracle_sid else f"gs://{gcs_bucket}/oracle/admin/utl_file"
    else:
        os.environ["DW_DIR_UTL_FILE"] = f"/appl/local/oracle/admin/{oracle_sid}/utl_file" if oracle_sid else "/appl/local/oracle/admin/utl_file"

def main():
    initialize_environment()
    # Output the initialized variables to confirm they are set in the current process
    for key, value in sorted(os.environ.items()):
        if key.startswith("DW_") or key == "ORACLE_HOME":
            print(f"{key}={value}")
    return 0

if __name__ == "__main__":
    sys.exit(main())