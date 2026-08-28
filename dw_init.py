#!/usr/bin/env python3
import os
import sys

def get_path(base, *parts):
    if base.startswith("gs://"):
        return "/".join([base.rstrip("/")] + list(parts))
    return os.path.join(base, *parts)

def main():
    # Step 1: Resolve base directory (GCS_BUCKET or HOME)
    gcs_bucket = os.environ.get("GCS_BUCKET")
    home_dir = os.environ.get("HOME", "")
    
    if gcs_bucket:
        base_dir = f"gs://{gcs_bucket}"
    else:
        base_dir = home_dir

    # Step 2: Set and export path variables
    os.environ["DW_DIR_ROOT"] = get_path(base_dir, "aktuell")
    os.environ["DW_DIR_PROT"] = get_path(base_dir, "daten/logfiles")
    os.environ["DW_DIR_CUBES"] = get_path(base_dir, "daten/cubes")
    os.environ["DW_DIR_IMP_D1"] = get_path(base_dir, "daten/d1")
    os.environ["DW_DIR_IMP_BWA"] = get_path(base_dir, "daten/dpps/bwa")
    os.environ["DW_DIR_IMP_XTRA"] = get_path(base_dir, "daten/xtra")
    os.environ["DW_DIR_IMP_CTEL"] = get_path(base_dir, "daten/ctel")
    os.environ["DW_DIR_IMP_VO"] = get_path(base_dir, "daten/vo")
    os.environ["DW_DIR_IMP_RV"] = get_path(base_dir, "daten/rv")
    os.environ["DW_DIR_IMP_IF"] = get_path(base_dir, "daten/ees")
    os.environ["DW_DIR_IMP_NNV"] = get_path(base_dir, "daten/nnv")
    os.environ["DW_DIR_IMP_SIGMA"] = get_path(base_dir, "daten/gd/sigma")
    os.environ["DW_DIR_EXP_SIGMA"] = get_path(base_dir, "daten/gd/sigma/export")
    os.environ["DW_DIR_IMP_TRF"] = get_path(base_dir, "daten/trf")
    os.environ["DW_DIR_IMP_AUF"] = get_path(base_dir, "daten/sd/auf")
    os.environ["DW_DIR_IMP_GUT"] = get_path(base_dir, "daten/sd/gut")
    os.environ["DW_DIR_IMP_KDG"] = get_path(base_dir, "daten/sd/kdg")
    os.environ["DW_DIR_IMP_MP_KDG"] = get_path(base_dir, "daten/mp/kdg")
    os.environ["DW_DIR_IMP_MP_TS"] = get_path(base_dir, "daten/mp/ts")
    os.environ["DW_DIR_IMP_MP_ZM"] = get_path(base_dir, "daten/mp/zm")
    os.environ["DW_DIR_IMP_TS"] = get_path(base_dir, "daten/sd/ts")
    os.environ["DW_DIR_IMP_ZM"] = get_path(base_dir, "daten/sd/zm")
    os.environ["DW_DIR_EXP"] = get_path(base_dir, "daten/exporter")
    os.environ["DW_DIR_IMP_BPM"] = get_path(base_dir, "daten/bm")
    os.environ["DW_DIR_IMP_ZTS"] = get_path(base_dir, "daten/zts")
    os.environ["DW_DIR_IMP_VRS"] = get_path(base_dir, "daten/vrs")
    os.environ["DW_DIR_IMP_BRUNET"] = get_path(base_dir, "daten/brunet")
    os.environ["DW_DIR_IMP_DWH"] = get_path(base_dir, "daten/dwh")
    os.environ["DW_DIR_IMP_PLATO"] = get_path(base_dir, "daten/dwh/plato")
    os.environ["DW_DIR_IMP_CARMEN"] = get_path(base_dir, "daten/carmen")
    os.environ["DW_DIR_IMP_SAP"] = get_path(base_dir, "daten/sap")
    os.environ["DW_DIR_IMP_SR_RV"] = get_path(base_dir, "daten/sap/sr_rv_dpps")

    # NOTE: Legacy export mismatch: DW_DIR_IMP_SAP_L_GUTGR is declared, but DW_DIR_IMP_SAP_L is exported.
    dw_dir_imp_sap_l_gutgr = get_path(base_dir, "daten/sap/sap_l_gutgr")
    os.environ["DW_DIR_IMP_SAP_L"] = dw_dir_imp_sap_l_gutgr

    os.environ["DW_DIR_IMP_L_MAHNSTYP_IST"] = get_path(base_dir, "daten/sap/mahn")
    os.environ["DW_DIR_IMP_L_MAHNV_FI"] = get_path(base_dir, "daten/sap/mahn")
    os.environ["DW_DIR_IMP_L_MAHNV_IST"] = get_path(base_dir, "daten/sap/mahn")
    os.environ["DW_DIR_IMP_L_GUTGR"] = get_path(base_dir, "daten/sd/l_gutschr")
    os.environ["DW_DIR_IMP_L_LEIST"] = get_path(base_dir, "daten/sd/l_leist")
    os.environ["DW_DIR_IMP_L_PROD"] = get_path(base_dir, "daten/sd/l_prod")
    os.environ["DW_DIR_IMP_LKODE"] = get_path(base_dir, "daten/sd/lkode")
    os.environ["DW_DIR_IMP_SUBSE"] = get_path(base_dir, "daten/subse")
    os.environ["DW_DIR_SMS_PRG"] = get_path(base_dir, "aktuell/allgemein/is/util")
    os.environ["DW_DIR_SMS_ADR"] = get_path(base_dir, "daten/sms/adressen")
    os.environ["DW_DIR_SMS_TMP"] = get_path(base_dir, "daten/sms/tmp")
    os.environ["DW_DIR_IMP_DPPS"] = get_path(base_dir, "daten/dpps")
    os.environ["DW_DIR_IMP_PLANF2"] = get_path(base_dir, "daten/planf2")

    # Step 3: Set and export remote host
    os.environ["DW_HOST_CUSTOMER"] = os.environ.get("DW_HOST_CUSTOMER", "dxcst3.bn.detemobil.de")

    # Step 4: Resolve ORACLE_HOME dynamically if unset
    # REVIEW: Since the target platform is confirmed as BIGQUERY, Oracle environment components (ORACLE_HOME, ORACLE_SID, and DW_DIR_UTL_FILE) may be obsolete or replaced by BigQuery/GCP resources (such as Google Cloud Storage buckets or Dataset locations).
    if not os.environ.get("ORACLE_HOME"):
        if os.path.isdir("/appl/local/oracle/12.2.0.1.0"):
            os.environ["ORACLE_HOME"] = "/appl/local/oracle/12.2.0.1.0"
        elif os.path.isdir("/appl/local/oracle/11.2.0"):
            os.environ["ORACLE_HOME"] = "/appl/local/oracle/11.2.0"
        else:
            print("Fehler in .dw_init:", file=sys.stderr)
            print("   Konnte ORACLE_HOME nicht setzen !", file=sys.stderr)

    # Step 5: Load global and local settings if existing
    # REVIEW-STRUCT: environment file [.dw_global] not supplied — variables it sets are unknown; do not guess their names or values
    # REVIEW-STRUCT: environment file [.dw_lokal] not supplied — variables it sets are unknown; do not guess their names or values
    dw_global_path = os.path.join(home_dir, ".dw_global")
    dw_lokal_path = os.path.join(home_dir, ".dw_lokal")

    # Step 6: Define and export UTL file directory
    oracle_sid = os.environ.get("ORACLE_SID", "")
    os.environ["DW_DIR_UTL_FILE"] = f"/appl/local/oracle/admin/{oracle_sid}/utl_file"

    return 0

if __name__ == "__main__":
    sys.exit(main())